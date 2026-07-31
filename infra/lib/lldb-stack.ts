import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';

/** The Flight port every worker listens on (matches `LLDB_WORKER_BIND` in the image). */
export const WORKER_PORT = 50051;

/** Postgres port for the services database. */
export const SERVICES_DB_PORT = 5432;
/** Database and login the engine expects on the services cluster. */
export const SERVICES_DB_NAME = 'lldb';
export const SERVICES_DB_USER = 'lldb';

/** Private DNS namespace the fleet registers into, e.g. `worker.lldb.local`. */
export const NAMESPACE = 'lldb.local';
/**
 * Name of the warehouse the stack deploys when none are declared. It is `worker` for a reason:
 * every pre-warehouse deploy pointed its coordinator at `worker.lldb.local`, and a default that
 * kept the name means adopting warehouses is not a breaking change to anything already running.
 */
export const WORKER_SERVICE_NAME = 'worker';

/**
 * Template the coordinator renders a warehouse name into (`LLDB_WAREHOUSE_ENDPOINT`). One Cloud
 * Map name per warehouse is the whole routing mechanism: each warehouse's ECS service registers
 * its tasks under its *own* name, so `<warehouse>.lldb.local` resolves to exactly that
 * warehouse's fleet and nothing else.
 *
 * The scheme is not decoration — it is the switch. `https://` dials TLS and `http://` does not,
 * with no fallback in either direction, so a fleet serving certificates needs every worker URL
 * rewritten or it fails loudly at the first query. See {@link TlsMode}.
 */
export const warehouseEndpointTemplate = (scheme: 'http' | 'https'): string =>
  `${scheme}://{warehouse}.${NAMESPACE}:${WORKER_PORT}`;

/** The plaintext template — what the stack renders in the default `tls: 'none'` mode. */
export const WAREHOUSE_ENDPOINT_TEMPLATE = warehouseEndpointTemplate('http');

/**
 * The one name every worker's certificate is issued for, and the name every client verifies
 * against (`LLDB_TLS_DOMAIN`).
 *
 * It is a fixed fleet-wide name rather than a per-warehouse one, and that is forced rather than
 * chosen — twice over. `lldb_qe_core::discovery` expands a Cloud Map name into one URL **per task
 * IP**, so what a client would otherwise verify is `10.0.1.47`, an address allocated at task start
 * that changes on every replacement and every scale event; no certificate minted in advance can
 * carry those as IP SANs. And the dialing trust is process-global (a `FlightReaderExec` is
 * serialized into a plan and can carry nothing per-call), while one coordinator dials several
 * warehouses — so there is exactly one name available to verify against, for all of them.
 *
 * The certificate therefore carries `DNS:fleet.lldb.local`: the URL's IP connects, this name
 * verifies, and it is a SAN the leaf genuinely has rather than a mismatch papered over.
 */
export const FLEET_TLS_DOMAIN = `fleet.${NAMESPACE}`;

/** Prefix of the three Secrets Manager secrets holding the fleet's PEM material. */
export const DEFAULT_TLS_SECRET_PREFIX = 'lldb/fleet-tls';

/**
 * The variable carrying the shared fleet secret into every task
 * (`lldb_qe_control::auth::FleetAuth`, and the HMAC key `lldb_qe_core::plan_assertion` derives from
 * it). Exported so a test can assert on the name rather than re-spelling it.
 */
export const FLEET_TOKEN_ENV = 'LLDB_FLEET_TOKEN';

/**
 * Length of the generated fleet secret, in characters.
 *
 * 64 alphanumerics is ~380 bits — far past anything guessable, and past the 256-bit input the
 * assertion's HMAC key derivation wants. Long rather than exotic: the value has to survive being an
 * environment variable, so entropy is bought with length instead of punctuation.
 */
const FLEET_TOKEN_LENGTH = 64;

/**
 * A virtual warehouse: a named, independently sized pool of workers.
 *
 * This is the infrastructure half of the concept whose control-plane half lives in the services
 * database's `warehouses` table. The database holds **desired state** — the engine deliberately
 * carries no AWS SDK and calls no ECS API (see `crates/lldb-qe-core/src/warehouse.rs`) — and this
 * stack is one of the two things that *applies* it. The other is an operator running
 * `aws ecs update-service --desired-count`, which is the no-deploy path for a resize or a
 * suspend; a redeploy with an edited `warehouses` list is the durable one.
 *
 * Keep the two in step: a warehouse row the stack does not know about has no compute, and a
 * service the database does not know about cannot be routed to. The coordinator logs a warning
 * when the size it read disagrees with the fleet that answered, which is what that drift looks
 * like from the inside.
 */
export interface WarehouseDefinition {
  /** Name — also the Cloud Map DNS label, so: lowercase `[a-z0-9-]`, not starting/ending with `-`. */
  readonly name: string;
  /** Desired worker count. Retained across a suspend; this is what `running` scales to. */
  readonly size: number;
  /** `suspended` deploys the service at `desiredCount: 0` — defined and sized, but costing nothing. */
  readonly state?: 'running' | 'suspended';
}

/**
 * How worker/coordinator tasks reach the services they need that are *not* the warehouse
 * (pulling from ECR, writing CloudWatch logs). Warehouse traffic never enters into it — that
 * rides a free S3 gateway endpoint in every mode.
 *
 * - `none` (default): tasks sit in public subnets with public IPs and egress straight through
 *   the internet gateway. **$0.** Inbound is still default-deny and the Flight port is only
 *   open to the coordinator's security group, so this is safe — it just isn't defense in depth.
 * - `nat-instance`: tasks move to private subnets behind a single small NAT instance
 *   (fck-nat on `t4g.nano` ≈ **$3/mo**). No public IPs on the data plane.
 * - `nat-gateway`: same private layout behind a managed NAT gateway (≈ **$33/mo** + $0.045/GB).
 *   Buys AWS-managed HA and no instance to patch.
 *
 * Worth knowing: the "no NAT at all, pure PrivateLink" alternative is the *most* expensive of
 * these — private subnets would need `ecr.api`, `ecr.dkr` and `logs` interface endpoints at
 * ~$7.30/mo each per AZ.
 */
export type EgressMode = 'none' | 'nat-instance' | 'nat-gateway';

/**
 * Whether the stack provisions the shared **services database** — the control plane holding
 * accounts, the SQL catalog, warehouses and query history.
 *
 * - `aurora` (default): an Aurora Serverless v2 PostgreSQL cluster in isolated subnets, with a
 *   generated password in Secrets Manager and ingress on 5432 restricted to the worker and
 *   coordinator security groups. Serverless v2 rather than a provisioned instance because a
 *   control plane's load is bursty and mostly idle — it scales to a fraction of an ACU between
 *   queries instead of billing for an always-on `db.r6g`.
 * - `none`: no database at all, and no `LLDB_METADATA_*` on the tasks. The engine treats an
 *   unconfigured services DB as a legitimate state, so this deploys a query-only fleet — useful
 *   for a throwaway benchmark stack that should not carry a cluster's cost or blast radius.
 */
export type ServicesDbMode = 'aurora' | 'none';

/**
 * Whether Flight traffic inside the VPC is encrypted.
 *
 * - `none` (default): every Flight port is plaintext and **no fleet secret is created**, as it has
 *   been since this stack existed. Legal, and not a hole: the engine refuses a plaintext port only
 *   when a credential is actually checked on it, and in this mode nothing checks one. Traffic is
 *   confined to the VPC by the security groups, and `LLDB_ALLOW_PLAINTEXT` is *not* set in this mode
 *   either — see below.
 * - `fleet`: workers serve TLS and every role dials `https://`, from PEM delivered as
 *   `LLDB_TLS_*_PEM` out of three Secrets Manager secrets — **and** every role gets the generated
 *   {@link FLEET_TOKEN_ENV}, which is what turns a worker's Flight port from routable-is-enough into
 *   a door. The two are one switch on purpose: a worker checking a credential refuses to bind a
 *   plaintext port, so "fleet secret without certificates" is not a configuration this stack can
 *   express rather than one it merely warns about. See the fleet-secret block in the constructor.
 *
 * **Why the material is imported rather than created here.** Fargate injects a Secrets Manager
 * value as an environment variable and by no other means, so the delivery mechanism was never in
 * question — the *issuance* was. A private key must never reach `cdk.out`, and the strongest form
 * of that guarantee is CDK never holding the key at all: `scripts/mint-fleet-tls.sh` mints a CA
 * and one leaf locally and writes them straight to Secrets Manager, and this stack imports them by
 * name. Creating them here instead would also deadlock: CloudFormation would make three empty
 * secrets, the ECS service would fail to stabilise on the empty PEM, the deployment circuit
 * breaker would fail the stack, and the rollback would delete the secrets you were about to fill.
 *
 * Three whole-string secrets rather than one with JSON keys, because that is what buys the IAM
 * boundary worth having: **the coordinator's execution role is granted the CA alone — never the
 * certificate and never the private key.** It binds no port, so an identity is material it has no
 * use for, and the least it can be given is the trust it dials with. (`ecs.Secret` grants read on
 * a whole secret; a JSON key selects a field, it does not scope access — which is why one secret
 * with three keys would make this a convention rather than a boundary.) It costs ~$1.20/month, and
 * only in this mode.
 */
export type TlsMode = 'none' | 'fleet';

/** fck-nat (https://fck-nat.dev) — a free, purpose-built NAT AMI. Published under this owner. */
const FCK_NAT_OWNER = '568608671756';
const FCK_NAT_AMI_NAME = 'fck-nat-amzn2-*-arm64-ebs';

export interface LldbStackProps extends cdk.StackProps {
  /**
   * Image tag both roles run. This is the whole point of the single-tag design: serialized
   * DataFusion physical plans are NOT cross-version compatible, so the coordinator and every
   * worker must be the *identical* build. CI stamps images `version+git-sha`; deploy that tag.
   */
  readonly imageTag: string;
  /**
   * Size of the single default warehouse. Shorthand for `warehouses: [{ name: 'worker', size: n }]`
   * — the two are mutually exclusive, because a stack that accepted both would have to pick one
   * silently.
   */
  readonly workerCount?: number;
  /**
   * The virtual warehouses to deploy: one ECS service each, `desiredCount` from `size`, and its
   * own Cloud Map name. Defaults to a single `worker` warehouse of `workerCount` tasks, which is
   * byte-for-byte the fleet this stack deployed before warehouses existed.
   */
  readonly warehouses?: WarehouseDefinition[];
  /** Fargate task sizing (analytical work is memory-hungry; defaults suit the POC). */
  readonly cpu?: number;
  readonly memoryLimitMiB?: number;
  /** How tasks reach ECR/CloudWatch. See {@link EgressMode}. Defaults to `none` ($0). */
  readonly egress?: EgressMode;
  /**
   * AMI for the `nat-instance` mode. Defaults to looking up the latest **fck-nat** arm64 image,
   * which requires a concrete account/region (a lookup hits the real EC2 API at synth time).
   * Pass an explicit image to keep synth hermetic — that is what the tests do.
   */
  readonly natMachineImage?: ec2.IMachineImage;
  /** Instance type for `nat-instance`. Defaults to `t4g.nano` (arm64, matches the fck-nat AMI). */
  readonly natInstanceType?: ec2.InstanceType;
  /** Whether to provision the services database. See {@link ServicesDbMode}. Defaults to `aurora`. */
  readonly servicesDb?: ServicesDbMode;
  /** Whether Flight traffic is encrypted. See {@link TlsMode}. Defaults to `none`. */
  readonly tls?: TlsMode;
  /**
   * Prefix of the three secrets `tls: 'fleet'` reads — `<prefix>-ca`, `<prefix>-cert`,
   * `<prefix>-key`. Defaults to {@link DEFAULT_TLS_SECRET_PREFIX}. Give two stacks in one account
   * different prefixes unless they are meant to be one trust domain.
   */
  readonly tlsSecretPrefix?: string;
}

/**
 * The lldb query engine on ECS Fargate: an ECR repo for the one image, an S3 Iceberg warehouse,
 * a service-discovered fleet of stateless workers, and a one-shot coordinator task definition.
 *
 * Cost note: the VPC is deliberately NAT-less. Tasks run in public subnets with public IPs so
 * they can pull from ECR, and warehouse traffic leaves through a *free* S3 gateway endpoint
 * rather than a per-hour NAT gateway. Fine for a POC; a production VPC would use private
 * subnets with NAT (or interface endpoints) instead.
 */
export class LldbStack extends cdk.Stack {
  public readonly repository: ecr.Repository;
  public readonly warehouse: s3.Bucket;
  public readonly cluster: ecs.Cluster;
  /** The default (first) warehouse's service — kept for callers that predate warehouses. */
  public readonly workerService: ecs.FargateService;
  /** Every warehouse's service, by warehouse name. */
  public readonly warehouseServices: Record<string, ecs.FargateService> = {};
  public readonly coordinatorTask: ecs.FargateTaskDefinition;
  /** The services database, when `servicesDb: 'aurora'`. */
  public readonly servicesDb?: rds.DatabaseCluster;
  /** The generated shared fleet secret, when `tls: 'fleet'`. Never created in any other mode. */
  public readonly fleetToken?: secretsmanager.Secret;

  constructor(scope: Construct, id: string, props: LldbStackProps) {
    super(scope, id, props);

    // Refuse an unpinned fleet at synth time. `latest` moves, and a coordinator on a different
    // build than its workers fails deep in plan deserialization rather than at deploy.
    if (!props.imageTag || props.imageTag === 'latest') {
      throw new Error(
        `imageTag must be an exact build tag (got ${JSON.stringify(props.imageTag)}). ` +
          'Deploy the CI-stamped `version+git-sha`, e.g. 0.1.0+8c6d8d6b57d8.',
      );
    }

    // `-c workerCount=…` arrives as an unvalidated string through `Number()`, so a typo becomes
    // NaN and a fractional/zero value would synthesize a nonsense DesiredCount. Fail at synth.
    const workerCount = props.workerCount ?? 2;
    if (!Number.isInteger(workerCount) || workerCount < 1) {
      throw new Error(`workerCount must be a positive integer, got ${JSON.stringify(props.workerCount)}`);
    }
    if (props.warehouses && props.workerCount !== undefined) {
      throw new Error(
        'workerCount and warehouses are mutually exclusive — workerCount sizes the single default ' +
          'warehouse, so pass sizes inside `warehouses` instead.',
      );
    }
    const warehouses = validateWarehouses(props.warehouses ?? [{ name: WORKER_SERVICE_NAME, size: workerCount }]);
    const cpu = props.cpu ?? 1024;
    const memoryLimitMiB = props.memoryLimitMiB ?? 4096;

    // ---- Image registry -----------------------------------------------------------------
    this.repository = new ecr.Repository(this, 'Repository', {
      repositoryName: 'lldb',
      imageScanOnPush: true,
      // Keep tagged builds; reap untagged layers so the repo doesn't grow without bound.
      lifecycleRules: [{ description: 'Expire untagged images', tagStatus: ecr.TagStatus.UNTAGGED, maxImageAge: cdk.Duration.days(14) }],
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      emptyOnDelete: true,
    });

    // ---- Warehouse ----------------------------------------------------------------------
    this.warehouse = new s3.Bucket(this, 'Warehouse', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      versioned: false,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // ---- Network ------------------------------------------------------------------------
    const egress: EgressMode = props.egress ?? 'none';
    const isPrivate = egress !== 'none';
    const servicesDbMode: ServicesDbMode = props.servicesDb ?? 'aurora';

    // The database gets *isolated* subnets — no route to the internet in either direction — in
    // every egress mode. Isolated subnets cost nothing (no NAT, no endpoint), and a control
    // plane that cannot be reached from outside the VPC is the cheapest security win available.
    const dbSubnets: ec2.SubnetConfiguration[] =
      servicesDbMode === 'aurora'
        ? [{ name: 'db', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 }]
        : [];

    // One NAT (instance or gateway) serves both AZs. Paying per-AZ for HA is not a POC concern;
    // raise `natGateways` to `maxAzs` when a NAT outage taking the fleet offline actually costs
    // more than the second NAT does.
    const natInstanceProvider =
      egress === 'nat-instance'
        ? ec2.NatProvider.instanceV2({
            instanceType: props.natInstanceType ?? ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.NANO),
            machineImage:
              props.natMachineImage ??
              ec2.MachineImage.lookup({ name: FCK_NAT_AMI_NAME, owners: [FCK_NAT_OWNER] }),
            // CDK's default here is INBOUND_AND_OUTBOUND, which opens the NAT instance to the
            // whole internet. It only ever needs to accept traffic from inside the VPC (that
            // rule is added below, once the CIDR exists).
            defaultAllowedTraffic: ec2.NatTrafficDirection.OUTBOUND_ONLY,
          })
        : undefined;
    const natGatewayProvider = natInstanceProvider;

    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: isPrivate ? 1 : 0,
      ...(natGatewayProvider ? { natGatewayProvider } : {}),
      subnetConfiguration: isPrivate
        ? [
            { name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
            { name: 'private', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS, cidrMask: 24 },
            ...dbSubnets,
          ]
        : [{ name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 }, ...dbSubnets],
    });
    // The NAT forwards traffic originating in the VPC, so it must accept exactly that and
    // nothing else. Without this the instance would be internet-reachable.
    natInstanceProvider?.connections.allowFrom(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.allTraffic(),
      'NAT forwards traffic from inside the VPC only',
    );

    // Free egress to S3 in every mode — warehouse traffic never touches the NAT (or the public
    // internet), which is exactly the traffic you would not want billed per-GB.
    vpc.addGatewayEndpoint('S3Endpoint', { service: ec2.GatewayVpcEndpointAwsService.S3 });

    // Where the tasks run, and whether they need a public IP to reach ECR.
    const taskSubnets: ec2.SubnetSelection = {
      subnetType: isPrivate ? ec2.SubnetType.PRIVATE_WITH_EGRESS : ec2.SubnetType.PUBLIC,
    };

    this.cluster = new ecs.Cluster(this, 'Cluster', {
      vpc,
      containerInsightsV2: ecs.ContainerInsights.ENABLED,
      defaultCloudMapNamespace: {
        name: NAMESPACE,
        type: servicediscovery.NamespaceType.DNS_PRIVATE,
        useForServiceConnect: false,
      },
    });

    // Workers accept Flight traffic ONLY from the coordinator, not from the whole VPC.
    const coordinatorSg = new ec2.SecurityGroup(this, 'CoordinatorSg', {
      vpc,
      description: 'lldb coordinator - ships plans to workers',
      allowAllOutbound: true,
    });
    const workerSg = new ec2.SecurityGroup(this, 'WorkerSg', {
      vpc,
      description: 'lldb worker fleet - serves Arrow Flight',
      allowAllOutbound: true,
    });
    workerSg.addIngressRule(coordinatorSg, ec2.Port.tcp(WORKER_PORT), 'Arrow Flight from coordinator');

    // ---- Services database (control plane) -----------------------------------------------
    // Env injected into both roles. Empty in `servicesDb: 'none'` mode, which the engine reads
    // as "no control plane" and tolerates rather than failing.
    const metadataEnv: Record<string, string> = {};
    const metadataSecrets: Record<string, ecs.Secret> = {};

    if (servicesDbMode === 'aurora') {
      // Same posture as the Flight port: reachable from the app's security groups and nothing
      // else. `allowAllOutbound: false` because a database has no business initiating traffic.
      const dbSg = new ec2.SecurityGroup(this, 'ServicesDbSg', {
        vpc,
        description: 'lldb services database - control plane',
        allowAllOutbound: false,
      });
      dbSg.addIngressRule(workerSg, ec2.Port.tcp(SERVICES_DB_PORT), 'Postgres from the worker fleet');
      dbSg.addIngressRule(coordinatorSg, ec2.Port.tcp(SERVICES_DB_PORT), 'Postgres from the coordinator');

      this.servicesDb = new rds.DatabaseCluster(this, 'ServicesDb', {
        // `.of()` rather than an `AuroraPostgresEngineVersion.VER_*` constant because the CDK
        // enum lags Aurora releases and does not carry 18.4.
        //
        // ⚠️ THE ONE KNOB TO CHECK BEFORE A REAL DEPLOY. Aurora PostgreSQL tracks community
        // Postgres at a lag, and the CloudFormation spec bundled with this CDK version tops out
        // at `18.3` — `cdk synth` emits a W/E9006 warning saying so, and a deploy against a
        // region that does not offer 18.4 will fail at CreateDBCluster. 18.4 is what compose and
        // CI run, so it is what this pins; confirm with
        // `aws rds describe-db-engine-versions --engine aurora-postgresql --query
        // 'DBEngineVersions[].EngineVersion'` and edit this line if your region disagrees.
        engine: rds.DatabaseClusterEngine.auroraPostgres({
          version: rds.AuroraPostgresEngineVersion.of('18.4', '18'),
        }),
        vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
        securityGroups: [dbSg],
        // Generated into Secrets Manager — the password never appears in the template, in the
        // repo, or in a task's plain environment.
        credentials: rds.Credentials.fromGeneratedSecret(SERVICES_DB_USER),
        defaultDatabaseName: SERVICES_DB_NAME,
        writer: rds.ClusterInstance.serverlessV2('writer'),
        // A control plane is bursty and mostly idle; scale down to near-nothing between queries.
        serverlessV2MinCapacity: 0.5,
        serverlessV2MaxCapacity: 4,
        storageEncrypted: true,
        // Matches the warehouse bucket's POC posture: a `cdk destroy` should actually destroy.
        // Flip both of these before anyone's data matters.
        deletionProtection: false,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

      metadataEnv.LLDB_METADATA_HOST = this.servicesDb.clusterEndpoint.hostname;
      metadataEnv.LLDB_METADATA_PORT = cdk.Tokenization.stringifyNumber(this.servicesDb.clusterEndpoint.port);
      metadataEnv.LLDB_METADATA_DATABASE = SERVICES_DB_NAME;
      metadataEnv.LLDB_METADATA_USER = SERVICES_DB_USER;
      // TLS on the wire without pinning a CA bundle into the image. `verify-full` is the next
      // step up and needs the RDS root shipped with the container.
      metadataEnv.LLDB_METADATA_SSLMODE = 'require';

      // The ONLY way the password reaches a task: ECS resolves it from Secrets Manager at start
      // and never writes it into the task definition. Adding this to a container also grants the
      // execution role read access to the secret.
      const secret = this.servicesDb.secret;
      if (!secret) {
        throw new Error('Aurora cluster did not generate a credentials secret');
      }
      metadataSecrets.LLDB_METADATA_PASSWORD = ecs.Secret.fromSecretsManager(secret, 'password');
    }

    // ---- Shared image + logging ---------------------------------------------------------
    // BOTH task definitions resolve this same object: one tag, one build, whole fleet.
    const image = ecs.ContainerImage.fromEcrRepository(this.repository, props.imageTag);
    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    /** Env shared by both roles: point the S3 storage arm at the warehouse bucket. */
    const storageEnv: Record<string, string> = {
      LLDB_STORAGE: 's3',
      LLDB_S3_BUCKET: this.warehouse.bucketName,
      LLDB_S3_REGION: this.region,
      RUST_LOG: 'info',
    };

    // ---- Transport security --------------------------------------------------------------
    // What each role needs is not the same thing, and the difference is the point: a worker
    // *serves* a Flight port and so holds an identity (certificate + private key), while the
    // coordinator binds nothing at all — it is a one-shot client, and its entire TLS surface is
    // "what do I trust, and under what name". So the private key is injected into worker tasks
    // only, and the coordinator's execution role is never granted it.
    //
    // The PEM travels as `LLDB_TLS_*_PEM` rather than as a path because ECS resolves a secret into
    // an environment variable and offers no way to put one on a filesystem.
    // `crates/lldb-qe-control/src/tls.rs` accepts either spelling for exactly this reason; compose
    // and a laptop still use files. Note what does NOT appear anywhere below:
    // `LLDB_ALLOW_PLAINTEXT`. Encrypting this
    // traffic is the point, and the escape hatch that disarms the engine's guard is not something
    // a stack sets on the way past (`infra/test/lldb-stack.test.ts` asserts its absence in *both*
    // modes).
    const tlsMode: TlsMode = props.tls ?? 'none';
    const scheme = tlsMode === 'fleet' ? 'https' : 'http';
    const tlsSecretPrefix = validateSecretPrefix(props.tlsSecretPrefix ?? DEFAULT_TLS_SECRET_PREFIX);

    const tlsEnv: Record<string, string> = {};
    const workerTlsSecrets: Record<string, ecs.Secret> = {};
    const clientTlsSecrets: Record<string, ecs.Secret> = {};

    if (tlsMode === 'fleet') {
      // Imported, never created — see {@link TlsMode} for why, and for the deadlock creating them
      // would cause. A secret that does not exist yet fails at task start with an ECS event naming
      // it, which is loud, immediate, and fixable by running the mint script.
      const caSecret = secretsmanager.Secret.fromSecretNameV2(this, 'FleetTlsCa', `${tlsSecretPrefix}-ca`);
      const certSecret = secretsmanager.Secret.fromSecretNameV2(this, 'FleetTlsCert', `${tlsSecretPrefix}-cert`);
      const keySecret = secretsmanager.Secret.fromSecretNameV2(this, 'FleetTlsKey', `${tlsSecretPrefix}-key`);

      // One name, every role, every warehouse. See {@link FLEET_TLS_DOMAIN}: this is what makes a
      // fleet dialed by task IP verifiable at all.
      tlsEnv.LLDB_TLS_DOMAIN = FLEET_TLS_DOMAIN;

      clientTlsSecrets.LLDB_TLS_CA_PEM = ecs.Secret.fromSecretsManager(caSecret);
      workerTlsSecrets.LLDB_TLS_CA_PEM = clientTlsSecrets.LLDB_TLS_CA_PEM;
      // A worker dials its peers as well as serving them (the worker-to-worker shuffle pulls map
      // stages directly), so it needs the trust *and* the identity. The coordinator needs the
      // trust alone.
      workerTlsSecrets.LLDB_TLS_CERT_PEM = ecs.Secret.fromSecretsManager(certSecret);
      workerTlsSecrets.LLDB_TLS_KEY_PEM = ecs.Secret.fromSecretsManager(keySecret);
    }

    // ---- Fleet secret ---------------------------------------------------------------------
    // The credential that makes a worker's Flight port a door rather than a hole. The coordinator
    // presents it on every call (`lldb_qe_control::auth::FleetAuth`), a worker configured with one
    // rejects anything else as `UNAUTHENTICATED` under a constant-time compare, and
    // `lldb_qe_core::plan_assertion` derives its HMAC key from the same value — so this one secret
    // is what closes *both* "is the caller part of this deployment" and "did a coordinator
    // authorize this particular plan to read these locations". Without it a worker executes any
    // physical plan anything routable in the VPC hands it, using this task role's S3 credentials.
    //
    // **It exists only under `tls: 'fleet'`, and that is the whole safety argument.** A worker that
    // checks a credential refuses to bind a plaintext port (`tls.rs`, `CredentialCheck::Enforced`),
    // so "fleet secret + plaintext" is a deploy that dies at task start — or, worse, one an operator
    // rescues with `LLDB_ALLOW_PLAINTEXT` and thereby ships the secret in the clear. Refusing that
    // combination at synth was the obvious fix; not being able to *write* it is the better one, so
    // the token is tied to the mode that satisfies the guard instead of being its own flag. Under
    // `tls: 'none'` nothing is created, nothing is injected, no credential is checked, and the fleet
    // is byte-for-byte what it was before this block existed.
    //
    // **Generated here, where the TLS material above is imported — deliberately, not by oversight.**
    // The PEM is imported because creating it would deadlock a first deploy: CloudFormation makes an
    // empty secret, the ECS service cannot stabilise on an empty PEM, the circuit breaker fails the
    // stack, and the rollback deletes the secret you were about to fill. None of that applies to a
    // token. CloudFormation generates the value as part of creating the resource, and the task
    // definitions reference its ARN, so no task ever starts against an unfilled one — and even if
    // one did, `FleetAuth::from_env` reads a blank value as *unset*, which is a fleet that starts
    // open rather than a fleet that cannot start at all. So generating it costs nothing and buys the
    // property that matters most for a symmetric secret: it exists in Secrets Manager and nowhere
    // else — not in source, not in CDK context, not in a `cdk.out` artifact, not in a shell history.
    //
    // Injected with `ecs.Secret`, never `environment`: a plain environment entry is stored verbatim
    // in the task definition and handed to anyone who can call `describe-task-definition`. That
    // injection grants each task definition's **execution** role `secretsmanager:GetSecretValue`,
    // which is the only role that needs it — ECS resolves the value at container start and the
    // process reads it from its own environment. The engine carries no AWS SDK and makes no
    // Secrets Manager call, so granting the *task* roles as well would be a permission nothing uses,
    // on a secret the container already holds, that would additionally survive a rotation. There is
    // a test asserting the task roles do not get it, so its absence reads as a decision.
    const fleetSecrets: Record<string, ecs.Secret> = {};

    if (tlsMode === 'fleet') {
      const fleetToken = new secretsmanager.Secret(this, 'FleetToken', {
        description: 'lldb shared fleet secret — worker Flight authentication and plan-assertion key',
        generateSecretString: {
          passwordLength: FLEET_TOKEN_LENGTH,
          // Alphanumerics only. The value's whole life is spent as an environment variable and in
          // the `aws ecs run-task --overrides` an operator writes while debugging, and punctuation
          // buys entropy that {@link FLEET_TOKEN_LENGTH} already has several times over while
          // costing a class of quoting bugs. `FleetAuth::from_env` trims, so a stray space would be
          // silently eaten rather than compared — another reason not to generate one.
          excludePunctuation: true,
          includeSpace: false,
        },
        // Matches the warehouse bucket and the services database: a `cdk destroy` should actually
        // destroy. Safe in a way the CA is not — the token is regenerable, and both roles read
        // whatever this resource holds, so a fresh value is a fresh fleet rather than a broken one.
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      this.fleetToken = fleetToken;
      // One entry, spread into BOTH task definitions below. Same argument as the single image tag:
      // a fleet whose halves hold different secrets is a fleet that cannot talk to itself.
      fleetSecrets[FLEET_TOKEN_ENV] = ecs.Secret.fromSecretsManager(fleetToken);
    }

    // ---- Warehouses: one worker fleet each ----------------------------------------------
    // Each warehouse gets its own task definition, its own ECS service, and — the part that makes
    // routing work — its own Cloud Map name. `analytics.lldb.local` resolves to the analytics
    // warehouse's tasks and to nothing else, so a coordinator handed a warehouse name cannot
    // reach another warehouse's compute even by accident.
    //
    // Transport and authentication: plaintext and wide open in the default mode, TLS plus the
    // shared fleet secret under `tls: 'fleet'` — see {@link TlsMode} and the two injection blocks
    // above. The pairing is not a convenience: a worker handed a fleet secret starts *checking* a
    // credential, and `tls.rs` then refuses to bind a plaintext port, so one without the other is a
    // fleet that will not start. Under `tls: 'none'` these ports are still reachable by anything in
    // the VPC that can resolve `<warehouse>.lldb.local`, and the mitigations are what they have
    // always been: the security group, and the loud warning every worker logs at startup.
    for (const definition of warehouses) {
      const id = `Warehouse${pascalCase(definition.name)}`;
      const task = new ecs.FargateTaskDefinition(this, `${id}Task`, { cpu, memoryLimitMiB });
      task.addContainer('worker', {
        image,
        command: ['lldb-qe-worker', '--bind', `0.0.0.0:${WORKER_PORT}`],
        environment: { ...storageEnv, ...metadataEnv, ...tlsEnv },
        secrets: { ...metadataSecrets, ...workerTlsSecrets, ...fleetSecrets },
        portMappings: [{ containerPort: WORKER_PORT }],
        logging: ecs.LogDrivers.awsLogs({ streamPrefix: `worker-${definition.name}`, logGroup }),
        // `nc -z` is in the runtime image precisely so orchestrators can probe the Flight port. A
        // TCP probe, so it is unchanged by TLS — it proves the port is bound, and the engine has
        // already failed at startup if the certificate did not parse.
        healthCheck: {
          command: ['CMD-SHELL', `nc -z 127.0.0.1 ${WORKER_PORT} || exit 1`],
          interval: cdk.Duration.seconds(15),
          timeout: cdk.Duration.seconds(5),
          retries: 3,
          startPeriod: cdk.Duration.seconds(30),
        },
      });
      // Every warehouse reads the same warehouse bucket. Compute is what is partitioned here,
      // never storage — that separation is the entire premise of the abstraction.
      this.warehouse.grantReadWrite(task.taskRole);

      const service = new ecs.FargateService(this, `${id}Service`, {
        cluster: this.cluster,
        taskDefinition: task,
        // Suspended means zero tasks: the definition survives, the bill does not. Resuming is
        // this number going back to `size` — a deploy, or one `aws ecs update-service` call.
        desiredCount: definition.state === 'suspended' ? 0 : definition.size,
        // Without a NAT the task needs its own public IP to reach ECR; behind one it must not
        // have a public IP at all (that is the whole point of moving it to a private subnet).
        assignPublicIp: !isPrivate,
        vpcSubnets: taskSubnets,
        securityGroups: [workerSg],
        // Fail a bad rollout fast (and roll back) instead of letting ECS grind for up to 3 hours.
        circuitBreaker: { rollback: true },
        minHealthyPercent: 50,
        // Registers each task in Cloud Map as `<warehouse>.lldb.local`. An A-record query returns
        // every healthy task IP, which is how the coordinator finds *this* warehouse's fleet.
        cloudMapOptions: {
          name: definition.name,
          dnsRecordType: servicediscovery.DnsRecordType.A,
          dnsTtl: cdk.Duration.seconds(10),
        },
      });
      this.warehouseServices[definition.name] = service;
    }
    // The first warehouse is the one a bare `run-task` coordinator points at.
    this.workerService = this.warehouseServices[warehouses[0].name];

    // ---- Coordinator --------------------------------------------------------------------
    // One-shot by nature (plan → fetch → print → exit), so it is a task definition to
    // `run-task`, not a long-running service.
    this.coordinatorTask = new ecs.FargateTaskDefinition(this, 'CoordinatorTask', { cpu, memoryLimitMiB });
    this.coordinatorTask.addContainer('coordinator', {
      image,
      command: ['lldb-qe-coordinator'],
      environment: {
        ...storageEnv,
        ...metadataEnv,
        ...tlsEnv,
        // Two routing paths, and the coordinator picks by whether `--warehouse` is set.
        // `LLDB_WORKERS` is the pre-warehouse behaviour (the first warehouse's fleet, verbatim),
        // so a `run-task` with no extra arguments works exactly as it always did.
        LLDB_WORKERS: `${scheme}://${warehouses[0].name}.${NAMESPACE}:${WORKER_PORT}`,
        // …and this is the template `--warehouse <name>` renders into. Override `LLDB_WAREHOUSE`
        // per `run-task` to send a query to a specific warehouse.
        LLDB_WAREHOUSE_ENDPOINT: warehouseEndpointTemplate(scheme),
      },
      // The same fleet secret the workers check, from the same Secrets Manager resource: this is the
      // credential the coordinator presents on every Flight call and the key it signs each request's
      // plan assertion with.
      secrets: { ...metadataSecrets, ...clientTlsSecrets, ...fleetSecrets },
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'coordinator', logGroup }),
    });
    this.warehouse.grantReadWrite(this.coordinatorTask.taskRole);

    // ---- Outputs ------------------------------------------------------------------------
    new cdk.CfnOutput(this, 'RepositoryUri', { value: this.repository.repositoryUri, description: 'Push the CI-built image here' });
    new cdk.CfnOutput(this, 'WarehouseBucket', { value: this.warehouse.bucketName, description: 'S3 Iceberg warehouse' });
    new cdk.CfnOutput(this, 'ClusterName', { value: this.cluster.clusterName });
    new cdk.CfnOutput(this, 'WorkerDns', { value: `${warehouses[0].name}.${NAMESPACE}:${WORKER_PORT}`, description: 'Cloud Map DNS for the default warehouse fleet' });
    new cdk.CfnOutput(this, 'Warehouses', {
      value: warehouses.map((w) => `${w.name}=${w.size}/${w.state ?? 'running'}`).join(','),
      description: 'Deployed warehouses as name=size/state',
    });
    // The names `aws ecs update-service --service …` wants, which is how a suspend/resume/resize
    // recorded in the services database gets applied without a redeploy.
    new cdk.CfnOutput(this, 'WarehouseServices', {
      value: cdk.Fn.join(',', warehouses.map((w) => `${w.name}=${this.warehouseServices[w.name].serviceName}`)),
      description: 'warehouse=ECS service name, for `aws ecs update-service --desired-count`',
    });
    new cdk.CfnOutput(this, 'CoordinatorTaskArn', { value: this.coordinatorTask.taskDefinitionArn, description: 'Run with `aws ecs run-task` to execute a query' });
    new cdk.CfnOutput(this, 'CoordinatorSecurityGroup', { value: coordinatorSg.securityGroupId, description: 'Use this SG when running the coordinator task' });
    // Everything `aws ecs run-task --network-configuration` needs, matching the egress mode.
    new cdk.CfnOutput(this, 'TaskSubnets', {
      value: cdk.Fn.join(',', vpc.selectSubnets(taskSubnets).subnetIds),
      description: 'Subnets to run the coordinator task in',
    });
    new cdk.CfnOutput(this, 'AssignPublicIp', {
      value: isPrivate ? 'DISABLED' : 'ENABLED',
      description: 'assignPublicIp for run-task (DISABLED behind a NAT)',
    });
    new cdk.CfnOutput(this, 'EgressMode', { value: egress });
    new cdk.CfnOutput(this, 'ServicesDbMode', { value: servicesDbMode });
    new cdk.CfnOutput(this, 'TlsMode', {
      value: tlsMode,
      description: 'none = plaintext Flight inside the VPC; fleet = TLS from the minted fleet certificate',
    });
    if (tlsMode === 'fleet') {
      // The names, never the material — `scripts/mint-fleet-tls.sh` writes to exactly these, and a
      // task that cannot start with a secrets error is almost always one of them missing.
      new cdk.CfnOutput(this, 'FleetTlsSecrets', {
        value: ['ca', 'cert', 'key'].map((part) => `${tlsSecretPrefix}-${part}`).join(','),
        description: 'Secrets Manager secrets holding the fleet CA, certificate and private key',
      });
      new cdk.CfnOutput(this, 'FleetTlsDomain', {
        value: FLEET_TLS_DOMAIN,
        description: 'The name the fleet certificate is issued for, and that every client verifies',
      });
      // The ARN, never the value — same rule as the services-database secret. Rotating is
      // `aws secretsmanager put-secret-value` against this ARN followed by a forced new deployment
      // of every service *and* the next coordinator run: the engine reads the variable once per
      // process, so a half-rotated fleet fails authentication until the restart completes.
      new cdk.CfnOutput(this, 'FleetTokenSecretArn', {
        value: this.fleetToken!.secretArn,
        description: 'Secrets Manager ARN holding the generated shared fleet secret',
      });
    }
    if (this.servicesDb) {
      new cdk.CfnOutput(this, 'ServicesDbEndpoint', {
        value: this.servicesDb.clusterEndpoint.socketAddress,
        description: 'Services-database writer endpoint (reachable only from inside the VPC)',
      });
      new cdk.CfnOutput(this, 'ServicesDbSecretArn', {
        // The ARN, never the value: read it with `aws secretsmanager get-secret-value` when you
        // need to run `lldb-qe-migrate` by hand.
        value: this.servicesDb.secret!.secretArn,
        description: 'Secrets Manager ARN holding the services-database credentials',
      });
    }
  }
}

/** A DNS label: lowercase alphanumerics and `-`, not leading or trailing. Mirrors the engine's
 * `validate_warehouse_name`, and for the same reason — the name becomes a hostname. */
const WAREHOUSE_NAME_RE = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/;
/** One DNS label's worth. */
const MAX_WAREHOUSE_NAME_LEN = 63;

/**
 * Refuse a warehouse list that cannot be deployed, at synth time.
 *
 * Every one of these is a failure that would otherwise surface late and confusingly: a name that
 * is not a DNS label produces a Cloud Map service nothing can resolve; two warehouses with one
 * name produce two services fighting over one record; a `size` from `-c warehouses=…` arrives as
 * a string through `Number()`, so a typo becomes `NaN` and CloudFormation gets a nonsense
 * `DesiredCount`. A suspended warehouse still needs a positive size — that is the number resume
 * scales back to, exactly as in the database.
 */
function validateWarehouses(warehouses: WarehouseDefinition[]): WarehouseDefinition[] {
  if (warehouses.length === 0) {
    throw new Error('at least one warehouse is required — a stack with no compute cannot run a query');
  }
  const seen = new Set<string>();
  for (const warehouse of warehouses) {
    if (typeof warehouse.name !== 'string' || !WAREHOUSE_NAME_RE.test(warehouse.name) || warehouse.name.length > MAX_WAREHOUSE_NAME_LEN) {
      throw new Error(
        `warehouse name ${JSON.stringify(warehouse.name)} must be a DNS label: 1-${MAX_WAREHOUSE_NAME_LEN} ` +
          'characters of lowercase [a-z0-9-], not starting or ending with `-` (it becomes <name>.lldb.local)',
      );
    }
    if (seen.has(warehouse.name)) {
      throw new Error(`duplicate warehouse name '${warehouse.name}' — names are Cloud Map records and must be unique`);
    }
    seen.add(warehouse.name);
    if (!Number.isInteger(warehouse.size) || warehouse.size < 1) {
      throw new Error(
        `warehouse '${warehouse.name}' size must be a positive integer, got ${JSON.stringify(warehouse.size)} ` +
          '(a suspended warehouse keeps its size; suspension is state, not size 0)',
      );
    }
    if (warehouse.state !== undefined && warehouse.state !== 'running' && warehouse.state !== 'suspended') {
      throw new Error(`warehouse '${warehouse.name}' has unknown state ${JSON.stringify(warehouse.state)} (expected running | suspended)`);
    }
  }
  return warehouses;
}

/** Secrets Manager names allow alphanumerics and `/_+=.@-`. Ours also append `-ca`/`-cert`/`-key`. */
const SECRET_PREFIX_RE = /^[A-Za-z0-9/_+=.@-]{1,400}$/;

/**
 * Refuse a secret prefix that could not name a secret, at synth time.
 *
 * The failure this prevents is the expensive kind: an unnameable secret synthesizes fine, deploys
 * fine, and surfaces as tasks that will not start with a `ResourceInitializationError` — after a
 * rollout, on a fleet that was working a minute ago.
 */
function validateSecretPrefix(prefix: string): string {
  if (!SECRET_PREFIX_RE.test(prefix)) {
    throw new Error(
      `tlsSecretPrefix ${JSON.stringify(prefix)} is not a usable Secrets Manager name: ` +
        'up to 400 characters of [A-Za-z0-9/_+=.@-] (it becomes <prefix>-ca, <prefix>-cert, <prefix>-key)',
    );
  }
  return prefix;
}

/** `wh-analytics` → `WhAnalytics`, for a construct id. Ids are alphanumeric by convention and
 * appear in every logical id, so a warehouse name's `-` cannot go through verbatim. */
function pascalCase(name: string): string {
  return name
    .split('-')
    .filter((part) => part.length > 0)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}
