import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';

/** The Flight port every worker listens on (matches `LLDB_WORKER_BIND` in the image). */
export const WORKER_PORT = 50051;

/** Private DNS namespace the fleet registers into, e.g. `worker.lldb.local`. */
export const NAMESPACE = 'lldb.local';
export const WORKER_SERVICE_NAME = 'worker';

export interface LldbStackProps extends cdk.StackProps {
  /**
   * Image tag both roles run. This is the whole point of the single-tag design: serialized
   * DataFusion physical plans are NOT cross-version compatible, so the coordinator and every
   * worker must be the *identical* build. CI stamps images `version+git-sha`; deploy that tag.
   */
  readonly imageTag: string;
  /** Number of worker tasks in the fleet. */
  readonly workerCount?: number;
  /** Fargate task sizing (analytical work is memory-hungry; defaults suit the POC). */
  readonly cpu?: number;
  readonly memoryLimitMiB?: number;
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
  public readonly workerService: ecs.FargateService;
  public readonly coordinatorTask: ecs.FargateTaskDefinition;

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

    const workerCount = props.workerCount ?? 2;
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
    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [{ name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 }],
    });
    // Free egress to S3 — keeps warehouse traffic off the public internet and off a NAT bill.
    vpc.addGatewayEndpoint('S3Endpoint', { service: ec2.GatewayVpcEndpointAwsService.S3 });

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

    // ---- Worker fleet -------------------------------------------------------------------
    const workerTask = new ecs.FargateTaskDefinition(this, 'WorkerTask', { cpu, memoryLimitMiB });
    workerTask.addContainer('worker', {
      image,
      command: ['lldb-qe-worker', '--bind', `0.0.0.0:${WORKER_PORT}`],
      environment: storageEnv,
      portMappings: [{ containerPort: WORKER_PORT }],
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'worker', logGroup }),
      // `nc -z` is in the runtime image precisely so orchestrators can probe the Flight port.
      healthCheck: {
        command: ['CMD-SHELL', `nc -z 127.0.0.1 ${WORKER_PORT} || exit 1`],
        interval: cdk.Duration.seconds(15),
        timeout: cdk.Duration.seconds(5),
        retries: 3,
        startPeriod: cdk.Duration.seconds(30),
      },
    });
    this.warehouse.grantReadWrite(workerTask.taskRole);

    this.workerService = new ecs.FargateService(this, 'WorkerService', {
      cluster: this.cluster,
      taskDefinition: workerTask,
      desiredCount: workerCount,
      assignPublicIp: true, // no NAT: tasks need a public IP to pull from ECR
      securityGroups: [workerSg],
      // Fail a bad rollout fast (and roll back) instead of letting ECS grind for up to 3 hours.
      circuitBreaker: { rollback: true },
      minHealthyPercent: 50,
      // Registers each task in Cloud Map as `worker.lldb.local`. An A-record query returns
      // every healthy task IP, which is how the coordinator finds the fleet.
      cloudMapOptions: {
        name: WORKER_SERVICE_NAME,
        dnsRecordType: servicediscovery.DnsRecordType.A,
        dnsTtl: cdk.Duration.seconds(10),
      },
    });

    // ---- Coordinator --------------------------------------------------------------------
    // One-shot by nature (plan → fetch → print → exit), so it is a task definition to
    // `run-task`, not a long-running service.
    this.coordinatorTask = new ecs.FargateTaskDefinition(this, 'CoordinatorTask', { cpu, memoryLimitMiB });
    this.coordinatorTask.addContainer('coordinator', {
      image,
      command: ['lldb-qe-coordinator'],
      environment: {
        ...storageEnv,
        LLDB_WORKERS: `http://${WORKER_SERVICE_NAME}.${NAMESPACE}:${WORKER_PORT}`,
      },
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'coordinator', logGroup }),
    });
    this.warehouse.grantReadWrite(this.coordinatorTask.taskRole);

    // ---- Outputs ------------------------------------------------------------------------
    new cdk.CfnOutput(this, 'RepositoryUri', { value: this.repository.repositoryUri, description: 'Push the CI-built image here' });
    new cdk.CfnOutput(this, 'WarehouseBucket', { value: this.warehouse.bucketName, description: 'S3 Iceberg warehouse' });
    new cdk.CfnOutput(this, 'ClusterName', { value: this.cluster.clusterName });
    new cdk.CfnOutput(this, 'WorkerDns', { value: `${WORKER_SERVICE_NAME}.${NAMESPACE}:${WORKER_PORT}`, description: 'Cloud Map DNS for the worker fleet' });
    new cdk.CfnOutput(this, 'CoordinatorTaskArn', { value: this.coordinatorTask.taskDefinitionArn, description: 'Run with `aws ecs run-task` to execute a query' });
    new cdk.CfnOutput(this, 'CoordinatorSecurityGroup', { value: coordinatorSg.securityGroupId, description: 'Use this SG when running the coordinator task' });
  }
}
