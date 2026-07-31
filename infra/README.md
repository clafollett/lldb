# lldb on AWS (CDK)

Deploys the query engine to **ECS Fargate**: an ECR repo for the one image, an S3 Iceberg
warehouse, an Aurora Serverless v2 Postgres services database, a service-discovered fleet of
stateless workers, and a one-shot coordinator task.

```
                 ┌──────────────── VPC (2 AZs, egress: see below) ───────────┐
  aws ecs        │                                                            │
  run-task ─────▶│  coordinator (one-shot)  ──Arrow Flight :50051──▶  worker  │
                 │       │ │                                          worker  │
                 │       │ └── DNS: worker.lldb.local (Cloud Map) ───┘   │    │
                 │       │                                               │    │
                 │       └────── :5432 ──▶ Aurora Serverless v2 ◀────────┘    │
                 │                         (isolated subnets, services DB)    │
                 │                          │                                 │
                 └──────────────────────────┼─────────────────────────────────┘
                                            ▼  S3 gateway endpoint (free)
                                    s3://…-warehouse  (Iceberg)
```

## One tag, whole fleet

Serialized DataFusion physical plans are **not** cross-version compatible, so the coordinator
and every worker must run the *identical* build. The stack takes a single `imageTag` and hands
the same image to both task definitions; synth **fails** on a missing tag or on `latest`.
CI stamps each image `version+git-sha` — deploy that exact tag, and confirm what's running with
`lldb-qe-worker --version`.

## Deploy

```bash
npm ci
npx cdk bootstrap                                    # first time per account/region

# 1. Create the stack (ECR repo included).
npx cdk deploy -c imageTag=0.1.0+8c6d8d6b57d8

# 2. Push that exact image to the ECR repo from the RepositoryUri output.
docker build -t "$REPO_URI:0.1.0+8c6d8d6b57d8" --build-arg GIT_SHA=8c6d8d6b57d8 ..
docker push "$REPO_URI:0.1.0+8c6d8d6b57d8"

# 3. Run a query: the coordinator ships a plan to a worker and prints the result to its log.
#    Subnets/AssignPublicIp come from the stack outputs, so this is the same in every egress mode.
aws ecs run-task --cluster <ClusterName> --task-definition <CoordinatorTaskArn> \
  --launch-type FARGATE --network-configuration \
  "awsvpcConfiguration={subnets=[<TaskSubnets>],securityGroups=[<CoordinatorSecurityGroup>],assignPublicIp=<AssignPublicIp>}"
```

Knobs: `-c workerCount=4` sizes the default fleet, `-c warehouses=analytics:4,etl:1` deploys named
warehouses instead (see below), `-c egress=nat-instance` moves it into private subnets (see below),
`-c servicesDb=none` drops the control plane (see below), `-c tls=fleet` encrypts **and
authenticates** Flight inside the VPC (see below); `cpu`/`memoryLimitMiB` are stack props. The
`TaskSubnets` and `AssignPublicIp` outputs give you the right `run-task` network config for
whichever mode you deployed.

## Virtual warehouses (`-c warehouses=…`)

A **warehouse** is a named, independently sized pool of workers. Here that is literally *one ECS
service per warehouse*, each registering its tasks under its own Cloud Map name, all reading the
same S3 warehouse bucket and the same catalog:

```bash
npx cdk deploy -c imageTag=0.1.0+<sha> -c warehouses=analytics:4,etl:1,nightly:8:suspended
```

| Piece | What it is |
| - | - |
| `name` | The Cloud Map DNS label — `analytics.lldb.local`. Must be a DNS label (lowercase `[a-z0-9-]`, ≤63 chars); synth fails otherwise. |
| `size` | The service's `desiredCount` while running. Kept even when suspended — it is what resume scales back to. |
| `state` | `running` (default) or `suspended`; **suspended deploys at `desiredCount: 0`**, so the definition and its DNS name survive while the compute bill does not. |

Omit the flag and you get one `worker` warehouse of `workerCount` tasks — byte for byte the fleet
this stack deployed before warehouses existed, at the same `worker.lldb.local`. (`workerCount` and
`warehouses` are mutually exclusive; passing both is a synth error rather than a silent choice.)

Routing: the coordinator task gets `LLDB_WAREHOUSE_ENDPOINT=http://{warehouse}.lldb.local:50051`,
and `--warehouse <name>` (`LLDB_WAREHOUSE`) renders into it. Because each warehouse's tasks are the
only ones registered under its name, a query fans across exactly that warehouse's fleet:

```bash
aws ecs run-task --cluster <ClusterName> --task-definition <CoordinatorTaskArn> \
  --launch-type FARGATE \
  --overrides '{"containerOverrides":[{"name":"coordinator","environment":[{"name":"LLDB_WAREHOUSE","value":"analytics"}]}]}' \
  --network-configuration "awsvpcConfiguration={subnets=[<TaskSubnets>],securityGroups=[<CoordinatorSecurityGroup>],assignPublicIp=<AssignPublicIp>}"
```

### Applying a resize, suspend or resume — the part you do by hand

The services database holds the **desired state** and this stack is one of the two things that
applies it. The engine binaries call no AWS API at all (`lldb-qe-warehouse` writes rows and prints
the command it did *not* run), deliberately: pulling in the AWS SDK fights the one-version
tree-wide dependency rule the Flight boundary depends on, and it would hard-code one cloud into
the control plane.

So a lifecycle change is two steps — state the desire, then apply it:

```bash
# 1. desired state (from anywhere that can reach the services DB)
lldb-qe-warehouse resize  --name analytics --size 8
lldb-qe-warehouse suspend --name nightly

# 2a. apply it now, no deploy — the fast path
aws ecs update-service --cluster <ClusterName> \
  --service <analytics, from the WarehouseServices output> --desired-count 8
aws ecs update-service --cluster <ClusterName> --service <nightly's service> --desired-count 0

# 2b. or apply it durably, so the next deploy does not undo it
npx cdk deploy -c imageTag=… -c warehouses=analytics:8,etl:1,nightly:8:suspended
```

Use **2a for speed and 2b to persist**: a bare `update-service` is reverted by the next
`cdk deploy`, which resets `DesiredCount` from the stack's own `warehouses`. The `Warehouses` and
`WarehouseServices` outputs give you the current definitions and the service names those commands
need. When the two drift, the coordinator logs a warning naming both numbers — what the warehouse
said, and what the fleet actually answered with.

Locally `docker compose` plays the same role: each warehouse is a Docker **network alias**, and
`docker compose up -d --scale worker-etl=3` is the `update-service` equivalent. See the header of
[`docker-compose.yml`](../docker-compose.yml).

## Services database (`-c servicesDb=…`)

The control plane — accounts, the Iceberg SQL catalog, virtual warehouses, query history — needs
one shared, transactional store that every role sees the same view of. That is an **Aurora
Serverless v2 PostgreSQL** cluster:

| Mode | What you get |
| - | - |
| `aurora` *(default)* | A serverless-v2 cluster (0.5–4 ACU) in **isolated** subnets, storage encrypted, a generated password in Secrets Manager, and ingress on 5432 restricted to the worker and coordinator security groups. |
| `none` | No cluster, no secret, and no `LLDB_METADATA_*` on the tasks. The engine treats an unconfigured services DB as legitimate, so this is a query-only fleet — useful for a throwaway benchmark stack. |

Serverless v2 rather than a provisioned instance because control-plane load is bursty and mostly
idle; it scales toward zero between queries instead of billing for an always-on instance. The
subnets are isolated (no NAT, no internet route in either direction) — free, and a control plane
unreachable from outside the VPC is the cheapest security win on offer.

The tasks receive `LLDB_METADATA_HOST/PORT/DATABASE/USER/SSLMODE` as plain environment and
`LLDB_METADATA_PASSWORD` as an **ECS secret** resolved from Secrets Manager at task start — the
password never appears in a task definition, the template, or the repo. That split is exactly why
the engine accepts the connection settings as discrete parts and not only as a single URL:
nothing in ECS can interpolate a secret into a URL string.

The engine version is pinned with `AuroraPostgresEngineVersion.of('18.4', '18')` rather than a
CDK enum constant, because the enum lags Aurora releases — 18.4 matches what compose and CI run,
so the same schema is exercised everywhere.

> **Check this before a real deploy.** Aurora PostgreSQL trails community Postgres, and the
> CloudFormation spec bundled with this CDK version only knows up to `18.3` — `cdk synth` prints
> a `W9006`/`E9006` warning about it, and a region that does not offer 18.4 will fail at
> `CreateDBCluster`. That call is the one line to change. List what a region actually offers with
> `aws rds describe-db-engine-versions --engine aurora-postgresql --query 'DBEngineVersions[].EngineVersion'`.

**Migrations are a separate step, on purpose.** Nothing migrates on boot (a fleet rollout would
have N tasks racing the same DDL). Run the `lldb-qe-migrate` binary from the same image as a
one-shot ECS task before rolling the services, the way compose's `db-migrate` does. Its
credentials come from the `ServicesDbSecretArn` output.

## Transport security and fleet authentication (`-c tls=…`)

| Mode | What you get |
| - | - |
| `none` *(default)* | Every Flight port is plaintext **and unauthenticated**. Legal, and not a hole in the engine's terms: it refuses a plaintext port only when a credential is actually checked on it, and in this mode nothing checks one. Traffic is confined to the VPC by the security groups — that, plus the warning every worker logs at startup, is the whole mitigation. |
| `fleet` | Workers serve TLS, every role dials `https://`, and the PEM arrives as `LLDB_TLS_*_PEM` from three Secrets Manager secrets — **and** both roles get a generated `LLDB_FLEET_TOKEN`, so a worker rejects a Flight call that does not present it. **~$1.60/mo.** |

**Mint before you deploy.** This is a prerequisite step, like `cdk bootstrap`:

```bash
./scripts/mint-fleet-tls.sh                                  # writes lldb/fleet-tls-{ca,cert,key}
cd infra && npx cdk deploy -c imageTag=0.1.0+<sha> -c tls=fleet
```

The stack **imports** those secrets rather than creating them, and both halves of that are
deliberate. A private key CDK holds is a key that can reach `cdk.out`, which is an artifact that
gets copied around; importing means CDK never sees the bytes at all. And creating them here would
deadlock a first deploy — CloudFormation would make three empty secrets, the ECS service would fail
to stabilise on the empty PEM, the deployment circuit breaker would fail the stack, and the
rollback would delete the secrets you were about to fill.

### The fleet secret rides the same switch, and that is the safety argument

`-c tls=fleet` also generates `LLDB_FLEET_TOKEN` into Secrets Manager and injects it into **both**
task definitions. That is the credential a worker's Flight port actually checks
(`lldb_qe_control::auth::FleetAuth`) and the key the coordinator signs each request's plan assertion
with (`lldb_qe_core::plan_assertion`) — without it, anything that can resolve
`<warehouse>.lldb.local` can have an arbitrary physical plan executed against the warehouse bucket
with the worker task role's credentials.

**There is no `-c fleetToken=…`, on purpose.** A worker that checks a credential refuses to bind a
plaintext port, so "fleet secret without certificates" is a deploy that dies at task start — or one
an operator rescues with `LLDB_ALLOW_PLAINTEXT` and thereby ships the secret in the clear. Refusing
that pairing at synth was the obvious fix; making it unrepresentable is the better one. So there is
one switch: `none` is today's plaintext, unauthenticated fleet, unchanged; `fleet` is encrypted and
authenticated. Nothing in between.

**Generated here, where the PEM above is imported.** The asymmetry is deliberate, and it is the
deadlock argument read backwards: an *empty* fleet token is not a fleet that cannot start, it is a
fleet that starts open (`FleetAuth::from_env` reads a blank value as unset) — and in practice not
even that, since CloudFormation mints the value as part of creating the resource that the task
definitions reference. With nothing to break, generating it buys the property that matters for a
symmetric secret: it exists in Secrets Manager and nowhere else — not in source, not in CDK context,
not in a `cdk.out` artifact, not in anyone's shell history. It is injected with `ecs.Secret`, never
`environment`, because a plain environment entry is stored verbatim in the task definition and
served to anyone who can call `describe-task-definition`.

Only the two **execution** roles are granted it — they are what resolve the value at container
start; the process then reads its own environment. The engine carries no AWS SDK and makes no
Secrets Manager call, so a *task* role grant would be a permission nothing uses, on a value the
container already holds, that would additionally outlive a rotation. There is a test asserting the
task roles do not have it.

Rotating is `aws secretsmanager put-secret-value` against the `FleetTokenSecretArn` output, then a
forced new deployment of every warehouse service. Same caveat as the certificates and for the same
reason — the value is read once per process — plus one more: during the rollout the halves of the
fleet disagree, and a coordinator run in that window fails against whichever half has not restarted.

### Why the PEM is an environment variable and not a file

The engine takes certificates as paths *or* as material (`LLDB_TLS_CERT` vs `LLDB_TLS_CERT_PEM`),
and on Fargate only the second works: **ECS resolves a Secrets Manager value into an environment
variable and offers no way to put one on a filesystem.** The alternatives were priced in issue #73
and each is worse — a container entrypoint that writes the PEM to disk puts the key in the
environment *and* on the task's ephemeral volume (Fargate does not support `tmpfs`, so that is a
disk, not RAM); an EFS volume adds a stateful dependency to the boot path of a fleet whose premise
is stateless workers, and nothing in CloudFormation can populate it without putting the key in a
deploy artifact; AWS Private CA costs **$400/month** and its one advantage, automatic renewal, is
inert here because the engine builds its TLS acceptor once at startup and never re-reads it.

Three separate whole-string secrets rather than one with JSON keys, because that is what makes the
split a boundary: `ecs.Secret` grants read on a *secret*, so **the coordinator's execution role is
granted the CA and never the fleet private key**. It binds no port, so an identity is material it
has no use for. There is a test asserting exactly that, in IAM and not only in the task definition.

### The name problem, and how it is actually solved

`discovery.rs` expands a Cloud Map name into one URL **per task IP**, so what a client would verify
is `https://10.0.1.47:50051` — and a certificate issued for `analytics.lldb.local` does not
validate against that. IP SANs are not available as a fix: a Fargate task's IP is allocated at task
start and changes on every replacement and every scale event, which is the elasticity `discovery.rs`
exists to deliver, so nothing minted in advance can name them.

So the fleet shares **one** certificate carrying `DNS:fleet.lldb.local`, and both roles get
`LLDB_TLS_DOMAIN=fleet.lldb.local`. The URL's IP connects; that SAN verifies. It is one fleet-wide
name rather than one per warehouse because the engine's dialing trust is process-global — a
`FlightReaderExec` is serialized into a plan and can carry nothing per-call — while one coordinator
dials several warehouses, so there is exactly one name available to verify against.

Be clear about the claim: a shared leaf authenticates **the fleet, not the member**. A client learns
that its peer holds the fleet's key, not which worker it is. That is already the documented scope of
this engine's TLS — it is server-authenticated, and per-member identity is `LLDB_FLEET_TOKEN`'s
claim with mTLS still an open decision (#106).

### Rotating

The engine reads its certificate once, at startup. So rotation is: re-run the mint script (it reuses
the CA in `./fleet-tls-ca` unless you delete it), then force a new deployment.

```bash
./scripts/mint-fleet-tls.sh
aws ecs update-service --cluster <ClusterName> --service <each WarehouseServices entry> \
  --force-new-deployment
```

Replacing the **CA** rather than the leaf is not rolling — every role has to restart together, since
half a fleet trusting a root the other half does not sign under fails every handshake between them.

## Tests

```bash
npm test        # assertion tests over the synthesized template
npm run synth   # emit CloudFormation to cdk.out/
```

The tests assert the invariants rather than a snapshot: both roles resolve the *same* image, an
unpinned tag is refused, the Flight port is reachable only from the coordinator's security group,
Postgres is reachable only from the task security groups, the database password is a secret
reference rather than plain environment, task roles are scoped to the warehouse bucket, each
egress and `servicesDb` mode builds what it claims to, warehouses become one service and one
Cloud Map name each (with a suspended one at `desiredCount: 0`, and an unroutable name refused at
synth), and — in *every* mode — no security group admits `0.0.0.0/0`.

For TLS specifically: no PEM material of any kind appears in the synthesized template, the private
key is unreadable by the coordinator's execution role in IAM, turning TLS on rewrites *every*
worker URL to `https://` with the `{warehouse}` placeholder intact, both roles verify under the one
fleet name, and `LLDB_ALLOW_PLAINTEXT` is absent in **both** modes.

For the fleet secret: `-c tls=fleet` puts `LLDB_FLEET_TOKEN` on *every* task definition as a secret
reference and in no `Environment` block anywhere, all of them resolve the same secret, no literal
secret value can reach the template (there is no `SecretString` property — only the
`GenerateSecretString` recipe), the task roles are not granted it, and `-c tls=none` creates and
injects nothing at all.

## Egress: how tasks reach ECR (`-c egress=…`)

Warehouse traffic never enters into this — it rides a free S3 *gateway* endpoint in every mode.
What needs a path out is pulling from ECR and writing CloudWatch logs:

| Mode | Tasks | Cost (us-east-1) | When |
| - | - | - | - |
| `none` *(default)* | public subnets, public IP | **$0** | POC. Inbound is default-deny and nothing opens `0.0.0.0/0`, so this is safe — just not defense in depth. |
| `nat-instance` | private subnets, no public IP | **~$3/mo** | Dev/staging. One [fck-nat](https://fck-nat.dev) `t4g.nano`. |
| `nat-gateway` | private subnets, no public IP | ~$33/mo + $0.045/GB | Production, where managed HA beats an instance to patch. |

```bash
npx cdk deploy -c imageTag=0.1.0+<sha> -c egress=nat-instance
```

The tempting fourth option — private subnets with **no** NAT, using PrivateLink instead — is the
*most* expensive of the lot: you would need `ecr.api`, `ecr.dkr` and `logs` interface endpoints
at ~$7.30/mo each per AZ, so ~$44/mo across two AZs.

Two things the NAT modes get right that are easy to miss:

- CDK's NAT instance defaults to `INBOUND_AND_OUTBOUND`, i.e. a security group open to the whole
  internet. This stack forces `OUTBOUND_ONLY` and admits only the VPC CIDR, since a NAT only ever
  forwards traffic that originated inside the VPC. There is a test per mode asserting nothing is
  world-open.
- `nat-instance` looks the fck-nat AMI up against the live EC2 API, so it needs real credentials
  at synth time. Pass `natMachineImage` to pin one instead (that is how the tests stay hermetic).

One NAT serves both AZs. That is a deliberate POC tradeoff — a NAT outage takes the fleet's
egress with it. Raise `natGateways` to `maxAzs` when that matters more than the second NAT costs.

## Other scope notes

- **`removalPolicy: DESTROY`** on the bucket, ECR repo, services database and fleet secret (plus
  `deletionProtection: false` on the database) keeps teardown clean for a POC. Change all of them
  before anything real lands in the warehouse or the control plane. The fleet secret is the one
  that is genuinely safe to destroy: it is regenerable, and both roles read whatever the resource
  holds, so a fresh value is a fresh fleet rather than a broken one.
- **`cdk synth` warns `W9008` — "RDS instance should have StorageEncrypted set to true"** — on
  the Aurora *writer*. It is a false positive: for Aurora, storage encryption is a cluster-level
  property, and `AWS::RDS::DBCluster.StorageEncrypted` is `true` in the synthesized template.
- **Warehouse state lives in two places and nothing reconciles them automatically.** The stack's
  `warehouses` list and the services database's `warehouses` table both describe the same pools,
  and keeping them in step is an operator's job (the coordinator warns on drift). A reconciler —
  something that reads the table and drives ECS — is a natural next step, and is deliberately not
  the engine's job.
