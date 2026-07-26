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
`-c servicesDb=none` drops the control plane (see below); `cpu`/`memoryLimitMiB` are stack props.
The `TaskSubnets` and `AssignPublicIp` outputs give you the right `run-task` network config for
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

- **`removalPolicy: DESTROY`** on the bucket, ECR repo and services database (plus
  `deletionProtection: false` on the latter) keeps teardown clean for a POC. Change all of them
  before anything real lands in the warehouse or the control plane.
- **`cdk synth` warns `W9008` — "RDS instance should have StorageEncrypted set to true"** — on
  the Aurora *writer*. It is a false positive: for Aurora, storage encryption is a cluster-level
  property, and `AWS::RDS::DBCluster.StorageEncrypted` is `true` in the synthesized template.
- **Warehouse state lives in two places and nothing reconciles them automatically.** The stack's
  `warehouses` list and the services database's `warehouses` table both describe the same pools,
  and keeping them in step is an operator's job (the coordinator warns on drift). A reconciler —
  something that reads the table and drives ECS — is a natural next step, and is deliberately not
  the engine's job.
