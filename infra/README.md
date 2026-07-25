# lldb on AWS (CDK)

Deploys the query engine to **ECS Fargate**: an ECR repo for the one image, an S3 Iceberg
warehouse, a service-discovered fleet of stateless workers, and a one-shot coordinator task.

```
                 ┌──────────────── VPC (2 AZs, egress: see below) ───────────┐
  aws ecs        │                                                            │
  run-task ─────▶│  coordinator (one-shot)  ──Arrow Flight :50051──▶  worker  │
                 │         │                                          worker  │
                 │         └── DNS: worker.lldb.local (Cloud Map) ─────┘      │
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

Knobs: `-c workerCount=4` sizes the fleet, `-c egress=nat-instance` moves it into private
subnets (see below); `cpu`/`memoryLimitMiB` are stack props. The `TaskSubnets` and
`AssignPublicIp` outputs give you the right `run-task` network config for whichever mode you
deployed.

## Tests

```bash
npm test        # assertion tests over the synthesized template
npm run synth   # emit CloudFormation to cdk.out/
```

The tests assert the invariants rather than a snapshot: both roles resolve the *same* image, an
unpinned tag is refused, the Flight port is reachable only from the coordinator's security group,
task roles are scoped to the warehouse bucket, each egress mode builds the network it claims to,
and — in *every* mode — no security group admits `0.0.0.0/0`.

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

- **`removalPolicy: DESTROY`** on the bucket and ECR repo keeps teardown clean for a POC. Change
  both before anything real lands in the warehouse.
- **Fan-out is single-worker today.** `worker.lldb.local` resolves to every healthy task IP, but
  the coordinator currently ships its plan to one address. Spreading a query across the fleet
  needs DNS enumeration (or a registry) plus scan-level slicing — tracked with the engine
  internals work, not an infrastructure gap.
