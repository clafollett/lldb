# lldb on AWS (CDK)

Deploys the query engine to **ECS Fargate**: an ECR repo for the one image, an S3 Iceberg
warehouse, a service-discovered fleet of stateless workers, and a one-shot coordinator task.

```
                 ┌─────────────────── VPC (2 AZs, no NAT) ───────────────────┐
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
aws ecs run-task --cluster <ClusterName> --task-definition <CoordinatorTaskArn> \
  --launch-type FARGATE --network-configuration \
  "awsvpcConfiguration={subnets=[<public-subnets>],securityGroups=[<CoordinatorSecurityGroup>],assignPublicIp=ENABLED}"
```

Knobs: `-c workerCount=4` sizes the fleet; `cpu`/`memoryLimitMiB` are stack props.

## Tests

```bash
npm test        # assertion tests over the synthesized template
npm run synth   # emit CloudFormation to cdk.out/
```

The tests assert the invariants rather than a snapshot: both roles resolve the *same* image, an
unpinned tag is refused, the Flight port is reachable only from the coordinator's security group
(never `0.0.0.0/0`), task roles are scoped to the warehouse bucket, and the VPC has no NAT.

## Cost & scope notes

- **No NAT gateways.** Tasks sit in public subnets with public IPs to pull from ECR, and
  warehouse traffic uses a free S3 *gateway* endpoint. A production VPC would use private
  subnets with NAT or interface endpoints — swap `subnetConfiguration`/`natGateways`.
- **`removalPolicy: DESTROY`** on the bucket and ECR repo keeps teardown clean for a POC. Change
  both before anything real lands in the warehouse.
- **Fan-out is single-worker today.** `worker.lldb.local` resolves to every healthy task IP, but
  the coordinator currently ships its plan to one address. Spreading a query across the fleet
  needs DNS enumeration (or a registry) plus scan-level slicing — tracked with the engine
  internals work, not an infrastructure gap.
