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
authenticates** Flight inside the VPC (with `-c tlsDomain=…` when the certificate was minted for a
name other than `fleet.lldb.local` — see below); `cpu`/`memoryLimitMiB` are stack props. The
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
| `fleet` | Workers serve TLS, every role dials `https://`, and the PEM arrives as `LLDB_TLS_*_PEM` from three Secrets Manager secrets — **and** both roles get a generated `LLDB_FLEET_TOKEN`, so a worker rejects a Flight call that does not present it, plus `LLDB_REQUIRE_FLEET_TOKEN`, which makes an emptied secret a refusal to start rather than an open fleet. **~$1.60/mo.** |

> **Changing this setting on a live stack is an expected query outage** of several minutes, in
> either direction. Read *"Flipping `-c tls=none` → `-c tls=fleet` is an expected query outage"*
> below before you run it.

**Mint before you deploy.** This is a prerequisite step, like `cdk bootstrap`:

```bash
./scripts/mint-fleet-tls.sh                                  # writes lldb/fleet-tls-{ca,cert,key}
cd infra && npx cdk deploy -c imageTag=0.1.0+<sha> -c tls=fleet

# …or for a deployment with its own DNS zone. The two names are ONE setting: the script puts it in
# the certificate's SAN, the stack puts it in every client's LLDB_TLS_DOMAIN.
./scripts/mint-fleet-tls.sh --domain fleet.example.com
cd infra && npx cdk deploy -c imageTag=0.1.0+<sha> -c tls=fleet -c tlsDomain=fleet.example.com
```

`-c tlsDomain` defaults to `fleet.lldb.local`, which is exactly what `mint-fleet-tls.sh` mints when
*it* is not told otherwise — so an existing `-c tls=fleet` deploy is unchanged and needs the flag on
neither side. Give both sides the same value or give neither: a certificate whose SAN and a client's
`LLDB_TLS_DOMAIN` disagree fails **every** handshake in the fleet, at handshake time, on a deployed
fleet. Passing `-c tlsDomain` **without** `-c tls=fleet` is a synth error rather than a setting
quietly dropped — in plaintext mode nothing verifies a name, and a certificate name that silently
does not reach the fleet is the failure the flag exists to prevent.

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

### `LLDB_REQUIRE_FLEET_TOKEN` — the deployment asserts that the fleet is closed

`-c tls=fleet` also sets `LLDB_REQUIRE_FLEET_TOKEN` on every task definition, as a **plain
environment entry** beside the secret. Its presence makes a blank or missing `LLDB_FLEET_TOKEN` a
**refusal to start** instead of a warning: no worker binds its port, no coordinator dials one.

| `LLDB_REQUIRE_FLEET_TOKEN` | Behaviour |
| - | - |
| absent (`tls=none`, compose, `cargo run`) | unchanged — an unset fleet secret means an open port and a loud startup warning |
| **present with any value, including empty** | `LLDB_FLEET_TOKEN` must be present and non-blank, or the process exits before binding |

**Why it exists.** `FleetAuth::from_env` reads a blank value as unset, and ECS injects `FOO=` for an
emptied secret. So an operator who emptied the fleet secret in the console got an **open fleet** at
the next task replacement, and the only signal was a `warn` line — which is the line nobody is
watching at the moment it matters. The closed posture was tamper-*logged*, not tamper-*evident*.

**Why presence and not `=1`.** A value-triggered guard is defeated by exactly the edit it exists to
catch: empty this variable, then empty the token, and the fleet is open again — two console edits
instead of one, and the guard has reproduced the defect it was added to close. Because presence is
what counts, blanking *either* variable leaves the strict posture in force. Weakening it means
**deleting the entry from the task definition** — a structural change that the next `cdk deploy`
reverts and that the task-definition revision history records. Do not add a prop that omits it; an
opt-out is a value edit by another name.

It is in `environment` and not `ecs.Secret` deliberately, which is the inverse of the rule the token
lives under. The token must not be readable from the task definition. This one must: it carries no
secret, it is a *claim about* the secret, and its legibility to `describe-task-definition` — and to
an auditor, and to a diff — is the entire mechanism.

Startup with it set and no usable token looks like this, from every role:

```
Error: LLDB_REQUIRE_FLEET_TOKEN is set, so this deployment asserts that its worker fleet is
closed — but LLDB_FLEET_TOKEN is present but blank, ... Refusing to start.
```

That is the guard working. The fix is to restore the secret, not to delete the assertion.

### Flipping `-c tls=none` → `-c tls=fleet` is an expected query outage

**Plan for it. It is not a rolling change**, it is a change with a window in which queries fail, and
the failures are loud and confusing rather than silent — which is the guard working, but is also
exactly what gets rolled back at 3am by someone who was not told to expect it.

**Why there is a window at all.** The mode changes both task definitions at once, but ECS replaces
tasks over time, so for the length of the rollout the fleet is *half-converted* — and a coordinator
discovers workers by `discovery.rs` expanding one Cloud Map name into one URL **per task IP**, so
it dials every task the record still returns, converted or not. Two failure shapes live in that
window, and it is worth being able to recognize both:

| Who is talking | What happens |
| - | - |
| a **new** coordinator (`https://`, holds the token) → a worker still on the old plaintext task | the TLS handshake fails against a plaintext port. That classifies as a transport fault (`retry.rs`), so the stage is reassigned to another worker — the query may still answer, slowly, while enough converted workers remain, and fails once the candidates are exhausted |
| a **old** coordinator (`http://`, no token) → a worker already converted | the TLS server refuses the plaintext client outright, and since #99 a converted worker also answers `UNAUTHENTICATED`. That is **fatal** rather than retriable, on purpose: an identical fleet would refuse identically, so it fails immediately instead of walking every worker |

Neither is silent and neither produces a wrong answer. Both mean: **no reliable query service until
every task in every warehouse is on the new definition.**

**How long.** Dominated by task replacement, not by CloudFormation: each warehouse service rolls at
`minHealthyPercent: 50`, so at least two waves, and each new task pays an image pull plus the
health check's 30 s start period before it counts. On a small stack that is roughly **5–15
minutes**; it grows with warehouse count, warehouse size and a cold image, and the Cloud Map record
carries a 10 s TTL on top. Measure your own rather than trusting this range — the point is that it
is minutes, not seconds.

**Drain first — recommended.** Suspend the warehouses (`-c warehouses=…:suspended`, or
`aws ecs update-service --desired-count 0`), deploy the mode change, then scale back up. It does not
make the outage shorter — it makes it *unambiguous*. A drained fleet fails with "no workers", which
is one legible state, instead of a mixture of handshake failures and `UNAUTHENTICATED` that an
operator cannot tell apart from a genuinely broken certificate or a bad secret. For a single-warehouse
POC that nobody is querying, rolling in place and accepting the window is fine; say so out loud
beforehand either way. Stop submitting queries for the duration — the coordinator here is a one-shot
`run-task`, so "draining" the client side is simply not starting new ones.

The same applies **in reverse**: `fleet` → `none` has an identical window, with the roles swapped.

**Why a genuinely rolling transition is not on offer.** It would need a mixed-mode window the engine
cannot express: a worker that served both plaintext and TLS on one port, or a coordinator that fell
back per peer. Neither exists, and the second is a *downgrade attack* wearing a convenience hat —
`tls.rs` states that the scheme is the switch and that a TLS server refuses a plaintext client
rather than obliging it. #83 is the related constraint: the engine builds its TLS acceptor once at
startup and never re-reads its material, so there is no certificate-reload path a task could use to
change posture without being replaced — and #83 decided against adding one, for the reasons under
*Rotating* below. So the flip costs a window, and the honest thing to do is schedule it.

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

So the fleet shares **one** certificate carrying `DNS:<the fleet name>`, and every role gets that
same name as `LLDB_TLS_DOMAIN`. The URL's IP connects; that SAN verifies. The name defaults to
`fleet.lldb.local` on both sides and `--domain` / `-c tlsDomain` change it on both sides together.

It is one fleet-wide name rather than one per warehouse because the engine's dialing trust is
process-global — a `FlightReaderExec` is serialized into a plan and can carry nothing per-call —
while one coordinator dials several warehouses, so there is exactly one name available to verify
against. That is why `tlsDomain` is a single stack-wide value and not a per-warehouse map: a map
would synthesize perfectly and then fail every cross-warehouse query. Per-warehouse certificates
become representable only if that trust stops being process-global.

Be clear about the claim: a shared leaf authenticates **the fleet, not the member**. A client learns
that its peer holds the fleet's key, not which worker it is. That is already the documented scope of
this engine's TLS — it is server-authenticated, and per-member identity is `LLDB_FLEET_TOKEN`'s
claim. **#106 decided not to change that**: a shared client certificate would authenticate the fleet
exactly as the secret already does, and per-member certificates buy revocation granularity that the
absence of a reload path cannot deliver — which #83 then decided to keep absent (see *Rotating*).
Revisit only if that changes, and after #127.

### Rotating

The engine reads its TLS material **once, at startup** — no file watch, no reload signal — so every
rotation is a restart. That is a decision and not an oversight; `crates/lldb-qe-core/CLAUDE.md`'s TLS
section argues it under #83. The fleet secret above and a new `imageTag` are restarts for the same
reason, so when more than one is due, do them in **one** restart rather than three.

Every `--secret-id` below spells the **default** prefix, `lldb/fleet-tls`. A stack deployed with
`-c tlsSecretPrefix=team/x` reads `team/x-{ca,cert,key}` instead, so substitute throughout and pass
the matching `--prefix` to every `mint-fleet-tls.sh` run — the script defaults the prefix on each
invocation exactly as it defaults `--domain`, and forgetting it writes a perfectly good certificate
to secrets the fleet does not read.

**A leaf near expiry, same CA — one rolling restart, no query outage.** Every task trusts the same
root before and after, so a half-replaced fleet still handshakes. This is the common case.

```bash
./scripts/mint-fleet-tls.sh                    # …and --domain again, if the fleet uses one
aws ecs update-service --cluster <ClusterName> --service <each WarehouseServices entry> \
  --force-new-deployment
```

Nothing to do for the coordinator — it is a one-shot `run-task`, so the next run reads whatever the
secrets then hold. Check **both** expiries, not just the leaf's: the script mints a leaf for 825 days
and its CA for twice that, so a CA covers roughly two leaf generations and then becomes the deadline
itself — and a leaf signed by an expired root fails every handshake exactly as an expired leaf does.

```bash
for part in ca cert; do
  aws secretsmanager get-secret-value --secret-id "lldb/fleet-tls-$part" \
    --query SecretString --output text | openssl x509 -noout -subject -enddate
done
```

**Re-pass `--domain` when rotating a fleet that uses one.** The script defaults to
`fleet.lldb.local` on every run, so a rotation that forgets it replaces a working certificate with
one the deployed `LLDB_TLS_DOMAIN` does not match — and the fleet fails every handshake as the
forced deployment rolls, without the stack having changed at all.

#### Replacing the CA is three restarts, or one outage — pick deliberately

Doing it in one pass is neither: mid-roll, half the fleet trusts a root the other half does not sign
under, so **every** handshake between the halves fails — worker to worker as much as coordinator to
worker — for the length of the rollout, while looking like an ordinary deployment. Losing
`./fleet-tls-ca` and re-minting against a live fleet is exactly that pass, which is why the script
says so on its way out.

**The cheap way, when a window is acceptable.** Drain, swap, scale back up:

```bash
aws ecs update-service --cluster <ClusterName> --service <each WarehouseServices entry> --desired-count 0
mv ./fleet-tls-ca ./fleet-tls-ca-old            # moved, not deleted: the way back if the deploy fails
./scripts/mint-fleet-tls.sh                     # a brand-new root, leaf and key
aws ecs update-service --cluster <ClusterName> --service <each WarehouseServices entry> --desired-count <size>
```

One legible outage ("no workers") instead of a mixture of handshake failures nobody can tell apart
from a bad certificate — the same argument as the `tls=none` → `tls=fleet` flip above. For a POC
stack nobody is querying, this is the right answer.

**The no-outage way: trust both roots for a window.** `LLDB_TLS_CA_PEM` is a *bundle* — every PEM
block in it becomes a trust anchor — so the old and the new root can both be live while the leaves
move across. `mint-fleet-tls.sh` will not do this for you: it uploads the single CA it just signed
with, so the CA secret is written by hand in passes 1 and 3.

```bash
# Pass 1 — both roots trusted, old leaf still served. This run exists to mint the new root: the
# three secrets it writes under the throwaway prefix are read by nothing. (--domain, in both runs
# below, if the fleet uses one: the rule from the leaf case applies to every mint.)
./scripts/mint-fleet-tls.sh --ca-dir ./fleet-tls-ca-new --prefix lldb/fleet-tls-unused
cat ./fleet-tls-ca/ca.crt ./fleet-tls-ca-new/ca.crt > ./both-roots.pem
aws secretsmanager put-secret-value --secret-id lldb/fleet-tls-ca --secret-string file://./both-roots.pem
aws ecs update-service --cluster <ClusterName> --service <each WarehouseServices entry> --force-new-deployment   # wait for it

# Pass 2 — leaves re-issued under the new root, both roots still trusted. The two commands go back
# to back: the script overwrites the CA secret with the new root alone, and a task that happens to
# start in that gap cannot dial a peer still serving the old leaf.
./scripts/mint-fleet-tls.sh --ca-dir ./fleet-tls-ca-new
aws secretsmanager put-secret-value --secret-id lldb/fleet-tls-ca --secret-string file://./both-roots.pem
aws ecs update-service --cluster <ClusterName> --service <each WarehouseServices entry> --force-new-deployment   # wait for it

# Pass 3 — old root retired.
aws secretsmanager put-secret-value --secret-id lldb/fleet-tls-ca \
  --secret-string file://./fleet-tls-ca-new/ca.crt
aws ecs update-service --cluster <ClusterName> --service <each WarehouseServices entry> --force-new-deployment
```

Each pass is rolling-safe on its own, because at every moment every task trusts the root that signed
every leaf being served. Let each deployment finish before starting the next — passes that overlap
are the one-pass swap by another route. Then clean up: keep `./fleet-tls-ca-new/ca.key` offline,
discard the old CA directory, and delete the three secrets pass 1 left behind — one of them is a
private key, minted only because the script has no CA-only mode, and read by nothing.

```bash
for part in ca cert key; do
  aws secretsmanager delete-secret --secret-id "lldb/fleet-tls-unused-$part" \
    --force-delete-without-recovery
done
```

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
worker URL to `https://` with the `{warehouse}` placeholder intact, every role verifies under the
one fleet name — default *or* `-c tlsDomain`, since the flag must change the value and never the
arity — the default is still the literal `fleet.lldb.local` that `mint-fleet-tls.sh` mints,
a `tlsDomain` that could not be a DNS name (a space, a comma, a wildcard, a port) and a `tlsDomain`
without `-c tls=fleet` both fail synth, and `LLDB_ALLOW_PLAINTEXT` is absent in **both** modes.

For the fleet secret: `-c tls=fleet` puts `LLDB_FLEET_TOKEN` on *every* task definition as a secret
reference and in no `Environment` block anywhere, all of them resolve the same secret, no literal
secret value can reach the template (there is no `SecretString` property — only the
`GenerateSecretString` recipe), the task roles are not granted it, and `-c tls=none` creates and
injects nothing at all. `LLDB_REQUIRE_FLEET_TOKEN` is asserted the opposite way round — present on
every role under `-c tls=fleet` and in `Environment` rather than `Secrets`, because its legibility
is the mechanism — and absent from every role under `-c tls=none` and under the default, which is
what keeps the plaintext mode deployable with no configuration.

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
