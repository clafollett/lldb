#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { LldbStack, EgressMode, ServicesDbMode, TlsMode, WarehouseDefinition } from '../lib/lldb-stack';

const app = new cdk.App();

// The image tag is REQUIRED and deliberately has no "latest" default: the whole fleet must run
// one identical, pinned build (serialized DataFusion plans are not cross-version compatible),
// and `latest` is a moving target that silently breaks that.
//
//   cdk deploy -c imageTag=0.1.0+8c6d8d6b57d8
const imageTag = app.node.tryGetContext('imageTag') ?? process.env.LLDB_IMAGE_TAG;
if (!imageTag) {
  throw new Error(
    'imageTag is required — deploy an exact build, e.g. `cdk deploy -c imageTag=0.1.0+<git-sha>` ' +
      '(or set LLDB_IMAGE_TAG). Every binary reports its tag via `--version`.',
  );
}

const workerCountCtx = app.node.tryGetContext('workerCount');

// `-c warehouses=analytics:4,etl:1:suspended` deploys one ECS service per virtual warehouse,
// each registered under its own Cloud Map name, each sized independently. Omitted, the stack
// deploys the single `worker` warehouse sized by `workerCount` — the pre-warehouse fleet, byte
// for byte, so adopting warehouses breaks nothing already deployed.
//
// This is the *infrastructure* half. The services database holds the same facts as rows, and the
// engine reads those rows to route a query; nothing in the engine calls the ECS API, so keeping
// the two in step is a deploy step (or one `aws ecs update-service`), not magic.
const warehousesCtx = app.node.tryGetContext('warehouses') as string | undefined;
const warehouses = warehousesCtx ? parseWarehouses(warehousesCtx) : undefined;

/**
 * Parse `name:size[:state],…`. Sizes go through `Number()`, so anything unparseable becomes NaN
 * here and is rejected by the stack's own validation with a message naming the warehouse — one
 * place that check lives, rather than two that can disagree.
 */
function parseWarehouses(raw: string): WarehouseDefinition[] {
  return raw
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
    .map((entry) => {
      const [name, size, state] = entry.split(':');
      if (size === undefined) {
        throw new Error(`warehouse '${entry}' is missing a size — use name:size[:running|suspended]`);
      }
      return {
        name,
        size: Number(size),
        ...(state ? { state: state as WarehouseDefinition['state'] } : {}),
      };
    });
}

// `-c egress=nat-instance` moves tasks into private subnets behind a ~$3/mo fck-nat box;
// `nat-gateway` uses the managed (~$33/mo) service. Default `none` keeps the fleet in public
// subnets at $0 — safe because inbound is default-deny, just not defense in depth.
const egress = app.node.tryGetContext('egress') as EgressMode | undefined;
if (egress && !['none', 'nat-instance', 'nat-gateway'].includes(egress)) {
  throw new Error(`unknown egress mode '${egress}' (expected none | nat-instance | nat-gateway)`);
}

// `-c servicesDb=none` deploys a query-only fleet with no control plane — no Aurora cluster, no
// LLDB_METADATA_* on the tasks. Default `aurora` provisions the shared services database that
// accounts, the SQL catalog, warehouses and query history all live in.
const servicesDb = app.node.tryGetContext('servicesDb') as ServicesDbMode | undefined;
if (servicesDb && !['aurora', 'none'].includes(servicesDb)) {
  throw new Error(`unknown servicesDb mode '${servicesDb}' (expected aurora | none)`);
}

// `-c tls=fleet` encrypts Flight inside the VPC, reading the fleet's PEM from three Secrets
// Manager secrets that `scripts/mint-fleet-tls.sh` fills. Run that script FIRST: the stack imports
// the secrets rather than creating them, deliberately, so that the private key is never something
// CDK holds and never something `cdk.out` could carry. Default `none` is today's plaintext fleet.
//
// This flag also decides whether worker Flight ports are *authenticated*: `fleet` generates the
// shared fleet secret and injects it into both roles, `none` creates none. One switch, because a
// worker checking a credential refuses to bind a plaintext port — so the insecure pairing is not
// something you can ask this stack for.
const tls = app.node.tryGetContext('tls') as TlsMode | undefined;
if (tls && !['none', 'fleet'].includes(tls)) {
  throw new Error(`unknown tls mode '${tls}' (expected none | fleet)`);
}
const tlsSecretPrefix = app.node.tryGetContext('tlsSecretPrefix') as string | undefined;

new LldbStack(app, 'LldbStack', {
  imageTag,
  egress,
  servicesDb,
  tls,
  tlsSecretPrefix,
  workerCount: workerCountCtx ? Number(workerCountCtx) : undefined,
  warehouses,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  description: 'lldb distributed query engine — ECS Fargate worker fleet + S3 Iceberg warehouse',
});
