#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { LldbStack, EgressMode } from '../lib/lldb-stack';

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

// `-c egress=nat-instance` moves tasks into private subnets behind a ~$3/mo fck-nat box;
// `nat-gateway` uses the managed (~$33/mo) service. Default `none` keeps the fleet in public
// subnets at $0 — safe because inbound is default-deny, just not defense in depth.
const egress = app.node.tryGetContext('egress') as EgressMode | undefined;
if (egress && !['none', 'nat-instance', 'nat-gateway'].includes(egress)) {
  throw new Error(`unknown egress mode '${egress}' (expected none | nat-instance | nat-gateway)`);
}

new LldbStack(app, 'LldbStack', {
  imageTag,
  egress,
  workerCount: workerCountCtx ? Number(workerCountCtx) : undefined,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  description: 'lldb distributed query engine — ECS Fargate worker fleet + S3 Iceberg warehouse',
});
