#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { LldbStack } from '../lib/lldb-stack';

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

new LldbStack(app, 'LldbStack', {
  imageTag,
  workerCount: workerCountCtx ? Number(workerCountCtx) : undefined,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  description: 'lldb distributed query engine — ECS Fargate worker fleet + S3 Iceberg warehouse',
});
