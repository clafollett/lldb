import * as cdk from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { LldbStack, LldbStackProps, WORKER_PORT, NAMESPACE, WORKER_SERVICE_NAME } from '../lib/lldb-stack';

const IMAGE_TAG = '0.1.0+abcdef123456';

function synth(props: Partial<LldbStackProps> = {}): Template {
  const app = new cdk.App();
  const stack = new LldbStack(app, 'TestStack', {
    imageTag: IMAGE_TAG,
    env: { account: '123456789012', region: 'us-east-1' },
    ...props,
  });
  return Template.fromStack(stack);
}

/** All container definitions across every task definition in the template. */
function containers(template: Template): any[] {
  return Object.values(template.findResources('AWS::ECS::TaskDefinition')).flatMap(
    (res: any) => res.Properties.ContainerDefinitions,
  );
}

describe('fleet build consistency', () => {
  // The load-bearing invariant: serialized DataFusion physical plans are not cross-version
  // compatible, so the coordinator and every worker MUST run the identical image.
  test('coordinator and worker resolve the exact same image', () => {
    const images = containers(synth()).map((c) => JSON.stringify(c.Image));
    expect(images).toHaveLength(2);
    expect(new Set(images).size).toBe(1);
  });

  test('the image is pinned to the requested tag, never `latest`', () => {
    const rendered = JSON.stringify(containers(synth()).map((c) => c.Image));
    expect(rendered).toContain(IMAGE_TAG);
    expect(rendered).not.toContain(':latest');
  });

  test.each([['', 'empty'], ['latest', 'a moving tag']])(
    'refuses to synth with %s (%s)',
    (tag) => {
      // An unpinned fleet must fail at deploy, not deep in plan deserialization at runtime.
      expect(() => synth({ imageTag: tag })).toThrow(/exact build tag/);
    },
  );
});

describe('worker fleet', () => {
  test('runs the worker binary bound on all interfaces', () => {
    const worker = containers(synth()).find((c) => c.Name === 'worker');
    expect(worker.Command).toEqual(['lldb-qe-worker', '--bind', `0.0.0.0:${WORKER_PORT}`]);
    expect(worker.PortMappings[0].ContainerPort).toBe(WORKER_PORT);
  });

  test('is discoverable at worker.lldb.local', () => {
    const template = synth();
    template.hasResourceProperties('AWS::ServiceDiscovery::Service', {
      Name: WORKER_SERVICE_NAME,
      DnsConfig: Match.objectLike({ DnsRecords: [Match.objectLike({ Type: 'A' })] }),
    });
    template.hasResourceProperties('AWS::ServiceDiscovery::PrivateDnsNamespace', { Name: NAMESPACE });
  });

  test('the coordinator is pointed at that DNS name', () => {
    const coordinator = containers(synth()).find((c) => c.Name === 'coordinator');
    const workers = coordinator.Environment.find((e: any) => e.Name === 'LLDB_WORKERS');
    expect(workers.Value).toBe(`http://${WORKER_SERVICE_NAME}.${NAMESPACE}:${WORKER_PORT}`);
  });

  test('fleet size is configurable', () => {
    synth({ workerCount: 5 } as any).hasResourceProperties('AWS::ECS::Service', { DesiredCount: 5 });
    synth().hasResourceProperties('AWS::ECS::Service', { DesiredCount: 2 });
  });
});

describe('security', () => {
  test('Flight port is reachable from the coordinator only — never the world', () => {
    const template = synth();
    // The ingress rule on 50051 must come from a security group, not a CIDR.
    template.hasResourceProperties('AWS::EC2::SecurityGroupIngress', {
      FromPort: WORKER_PORT,
      ToPort: WORKER_PORT,
      SourceSecurityGroupId: Match.anyValue(),
    });
    const openToWorld = Object.values(template.findResources('AWS::EC2::SecurityGroupIngress')).filter(
      (r: any) => r.Properties.CidrIp === '0.0.0.0/0',
    );
    expect(openToWorld).toHaveLength(0);
  });

  test('the warehouse bucket is private and TLS-only', () => {
    const template = synth();
    template.hasResourceProperties('AWS::S3::Bucket', {
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
    });
    template.hasResourceProperties('AWS::S3::BucketPolicy', {
      PolicyDocument: Match.objectLike({
        Statement: Match.arrayWith([
          Match.objectLike({ Effect: 'Deny', Condition: { Bool: { 'aws:SecureTransport': 'false' } } }),
        ]),
      }),
    });
  });

  test('both roles get warehouse access, scoped to the warehouse', () => {
    const template = synth();
    const policies = template.findResources('AWS::IAM::Policy');

    // The *task* roles are the app's data-plane identity: worker + coordinator, each scoped to
    // the warehouse bucket. (Execution roles are separate and legitimately need
    // `ecr:GetAuthorizationToken` on `*` — AWS allows no resource scoping on that call.)
    const taskRolePolicies = Object.entries(policies).filter(([name]) => name.includes('TaskRoleDefaultPolicy'));
    expect(taskRolePolicies).toHaveLength(2);

    for (const [name, policy] of taskRolePolicies) {
      const statements = (policy as any).Properties.PolicyDocument.Statement;
      for (const statement of statements) {
        expect(statement.Action.join(' ')).toContain('s3:');
        // Every resource must reference the warehouse bucket — no wildcard data access.
        expect(JSON.stringify(statement.Resource)).toContain('Warehouse');
        expect(statement.Resource).not.toBe('*');
      }
      expect(name).toMatch(/Worker|Coordinator/);
    }
  });
});

describe('cost posture', () => {
  test('no NAT gateways; S3 egress rides a free gateway endpoint', () => {
    const template = synth();
    template.resourceCountIs('AWS::EC2::NatGateway', 0);
    template.hasResourceProperties('AWS::EC2::VPCEndpoint', {
      VpcEndpointType: 'Gateway',
      ServiceName: Match.anyValue(),
    });
  });

  test('logs are retained, not kept forever', () => {
    synth().hasResourceProperties('AWS::Logs::LogGroup', { RetentionInDays: 7 });
  });
});
