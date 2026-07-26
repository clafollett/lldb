import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { LldbStack, LldbStackProps, WORKER_PORT, NAMESPACE, WORKER_SERVICE_NAME } from '../lib/lldb-stack';

const IMAGE_TAG = '0.1.0+abcdef123456';

/**
 * The real `nat-instance` default looks the fck-nat AMI up against the live EC2 API, which
 * needs credentials. Tests inject a fixed image so synth stays hermetic.
 */
const STUB_AMI = ec2.MachineImage.genericLinux({ 'us-east-1': 'ami-0abcdef1234567890' });

function synth(props: Partial<LldbStackProps> = {}): Template {
  const app = new cdk.App();
  const stack = new LldbStack(app, 'TestStack', {
    imageTag: IMAGE_TAG,
    env: { account: '123456789012', region: 'us-east-1' },
    ...props,
  });
  return Template.fromStack(stack);
}

/**
 * Assert no security group anywhere admits the public internet. Checks BOTH shapes CDK emits:
 * standalone `AWS::EC2::SecurityGroupIngress` resources and rules written inline into a
 * security group's `SecurityGroupIngress` property.
 */
function expectNoWorldOpenIngress(template: Template): void {
  const isWorldOpen = (rule: any) => rule?.CidrIp === '0.0.0.0/0' || rule?.CidrIpv6 === '::/0';

  const standalone = Object.values(template.findResources('AWS::EC2::SecurityGroupIngress')).filter((r: any) =>
    isWorldOpen(r.Properties),
  );
  const inline = Object.entries(template.findResources('AWS::EC2::SecurityGroup')).flatMap(([name, sg]: [string, any]) =>
    ((sg.Properties.SecurityGroupIngress ?? []) as any[]).filter(isWorldOpen).map((rule) => ({ name, rule })),
  );

  expect({ standalone, inline }).toEqual({ standalone: [], inline: [] });
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
    synth({ workerCount: 5 }).hasResourceProperties('AWS::ECS::Service', { DesiredCount: 5 });
    synth().hasResourceProperties('AWS::ECS::Service', { DesiredCount: 2 });
  });

  // `-c workerCount=…` is a string run through Number(), so a typo yields NaN. Catch it at
  // synth rather than shipping a service with a nonsense DesiredCount.
  test.each([[NaN, 'a typo'], [0, 'zero'], [-1, 'negative'], [1.5, 'fractional']])(
    'rejects workerCount=%s (%s)',
    (workerCount) => {
      expect(() => synth({ workerCount })).toThrow(/positive integer/);
    },
  );
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
    expectNoWorldOpenIngress(template);
  });

  // Regression: CDK's NAT instance defaults to INBOUND_AND_OUTBOUND, which puts an
  // internet-open security group in the VPC. The original version of this check only looked at
  // standalone AWS::EC2::SecurityGroupIngress resources and sailed straight past it, because
  // the VPC construct writes that rule *inline* on the security group instead.
  test.each(['none', 'nat-instance', 'nat-gateway'] as const)(
    'nothing is open to 0.0.0.0/0 with egress=%s',
    (egress) => {
      expectNoWorldOpenIngress(synth({ egress, natMachineImage: STUB_AMI }));
    },
  );

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

describe('egress modes', () => {
  test('default is the $0 path: public subnets, public IPs, no NAT of any kind', () => {
    const template = synth();
    template.resourceCountIs('AWS::EC2::NatGateway', 0);
    template.resourceCountIs('AWS::EC2::Instance', 0);
    template.hasResourceProperties('AWS::ECS::Service', {
      NetworkConfiguration: Match.objectLike({
        AwsvpcConfiguration: Match.objectLike({ AssignPublicIp: 'ENABLED' }),
      }),
    });
  });

  test('nat-instance puts tasks in private subnets behind one small instance', () => {
    const template = synth({ egress: 'nat-instance', natMachineImage: STUB_AMI });
    // A NAT *instance*, not a billed-by-the-hour managed gateway.
    template.resourceCountIs('AWS::EC2::NatGateway', 0);
    template.resourceCountIs('AWS::EC2::Instance', 1);
    template.hasResourceProperties('AWS::EC2::Instance', { InstanceType: 't4g.nano' });
    // The data plane loses its public IPs — the entire reason to pay for a NAT.
    template.hasResourceProperties('AWS::ECS::Service', {
      NetworkConfiguration: Match.objectLike({
        AwsvpcConfiguration: Match.objectLike({ AssignPublicIp: 'DISABLED' }),
      }),
    });
  });

  test('nat-gateway uses the managed service instead', () => {
    const template = synth({ egress: 'nat-gateway' });
    template.resourceCountIs('AWS::EC2::NatGateway', 1);
    template.resourceCountIs('AWS::EC2::Instance', 0);
    template.hasResourceProperties('AWS::ECS::Service', {
      NetworkConfiguration: Match.objectLike({
        AwsvpcConfiguration: Match.objectLike({ AssignPublicIp: 'DISABLED' }),
      }),
    });
  });

  test('one NAT serves both AZs rather than one per AZ', () => {
    // Two private subnets, a single NAT — the POC tradeoff, made explicit so a change is visible.
    const template = synth({ egress: 'nat-gateway' });
    const privateSubnets = Object.values(template.findResources('AWS::EC2::Subnet')).filter((s: any) =>
      JSON.stringify(s.Properties.Tags ?? []).includes('Private'),
    );
    expect(privateSubnets).toHaveLength(2);
    template.resourceCountIs('AWS::EC2::NatGateway', 1);
  });
});

describe('services database', () => {
  test('the default provisions an Aurora Serverless v2 Postgres cluster with a generated secret', () => {
    const template = synth();
    template.hasResourceProperties('AWS::RDS::DBCluster', {
      Engine: 'aurora-postgresql',
      EngineVersion: '18.4',
      DatabaseName: 'lldb',
      StorageEncrypted: true,
      ServerlessV2ScalingConfiguration: Match.objectLike({ MinCapacity: 0.5, MaxCapacity: 4 }),
    });
    // Serverless v2 writer, not a provisioned instance class.
    template.hasResourceProperties('AWS::RDS::DBInstance', { DBInstanceClass: 'db.serverless' });
    // The password is generated into Secrets Manager, never written into the template.
    template.resourceCountIs('AWS::SecretsManager::Secret', 1);
    expect(JSON.stringify(template.toJSON())).not.toContain('GeneratePassword');
  });

  test('Postgres is reachable from the task security groups only — never the world', () => {
    const template = synth();
    const dbIngress = Object.values(template.findResources('AWS::EC2::SecurityGroupIngress')).filter(
      (r: any) => r.Properties.FromPort === 5432,
    );
    // One rule per role: worker and coordinator.
    expect(dbIngress).toHaveLength(2);
    for (const rule of dbIngress as any[]) {
      expect(rule.Properties.SourceSecurityGroupId).toBeDefined();
      expect(rule.Properties.CidrIp).toBeUndefined();
      expect(JSON.stringify(rule.Properties)).not.toContain('0.0.0.0/0');
    }
    expectNoWorldOpenIngress(template);
  });

  test('the cluster sits in isolated subnets with no route out', () => {
    const template = synth();
    // A DB subnet group exists, and none of the routes for those subnets reach a gateway.
    template.resourceCountIs('AWS::RDS::DBSubnetGroup', 1);
    const isolated = Object.values(template.findResources('AWS::EC2::Subnet')).filter((s: any) =>
      JSON.stringify(s.Properties.Tags ?? []).includes('Isolated'),
    );
    expect(isolated).toHaveLength(2);
  });

  test('both roles get the connection env, with the password as a secret reference', () => {
    for (const container of containers(synth())) {
      const env = Object.fromEntries(container.Environment.map((e: any) => [e.Name, e.Value]));
      expect(env.LLDB_METADATA_DATABASE).toBe('lldb');
      expect(env.LLDB_METADATA_USER).toBe('lldb');
      expect(env.LLDB_METADATA_SSLMODE).toBe('require');
      expect(env.LLDB_METADATA_HOST).toBeDefined();
      expect(env.LLDB_METADATA_PORT).toBeDefined();
      // The password must NOT be plain environment…
      expect(env.LLDB_METADATA_PASSWORD).toBeUndefined();
      // …it must be an ECS secret resolved from Secrets Manager at task start.
      const secret = container.Secrets.find((s: any) => s.Name === 'LLDB_METADATA_PASSWORD');
      expect(secret).toBeDefined();
      expect(JSON.stringify(secret.ValueFrom)).toContain(':password::');
    }
  });

  test('servicesDb=none provisions nothing and leaves the tasks unconfigured', () => {
    const template = synth({ servicesDb: 'none' });
    template.resourceCountIs('AWS::RDS::DBCluster', 0);
    template.resourceCountIs('AWS::RDS::DBInstance', 0);
    template.resourceCountIs('AWS::RDS::DBSubnetGroup', 0);
    template.resourceCountIs('AWS::SecretsManager::Secret', 0);
    for (const container of containers(template)) {
      const metadata = (container.Environment as any[]).filter((e) => e.Name.startsWith('LLDB_METADATA'));
      expect(metadata).toEqual([]);
      expect(container.Secrets ?? []).toEqual([]);
    }
  });
});

describe('cost posture', () => {
  test('warehouse traffic always rides the free S3 gateway endpoint', () => {
    // True in every egress mode: the per-GB-heavy traffic must never cross a NAT.
    for (const egress of ['none', 'nat-instance', 'nat-gateway'] as const) {
      const template = synth({ egress, natMachineImage: STUB_AMI });
      template.hasResourceProperties('AWS::EC2::VPCEndpoint', {
        VpcEndpointType: 'Gateway',
        ServiceName: Match.anyValue(),
      });
    }
  });

  test('no PrivateLink interface endpoints (they would cost more than the NAT)', () => {
    const endpoints = Object.values(synth({ egress: 'nat-instance', natMachineImage: STUB_AMI }).findResources('AWS::EC2::VPCEndpoint'));
    expect(endpoints.every((e: any) => e.Properties.VpcEndpointType === 'Gateway')).toBe(true);
  });

  test('logs are retained, not kept forever', () => {
    synth().hasResourceProperties('AWS::Logs::LogGroup', { RetentionInDays: 7 });
  });
});
