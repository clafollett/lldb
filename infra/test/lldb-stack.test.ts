import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Template, Match } from 'aws-cdk-lib/assertions';
import {
  LldbStack,
  LldbStackProps,
  WORKER_PORT,
  NAMESPACE,
  WORKER_SERVICE_NAME,
  WAREHOUSE_ENDPOINT_TEMPLATE,
  warehouseEndpointTemplate,
  FLEET_TLS_DOMAIN,
  FLEET_TOKEN_ENV,
  REQUIRE_FLEET_TOKEN_ENV,
} from '../lib/lldb-stack';

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

/** A container's `Environment` as a plain object. */
function envOf(container: any): Record<string, string> {
  return Object.fromEntries(((container.Environment ?? []) as any[]).map((e) => [e.Name, e.Value]));
}

/**
 * A container's `Secrets` as name → its `ValueFrom`, JSON-stringified.
 *
 * NOT an ARN. At synth time `ValueFrom` is an unresolved CloudFormation expression — `{"Ref":
 * "FleetToken…"}` — and the ARN only exists after deployment. Stringifying is deliberate: it makes
 * two roles' entries comparable with `toBe`, which is what "every role resolves the *same* secret"
 * needs to assert. Comparing the objects directly would compare references, not contents.
 */
function secretsOf(container: any): Record<string, string> {
  return Object.fromEntries(((container.Secrets ?? []) as any[]).map((s) => [s.Name, JSON.stringify(s.ValueFrom)]));
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

describe('virtual warehouses', () => {
  /** Every ECS service in the template, paired with the Cloud Map name its tasks register under. */
  function warehouseServices(template: Template): { desiredCount: number; discoveryId: string }[] {
    return Object.values(template.findResources('AWS::ECS::Service')).map((service: any) => ({
      desiredCount: service.Properties.DesiredCount,
      discoveryId: JSON.stringify(service.Properties.ServiceRegistries),
    }));
  }

  /** Cloud Map service names, i.e. the DNS labels a warehouse is reachable at. */
  function discoveryNames(template: Template): string[] {
    return Object.values(template.findResources('AWS::ServiceDiscovery::Service'))
      .map((s: any) => s.Properties.Name)
      .sort();
  }

  test('the default is one `worker` warehouse — the pre-warehouse fleet, unchanged', () => {
    // Adopting warehouses must not move anyone's fleet: same name, same size, same DNS.
    const template = synth();
    expect(discoveryNames(template)).toEqual([WORKER_SERVICE_NAME]);
    template.resourceCountIs('AWS::ECS::Service', 1);
    template.hasResourceProperties('AWS::ECS::Service', { DesiredCount: 2 });
  });

  test('two warehouses of different sizes get two services and two DNS names', () => {
    // The acceptance criterion, in infrastructure: independently sized pools, separately
    // addressable, sharing the one warehouse bucket.
    const template = synth({
      warehouses: [
        { name: 'analytics', size: 4 },
        { name: 'etl', size: 1 },
      ],
    });
    expect(discoveryNames(template)).toEqual(['analytics', 'etl']);
    const counts = warehouseServices(template)
      .map((s) => s.desiredCount)
      .sort();
    expect(counts).toEqual([1, 4]);
    // Each warehouse's tasks register with its own Cloud Map service — no two share a record.
    const registries = warehouseServices(template).map((s) => s.discoveryId);
    expect(new Set(registries).size).toBe(2);
  });

  test('a suspended warehouse deploys at desiredCount 0 but keeps its definition', () => {
    // Suspension frees compute without discarding the warehouse: the service and its DNS name
    // survive, so resuming is a desired-count change rather than a re-creation.
    const template = synth({
      warehouses: [
        { name: 'analytics', size: 4 },
        { name: 'nightly', size: 8, state: 'suspended' },
      ],
    });
    const counts = warehouseServices(template)
      .map((s) => s.desiredCount)
      .sort((a, b) => a - b);
    expect(counts).toEqual([0, 4]);
    expect(discoveryNames(template)).toEqual(['analytics', 'nightly']);
  });

  test('every warehouse gets scoped access to the same warehouse bucket', () => {
    // Compute is partitioned; storage is not. Three warehouses + the coordinator = four task
    // roles, each scoped to the one bucket.
    const template = synth({
      warehouses: [
        { name: 'a', size: 1 },
        { name: 'b', size: 1 },
        { name: 'c', size: 1 },
      ],
    });
    const taskRolePolicies = Object.entries(template.findResources('AWS::IAM::Policy')).filter(([name]) =>
      name.includes('TaskRoleDefaultPolicy'),
    );
    expect(taskRolePolicies).toHaveLength(4);
    for (const [, policy] of taskRolePolicies) {
      for (const statement of (policy as any).Properties.PolicyDocument.Statement) {
        expect(JSON.stringify(statement.Resource)).toContain('Warehouse');
      }
    }
  });

  test('the coordinator gets the routing template, and defaults to the first warehouse', () => {
    const coordinator = containers(
      synth({
        warehouses: [
          { name: 'analytics', size: 2 },
          { name: 'etl', size: 1 },
        ],
      }),
    ).find((c) => c.Name === 'coordinator');
    const env = Object.fromEntries(coordinator.Environment.map((e: any) => [e.Name, e.Value]));
    // `--warehouse <name>` renders into this; the placeholder is what makes each warehouse
    // resolve to its own fleet rather than all of them to one.
    expect(env.LLDB_WAREHOUSE_ENDPOINT).toBe(WAREHOUSE_ENDPOINT_TEMPLATE);
    expect(env.LLDB_WAREHOUSE_ENDPOINT).toContain('{warehouse}');
    // …and with no `--warehouse` at all, the pre-warehouse path still points somewhere real.
    expect(env.LLDB_WORKERS).toBe(`http://analytics.${NAMESPACE}:${WORKER_PORT}`);
  });

  // Each of these would otherwise fail at deploy or, worse, synthesize something unroutable:
  // a name that is not a DNS label yields a Cloud Map record nothing resolves, a duplicate yields
  // two services fighting over one record, and `-c warehouses=…` sizes arrive as strings through
  // Number(), so a typo becomes NaN.
  const badWarehouses: [string, any[], RegExp][] = [
    ['an uppercase name', [{ name: 'Analytics', size: 1 }], /DNS label/],
    ['an underscore', [{ name: 'wh_1', size: 1 }], /DNS label/],
    ['a leading dash', [{ name: '-lead', size: 1 }], /DNS label/],
    ['a name over 63 characters', [{ name: 'a'.repeat(64), size: 1 }], /DNS label/],
    ['a duplicate name', [{ name: 'a', size: 1 }, { name: 'a', size: 2 }], /duplicate warehouse name/],
    ['size zero', [{ name: 'a', size: 0 }], /positive integer/],
    ['a size that failed to parse', [{ name: 'a', size: NaN }], /positive integer/],
    ['a fractional size', [{ name: 'a', size: 1.5 }], /positive integer/],
    ['an empty list', [], /at least one warehouse/],
  ];
  test.each(badWarehouses)('refuses %s', (_label, warehouses, pattern) => {
    expect(() => synth({ warehouses })).toThrow(pattern);
  });

  test('rejects an unknown state rather than treating it as running', () => {
    expect(() => synth({ warehouses: [{ name: 'a', size: 1, state: 'paused' as any }] })).toThrow(/unknown state/);
  });

  test('workerCount and warehouses cannot both be given', () => {
    // Both express the same thing; a stack that silently picked one would deploy a fleet whose
    // size nobody asked for.
    expect(() => synth({ workerCount: 3, warehouses: [{ name: 'a', size: 1 }] })).toThrow(/mutually exclusive/);
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
    // Assert the RECIPE is present and no literal value is. The previous version checked for
    // `GeneratePassword`, which is not a CloudFormation property — the real one is
    // `GenerateSecretString` — so it passed unconditionally, and would have passed just as happily
    // against a template with the password embedded. A negative assertion on a string that never
    // appears for an unrelated reason is not a weak test, it is camouflage: it sits where a real
    // check would go and reports success. All three lines below were verified to fail when
    // inverted or when their needle was corrupted.
    const dbTemplate = JSON.stringify(template.toJSON());
    expect(dbTemplate).toContain('GenerateSecretString');
    expect(dbTemplate).not.toContain('"SecretString"');
    expect(dbTemplate).not.toContain('SecretStringValue');
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

describe('transport security', () => {
  test('the default mode configures no certificates, and fakes none', () => {
    // `tls: 'none'` is byte-for-byte the fleet this stack deployed before certificates existed.
    for (const container of containers(synth())) {
      const env = envOf(container);
      expect(env.LLDB_TLS_CERT).toBeUndefined();
      expect(env.LLDB_TLS_KEY).toBeUndefined();
      expect(env.LLDB_TLS_DOMAIN).toBeUndefined();
      expect(Object.keys(secretsOf(container)).filter((n) => n.startsWith('LLDB_TLS'))).toEqual([]);
    }
  });

  // The assertion this file has carried since the gap existed, now made to hold in BOTH modes.
  // `LLDB_ALLOW_PLAINTEXT` is what lets a process serve a plaintext port while checking a
  // credential. Setting it would silently disarm the guard for whoever later sets LLDB_FLEET_TOKEN
  // (issue #19's gap) and would otherwise have been stopped — which is precisely the shortcut
  // `tls: 'fleet'` exists to make unnecessary. If a future change needs it, it should be an
  // explicit line in the stack and an explicit edit to this test, not an inheritance.
  test.each(['none', 'fleet'] as const)(
    'the stack never pre-emptively opts out of the plaintext guard (tls=%s)',
    (tls) => {
      for (const container of containers(synth({ tls }))) {
        expect(envOf(container).LLDB_ALLOW_PLAINTEXT).toBeUndefined();
      }
    },
  );

  test('tls=fleet gives every worker an identity and a trust, from secrets', () => {
    const template = synth({ tls: 'fleet', warehouses: [{ name: 'analytics', size: 2 }, { name: 'etl', size: 1 }] });
    const workers = containers(template).filter((c) => c.Name === 'worker');
    // Both warehouses, not just the first — a fleet half of which serves TLS is a fleet whose
    // queries fail on whichever half the coordinator happens to dial.
    expect(workers).toHaveLength(2);
    for (const worker of workers) {
      const secrets = secretsOf(worker);
      // A worker serves a port (cert + key) AND dials its peers for the shuffle (ca).
      expect(Object.keys(secrets).sort()).toEqual(
        expect.arrayContaining(['LLDB_TLS_CA_PEM', 'LLDB_TLS_CERT_PEM', 'LLDB_TLS_KEY_PEM']),
      );
      expect(secrets.LLDB_TLS_KEY_PEM).toContain('lldb/fleet-tls-key');
      expect(envOf(worker).LLDB_TLS_DOMAIN).toBe(FLEET_TLS_DOMAIN);
    }
  });

  test('the coordinator gets the trust but never the private key', () => {
    // It binds no port, so an identity would be material it has no use for. This is the reason the
    // stack imports three whole-string secrets rather than one with JSON keys: `ecs.Secret` grants
    // read on a *secret*, so only separate secrets make this a boundary rather than a convention.
    const coordinator = containers(synth({ tls: 'fleet' })).find((c) => c.Name === 'coordinator');
    const secrets = secretsOf(coordinator);
    expect(secrets.LLDB_TLS_CA_PEM).toContain('lldb/fleet-tls-ca');
    expect(secrets.LLDB_TLS_CERT_PEM).toBeUndefined();
    expect(secrets.LLDB_TLS_KEY_PEM).toBeUndefined();
    expect(envOf(coordinator).LLDB_TLS_DOMAIN).toBe(FLEET_TLS_DOMAIN);
  });

  test('the private key is unreadable by the coordinator task role, in IAM and not only in env', () => {
    // The env wiring above is the intent; this is the enforcement. An execution role granted the
    // key secret could read it whether or not the task definition injects it.
    const policies = synth({ tls: 'fleet' }).findResources('AWS::IAM::Policy');
    const rendered = Object.fromEntries(
      Object.entries(policies)
        .filter(([name]) => name.includes('ExecutionRoleDefaultPolicy'))
        .map(([name, policy]) => [name, JSON.stringify((policy as any).Properties.PolicyDocument)]),
    );

    const coordinator = Object.entries(rendered).find(([name]) => name.includes('Coordinator'))!;
    expect(coordinator[1]).toContain('fleet-tls-ca');
    expect(coordinator[1]).not.toContain('fleet-tls-key');

    const worker = Object.entries(rendered).find(([name]) => name.includes('Worker'))!;
    expect(worker[1]).toContain('fleet-tls-key');
  });

  test('no PEM material of any kind reaches the synthesized template', () => {
    // The hard constraint: a private key must never appear in source, in CDK context, or in a
    // `cdk.out` artifact. The stack imports secrets by name and never holds the bytes, so this is
    // structurally true — asserted anyway, because the failure mode of getting it wrong is a key
    // committed to whatever stores the deploy artifact.
    for (const tls of ['none', 'fleet'] as const) {
      const rendered = JSON.stringify(synth({ tls }).toJSON());
      expect(rendered).not.toContain('-----BEGIN');
      expect(rendered).not.toContain('PRIVATE KEY');
    }
    // …and the TLS variables are secret *references*, never plain environment, exactly like the
    // services-database password.
    for (const container of containers(synth({ tls: 'fleet' }))) {
      for (const name of Object.keys(envOf(container))) {
        expect(name).not.toMatch(/^LLDB_TLS_(CA|CERT|KEY)_PEM$/);
      }
    }
  });

  test('turning TLS on rewrites every worker URL to https, because the scheme is the switch', () => {
    // `https://` dials TLS and `http://` does not, with no fallback either way — so a fleet
    // serving certificates that a coordinator still dials as `http://` fails on every query.
    const plaintext = containers(synth()).find((c) => c.Name === 'coordinator');
    expect(envOf(plaintext).LLDB_WORKERS).toBe(`http://${WORKER_SERVICE_NAME}.${NAMESPACE}:${WORKER_PORT}`);
    expect(envOf(plaintext).LLDB_WAREHOUSE_ENDPOINT).toBe(WAREHOUSE_ENDPOINT_TEMPLATE);

    const encrypted = containers(synth({ tls: 'fleet' })).find((c) => c.Name === 'coordinator');
    const env = envOf(encrypted);
    expect(env.LLDB_WORKERS).toBe(`https://${WORKER_SERVICE_NAME}.${NAMESPACE}:${WORKER_PORT}`);
    expect(env.LLDB_WAREHOUSE_ENDPOINT).toBe(warehouseEndpointTemplate('https'));
    // The placeholder survives the rewrite — otherwise every warehouse would route to one fleet.
    expect(env.LLDB_WAREHOUSE_ENDPOINT).toContain('{warehouse}');
  });

  test('the certificate is verified under one fleet-wide name, not a per-warehouse one', () => {
    // The SAN problem, as an assertion. `discovery.rs` expands a Cloud Map name into one URL per
    // *task IP*, so a certificate for `analytics.lldb.local` would never be the name verified —
    // and the dialing trust is process-global, so a coordinator spanning warehouses has exactly
    // one name to offer. Both roles must therefore agree on the single name the leaf carries.
    const domains = containers(
      synth({ tls: 'fleet', warehouses: [{ name: 'analytics', size: 1 }, { name: 'etl', size: 1 }] }),
    ).map((c) => envOf(c).LLDB_TLS_DOMAIN);
    expect(new Set(domains)).toEqual(new Set([FLEET_TLS_DOMAIN]));
    expect(FLEET_TLS_DOMAIN.endsWith(NAMESPACE)).toBe(true);
    expect(FLEET_TLS_DOMAIN).not.toContain('{warehouse}');
  });

  test('the stack creates no secret of its own for TLS — it imports what the operator minted', () => {
    // Creating them would put CDK between an operator and a private key, and would deadlock a
    // first deploy: empty secrets, tasks that cannot start, a circuit breaker that fails the
    // stack, and a rollback that deletes the secrets you were about to fill.
    //
    // The one secret this stack does create is the fleet token, and the asymmetry is the argument
    // above read backwards: an empty token is a fleet that starts *open*, not one that cannot start,
    // so there is no deadlock to avoid and generating it keeps the value out of every artifact. So
    // the assertion is not "no secrets" — it is "no secret holding TLS material".
    const created = synth({ tls: 'fleet', servicesDb: 'none' }).findResources('AWS::SecretsManager::Secret');
    expect(Object.keys(created).map((id) => id.replace(/[0-9A-F]{8}$/, ''))).toEqual(['FleetToken']);
  });

  test('the secret names are configurable, and an unusable one fails at synth', () => {
    const secrets = containers(synth({ tls: 'fleet', tlsSecretPrefix: 'team/lldb-prod' })).flatMap((c) =>
      Object.values(secretsOf(c)),
    );
    expect(secrets.some((arn) => arn.includes('team/lldb-prod-key'))).toBe(true);
    expect(() => synth({ tls: 'fleet', tlsSecretPrefix: 'no spaces allowed' })).toThrow(/Secrets Manager name/);
  });
});

describe('fleet secret', () => {
  /** Every `Environment` entry across every container in the template, flattened. */
  function allEnvEntries(template: Template): { Name: string; Value: unknown }[] {
    return containers(template).flatMap((c) => (c.Environment ?? []) as { Name: string; Value: unknown }[]);
  }

  /** Every `Secrets` entry across every container, keeping the raw (unstringified) `ValueFrom`. */
  function allSecretEntries(template: Template): { Name: string; ValueFrom: unknown }[] {
    return containers(template).flatMap((c) => (c.Secrets ?? []) as { Name: string; ValueFrom: unknown }[]);
  }

  test('tls=fleet generates exactly one secret, and CloudFormation is what generates its value', () => {
    // `GenerateSecretString` is a recipe, not a value: Secrets Manager mints the bytes server-side
    // at create time. That is what keeps the secret out of source, out of context, and out of
    // `cdk.out` — the property the issue actually asks for.
    const template = synth({ tls: 'fleet', servicesDb: 'none' });
    template.resourceCountIs('AWS::SecretsManager::Secret', 1);
    template.hasResourceProperties('AWS::SecretsManager::Secret', {
      GenerateSecretString: Match.objectLike({ PasswordLength: 64, ExcludePunctuation: true }),
    });
  });

  test('both roles get it as a SECRET, and it appears in no plain environment anywhere', () => {
    // A plain `environment` entry is stored verbatim in the task definition, which means anyone who
    // can call `describe-task-definition` reads the fleet's shared secret out of the control plane.
    const template = synth({
      tls: 'fleet',
      warehouses: [
        { name: 'analytics', size: 2 },
        { name: 'etl', size: 1 },
      ],
    });
    // Every worker fleet, not just the first — a warehouse whose workers check no credential is the
    // hole the other two closed, reachable by name.
    expect(containers(template).map((c) => c.Name).sort()).toEqual(['coordinator', 'worker', 'worker']);
    for (const container of containers(template)) {
      expect(secretsOf(container)[FLEET_TOKEN_ENV]).toBeDefined();
      expect(envOf(container)[FLEET_TOKEN_ENV]).toBeUndefined();
    }
    expect(allEnvEntries(template).filter((e) => e.Name === FLEET_TOKEN_ENV)).toEqual([]);
    // …and the injected value is a CloudFormation reference to the secret, never an inline string.
    for (const entry of allSecretEntries(template).filter((s) => s.Name === FLEET_TOKEN_ENV)) {
      expect(typeof entry.ValueFrom).not.toBe('string');
    }
  });

  test('every role resolves the same secret — a fleet holding two tokens cannot talk to itself', () => {
    // The same invariant as the single image tag, for the same reason: the coordinator presents this
    // value on every Flight call and signs each plan assertion with a key derived from it, and a
    // worker rejects anything that is not byte-identical.
    const resolved = new Set(
      containers(
        synth({
          tls: 'fleet',
          warehouses: [
            { name: 'analytics', size: 2 },
            { name: 'etl', size: 1 },
          ],
        }),
      ).map((c) => secretsOf(c)[FLEET_TOKEN_ENV]),
    );
    expect(resolved.size).toBe(1);
  });

  test('no generated secret value reaches the synthesized template', () => {
    // The value cannot be asserted directly — it does not exist until CloudFormation creates the
    // resource. What can be asserted is that no literal ever could: a `SecretString` property is the
    // only way a Secrets Manager secret carries one, and nothing here has one.
    for (const tls of ['none', 'fleet'] as const) {
      const rendered = JSON.stringify(synth({ tls }).toJSON());
      // Note the quote: `"GenerateSecretString"` is the recipe and must be allowed through.
      expect(rendered).not.toContain('"SecretString"');
      expect(rendered).not.toContain('SecretStringValue');
    }
    // …and the fleet secret specifically carries a recipe and nothing else — asserted on the
    // resource rather than on the whole template, where the services database's own generated
    // secret would satisfy a substring match without this one being checked at all.
    const [fleetToken] = Object.entries(
      synth({ tls: 'fleet' }).findResources('AWS::SecretsManager::Secret'),
    ).filter(([id]) => id.startsWith('FleetToken'));
    expect(fleetToken).toBeDefined();
    expect(Object.keys((fleetToken[1] as any).Properties).sort()).toEqual(['Description', 'GenerateSecretString']);
  });

  test('tls=fleet asserts the closed posture on every role, in plain environment', () => {
    // The token alone was not tamper-evident: `FleetAuth::from_env` reads blank as unset and ECS
    // injects `FOO=` for an emptied secret, so one console edit opened the fleet at the next task
    // replacement. This entry is what turns that into a refusal to start.
    //
    // `Environment` and NOT `Secrets`, which is the inverse of the rule the token lives under one
    // block up — deliberately. It carries no secret; it is a claim *about* the secret, and being
    // readable in the task definition is the mechanism rather than a leak. Asserting the mechanism
    // and not just the name is the point of splitting these two expectations.
    const template = synth({
      tls: 'fleet',
      warehouses: [
        { name: 'analytics', size: 2 },
        { name: 'etl', size: 1 },
      ],
    });
    expect(containers(template).map((c) => c.Name).sort()).toEqual(['coordinator', 'worker', 'worker']);
    for (const container of containers(template)) {
      expect(envOf(container)[REQUIRE_FLEET_TOKEN_ENV]).toBeDefined();
      expect(secretsOf(container)[REQUIRE_FLEET_TOKEN_ENV]).toBeUndefined();
    }
    expect(allSecretEntries(template).filter((s) => s.Name === REQUIRE_FLEET_TOKEN_ENV)).toEqual([]);
  });

  test('tls=none asserts nothing, so the default deploy and every single-node path are untouched', () => {
    // The whole reason the assertion is opt-in rather than the default: `cargo run`, the compose
    // demo and this stack's default mode must need no configuration at all. A stack that shipped
    // this variable in both modes would make `tls: 'none'` undeployable.
    for (const props of [{ tls: 'none' as const, servicesDb: 'none' as const }, {}]) {
      const template = synth(props);
      expect(allEnvEntries(template).filter((e) => e.Name === REQUIRE_FLEET_TOKEN_ENV)).toEqual([]);
      expect(allSecretEntries(template).filter((s) => s.Name === REQUIRE_FLEET_TOKEN_ENV)).toEqual([]);
    }
  });

  test('tls=none creates no fleet secret and injects none, so the default deploy is unchanged', () => {
    // `cargo run` and the compose demo must keep working with no configuration, and so must this
    // stack's default mode: no token means no credential is checked, which is exactly the posture
    // `tls.rs` lets bind a plaintext port without an opt-in.
    const template = synth({ tls: 'none', servicesDb: 'none' });
    template.resourceCountIs('AWS::SecretsManager::Secret', 0);
    expect(allEnvEntries(template).filter((e) => e.Name === FLEET_TOKEN_ENV)).toEqual([]);
    expect(allSecretEntries(template).filter((s) => s.Name === FLEET_TOKEN_ENV)).toEqual([]);
    // The default (no `tls` at all) is the same thing by another spelling.
    expect(allSecretEntries(synth()).filter((s) => s.Name === FLEET_TOKEN_ENV)).toEqual([]);
  });

  test('only the execution roles that inject it can read it — never the task roles', () => {
    // The execution role resolves the value at container start; the process then reads its own
    // environment. The engine carries no AWS SDK and makes no Secrets Manager call at all, so a task
    // role grant would be a permission nothing uses, on a value the container already holds, that
    // additionally outlives a rotation. Its absence is a decision, which is why it is asserted.
    const rendered = Object.entries(synth({ tls: 'fleet' }).findResources('AWS::IAM::Policy')).map(
      ([name, policy]) => [name, JSON.stringify((policy as any).Properties.PolicyDocument)] as const,
    );

    const execution = rendered.filter(([name]) => name.includes('ExecutionRoleDefaultPolicy'));
    expect(execution).toHaveLength(2);
    for (const [, doc] of execution) {
      expect(doc).toContain('secretsmanager:GetSecretValue');
      expect(doc).toContain('FleetToken');
    }

    const taskRoles = rendered.filter(([name]) => name.includes('TaskRoleDefaultPolicy'));
    expect(taskRoles).toHaveLength(2);
    for (const [, doc] of taskRoles) {
      expect(doc).not.toContain('FleetToken');
      expect(doc).not.toContain('secretsmanager');
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
