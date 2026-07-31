import * as fs from 'fs';
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { LldbStack, LldbStackProps, FLEET_TOKEN_ENV, REQUIRE_FLEET_TOKEN_ENV } from '../lib/lldb-stack';

/**
 * The deploy-side half of `scripts/check-fleet-posture.sh` (#126).
 *
 * That script guards the **code**: every binary this workspace builds either calls
 * `check_fleet_posture_from_env()` or says in its own source why it does not. It cannot guard the
 * **deploy**, and those are different failures. The deploy failure is *right code, wrong container*:
 * `lib/lldb-stack.ts` injects `LLDB_REQUIRE_FLEET_TOKEN` into task definitions, and a container that
 * receives it while running an opted-out binary — or a future one running something new — deploys
 * clean and asserts nothing at all. Which is the silence #116 was filed about, one layer out.
 *
 * **The set of checking binaries is read, never restated.** Hard-coding
 * `['lldb-qe-coordinator', 'lldb-qe-server', 'lldb-qe-worker']` here is precisely the staleness
 * #125 designed away: a fourth checking binary would leave the list wrong and the failure would
 * again be silence. So the list is `../fleet-posture.json`, which
 * `scripts/check-fleet-posture.sh --json` derives from `cargo metadata` and whose freshness that
 * same script's normal run enforces in CI's `Fleet posture` step — the one job that already has
 * cargo and jq. Nothing here shells out to either: the `cdk synth • test` job has node and nothing
 * else, and a test written to skip when the tools are missing reinvents the same silence.
 *
 * The consequence to keep: a container running a binary **absent from the roster** fails, rather
 * than being skipped as unknown. That is what makes a new binary break the build by default.
 */

const IMAGE_TAG = '0.1.0+abcdef123456';

/** The classification `scripts/check-fleet-posture.sh --json` writes. */
type Posture = 'checks' | 'opted-out';

interface RosterEntry {
  readonly name: string;
  readonly package: string;
  readonly source: string;
  readonly posture: Posture;
  readonly reason: string | null;
}

interface Roster {
  readonly _generated: string;
  readonly generator: string;
  readonly binaries: RosterEntry[];
}

const ROSTER_PATH = path.join(__dirname, '..', 'fleet-posture.json');

/**
 * Read the roster, failing loudly rather than degrading. A missing or unparseable file is a broken
 * guard, and a broken guard that reports success is worse than no guard: every assertion below
 * would pass vacuously.
 */
function loadRoster(): Roster {
  let raw: string;
  try {
    raw = fs.readFileSync(ROSTER_PATH, 'utf8');
  } catch (cause) {
    throw new Error(
      `cannot read ${ROSTER_PATH} (${String(cause)}). It is generated: run ` +
        '`./scripts/check-fleet-posture.sh --json > infra/fleet-posture.json` from the repo root.',
    );
  }
  const roster = JSON.parse(raw) as Roster;
  if (!Array.isArray(roster.binaries) || roster.binaries.length === 0) {
    throw new Error(`${ROSTER_PATH} carries no binaries — regenerate it with check-fleet-posture.sh --json`);
  }
  return roster;
}

const roster = loadRoster();
const postureOf = new Map<string, RosterEntry>(roster.binaries.map((entry) => [entry.name, entry]));

/**
 * `synth`, and the three template accessors below, are deliberately re-declared here rather than
 * imported from `lldb-stack.test.ts`: jest loads each test file in its own module registry, so
 * importing one test file from another re-registers its whole suite inside this one.
 */
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

/** The names a container receives in plain `Environment` — presence, not value. */
function envNames(container: any): Set<string> {
  return new Set(((container.Environment ?? []) as any[]).map((e) => e.Name));
}

/** The names a container receives as ECS `Secrets`, resolved from Secrets Manager at task start. */
function secretNames(container: any): Set<string> {
  return new Set(((container.Secrets ?? []) as any[]).map((s) => s.Name));
}

type Classified = { readonly ok: true; readonly binary: string } | { readonly ok: false; readonly why: string };

/**
 * Anything in `argv[0]` that means it is a shell fragment, an argument, or an unresolved
 * CloudFormation expression rather than a program to exec. ECS `Command` is exec-form — argv, not a
 * shell line — so a space alone already means the shape is not one this can classify.
 */
const NOT_AN_EXECUTABLE = /[\s;&|<>$`"'\\()]/;

/**
 * Map a container's `Command` to the binary it runs.
 *
 * ECS runs `Command` as argv, so `argv[0]` **is** the program — the mapping is a basename, and the
 * work here is refusing every shape where that is not true. Each refusal is a FAILURE at the call
 * site, never a skip: an unclassifiable container is one this guard cannot vouch for, and a guard
 * that passes what it cannot read is the defect it exists to catch.
 *
 * The shapes refused, and why each is unreadable rather than merely unusual:
 *
 * - **No `Command`.** The image's `CMD` decides (the Dockerfile defaults to `lldb-qe-worker`), and
 *   the template does not say. Deploy-time truth that synth cannot see is exactly what this cannot
 *   vouch for.
 * - **`argv[0]` is not a string.** A `{"Ref": …}` or `Fn::Join` resolves at deploy time; at synth
 *   there is no name to look up.
 * - **A shell wrapper** (`['sh', '-c', 'lldb-qe-worker …']`). `argv[0]` is `sh`, and what it
 *   eventually execs is a string this has no business parsing.
 *
 * Note what is NOT special-cased: a wrapper this does not know about (`tini`, an entrypoint script)
 * classifies fine as its own basename and then fails the roster lookup, because it is not a binary
 * this workspace builds. Fail-closed falls out of the roster rather than needing a list here.
 */
function binaryFromCommand(command: unknown): Classified {
  if (command === undefined || command === null) {
    return {
      ok: false,
      why: 'it declares no Command, so which binary it runs is the image CMD — invisible to synth',
    };
  }
  if (!Array.isArray(command) || command.length === 0) {
    return { ok: false, why: `its Command is ${JSON.stringify(command)}, which names no program` };
  }
  const head: unknown = command[0];
  if (typeof head !== 'string') {
    return {
      ok: false,
      why: `its Command[0] is ${JSON.stringify(head)} — an unresolved CloudFormation expression, not a name`,
    };
  }
  if (head === '' || NOT_AN_EXECUTABLE.test(head) || head.startsWith('-') || head.endsWith('/')) {
    return { ok: false, why: `its Command[0] ${JSON.stringify(head)} is not a plain executable path` };
  }
  const binary = head.slice(head.lastIndexOf('/') + 1);
  if (binary === '' || binary === '.' || binary === '..') {
    return { ok: false, why: `its Command[0] ${JSON.stringify(head)} has no basename to look up` };
  }
  return { ok: true, binary };
}

/**
 * Every complaint about the containers that assert the closed posture, plus how many were audited.
 *
 * The count is returned because "no complaints" is also what an audit of *zero* containers produces,
 * and a guard that silently stops looking is the failure mode this whole file is about.
 */
function auditPosture(all: any[]): { problems: string[]; audited: string[] } {
  const problems: string[] = [];
  const audited: string[] = [];

  for (const container of all) {
    const env = envNames(container);
    // Presence is the assertion and the value is never read — `REQUIRE_FLEET_TOKEN_ENV`'s own rule,
    // so this must test membership rather than truthiness.
    if (!env.has(REQUIRE_FLEET_TOKEN_ENV)) continue;
    const name = String(container.Name);
    audited.push(name);

    // (a) It must supply the secret it claims to require. Independent of the roster and cheaper:
    // this catches the incoherent deployment — asserting a closed fleet while handing over no
    // credential — without knowing anything about which binaries check anything.
    if (!secretNames(container).has(FLEET_TOKEN_ENV) && !env.has(FLEET_TOKEN_ENV)) {
      problems.push(
        `container '${name}' sets ${REQUIRE_FLEET_TOKEN_ENV} but receives no ${FLEET_TOKEN_ENV}: ` +
          'it asserts a closed fleet and supplies no secret, so it refuses to start',
      );
    }

    // (b) …and it must run a binary that evaluates that assertion.
    const classified = binaryFromCommand(container.Command);
    if (!classified.ok) {
      problems.push(
        `container '${name}' sets ${REQUIRE_FLEET_TOKEN_ENV}, but ${classified.why} — ` +
          'so nothing here can tell whether the binary it runs checks the posture',
      );
      continue;
    }
    const entry = postureOf.get(classified.binary);
    if (!entry) {
      problems.push(
        `container '${name}' sets ${REQUIRE_FLEET_TOKEN_ENV} and runs '${classified.binary}', which is ` +
          'in no fleet-posture.json entry. Either the roster is stale (regenerate: ' +
          '`./scripts/check-fleet-posture.sh --json > infra/fleet-posture.json`) or this container runs ' +
          'something this workspace does not build',
      );
      continue;
    }
    if (entry.posture !== 'checks') {
      problems.push(
        `container '${name}' sets ${REQUIRE_FLEET_TOKEN_ENV} but runs '${classified.binary}', which ` +
          `${entry.source} opts out of the posture check: ${entry.reason}. The assertion is not ` +
          'evaluated on that process',
      );
    }
  }

  return { problems, audited };
}

describe('the derived roster', () => {
  test('says it is generated, and every entry is a usable classification', () => {
    // A hand-edited roster is the one way this guard degrades quietly, so the file carries its own
    // provenance and this asserts the file says so. The *freshness* of the derivation is not checked
    // here and cannot be: it is `./scripts/check-fleet-posture.sh`'s normal run, in CI's `Fleet
    // posture` step, which has the cargo this job does not.
    expect(roster._generated).toMatch(/GENERATED/);
    expect(roster._generated).toContain('scripts/check-fleet-posture.sh --json');
    expect(roster.generator).toBe('scripts/check-fleet-posture.sh --json');

    for (const entry of roster.binaries) {
      expect(typeof entry.name).toBe('string');
      expect(entry.name.length).toBeGreaterThan(0);
      expect(['checks', 'opted-out']).toContain(entry.posture);
      // An opt-out with no reason is a list of names, and a list of names is what a reviewer skims
      // past — the script refuses to emit one, and this refuses to read one.
      if (entry.posture === 'opted-out') {
        expect(typeof entry.reason).toBe('string');
        expect((entry.reason ?? '').length).toBeGreaterThan(0);
      } else {
        expect(entry.reason).toBeNull();
      }
    }
    expect(new Set(roster.binaries.map((b) => b.name)).size).toBe(roster.binaries.length);
  });

  test('classifies binaries both ways, so neither verdict is unreachable', () => {
    // A roster where nothing checks would make every assertion below fail for the wrong reason; one
    // where nothing opts out would make the opted-out branch dead code that nobody notices rotting.
    expect(roster.binaries.filter((b) => b.posture === 'checks').length).toBeGreaterThan(0);
    expect(roster.binaries.filter((b) => b.posture === 'opted-out').length).toBeGreaterThan(0);
  });
});

describe('every container asserting the closed posture runs a binary that checks it', () => {
  test('tls=fleet, across every warehouse and the coordinator', () => {
    const template = synth({
      tls: 'fleet',
      warehouses: [
        { name: 'analytics', size: 2 },
        { name: 'etl', size: 1 },
      ],
    });
    const { problems, audited } = auditPosture(containers(template));
    expect(problems).toEqual([]);
    // The coverage half: every container this stack deploys receives the assertion in fleet mode, so
    // an audit that found fewer means the guard stopped looking at something rather than that the
    // stack got safer.
    expect(audited.sort()).toEqual(['coordinator', 'worker', 'worker']);
  });

  test('tls=fleet with the default single warehouse', () => {
    const { problems, audited } = auditPosture(containers(synth({ tls: 'fleet' })));
    expect(problems).toEqual([]);
    expect(audited.sort()).toEqual(['coordinator', 'worker']);
  });

  test('the default mode asserts nothing, so there is nothing to audit', () => {
    // Not a hole: `tls: 'none'` sets no assertion, checks no credential and creates no secret. The
    // audit is empty because the property is vacuous here, and saying so keeps the count assertions
    // above from reading as a mode-independent claim.
    for (const props of [{}, { tls: 'none' as const }]) {
      expect(auditPosture(containers(synth(props)))).toEqual({ problems: [], audited: [] });
    }
  });
});

describe('the guard fails closed', () => {
  /** A synthetic container: the audit's input shape, without a stack that would refuse to build it. */
  function container(command: unknown, env: string[] = [REQUIRE_FLEET_TOKEN_ENV]): any {
    return {
      Name: 'probe',
      Command: command,
      Environment: env.map((name) => ({ Name: name, Value: '1' })),
      Secrets: [{ Name: FLEET_TOKEN_ENV, ValueFrom: { Ref: 'FleetToken' } }],
    };
  }

  test('a binary absent from the roster FAILS — it is not skipped as unknown', () => {
    // THE point of the whole file. A new binary must break the build by default; anything that
    // treats "I have never heard of this" as "probably fine" is the staleness this design removed.
    const { problems, audited } = auditPosture([container(['lldb-qe-something-new'])]);
    expect(audited).toEqual(['probe']);
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('lldb-qe-something-new');
    expect(problems[0]).toContain('in no fleet-posture.json entry');
  });

  test('a binary the roster says opts out FAILS', () => {
    // Read out of the roster rather than named, for the same reason the checking set is: a literal
    // here would be a second hand-maintained list, and it would go stale in the same way.
    const optedOut = roster.binaries.find((b) => b.posture === 'opted-out')!;
    const { problems } = auditPosture([container([optedOut.name])]);
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain(optedOut.name);
    expect(problems[0]).toContain('opts out of the posture check');
  });

  test('a binary the roster says checks PASSES, so the guard is not simply always red', () => {
    const checks = roster.binaries.find((b) => b.posture === 'checks')!;
    expect(auditPosture([container([checks.name])]).problems).toEqual([]);
    // …and an absolute path is the same binary: ECS argv[0] may or may not be resolved.
    expect(auditPosture([container([`/usr/local/bin/${checks.name}`])]).problems).toEqual([]);
  });

  // Every command shape the mapping cannot read must be a failure rather than a pass. A container
  // whose binary is unknowable at synth is one this guard cannot vouch for, and the alternative —
  // letting it through — is the exact silence being closed.
  const unreadable: [string, unknown][] = [
    ['no Command at all (the image CMD decides)', undefined],
    ['a null Command', null],
    ['an empty Command', []],
    ['a Command that is not a list', 'lldb-qe-worker'],
    ['an unresolved CloudFormation expression', [{ Ref: 'SomeParameter' }]],
    ['a shell wrapper', ['sh', '-c', 'lldb-qe-worker --bind 0.0.0.0:50051']],
    ['a shell line smuggled into argv[0]', ['lldb-qe-worker; lldb-qe-migrate']],
    ['a bare flag', ['--bind']],
    ['a directory', ['/usr/local/bin/']],
  ];
  test.each(unreadable)('a container with %s FAILS rather than passing', (_label, command) => {
    const { problems } = auditPosture([container(command)]);
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain(REQUIRE_FLEET_TOKEN_ENV);
  });

  test('asserting the closed posture while supplying no secret FAILS', () => {
    // The independent, cheaper property (#126's option 3): it needs no roster and catches a
    // different deployment — one that claims a closed fleet and hands over no credential, which is a
    // process that refuses to start. Asserted on a container that is otherwise perfectly fine, so a
    // regression here cannot hide behind the binary check.
    const checks = roster.binaries.find((b) => b.posture === 'checks')!;
    const withoutSecret = { ...container([checks.name]), Secrets: [] };
    const { problems } = auditPosture([withoutSecret]);
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain(`receives no ${FLEET_TOKEN_ENV}`);
  });
});
