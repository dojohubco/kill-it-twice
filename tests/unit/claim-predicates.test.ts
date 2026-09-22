import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
function definition(sql: string, name: string): string {
  const start = sql.indexOf(
    `CREATE OR REPLACE FUNCTION pipeline.${name}_claim(`,
  );
  assert.ok(start >= 0);
  const end = sql.indexOf('END $$;', start);
  assert.ok(end > start);
  return sql.slice(start, end + 'END $$;'.length);
}
void test('due-selector migration preserves all claim semantics except a redundant eligible-state predicate', async () => {
  const next = await readFile(
    'migrations/pipeline/015-explicit-due-predicates.sql',
    'utf8',
  );
  for (const [name, file, kind] of [
    ['es', '008-recovery-controls.sql', 'elasticsearch'],
    ['rabbit', '005-rabbit-probe.sql', 'rabbitmq'],
  ] as const) {
    const old = definition(
      await readFile(`migrations/pipeline/${file}`, 'utf8'),
      name,
    );
    const predicate = `i.destination_id=target AND i.kind='${kind}' AND\n`;
    assert.equal(old.split(predicate).length, 2);
    assert.equal(
      definition(next, name),
      old.replace(
        predicate,
        `i.destination_id=target AND i.kind='${kind}' AND i.state IN ('pending','leased','retry_wait') AND\n`,
      ),
    );
  }
  assert.match(next, /REVOKE ALL ON FUNCTION pipeline\.es_claim/);
  assert.match(next, /SET LOCAL ROLE pipeline_owner/);
});
