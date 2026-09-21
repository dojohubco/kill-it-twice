"""Independent bounded-export oracle. No production normalizer or state query is imported."""
import argparse
import datetime
import decimal
import hashlib
import json
from pathlib import Path
import sqlite3

COLLECTIONS = ('baselines', 'source', 'mutations', 'commands', 'work', 'pipeline', 'consumer', 'totals', 'projection', 'receiver')

def parse(text):
    return json.loads(text, parse_float=decimal.Decimal)

def utc(text):
    return datetime.datetime.fromisoformat(text.replace('Z', '+00:00')).astimezone(datetime.timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')

def event_id(row):
    return f"{row['source_epoch']}:{row['entity_id']}:{row['entity_version']}"

def seed_payload(ordinal):
    return dict(name=f'Seed local-runtime-v1 #{ordinal}', country='GE' if ordinal % 2 == 0 else 'FR', loyalty_points=ordinal % 1000,
        seed_ordinal=str(ordinal), tags=['ქართული', 'café', ordinal % 7, None], optional=None if ordinal % 3 == 0 else 'value',
        exact=9007199254740993 + ordinal, decimal=decimal.Decimal('0.123456789012345678901234567890'), padding=chr(97 + ordinal % 26) * 768)

def body(row):
    value = dict(schema_version=1, source_epoch=row['source_epoch'], entity_id=row['entity_id'], entity_version=row['entity_version'],
        event_id=event_id(row), source_change_id=row['change_id'], source_recorded_at=utc(row['recorded_at']),
        kind='baseline' if row['change_id'] is None else 'mutation', is_deleted=row['is_deleted'],
        payload_encoding='pg18-jsonb-text/v1', payload_json=row['payload_json'])
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))

def inspect(directory, expected_count, journal, database):
    connection = sqlite3.connect(database)
    connection.execute('PRAGMA cache_size=-4096')
    connection.execute('PRAGMA temp_store=FILE')
    connection.execute('CREATE TABLE records (collection TEXT,key TEXT,value TEXT,PRIMARY KEY(collection,key))')
    for name in COLLECTIONS:
        with (directory / f'{name}.jsonl').open() as stream:
            for line in stream:
                if not line.strip(): continue
                assert len(line.encode('utf8')) <= 524288, (name, 'oversized export row')
                row = parse(line)
                key = row['document_id'] if name == 'receiver' else row['event_id'] if name in ('pipeline', 'consumer') else row['command_id'] if name == 'commands' else row['cursor']
                assert isinstance(key, str), (name, 'missing identity')
                connection.execute('INSERT INTO records VALUES(?,?,?)', (name, key, line))
    connection.execute('CREATE TABLE expected (entity TEXT PRIMARY KEY,event TEXT NOT NULL)')
    connection.execute('CREATE TABLE history (event TEXT PRIMARY KEY,entity TEXT,version INTEGER,kind TEXT,body TEXT,hash TEXT)')
    connection.execute('CREATE TABLE requests (command TEXT PRIMARY KEY,request TEXT)')
    def count(name): return connection.execute('SELECT count(*) FROM records WHERE collection=?', (name,)).fetchone()[0]
    def get(name, key):
        found = connection.execute('SELECT value FROM records WHERE collection=? AND key=?', (name, key)).fetchone()
        assert found, (name, key, 'missing')
        return parse(found[0])
    def each(name):
        for item in connection.execute('SELECT value FROM records WHERE collection=? ORDER BY key', (name,)): yield parse(item[0])
    assert count('baselines') == expected_count
    epoch = None
    ordinals = set()
    for row in each('baselines'):
        payload = parse(row['payload_json'])
        ordinal = int(payload['seed_ordinal'])
        assert 1 <= ordinal <= expected_count and ordinal not in ordinals
        ordinals.add(ordinal)
        assert payload == seed_payload(ordinal), ('seed recipe', ordinal)
        assert row['entity_version'] == '1' and row['change_id'] is None and not row['is_deleted']
        epoch = epoch or row['source_epoch']
        assert epoch == row['source_epoch']
        encoded = body(row)
        connection.execute('INSERT INTO history VALUES(?,?,?,?,?,?)', (event_id(row), row['entity_id'], 1, 'baseline', encoded, hashlib.sha256(encoded.encode()).hexdigest()))
        connection.execute('INSERT INTO expected VALUES(?,?)', (row['entity_id'], event_id(row)))
    mutation_by_id = {}
    for row in each('mutations'):
        assert row['change_id'] is not None and row['source_epoch'] == epoch
        key = event_id(row)
        assert key not in mutation_by_id
        mutation_by_id[key] = row
    requests = []
    for line in journal.read_text().splitlines():
        request = parse(line)
        prior = connection.execute('SELECT request FROM requests WHERE command=?', (request['command_id'],)).fetchone()
        if prior:
            assert parse(prior[0]) == request, 'Conflicting journal key'
            continue
        connection.execute('INSERT INTO requests VALUES(?,?)', (request['command_id'], line))
        requests.append(request)
    assert count('commands') == len(requests)
    expected_mutations = set()
    for request in requests:
        receipt = get('commands', request['command_id'])
        assert receipt['completed'] and receipt['source_epoch'] == epoch
        assert receipt['operation'] == request['operation'] and receipt['target_id'] == request['entity_id']
        wanted = parse(request['payload_json']) if request['payload_json'] is not None else None
        actual_request = parse(receipt['request_payload']) if receipt['request_payload'] is not None else None
        assert wanted == actual_request
        entity = receipt['result_entity_id']
        previous = connection.execute('SELECT h.body FROM expected x JOIN history h ON h.event=x.event WHERE x.entity=?', (entity,)).fetchone()
        if request['operation'] == 'create':
            assert previous is None and request['entity_id'] is None
            version, changed = 1, True
        else:
            assert previous and entity == request['entity_id']
            old = parse(previous[0])
            old_payload = parse(old['payload_json']) if old['payload_json'] is not None else None
            deleted = request['operation'] == 'delete'
            changed = deleted != old['is_deleted'] or wanted != old_payload
            version = int(old['entity_version']) + (1 if changed else 0)
        assert receipt['result_version'] == str(version)
        assert receipt['result_deleted'] == (request['operation'] == 'delete')
        assert (parse(receipt['result_payload']) if receipt['result_payload'] is not None else None) == wanted
        key = f'{epoch}:{entity}:{version}'
        if changed:
            assert key in mutation_by_id and key not in expected_mutations
            revision = mutation_by_id[key]
            assert revision['change_id'] == receipt['result_change_id']
            assert utc(revision['recorded_at']) == utc(receipt['result_recorded_at'])
            assert (parse(revision['payload_json']) if revision['payload_json'] is not None else None) == wanted
            assert revision['is_deleted'] == receipt['result_deleted']
            encoded = body(revision)
            connection.execute('INSERT INTO history VALUES(?,?,?,?,?,?)', (key, entity, version, 'mutation', encoded, hashlib.sha256(encoded.encode()).hexdigest()))
            expected_mutations.add(key)
        else:
            assert previous and parse(previous[0])['source_change_id'] == receipt['result_change_id']
        connection.execute('INSERT INTO expected VALUES(?,?) ON CONFLICT(entity) DO UPDATE SET event=excluded.event', (entity, key))
    assert set(mutation_by_id) == expected_mutations, 'Missing/extra committed source mutations'
    total = connection.execute('SELECT count(*) FROM history').fetchone()[0]
    entities = connection.execute('SELECT count(*) FROM expected').fetchone()[0]
    assert count('pipeline') == count('consumer') == total
    assert count('source') == count('receiver') == count('projection') == count('totals') == entities
    assert count('work') == len(expected_mutations)
    work = {event_id(row): row for row in each('work')}
    for key, entity, version, kind, encoded, digest in connection.execute('SELECT * FROM history'):
        event, inbox = get('pipeline', key), get('consumer', key)
        assert event['body'] == inbox['body'] == encoded
        assert event['content_sha256'] == inbox['content_sha256'] == digest
        assert event['es_state'] == event['rabbit_state'] == 'satisfied' and event['consumer_state'] == 'processed'
        assert inbox['has_effect'] == (kind == 'mutation')
        if kind == 'mutation':
            ack = work[key]
            assert ack['state'] == 'acknowledged' and ack['acknowledged_hash'] == digest
            assert ack['generation'] == ack['acknowledged_generation'] and int(ack['generation']) > 0
        current = connection.execute('SELECT event FROM expected WHERE entity=?', (entity,)).fetchone()[0]
        assert inbox['is_current'] == (key == current)
        effects = connection.execute("SELECT count(*) FROM history WHERE entity=? AND kind='mutation'", (entity,)).fetchone()[0]
        assert int(inbox['units']) == effects
    for entity, key in connection.execute('SELECT * FROM expected'):
        h = connection.execute('SELECT body,hash FROM history WHERE event=?', (key,)).fetchone()
        value = parse(h[0])
        current = get('source', entity)
        assert body(current) == h[0]
        projected = get('projection', entity)
        assert projected['event_id'] == key and projected['entity_version'] == value['entity_version']
        expected_units = connection.execute("SELECT count(*) FROM history WHERE entity=? AND kind='mutation'", (entity,)).fetchone()[0]
        assert int(get('totals', entity)['units']) == expected_units
        remote = get('receiver', f'{epoch}:{entity}')
        assert str(remote['receiver_version']) == value['entity_version']
        payload = parse(value['payload_json']) if value['payload_json'] is not None else {}
        fields = {k: payload[k] for k in ('name', 'country', 'loyalty_points') if k in payload}
        expected_document = dict(projection_schema='search-v1', source_epoch=epoch, entity_id=entity,
            entity_version=value['entity_version'], is_deleted=value['is_deleted'],
            canonical_body_json=h[0], content_sha256=h[1], search_fields=fields)
        assert remote['source'] == expected_document, ('receiver content', entity)
    connection.commit()
    connection.close()
    return dict(status='PASS', scope='Independent bounded runtime fixture; not final gates or capacity',
        source_epoch=epoch, baselines=expected_count, entities=entities, historical_events=total,
        mutation_effects=len(expected_mutations), oracle='verifier-owned commands and seed recipe, exact revisions/bytes/relations/values')

if __name__ == '__main__':
    arguments = argparse.ArgumentParser()
    arguments.add_argument('directory', type=Path)
    arguments.add_argument('--count', required=True, type=int)
    arguments.add_argument('--journal', required=True, type=Path)
    args = arguments.parse_args()
    assert 1 <= args.count <= 4096, 'This bounded fixture is not a scale benchmark'
    print(json.dumps(inspect(args.directory, args.count, args.journal, args.directory / 'oracle.sqlite'), indent=2))
