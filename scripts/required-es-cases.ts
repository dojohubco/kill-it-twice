// Independent inventory derived from the committed M3 design, never discovery.
export const esCases = [
  ['ES01', 'setup codec and exact external versions'],
  ['ES02', 'full source capture Elasticsearch slice'],
  ['ES03', 'ordering tombstones and restore'],
  ['ES04', 'duplicate and forged conflict evidence'],
  ['ES05', 'real 500 item bulk with three mapper failures'],
  ['ES06', 'restaging preserves every Elasticsearch state'],
  ['ES07', 'real process deaths and healthy boundary controls'],
  ['ES08', 'real downstream response loss remains unresolved'],
  ['ES09', 'expired overlapping claims fence old results'],
  ['ES10', 'measured sixty second outage and recovery'],
  ['ES11', 'identity auth and restricted permissions'],
  ['ES12', 'limits and malformed response rejection'],
  ['ES13', 'populated upgrade and other sink independence'],
  ['ES14', 'independent reconciliation negative controls and retained restart'],
].map(([id, label]) => {
  if (!id || !label) throw new Error('Invalid ES inventory');
  return { id, name: `${id} ${label}`, file: 'tests/es/es.test.ts' };
});
