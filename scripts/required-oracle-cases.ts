// Independently authorized M3.1 historical reproduction, not healthy acceptance.
export const oracleReproductionCases = [
  {
    id: 'OR00',
    name: 'OR00 historical oracle rejects a retained valid document after a rejected update',
    file: 'tests/es/oracle-reproduction.test.ts',
  },
];

export const oracleCases = [
  ['O01', 'rejected update retains exact prior admissible document'],
  ['O02', 'rejected initial revision requires receiver absence'],
  ['O03', 'valid correction advances without rewriting rejected history'],
  ['O04', 'rejected restore retains an admissible tombstone'],
  ['O05', 'undeclared pipeline failure cannot excuse a missing valid effect'],
  ['O06', 'retained older receiver corruption is independently detected'],
].map(([id, label]) => {
  if (!id || !label) throw new Error('Invalid oracle inventory');
  return { id, name: `${id} ${label}`, file: 'tests/es/oracle.test.ts' };
});
export const expectationInventoryCase = {
  id: 'O07',
  name: 'O07 original bulk and reconciliation use explicit workload rejections',
  file: 'tests/es/es.test.ts',
};
