// Independent M4.1 inventory authorized before reproduction or production correction.
export const batchReproductionCases = [
  {
    id: 'B01',
    name: 'B01 historical SQL and production consumer reject an admitted wire batch',
    file: 'tests/rabbitmq/batch-repro.test.ts',
  },
];
export const batchCases = [
  ['B02', 'exact source-derived wire boundary commits and acknowledges'],
  ['B03', 'one byte above partitions without lost messages'],
  ['B04', 'SQL and application retain count and byte bounds'],
  ['B05', 'representations and original duplicate bytes agree'],
  ['B06', 'boundary COMMIT before ACK SIGKILL redelivers once'],
  ['B06H', 'healthy boundary release acknowledges exactly once'],
  ['B07', 'populated forward migration preserves data and privileges'],
  ['B08', 'same consumer continues from boundary to small work'],
].map(([id, label]) => {
  if (!id || !label) throw new Error('Invalid byte-boundary inventory');
  return { id, name: `${id} ${label}`, file: 'tests/rabbitmq/batch.test.ts' };
});
