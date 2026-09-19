// Authorized independent M4 inventory, committed before implementation/execution.
export const rabbitCases = [
  ['MQ01', 'real TLS topology registration and restricted permissions'],
  ['MQ02', 'integrated source both sinks and transactional consumer'],
  ['MQ03', 'publisher SIGKILL boundaries preserve one consumer effect'],
  ['MQ04', 'real downstream confirm loss remains unresolved'],
  ['MQ05', 'mandatory return overflow and flow control'],
  ['MQ06', 'consumer SIGKILL transaction and ACK boundaries'],
  ['MQ07', 'one publication survives twenty five consumer crashes'],
  ['MQ08', 'ordered effects concurrent batches and synthetic baseline'],
  ['MQ09', 'stale publisher ownership and original channel ACK fencing'],
  ['MQ10', 'independent broker database and Elasticsearch outages'],
  ['MQ11', 'paused consumption and consumer first receipt evidence'],
  ['MQ12', 'durable poison quarantine and crash recovery'],
  ['MQ13', 'fair explicit receipt observations reject mismatched identity'],
  ['MQ14', 'independent finite reconciliation and negative controls'],
  ['MQ15', 'populated upgrade and progressed restaging preserve evidence'],
  ['MQ16', 'retained restart bounded loops and scoped cleanup'],
  ['MQH01', 'healthy publisher release after claim'],
  ['MQH02', 'healthy publisher release after confirm'],
  ['MQH03', 'healthy publisher release after local settlement'],
  ['MQH04', 'healthy consumer release before database commit'],
  ['MQH05', 'healthy consumer release after database commit'],
  ['MQH06', 'healthy consumer release after quarantine commit'],
].map(([id, label]) => {
  if (!id || !label) throw new Error('Invalid RabbitMQ inventory');
  return { id, name: `${id} ${label}`, file: 'tests/rabbitmq/rabbit.test.ts' };
});
