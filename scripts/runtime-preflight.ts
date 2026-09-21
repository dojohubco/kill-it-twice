import { observePrerequisite, requirePrerequisite } from './es-prerequisite.ts';
const observation = await observePrerequisite('retained-local-runtime');
console.log(JSON.stringify(observation));
requirePrerequisite(observation);
