import { spawn } from 'node:child_process';
// A cooperative fixture deliberately stays in the command() owned process group.
const grandchild = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: ['ignore', 'inherit', 'inherit'] });
process.stdout.write(JSON.stringify({ child: process.pid, grandchild: grandchild.pid }) + '\n');
setInterval(() => undefined, 1_000);
