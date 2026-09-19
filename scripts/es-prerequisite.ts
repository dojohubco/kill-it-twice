// Read-only prerequisite; injected observations are for unit callers, never CLI/env overrides.
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
const requiredMaxMapCount = '1048576';
export interface PrerequisiteObservation {
  required: string;
  observed: string | null;
  platform: string;
  phase: string;
  observedAt: string;
  status: 'PASS' | 'FAIL';
  reason: string | null;
  assumption: string;
}
export async function observePrerequisite(
  phase: string,
  read: () => Promise<string> = () =>
    readFile('/proc/sys/vm/max_map_count', 'utf8'),
  platform = process.platform,
): Promise<PrerequisiteObservation> {
  const result: PrerequisiteObservation = {
    required: requiredMaxMapCount,
    observed: null,
    platform,
    phase,
    observedAt: new Date().toISOString(),
    status: 'FAIL',
    reason: null,
    assumption:
      'Local Linux kernel shared with the Docker daemon. Local /proc does not configure or prove a remote Docker host.',
  };
  try {
    const raw = (await read()).trim();
    if (!/^[0-9]{1,20}$/.test(raw)) throw new Error('Invalid kernel value');
    result.observed = BigInt(raw).toString();
    if (platform !== 'linux')
      result.reason = 'A local Linux Docker host is required';
    else if (BigInt(raw) < BigInt(requiredMaxMapCount))
      result.reason = 'vm.max_map_count is below the project prerequisite';
    else result.status = 'PASS';
  } catch {
    result.reason = 'Unable to observe a valid local vm.max_map_count';
  }
  return result;
}
export function requirePrerequisite(
  observation: PrerequisiteObservation,
): void {
  if (
    observation.status !== 'PASS' ||
    observation.platform !== 'linux' ||
    observation.observed === null ||
    !/^[0-9]{1,20}$/.test(observation.observed) ||
    BigInt(observation.observed) < BigInt(requiredMaxMapCount)
  )
    throw new Error(
      `Elasticsearch prerequisite failed: required=${observation.required} observed=${observation.observed ?? 'unknown'} platform=${observation.platform} phase=${observation.phase}. ${observation.reason ?? ''}. See README local operator remediation; this runner changes no host setting.`,
    );
}
export async function retainPrerequisite(
  observation: PrerequisiteObservation,
  directory?: string,
): Promise<string> {
  const destination =
    directory ??
    `artifacts/m3.1/prerequisite-${Date.now()}-${randomUUID()}/public`;
  await mkdir(destination, { recursive: true });
  const path = join(destination, 'prerequisite.json');
  await writeFile(
    path,
    JSON.stringify(
      {
        ...observation,
        acceptanceStatus: 'NOT RUN at prerequisite observation',
      },
      null,
      2,
    ) + '\n',
  );
  return path;
}
