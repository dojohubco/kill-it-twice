import { access } from 'node:fs/promises';
import { constants } from 'node:fs';
/** Use an explicitly provisioned browser; never install system dependencies during verification. */
export async function uiBrowserExecutable(): Promise<string> {
  const configured = process.env['UI_CHROMIUM_PATH'];
  const paths = configured
    ? [configured]
    : [
        '/usr/bin/chromium-browser',
        '/usr/bin/chromium',
        '/usr/bin/google-chrome',
        '/usr/bin/google-chrome-stable',
      ];
  for (const path of paths) {
    try {
      await access(path, constants.X_OK);
      return path;
    } catch {
      /* Try the next known local executable. */
    }
  }
  throw new Error(
    'A local Chromium/Chrome executable is required for UI verification. Set UI_CHROMIUM_PATH to an already installed executable. No browser/system install is performed by this command.',
  );
}
