import { Injectable, inject, signal } from '@angular/core';
import { Api, ApiFailure } from './api';
import { record, type Data } from './value';
@Injectable({ providedIn: 'root' })
export class Workspace {
  readonly api = inject(Api);
  readonly snapshot = signal<Data | null>(null);
  readonly configuration = signal<Data | null>(null);
  readonly configurationFresh = signal(false);
  readonly loading = signal(false);
  readonly error = signal<ApiFailure | null>(null);
  readonly receivedAt = signal<string | null>(null);
  readonly automatic = signal(true);
  readonly rates = signal<Record<string, string | null>>({});
  #timer: ReturnType<typeof setTimeout> | undefined;
  #started = false;
  #controller: AbortController | undefined;
  readonly #visibility = () => {
    if (!document.hidden && this.automatic()) void this.refresh();
  };
  start(): void {
    if (this.#started) return;
    this.#started = true;
    document.addEventListener('visibilitychange', this.#visibility);
    void this.refresh();
  }
  stop(): void {
    this.#started = false;
    clearTimeout(this.#timer);
    this.#controller?.abort();
    document.removeEventListener('visibilitychange', this.#visibility);
  }
  toggle(): void {
    this.automatic.update((v) => !v);
    if (this.automatic()) void this.refresh();
    else clearTimeout(this.#timer);
  }
  async refresh(): Promise<void> {
    if (this.loading()) return;
    clearTimeout(this.#timer);
    this.loading.set(true);
    this.#controller = new AbortController();
    const [status, config] = await Promise.allSettled([
      this.api.read('/api/v1/status', this.#controller.signal),
      this.api.read('/api/v1/config', this.#controller.signal),
    ]);
    this.configurationFresh.set(config.status === 'fulfilled');
    if (config.status === 'fulfilled') this.configuration.set(config.value);
    if (status.status === 'fulfilled') {
      this.snapshot.set(status.value);
      this.receivedAt.set(new Date().toISOString());
      this.error.set(null);
      const throughput = record(status.value['throughput']);
      const keys = {
        staged: 'staged',
        elasticsearch: 'elasticsearch_satisfied',
        rabbitmq: 'rabbitmq_confirmed',
        consumer: 'consumer_processed',
      };
      const rates: Record<string, string | null> = {};
      for (const [name, key] of Object.entries(keys)) {
        const sample = record(throughput[key]);
        const n = sample['per_second'];
        rates[name] =
          sample['state'] === 'known' &&
          typeof n === 'string' &&
          /^(0|[1-9][0-9]*)\.[0-9]+$/.test(n)
            ? n
            : null;
      }
      this.rates.set(rates);
    } else {
      this.error.set(
        status.reason instanceof ApiFailure
          ? status.reason
          : new ApiFailure(0, 'unavailable', null),
      );
      this.rates.set({});
    }
    this.loading.set(false);
    this.schedule();
  }
  private schedule(): void {
    if (this.#started && this.automatic())
      this.#timer = setTimeout(() => {
        if (document.hidden) this.schedule();
        else void this.refresh();
      }, 5000);
  }
  dependency(name: string): Data {
    return record(record(this.snapshot()?.['dependencies'])[name]);
  }
  data(name: string): Data {
    const d = this.dependency(name);
    return d['freshness'] === 'fresh' && !this.error() ? record(d['data']) : {};
  }
}
