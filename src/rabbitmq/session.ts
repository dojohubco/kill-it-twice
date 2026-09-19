// Internal AMQP connection lifetime. Only documented public driver/socket APIs.
import { randomUUID } from 'node:crypto';
import {
  connect,
  type ChannelModel,
  type Channel,
  type ConfirmChannel,
  type SocketOptions,
} from 'amqplib';
import { BrokerFailure } from './metadata.ts';
export interface AmqpConnection {
  host: string;
  port: number;
  username: string;
  password: string;
  ca: string;
  vhost: string;
}
export class AmqpSession {
  readonly id = randomUUID();
  readonly #abort = new AbortController();
  #model: ChannelModel | undefined;
  #closed = false;
  #closing = false;
  readonly #channels: Channel[] = [];
  #blocked = false;
  #failure: BrokerFailure | undefined;
  readonly #waiters = new Set<() => void>();
  get alive() {
    return !this.#closed && !!this.#model;
  }
  get blocked() {
    return this.#blocked;
  }
  get failure() {
    return this.#failure;
  }
  #notify() {
    for (const wake of this.#waiters) wake();
  }
  retire(
    error: BrokerFailure = new BrokerFailure(
      'transient',
      'AMQP channel retired',
    ),
  ) {
    if (this.#closed) return;
    this.#failure ??= error;
    this.#closed = true;
    this.#abort.abort();
    this.#notify();
  }
  async open(config: AmqpConnection): Promise<void> {
    if (this.#model || this.#closed)
      throw new Error('AMQP session is single-use');
    const timer = setTimeout(() => this.retire(), 5000);
    try {
      // amqplib forwards socket options to Node TLS; its bundled type omits Node's
      // documented AbortSignal socket option. This intersection preserves that public contract.
      const socketOptions: SocketOptions & {
        signal: AbortSignal;
        recovery: false;
      } = {
        recovery: false,
        ca: config.ca,
        servername: 'localhost',
        rejectUnauthorized: true,
        minVersion: 'TLSv1.2',
        signal: this.#abort.signal,
        timeout: 5000,
        noDelay: true,
        clientProperties: { connection_name: this.id },
      };
      this.#model = await connect(
        {
          protocol: 'amqps',
          hostname: config.host,
          port: config.port,
          username: config.username,
          password: config.password,
          vhost: config.vhost,
          heartbeat: 15,
          frameMax: 16384,
          channelMax: 8,
        },
        socketOptions,
      );
      // Automatic recovery is disabled; each channel incarnation is explicit.
      this.#model.on('error', (error: Error) =>
        this.retire(amqpFailure(error)),
      );
      this.#model.on('close', () => this.retire());
      this.#model.on('handler-error', () =>
        this.retire(new BrokerFailure('integrity', 'AMQP handler failure')),
      );
      this.#model.on('blocked', () => {
        this.#blocked = true;
        this.#notify();
      });
      this.#model.on('unblocked', () => {
        this.#blocked = false;
        this.#notify();
      });
      if (this.#closed) throw this.#failure ?? new Error('Connection deadline');
    } catch (error) {
      const failure = amqpFailure(error);
      this.retire(failure);
      throw failure;
    } finally {
      clearTimeout(timer);
    }
  }
  async channel(confirm: true, highWaterMark?: number): Promise<ConfirmChannel>;
  async channel(confirm: false, highWaterMark?: number): Promise<Channel>;
  async channel(
    confirm: boolean,
    highWaterMark = 16,
  ): Promise<Channel | ConfirmChannel> {
    if (!this.#model || !this.alive)
      throw this.#failure ?? new Error('AMQP session unavailable');
    if (
      !Number.isInteger(highWaterMark) ||
      highWaterMark < 1 ||
      highWaterMark > 128
    )
      throw new Error('Invalid AMQP buffer bound');
    const timer = setTimeout(() => this.retire(), 5000);
    try {
      const c = confirm
        ? await this.#model.createConfirmChannel({ highWaterMark })
        : await this.#model.createChannel({ highWaterMark });
      c.on('error', (error: Error) => this.retire(amqpFailure(error)));
      c.on('close', () => {
        if (!this.#closing) this.retire();
      });
      this.#channels.push(c);
      c.on('handler-error', () =>
        this.retire(new BrokerFailure('integrity', 'AMQP handler failure')),
      );
      return c;
    } finally {
      clearTimeout(timer);
    }
  }
  changed(timeout: number): Promise<void> {
    if (this.#closed) return Promise.resolve();
    if (this.#waiters.size >= 2) throw new Error('Bounded AMQP waiter limit');
    return new Promise((resolve) => {
      const wake = () => {
        clearTimeout(timer);
        this.#waiters.delete(wake);
        resolve();
      };
      const timer = setTimeout(wake, timeout);
      this.#waiters.add(wake);
    });
  }
  wake() {
    this.#notify();
  }
  async close(): Promise<void> {
    if (!this.#model || this.#closed) {
      this.retire();
      return;
    }
    this.#closing = true;
    const timer = setTimeout(() => this.retire(), 1000);
    try {
      // Channel close is an ordered RPC: buffered individual ACKs must reach the
      // broker before connection.close (a different channel's control frame).
      for (const channel of this.#channels) {
        if (!this.alive) break;
        await channel.close();
      }
      if (this.alive) await this.#model.close();
    } catch {
      this.retire();
    } finally {
      clearTimeout(timer);
      this.retire();
    }
  }
}
export function amqpFailure(error: unknown): BrokerFailure {
  if (error instanceof BrokerFailure) return error;
  const message = error instanceof Error ? error.message : '';
  return new BrokerFailure(
    /\bACCESS[_-]REFUSED\b|\b403\b|authentication failed/i.test(message)
      ? 'auth'
      : /\bNOT[_-]FOUND\b|\b404\b|\bPRECONDITION[_-]FAILED\b|\b406\b/.test(
            message,
          )
        ? 'configuration'
        : 'transient',
    'AMQP operation failed; channel outcome unresolved',
  );
}
