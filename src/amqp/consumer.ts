import { Channel, ConsumeMessage } from "amqplib";
import handler from "@sintese/nodejs-async-handler";

import {
  ClientLoggerInterface,
  ConsumerOptions,
} from "./interfaces/client-logger-interface";
import MessageHeaders from "./common/message-headers";

class Consumer {
  constructor(private readonly options: ConsumerOptions) {}

  async consume(
    queue: string,
    callback: (msg: ConsumeMessage) => void
  ): Promise<void> {
    const { channel } = this.options.channelConnection;
    await channel.prefetch(this.options.prefetch || 10);
    await channel.consume(
      queue,
      async (msg: ConsumeMessage | null): Promise<void> => {
        if (!msg) return;

        const attempt = msg?.properties?.headers?.["x-attempt"] || 1;
        if (this.options.isRetryEnabled) {
          this.logger.info(
            `[retry][attempt:${attempt}]: started msg reprocessing`
          );
        }

        const [err] = (await handler(async () => callback(msg))) as [
          Error,
          unknown
        ];
        if (!err) {
          channel.ack(msg);
          return;
        }

        if (!this.options.isRetryEnabled) {
          channel.nack(msg, false, false);
          this.logger.error(`[discard]: ${err.message}`);
          return;
        }

        if (attempt >= (this.options.retryMaxAttempts || 1)) {
          this.logger.error(`[discard][attempt:${attempt}]: ${err.message}`);
          channel.nack(msg, false, false);
          return;
        }

        this.logger.error(
          `[retry][attempt:${attempt}]: (enqueuing retry) ${err.message}`
        );
        await this.retry({ channel, msg, err });
      }
    );
  }

  private async retry({
    channel,
    msg,
    err,
  }: {
    channel: Channel;
    msg: ConsumeMessage;
    err: Error;
  }): Promise<void> {
    if (!this.options.retryExchangeName || !this.options.retryRoutingKey)
      return;

    const header = new MessageHeaders(msg);
    const attempt = ((header.getHeader("x-attempt") as number) || 1) + 1;

    const headers = (this.options.retryHeaders || [])?.reduce(
      (acc: { [key: string]: unknown }, itn: string) =>
        header.getHeader(itn) ? { [itn]: header.getHeader(itn) } : acc,
      {}
    );

    channel.ack(msg);
    channel.publish(
      this.options.retryExchangeName,
      this.options.retryRoutingKey,
      Buffer.from(msg.content?.toString()),
      {
        headers: {
          ...headers,
          "x-attempt": attempt,
          "x-delay": await this.calcRetryDelay(attempt, err),
        },
      }
    );
  }

  private async calcRetryDelay(attempt: number, err: Error): Promise<number> {
    if (typeof this.options.retryDelay === "function") {
      return this.options.retryDelay(err);
    }
    if (typeof this.options.retryDelay === "number") {
      return this.options.retryDelay;
    }
    return 1000 * 10 * attempt;
  }

  get logger(): ClientLoggerInterface {
    return this.options.logger;
  }
}

export default Consumer;
