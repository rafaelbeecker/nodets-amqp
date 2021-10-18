import { Channel, ConsumeMessage } from "amqplib";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import handler from "@sintese/nodejs-async-handler";

import { ClientLoggerInterface } from "./interfaces/client-logger-interface";
import { ChannelConnection } from "./amqp-client";

export type ConsumerOptions = {
  channelConnection: ChannelConnection;
  prefetch?: number;
  isRetryEnabled?: boolean;
  retryMaxAttempts?: number;
  retryExchangeName?: string;
  retryRoutingKey?: string;
  logger?: ClientLoggerInterface;
};

export class Consumer {
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

        const [err] = await handler(async () => callback(msg));
        if (!err) {
          channel.ack(msg);
          return;
        }

        const attempt = msg?.properties?.headers?.["x-attempt"] || 1;
        if (
          this.options.isRetryEnabled &&
          attempt <= (this.options.retryMaxAttempts || 1)
        ) {
          this.options.logger &&
            this.options.logger.error(`[attempt:${attempt}]: ${err.message}`);
          await this.retry({ channel: channel, msg });
          return;
        }

        this.options.logger &&
          this.options.logger.error(`[discard]: ${err.message}`);
        channel.nack(msg, false, false);
      }
    );
  }

  private async retry({
    channel,
    msg,
  }: {
    channel: Channel;
    msg: ConsumeMessage;
  }): Promise<void> {
    if (!this.options.retryExchangeName || !this.options.retryRoutingKey)
      return;

    const attempt = (msg?.properties?.headers?.["x-attempt"] || 0) + 1;

    channel.ack(msg);
    channel.publish(
      this.options.retryExchangeName,
      this.options.retryRoutingKey,
      Buffer.from(msg.content?.toString()),
      {
        headers: {
          "x-attempt": attempt,
          "x-delay": 1000 * 10 * attempt,
        },
      }
    );
  }
}

export default Consumer;
