import { Channel, ConsumeMessage } from "amqplib";
import handler from "@sintese/nodejs-async-handler";

import MessageHeaders from "./message-headers";
import { ChannelConnection } from "./channel";
import { ClientLoggerInterface } from "./logger";
import { CreateConsumerOptions } from "./amqp-client";

export interface ConsumerOptions extends CreateConsumerOptions {
  channelConnection: ChannelConnection;
  logger: ClientLoggerInterface;
}

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
          this.options.logger.info(`[retry][attempt:${attempt}]: reprocessing`);
        }

        const [err] = (await handler(async () => callback(msg))) as [
          Error,
          unknown
        ];
        if (!err) {
          channel.ack(msg);
          return;
        }

        // 1. A mensagem é reprocessada apenas se o retry estiver habilitado
        if (!this.options.isRetryEnabled) {
          channel.nack(msg, false, false);
          this.options.logger.error(`[discard]: ${err.message}`);
          return;
        }

        // 2. O retryCheck callback (opcional) verifica se é permitido continuar com o retry
        if (
          typeof this.options.retryCheck === "function" &&
          !(await this.options.retryCheck(err))
        ) {
          channel.nack(msg, false, false);
          this.options.logger.error(`[retry-check][discard]: ${err.message}`);
          return;
        }

        // 3. O incremental de tentativas é controlado a partir de um limite máximo
        if (attempt >= (this.options.retryMaxAttempts || 1)) {
          this.options.logger.error(
            `[discard][attempt:${attempt}]: ${err.message}`
          );
          channel.nack(msg, false, false);
          return;
        }

        if (!this.options.retryExchangeName || !this.options.retryRoutingKey)
          return;

        this.options.logger.error(
          `[retry][attempt:${attempt}]: (requeue) ${err.message}`
        );

        // 4. O envio do retry trata-se de uma republicação da mensagem
        await this.retry({
          channel,
          retryExchangeName: this.options.retryExchangeName,
          retryRoutingKey: this.options.retryRoutingKey,
          msg,
          err,
        });
      }
    );
  }

  private async retry({
    channel,
    retryExchangeName,
    retryRoutingKey,
    msg,
    err,
  }: {
    channel: Channel;
    retryExchangeName: string;
    retryRoutingKey: string;
    msg: ConsumeMessage;
    err: Error;
  }): Promise<void> {
    const header = new MessageHeaders(msg);
    const attempt = ((header.getHeader("x-attempt") as number) || 1) + 1;

    const headers = (this.options.retryHeaders || [])?.reduce(
      (acc: { [key: string]: unknown }, itn: string) =>
        header.getHeader(itn) ? { [itn]: header.getHeader(itn) } : acc,
      {}
    );

    channel.ack(msg);
    channel.publish(
      retryExchangeName,
      retryRoutingKey,
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
}
export default Consumer;
