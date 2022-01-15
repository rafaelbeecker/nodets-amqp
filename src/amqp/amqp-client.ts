import amqp, { Channel, Connection } from "amqplib";

import Producer from "./producer";
import Consumer from "./consumer";
import { ChannelConnection } from "./channel";
import { ClientLoggerInterface } from "./logger";

export type CreateClientOptions = {
  amqpUrl: string;
  connectionTimeout: number;
  logger: ClientLoggerInterface;
};

export type CreateProducerOptions = {
  exchangeName: string;
  exchangeType: string;
  routingKey: string;
};

export type CreateConsumerOptions = {
  prefetch?: number;
  isRetryEnabled?: boolean;
  retryMaxAttempts?: number;
  retryExchangeName?: string;
  retryRoutingKey?: string;
  retryHeaders?: string[];
  retryDelay?: number | ((err: Error) => Promise<number>);
  retryCheck?: (err: Error) => Promise<boolean>;
};

class AmqpClient {
  public connection: Connection;
  public channel: Channel;

  constructor(private readonly options: CreateClientOptions) {}

  async createProducer(options: CreateProducerOptions): Promise<Producer> {
    if (!this.connection) {
      await this.connect({ timeout: this.options.connectionTimeout });
    }
    return new Producer({
      ...options,
      channelConnection: {
        channel: this.channel,
        connection: this.connection,
      },
    });
  }

  async createConsumer(options: CreateConsumerOptions): Promise<Consumer> {
    if (!this.connection) {
      await this.connect({ timeout: this.options.connectionTimeout });
    }
    return new Consumer({
      ...options,
      logger: this.options.logger,
      channelConnection: {
        channel: this.channel,
        connection: this.connection,
      },
    });
  }

  private async connect(options: { timeout: number }): Promise<void> {
    this.connection = await amqp.connect(this.options.amqpUrl, options);
    this.channel = await this.connection.createChannel();
  }
}
export default AmqpClient;
