import amqp, { Channel, Connection } from "amqplib";

import Producer from "./producer";
import Consumer from "./consumer";
import { ClientLoggerInterface } from "./interfaces/client-logger-interface";

export type ChannelConnection = {
  connection: Connection;
  channel: Channel;
};

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
};

type CreateChannelConnectionOptions = {
  timeout: number;
};

class AmqpClient {
  constructor(private readonly options: CreateClientOptions) {}

  async createProducer(options: CreateProducerOptions): Promise<Producer> {
    return new Producer({
      ...options,
      channelConnection: await this.createChannelConnection({
        timeout: this.options.connectionTimeout,
      }),
    });
  }

  async createConsumer(options: CreateConsumerOptions): Promise<Consumer> {
    return new Consumer({
      ...options,
      logger: this.options.logger,
      channelConnection: await this.createChannelConnection({
        timeout: this.options.connectionTimeout,
      }),
    });
  }

  private async createChannelConnection(
    options: CreateChannelConnectionOptions
  ): Promise<ChannelConnection> {
    const connection = await amqp.connect(this.options.amqpUrl, options);
    const channel = await connection.createChannel();
    return { channel, connection };
  }
}
export default AmqpClient;
