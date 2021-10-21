import { ChannelConnection } from "./amqp-client";
import { Connection } from "amqplib";

export type ProducerOptions = {
  channelConnection: ChannelConnection;
  exchangeName: string;
  exchangeType: string;
  routingKey: string;
};

class Producer {
  constructor(private readonly options: ProducerOptions) {}

  async publish(content: string): Promise<void> {
    const { channel } = this.options.channelConnection;
    await channel.assertExchange(
      this.options.exchangeName,
      this.options.exchangeType
    );
    channel.publish(
      this.options.exchangeName,
      this.options.routingKey,
      Buffer.from(content),
      {
        headers: { "x-attempt": 1 },
      }
    );
  }

  get connection(): Connection {
    return this.options.channelConnection.connection;
  }
}

export default Producer;
