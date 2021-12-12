import { ChannelConnection } from "./channel";
import { CreateProducerOptions } from "./amqp-client";

export interface ProducerOptions extends CreateProducerOptions {
  channelConnection: ChannelConnection;
}

class Producer {
  constructor(private readonly options: ProducerOptions) {}

  async publish(
    content: string,
    headers: { [key: string]: unknown } = {}
  ): Promise<void> {
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
        headers: {
          ...headers,
          "x-attempt": 1,
        },
      }
    );
  }
}

export default Producer;
