import sleep from "../common/sleep";
import { ChannelConnection } from "./amqp-client";

export type ProducerOptions = {
  channelConnection: ChannelConnection;
  exchangeName: string;
  exchangeType: string;
  routingKey: string;
};

class Producer {
  constructor(private readonly options: ProducerOptions) {}

  async publish(content: string): Promise<void> {
    const { channel, connection } = this.options.channelConnection;
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
    await sleep(500);
    await connection.close();
  }
}

export default Producer;
