import amqp from "amqplib";
import sleep from "../common/sleep";

export type ProducerOptions = {
  amqpUrl: string;
  exchangeName: string;
  exchangeType: string;
  routingKey: string;
  connectionTimeout: number;
};

class Producer {
  constructor(private readonly options: ProducerOptions) {}

  // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
  async publish(content: never) {
    const connection = await amqp.connect(this.options.amqpUrl, {
      timeout: this.options.connectionTimeout || 3000,
    });

    const channel = await connection.createChannel();
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
