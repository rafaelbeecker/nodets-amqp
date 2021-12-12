import { Channel, Connection } from "amqplib";

export interface ChannelConnection {
  connection: Connection;
  channel: Channel;
}
