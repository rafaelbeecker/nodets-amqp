import { Message } from "amqplib";

class MessageHeaders {
  constructor(private readonly msg: Message) {}

  getHeader(prop: string): unknown {
    return this.msg?.properties?.headers?.[prop];
  }
}

export default MessageHeaders;
