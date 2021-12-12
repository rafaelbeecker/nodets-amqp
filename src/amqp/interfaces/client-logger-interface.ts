import { ChannelConnection } from "../amqp-client";

export interface ClientLoggerInterface {
  error: (message: string, label?: string) => void;
  info: (message: string, label?: string) => void;
}

export interface ConsumerOptions {
  channelConnection: ChannelConnection;
  prefetch?: number;
  isRetryEnabled?: boolean;
  retryMaxAttempts?: number;
  retryExchangeName?: string;
  retryRoutingKey?: string;
  retryHeaders?: string[];
  logger: ClientLoggerInterface;
  retryDelay?: number | ((err: Error) => Promise<number>);
}
