export interface ConsumerLoggerInterface {
  error: (message: string, label?: string) => void;
}
