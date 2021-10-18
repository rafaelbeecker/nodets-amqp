export interface ClientLoggerInterface {
  error: (message: string, label?: string) => void;
}
