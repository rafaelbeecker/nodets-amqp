export interface ClientLoggerInterface {
  error: (message: string, label?: string) => void;
  info: (message: string, label?: string) => void;
}
