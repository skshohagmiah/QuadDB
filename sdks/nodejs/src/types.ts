// Client configuration
export interface fluxdlConfig {
  address?: string;
  timeout?: number;
  credentials?: any;
}

// Key-Value types
export interface KVSetOptions {
  ttl?: number;
}

// Queue types
export interface QueueStats {
  name: string;
  messages: number;
  size: number;
}

export interface QueueMessage {
  id: string;
  payload: string;
  timestamp: number;
}

// Stream types
export interface StreamMessage {
  stream: string;
  partition: number;
  offset: number;
  key: string;
  value: string;
  timestamp: number;
}

export interface StreamInfo {
  name: string;
  partitions: number;
  messages: number;
}

export interface SubscribeOptions {
  group?: string;
  partition?: number;
  offset?: number;
}

export type MessageHandler = (message: StreamMessage) => Promise<void> | void;

// Error types
export class fluxdlError extends Error {
  constructor(message: string, public code?: string) {
    super(message);
    this.name = 'fluxdlError';
  }
}

export class ConnectionError extends fluxdlError {
  constructor(message: string) {
    super(message, 'CONNECTION_ERROR');
    this.name = 'ConnectionError';
  }
}

export class TimeoutError extends fluxdlError {
  constructor(message: string) {
    super(message, 'TIMEOUT_ERROR');
    this.name = 'TimeoutError';
  }
}
