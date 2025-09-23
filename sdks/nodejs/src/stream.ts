import * as grpc from '@grpc/grpc-js';
import { StreamMessage, StreamInfo, MessageHandler, SubscribeOptions, fluxdlError } from './types';

export class StreamClient {
  constructor(private connection?: grpc.Client) {}

  /**
   * Publish a message to a stream
   */
  async publish(stream: string, message: string, key?: string): Promise<void> {
    console.log(`STREAM PUBLISH: ${stream} <- ${message} (key: ${key || 'none'})`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (stream && message) {
          resolve();
        } else {
          reject(new fluxdlError('Invalid stream name or message'));
        }
      }, 10);
    });
  }

  /**
   * Subscribe to a stream
   */
  async subscribe(stream: string, handler: MessageHandler, options?: SubscribeOptions): Promise<void> {
    console.log(`STREAM SUBSCRIBE: ${stream}`, options);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (stream && handler) {
          // Simulate receiving messages
          const simulateMessage = () => {
            const message: StreamMessage = {
              stream,
              partition: 0,
              offset: Date.now(),
              key: 'test-key',
              value: `simulated-message-${Date.now()}`,
              timestamp: Date.now()
            };
            handler(message);
          };

          // Send a few simulated messages
          setTimeout(simulateMessage, 100);
          setTimeout(simulateMessage, 200);
          setTimeout(simulateMessage, 300);
          
          resolve();
        } else {
          reject(new fluxdlError('Invalid stream name or handler'));
        }
      }, 10);
    });
  }

  /**
   * Create a new stream
   */
  async createStream(stream: string, partitions: number = 1): Promise<void> {
    console.log(`STREAM CREATE: ${stream} (${partitions} partitions)`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (stream && partitions > 0) {
          resolve();
        } else {
          reject(new fluxdlError('Invalid stream name or partition count'));
        }
      }, 10);
    });
  }

  /**
   * List all streams
   */
  async listStreams(): Promise<string[]> {
    console.log('STREAM LIST');
    
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(['events', 'logs', 'metrics']);
      }, 10);
    });
  }

  /**
   * Get stream information
   */
  async getStreamInfo(stream: string): Promise<StreamInfo> {
    console.log(`STREAM INFO: ${stream}`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (stream) {
          resolve({
            name: stream,
            partitions: 3,
            messages: 1000
          });
        } else {
          reject(new fluxdlError('Invalid stream name'));
        }
      }, 10);
    });
  }

  /**
   * Delete a stream
   */
  async deleteStream(stream: string): Promise<void> {
    console.log(`STREAM DELETE: ${stream}`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (stream) {
          resolve();
        } else {
          reject(new fluxdlError('Invalid stream name'));
        }
      }, 10);
    });
  }

  /**
   * Publish multiple messages to a stream
   */
  async publishBatch(stream: string, messages: Array<{key?: string, value: string}>): Promise<void> {
    console.log(`STREAM PUBLISH BATCH: ${stream} <- ${messages.length} messages`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (stream && messages.length > 0) {
          resolve();
        } else {
          reject(new fluxdlError('Invalid stream name or empty messages'));
        }
      }, 10);
    });
  }

  /**
   * Get messages from a stream partition
   */
  async getMessages(stream: string, partition: number = 0, offset: number = 0, limit: number = 10): Promise<StreamMessage[]> {
    console.log(`STREAM GET MESSAGES: ${stream} partition=${partition} offset=${offset} limit=${limit}`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (stream) {
          const messages: StreamMessage[] = Array.from({ length: Math.min(limit, 5) }, (_, i) => ({
            stream,
            partition,
            offset: offset + i,
            key: `key-${i}`,
            value: `message-${i}`,
            timestamp: Date.now() - (i * 1000)
          }));
          resolve(messages);
        } else {
          reject(new fluxdlError('Invalid stream name'));
        }
      }, 10);
    });
  }
}
