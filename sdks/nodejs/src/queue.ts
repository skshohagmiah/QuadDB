import * as grpc from '@grpc/grpc-js';
import { QueueStats, QueueMessage, GoMsgError } from './types';

export class QueueClient {
  constructor(private connection?: grpc.Client) {}

  /**
   * Push a message to a queue
   */
  async push(queue: string, message: string): Promise<void> {
    console.log(`QUEUE PUSH: ${queue} <- ${message}`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (queue && message) {
          resolve();
        } else {
          reject(new GoMsgError('Invalid queue name or message'));
        }
      }, 10);
    });
  }

  /**
   * Pop a message from a queue
   */
  async pop(queue: string): Promise<string | null> {
    console.log(`QUEUE POP: ${queue}`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (queue) {
          resolve(`message-from-${queue}`);
        } else {
          reject(new GoMsgError('Invalid queue name'));
        }
      }, 10);
    });
  }

  /**
   * Peek at a message without removing it
   */
  async peek(queue: string): Promise<string | null> {
    console.log(`QUEUE PEEK: ${queue}`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (queue) {
          resolve(`peeked-message-from-${queue}`);
        } else {
          reject(new GoMsgError('Invalid queue name'));
        }
      }, 10);
    });
  }

  /**
   * List all queues
   */
  async list(): Promise<string[]> {
    console.log('QUEUE LIST');
    
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(['queue1', 'queue2', 'notifications']);
      }, 10);
    });
  }

  /**
   * Get queue statistics
   */
  async stats(queue: string): Promise<QueueStats> {
    console.log(`QUEUE STATS: ${queue}`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (queue) {
          resolve({
            name: queue,
            messages: 42,
            size: 1024
          });
        } else {
          reject(new GoMsgError('Invalid queue name'));
        }
      }, 10);
    });
  }

  /**
   * Purge all messages from a queue
   */
  async purge(queue: string): Promise<number> {
    console.log(`QUEUE PURGE: ${queue}`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (queue) {
          resolve(10); // Number of messages purged
        } else {
          reject(new GoMsgError('Invalid queue name'));
        }
      }, 10);
    });
  }

  /**
   * Push multiple messages to a queue
   */
  async pushBatch(queue: string, messages: string[]): Promise<void> {
    console.log(`QUEUE PUSH BATCH: ${queue} <- ${messages.length} messages`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (queue && messages.length > 0) {
          resolve();
        } else {
          reject(new GoMsgError('Invalid queue name or empty messages'));
        }
      }, 10);
    });
  }

  /**
   * Pop multiple messages from a queue
   */
  async popBatch(queue: string, count: number = 10): Promise<string[]> {
    console.log(`QUEUE POP BATCH: ${queue} (${count} messages)`);
    
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (queue && count > 0) {
          const messages = Array.from({ length: Math.min(count, 5) }, 
            (_, i) => `batch-message-${i + 1}`);
          resolve(messages);
        } else {
          reject(new GoMsgError('Invalid queue name or count'));
        }
      }, 10);
    });
  }
}
