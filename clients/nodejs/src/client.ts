import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { EventEmitter } from 'events';

// Types for GoMsg operations
export interface GoMsgConfig {
  nodes: string[];           // GoMsg cluster nodes (e.g., ["localhost:8080", "localhost:8081"])
  connectTimeout?: number;   // Connection timeout in ms (default: 10000)
  requestTimeout?: number;   // Request timeout in ms (default: 30000)
  retryAttempts?: number;    // Retry attempts (default: 3)
}

export interface QueueMessage {
  id: string;
  queue: string;
  data: Buffer;
  timestamp: Date;
  headers: { [key: string]: string };
}

export interface StreamMessage {
  id: string;
  topic: string;
  partitionKey: string;
  data: Buffer;
  offset: number;
  timestamp: Date;
  headers: { [key: string]: string };
  partition: number;
}

/**
 * GoMsg Client - Connects to GoMsg Docker containers via gRPC
 * 
 * This client is a thin wrapper that handles:
 * - Connection management to GoMsg cluster
 * - Automatic failover and retry logic
 * - Simple API that mirrors server operations
 * 
 * All KV, Queue, and Stream logic is handled by the GoMsg server.
 */
export class GoMsgClient extends EventEmitter {
  private config: Required<GoMsgConfig>;
  private connections: Map<string, grpc.Client> = new Map();
  private kvClients: Map<string, any> = new Map();
  private queueClients: Map<string, any> = new Map();
  private streamClients: Map<string, any> = new Map();
  private isConnected = false;

  // Service-specific clients
  public readonly kv: KVClient;
  public readonly queue: QueueClient;
  public readonly stream: StreamClient;

  constructor(config: GoMsgConfig) {
    super();
    
    this.config = {
      nodes: config.nodes,
      connectTimeout: config.connectTimeout || 10000,
      requestTimeout: config.requestTimeout || 30000,
      retryAttempts: config.retryAttempts || 3,
    };

    if (this.config.nodes.length === 0) {
      throw new Error('At least one GoMsg node must be specified');
    }

    // Initialize service-specific clients
    this.kv = new KVClient(this);
    this.queue = new QueueClient(this);
    this.stream = new StreamClient(this);
  }

  /**
   * Connect to GoMsg cluster
   */
  async connect(): Promise<void> {
    try {
      // Load protobuf definitions (these would be generated from your .proto files)
      const kvProto = await this.loadProto('kv.proto');
      const queueProto = await this.loadProto('queue.proto');
      const streamProto = await this.loadProto('stream.proto');

      // Connect to all nodes
      for (const node of this.config.nodes) {
        const connection = new grpc.Client(node, grpc.credentials.createInsecure());
        
        this.connections.set(node, connection);
        this.kvClients.set(node, new (kvProto as any).KVService(node, grpc.credentials.createInsecure()));
        this.queueClients.set(node, new (queueProto as any).QueueService(node, grpc.credentials.createInsecure()));
        this.streamClients.set(node, new (streamProto as any).StreamService(node, grpc.credentials.createInsecure()));
      }

      this.isConnected = true;
      this.emit('connected');
    } catch (error) {
      throw new Error(`Failed to connect to GoMsg cluster: ${error}`);
    }
  }

  /**
   * Load protobuf definition
   */
  private async loadProto(protoFile: string): Promise<grpc.GrpcObject> {
    const packageDefinition = await protoLoader.load(protoFile, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });
    
    return grpc.loadPackageDefinition(packageDefinition);
  }

  /**
   * Get a random node for load balancing
   */
  private getRandomNode(): string {
    const nodes = Array.from(this.connections.keys());
    return nodes[Math.floor(Math.random() * nodes.length)];
  }

  /**
   * Execute operation with retry logic
   */
  private async executeWithRetry<T>(operation: (node: string) => Promise<T>): Promise<T> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt < this.config.retryAttempts; attempt++) {
      try {
        const node = this.getRandomNode();
        return await operation(node);
      } catch (error) {
        lastError = error as Error;
        if (attempt < this.config.retryAttempts - 1) {
          await this.sleep((attempt + 1) * 100);
        }
      }
    }

    throw new Error(`All retry attempts failed: ${lastError?.message}`);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Check cluster health
   */
  async health(): Promise<{ [node: string]: boolean }> {
    const results: { [node: string]: boolean } = {};
    
    for (const node of this.config.nodes) {
      try {
        const client = this.kvClients.get(node);
        await new Promise((resolve, reject) => {
          client.Get({ key: '__health_check__' }, (error: any) => {
            // Node is healthy if it responds (even with "key not found")
            results[node] = !error || error.code !== grpc.status.UNAVAILABLE;
            resolve(null);
          });
        });
      } catch {
        results[node] = false;
      }
    }
    
    return results;
  }

  /**
   * Close all connections
   */
  async close(): Promise<void> {
    for (const connection of this.connections.values()) {
      connection.close();
    }
    
    this.connections.clear();
    this.kvClients.clear();
    this.queueClients.clear();
    this.streamClients.clear();
    this.isConnected = false;
    
    this.emit('disconnected');
  }

  // Internal methods for service clients (public for service access)
  public getRandomNode(): string {
    const nodes = Array.from(this.connections.keys());
    return nodes[Math.floor(Math.random() * nodes.length)];
  }

  public async executeWithRetry<T>(operation: (node: string) => Promise<T>): Promise<T> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt < this.config.retryAttempts; attempt++) {
      try {
        const node = this.getRandomNode();
        return await operation(node);
      } catch (error) {
        lastError = error as Error;
        if (attempt < this.config.retryAttempts - 1) {
          await this.sleep((attempt + 1) * 100);
        }
      }
    }

    throw new Error(`All retry attempts failed: ${lastError?.message}`);
  }

  public getKVClient(node: string): any {
    return this.kvClients.get(node);
  }

  public getQueueClient(node: string): any {
    return this.queueClients.get(node);
  }

  public getStreamClient(node: string): any {
    return this.streamClients.get(node);
  }
}

// =============================================================================
// KV CLIENT - Service-specific operations
// =============================================================================

export class KVClient {
  constructor(private client: GoMsgClient) {}

  async set(key: string, value: Buffer, ttlSeconds?: number): Promise<void> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<void>((resolve, reject) => {
        const request = { key, value, ttlSeconds };
        
        client.Set(request, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Set failed: ${response.status.message}`));
            return;
          }
          
          resolve();
        });
      });
    });
  }

  async get(key: string): Promise<{ value: Buffer | null; found: boolean }> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<{ value: Buffer | null; found: boolean }>((resolve, reject) => {
        client.Get({ key }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Get failed: ${response.status.message}`));
            return;
          }
          
          resolve({
            value: response.found ? Buffer.from(response.value) : null,
            found: response.found,
          });
        });
      });
    });
  }

  async mset(pairs: { [key: string]: Buffer }): Promise<void> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<void>((resolve, reject) => {
        client.MSet({ pairs }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`MSet failed: ${response.status.message}`));
            return;
          }
          
          resolve();
        });
      });
    });
  }

  async mget(keys: string[]): Promise<{ [key: string]: Buffer }> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<{ [key: string]: Buffer }>((resolve, reject) => {
        client.MGet({ keys }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`MGet failed: ${response.status.message}`));
            return;
          }
          
          const results: { [key: string]: Buffer } = {};
          for (const [key, value] of Object.entries(response.values)) {
            results[key] = Buffer.from(value as any);
          }
          
          resolve(results);
        });
      });
    });
  }

  async delete(...keys: string[]): Promise<number> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<number>((resolve, reject) => {
        client.Delete({ keys }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Delete failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.deletedCount);
        });
      });
    });
  }

  async increment(key: string, delta: number = 1): Promise<number> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<number>((resolve, reject) => {
        client.Increment({ key, delta }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Increment failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.value);
        });
      });
    });
  }

  async exists(key: string): Promise<boolean> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<boolean>((resolve, reject) => {
        client.Exists({ key }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Exists failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.exists);
        });
      });
    });
  }

  async keys(pattern: string): Promise<string[]> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<string[]>((resolve, reject) => {
        client.Keys({ pattern }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Keys failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.keys);
        });
      });
    });
  }

  async ttl(key: string): Promise<number> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getKVClient(node);
      
      return new Promise<number>((resolve, reject) => {
        client.TTL({ key }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`TTL failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.ttlSeconds);
        });
      });
    });
  }
}

// =============================================================================
// QUEUE CLIENT - Service-specific operations
// =============================================================================

export class QueueClient {
  constructor(private client: GoMsgClient) {}

  async push(queue: string, data: Buffer, delaySeconds?: number): Promise<string> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getQueueClient(node);
      
      return new Promise<string>((resolve, reject) => {
        const request = { queue, data, delaySeconds };
        
        client.Push(request, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Queue push failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.messageId);
        });
      });
    });
  }

  async pop(queue: string, timeoutSeconds?: number): Promise<QueueMessage | null> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getQueueClient(node);
      
      return new Promise<QueueMessage | null>((resolve, reject) => {
        const request = { queue, timeoutSeconds };
        
        client.Pop(request, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Queue pop failed: ${response.status.message}`));
            return;
          }
          
          if (!response.message) {
            resolve(null);
            return;
          }
          
          resolve({
            id: response.message.id,
            queue: response.message.queue,
            data: Buffer.from(response.message.data),
            timestamp: new Date(response.message.timestamp * 1000),
            headers: response.message.headers || {},
          });
        });
      });
    });
  }

  async pushBatch(queue: string, messages: Buffer[], delaySeconds?: number[]): Promise<string[]> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getQueueClient(node);
      
      return new Promise<string[]>((resolve, reject) => {
        const request = { queue, messages, delaySeconds };
        
        client.PushBatch(request, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Queue push batch failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.messageIds);
        });
      });
    });
  }

  async popBatch(queue: string, limit: number, timeoutSeconds?: number): Promise<QueueMessage[]> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getQueueClient(node);
      
      return new Promise<QueueMessage[]>((resolve, reject) => {
        const request = { queue, limit, timeoutSeconds };
        
        client.PopBatch(request, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Queue pop batch failed: ${response.status.message}`));
            return;
          }
          
          const messages = response.messages.map((msg: any) => ({
            id: msg.id,
            queue: msg.queue,
            data: Buffer.from(msg.data),
            timestamp: new Date(msg.timestamp * 1000),
            headers: msg.headers || {},
          }));
          
          resolve(messages);
        });
      });
    });
  }

  async size(queue: string): Promise<number> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getQueueClient(node);
      
      return new Promise<number>((resolve, reject) => {
        client.Size({ queue }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Queue size failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.size);
        });
      });
    });
  }

  async purge(queue: string): Promise<number> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getQueueClient(node);
      
      return new Promise<number>((resolve, reject) => {
        client.Purge({ queue }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Queue purge failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.purgedCount);
        });
      });
    });
  }
}

// =============================================================================
// STREAM CLIENT - Service-specific operations
// =============================================================================

export class StreamClient {
  constructor(private client: GoMsgClient) {}

  async publish(
    topic: string,
    partitionKey: string,
    data: Buffer,
    headers?: { [key: string]: string }
  ): Promise<StreamMessage> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<StreamMessage>((resolve, reject) => {
        const request = { topic, partitionKey, data, headers };
        
        client.Publish(request, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Stream publish failed: ${response.status.message}`));
            return;
          }
          
          resolve({
            id: response.messageId,
            topic,
            partitionKey,
            data,
            offset: response.offset,
            timestamp: new Date(),
            headers: headers || {},
            partition: response.partition,
          });
        });
      });
    });
  }

  async read(
    topic: string,
    partition: number,
    fromOffset: number,
    limit: number
  ): Promise<StreamMessage[]> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<StreamMessage[]>((resolve, reject) => {
        const request = { topic, partition, fromOffset, limit };
        
        client.Read(request, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Stream read failed: ${response.status.message}`));
            return;
          }
          
          const messages = response.messages.map((msg: any) => ({
            id: msg.id,
            topic: msg.topic,
            partitionKey: msg.partitionKey,
            data: Buffer.from(msg.data),
            offset: msg.offset,
            timestamp: new Date(msg.timestamp * 1000),
            headers: msg.headers || {},
            partition: msg.partition || 0,
          }));
          
          resolve(messages);
        });
      });
    });
  }

  async readFrom(
    topic: string,
    partition: number,
    fromTime: Date,
    limit: number
  ): Promise<StreamMessage[]> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<StreamMessage[]>((resolve, reject) => {
        const request = { topic, partition, fromTime: Math.floor(fromTime.getTime() / 1000), limit };
        
        client.ReadFrom(request, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Stream read from failed: ${response.status.message}`));
            return;
          }
          
          const messages = response.messages.map((msg: any) => ({
            id: msg.id,
            topic: msg.topic,
            partitionKey: msg.partitionKey,
            data: Buffer.from(msg.data),
            offset: msg.offset,
            timestamp: new Date(msg.timestamp * 1000),
            headers: msg.headers || {},
            partition: msg.partition || 0,
          }));
          
          resolve(messages);
        });
      });
    });
  }

  async createTopic(topic: string, partitions: number): Promise<void> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<void>((resolve, reject) => {
        client.CreateTopic({ name: topic, partitions }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Create topic failed: ${response.status.message}`));
            return;
          }
          
          resolve();
        });
      });
    });
  }

  async deleteTopic(topic: string): Promise<void> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<void>((resolve, reject) => {
        client.DeleteTopic({ name: topic }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Delete topic failed: ${response.status.message}`));
            return;
          }
          
          resolve();
        });
      });
    });
  }

  async listTopics(): Promise<string[]> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<string[]>((resolve, reject) => {
        client.ListTopics({}, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`List topics failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.topics);
        });
      });
    });
  }

  async purge(topic: string): Promise<number> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<number>((resolve, reject) => {
        client.Purge({ topic }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Stream purge failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.purgedCount);
        });
      });
    });
  }

  async commitGroupOffset(topic: string, groupId: string, partition: number, offset: number): Promise<void> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<void>((resolve, reject) => {
        client.CommitGroupOffset({ topic, groupId, partition, offset }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Commit group offset failed: ${response.status.message}`));
            return;
          }
          
          resolve();
        });
      });
    });
  }

  async getGroupOffset(topic: string, groupId: string, partition: number): Promise<number> {
    return this.client.executeWithRetry(async (node) => {
      const client = this.client.getStreamClient(node);
      
      return new Promise<number>((resolve, reject) => {
        client.GetGroupOffset({ topic, groupId, partition }, (error: any, response: any) => {
          if (error) {
            reject(error);
            return;
          }
          
          if (!response.status.success) {
            reject(new Error(`Get group offset failed: ${response.status.message}`));
            return;
          }
          
          resolve(response.offset);
        });
      });
    });
  }
}
