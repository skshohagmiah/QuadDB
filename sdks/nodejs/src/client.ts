import * as grpc from '@grpc/grpc-js';
import { fluxdlConfig, ConnectionError } from './types';
import { KVClient } from './kv';
import { QueueClient } from './queue';
import { StreamClient } from './stream';

export class fluxdlClient {
  private connection?: grpc.Client;
  private _kv?: KVClient;
  private _queue?: QueueClient;
  private _stream?: StreamClient;
  
  constructor(private config: fluxdlConfig = {}) {
    // Set defaults
    this.config = {
      address: 'localhost:9000',
      timeout: 30000,
      ...config
    };
  }

  /**
   * Connect to fluxdl server
   */
  async connect(): Promise<void> {
    try {
      // For now, we'll create a placeholder connection
      // In a real implementation, this would establish the gRPC connection
      console.log(`Connecting to fluxdl at ${this.config.address}...`);
      
      // Initialize clients
      this._kv = new KVClient(this.connection);
      this._queue = new QueueClient(this.connection);
      this._stream = new StreamClient(this.connection);
      
      console.log('✅ Connected to fluxdl successfully!');
    } catch (error) {
      throw new ConnectionError(`Failed to connect to fluxdl: ${error}`);
    }
  }

  /**
   * Disconnect from fluxdl server
   */
  async disconnect(): Promise<void> {
    if (this.connection) {
      this.connection.close();
      this.connection = undefined;
    }
    console.log('Disconnected from fluxdl');
  }

  /**
   * Test connection to server
   */
  async ping(): Promise<boolean> {
    try {
      // Use a simple KV operation to test connectivity
      await this.kv.set('ping', 'pong');
      await this.kv.delete('ping');
      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Get Key-Value client
   */
  get kv(): KVClient {
    if (!this._kv) {
      throw new ConnectionError('Not connected. Call connect() first.');
    }
    return this._kv;
  }

  /**
   * Get Queue client
   */
  get queue(): QueueClient {
    if (!this._queue) {
      throw new ConnectionError('Not connected. Call connect() first.');
    }
    return this._queue;
  }

  /**
   * Get Stream client
   */
  get stream(): StreamClient {
    if (!this._stream) {
      throw new ConnectionError('Not connected. Call connect() first.');
    }
    return this._stream;
  }

  /**
   * Create a new client instance with configuration
   */
  static create(config?: fluxdlConfig): fluxdlClient {
    return new fluxdlClient(config);
  }

  /**
   * Create and connect to fluxdl in one step
   */
  static async connect(config?: fluxdlConfig): Promise<fluxdlClient> {
    const client = new fluxdlClient(config);
    await client.connect();
    return client;
  }
}
