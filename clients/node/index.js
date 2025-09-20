import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'node:path';
import url from 'node:url';

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));

// Load protos at runtime from api/proto
const defaultProtoOptions = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
};

function loadPackage(defPath, options) {
    const pkgDef = protoLoader.loadSync(defPath, { ...defaultProtoOptions, ...options });
    return grpc.loadPackageDefinition(pkgDef);
}

export class Client {
    constructor(address, opts = {}) {
        const root = path.resolve(__dirname, '..', '..', 'api', 'proto');
        const clusterPkg = loadPackage(path.join(root, 'cluster.proto'), opts);
        const kvPkg = loadPackage(path.join(root, 'kv.proto'), opts);
        const queuePkg = loadPackage(path.join(root, 'queue.proto'), opts);

        const credentials = opts.credentials || grpc.credentials.createInsecure();

        const kvStub = new kvPkg.kv.KVService(address, credentials);
        const queueStub = new queuePkg.queue.QueueService(address, credentials);
        const clusterStub = new clusterPkg.cluster.ClusterService(address, credentials);

        this.kv = new KV(kvStub);
        this.queue = new Queue(queueStub);
        this.cluster = new Cluster(clusterStub);
    }
}

// Back-compat: simple factory
export function createClient(address, opts) { return new Client(address, opts); }

// ---------------- Namespaced classes ----------------

class KV {
    constructor(stub) { this._kv = stub; }
    async set(key, value, ttlSec = 0, metadata) {
        return new Promise((resolve, reject) => {
            this._kv.Set({ key, value, ttl: ttlSec }, metadata, (err, resp) => {
                if (err) return reject(err);
                resolve(resp);
            });
        });
    }
    async get(key, metadata) {
        return new Promise((resolve, reject) => {
            this._kv.Get({ key }, metadata, (err, resp) => {
                if (err) return reject(err);
                resolve(resp);
            });
        });
    }
    async del(keys, metadata) {
        return new Promise((resolve, reject) => {
            this._kv.Del({ keys }, metadata, (err, resp) => {
                if (err) return reject(err);
                resolve(resp);
            });
        });
    }
}

class Queue {
    constructor(stub) { this._q = stub; }
    async push(queue, data, delaySeconds = 0, metadata) {
        return new Promise((resolve, reject) => {
            this._q.Push({ queue, data, delaySeconds }, metadata, (err, resp) => {
                if (err) return reject(err);
                resolve(resp);
            });
        });
    }
    async pop(queue, timeoutSeconds = 0, metadata) {
        return new Promise((resolve, reject) => {
            this._q.Pop({ queue, timeoutSeconds }, metadata, (err, resp) => {
                if (err) return reject(err);
                resolve(resp);
            });
        });
    }
    async ack(messageId, metadata) {
        return new Promise((resolve, reject) => {
            this._q.Ack({ messageId }, metadata, (err, resp) => {
                if (err) return reject(err);
                resolve(resp);
            });
        });
    }
}

class Cluster {
    constructor(stub) { this._c = stub; }
    async leader(metadata) {
        return new Promise((resolve, reject) => {
            this._c.GetLeader({}, metadata, (err, resp) => {
                if (err) return reject(err);
                resolve(resp);
            });
        });
    }
}
