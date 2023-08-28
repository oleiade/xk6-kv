# Key-Value Store Extension for k6

`xk6-kv` is a k6 extension that provides a persistent key-value store, enabling shared state across VUs during script execution. This store is persisted to disk as a single file. The extension module offers a high-read optimized store with support for up to 10,000 writes/s.

## Features

- **Persistent Storage**: Maintains a disk-persisted key-value store.
- **Shared Across VUs**: Perfect for sharing state across VUs in your k6 scripts.
- **High-Read Optimized**: While it excels in read operations, it's also capable of supporting up to 10,000 writes/s.

## Why Use xk6-kv?

**Ease of State Sharing**: Managing state across multiple VUs can be challenging. **xk6-kv** alleviates this complexity by offering a straightforward way to share state, ensuring that your scripting remains as seamless as possible.

**Safety First**: One of the primary concerns with shared state is ensuring thread safety. With **xk6-kv**, you can be confident that your shared state operations are safe, thanks to its design optimized for k6's parallel VU execution model.

**Lightweight Alternative**: While there are other options like the redis module for state sharing, **xk6-kv** stands out as a more lightweight solution. It's particularly useful if you're looking for a simpler and more integrated solution without external dependencies. Though keep in mind that for extremely high-performance requirements, the redis module may offer better throughput, but for many users, **xk6-kv** will be more than sufficient.

## Installation
To use `xk6-kv`, you first need to build `k6` with it:
```
xk6 build --with github.com/oleiade/xk6-kv
```

## Usage

```javascript
import { openKv } from "k6/x/kv";

const kv = openKv();

export async function setup() {
    // (optional) Clear the key-value store to start from a clean state
    await kv.clear();
}

export default async function () {
    // Set a bunch of keys
    await kv.set("foo", "bar");
    await kv.set("abc", 123);
    await kv.set("easy as", [1, 2, 3]);
    await kv.set("a b c", { "123": "baby you and me girl"});
    console.log(`current size of the KV store: ${kv.size()}`)

    const entries = await kv.list({ prefix: "a" });
    for (const entry of entries) {
        console.log(`found entry: ${JSON.stringify(entry)}`);
    }

    await kv.del("foo");
}
```

## API Documentation

- `openKv(): KV`: Opens a key-value store persisted on disk. Should be called only in the init context.
- `KV.set(key: string, value: any): Promise<any>`: Sets a key-value pair in the store. Accepts any JSON-serializable value.
- `KV.get(key: string): Promise<any>`: Retrieves a value based on its key. If the key doesn't exist, an error is thrown.
- `KV.delete(key: string)`: Removes a specific key-value pair from the store.
- `KV.list(options: ListOptions)`: Returns key-value pairs from the store filtered by the provided options.
- `KV.clear()`: Removes all key-value pairs from the store. Useful when starting with a clean state, e.g., in the setup() function.
- `KV.size()`: Provides the count of key-value pairs currently in the store.
- `ListOptions` interface, used in `KV.list()`, it includes:
    - `prefix: string`: Filters results to keys that have the specified prefix.
    - `limit`: number: Restricts results to a maximum count.
