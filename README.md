# xk6-kv

[![Go Report Card](https://goreportcard.com/badge/github.com/oleiade/xk6-kv)](https://goreportcard.com/report/github.com/oleiade/xk6-kv)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

A k6 extension providing a persistent key-value store for sharing state across Virtual Users (VUs) during load testing.

## Table of Contents
- [Features](#features)
- [Why Use xk6-kv](#why-use-xk6-kv)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Features

- 🔒 **Thread-Safe**: Secure state sharing across Virtual Users
- 🔌 **Easy Integration**: Simple API that works seamlessly with k6
- 🔄 **Flexible Storage**: Choose between in-memory or disk-based persistence
- 🪶 **Lightweight**: No external dependencies required

## Why Use xk6-kv?

- **State Sharing Made Simple**: Managing state across multiple VUs in k6 can be challenging. **xk6-kv** provides a straightforward solution for sharing state, making your load testing scripts cleaner and more maintainable.
- **Built for Safety**: Thread safety is crucial in load testing. **xk6-kv** is designed specifically for k6's parallel VU execution model, ensuring your shared state operations remain safe and reliable.
- **Storage Options**: Choose the backend that fits your needs:
  - **Memory**: Fast, ephemeral storage that's shared across VUs
  - **Disk**: Persistent storage using BoltDB for data that needs to survive between test runs
- **Lightweight Alternative**: While other solutions like Redis exist and are compatible with k6 for state sharing, **xk6-kv** offers a more lightweight, integrated approach:
    - No external services required
    - Simple setup and configuration

> **Note**: For extremely high-performance requirements, consider using the k6 Redis module instead.

## Installation

1. First, ensure you have [xk6](https://github.com/grafana/xk6) installed:
```bash
go install go.k6.io/xk6/cmd/xk6@latest
```

2. Build a k6 binary with the xk6-kv extension:
```bash
xk6 build --with github.com/oleiade/xk6-kv
```

3. Import the kv module in your script, at the top of your test script:
```javascript
import { openKv } from "k6/x/kv";
```

4. The built binary will be in your current directory. You can move it to your PATH or use it directly:
```bash
./k6 run script.js
```

## Quickstart

```javascript
import { openKv } from "k6/x/kv";

// Open a key-value store with the default backend (disk)
const kv = openKv();

// Or specify a backend explicitly
// const kv = openKv({ backend: "disk" });   // Disk-based persistent backend (default)
// const kv = openKv({ backend: "memory" }); // In-memory backend

export async function setup() {
    // Start with a clean state
    await kv.clear();
}

export default async function () {
        // Set a bunch of keys
    await kv.set("foo", "bar");
    await kv.set("abc", 123);
    await kv.set("easy as", [1, 2, 3]);

    const abcExists = await kv.exists("a b c")
    if (!abcExists) {
      await kv.set("a b c", { "123": "baby you and me girl"});
    }

    console.log(`current size of the KV store: ${kv.size()}`)

    const entries = await kv.list({ prefix: "a" });
    for (const entry of entries) {
        console.log(`found entry: ${JSON.stringify(entry)}`);
    }

    await kv.delete("foo");
}
```


## API Reference

### Core Functions

#### `openKv(options?: OpenKvOptions): KV`
Opens a key-value store with the specified backend. Must be called in the init context.

```typescript
interface OpenKvOptions {
    backend?: "memory" | "disk"; // Default is "memory"
}
```

- **memory**: In-memory backend that's fast and shared across all VUs (default)
- **disk**: Persistent BoltDB-based backend that survives between test runs

#### Performance Considerations

While both backends are optimized for performance and suitable for most load testing scenarios, be aware that:
- There is some overhead due to synchronization between VUs
- Consider this overhead when analyzing your test results
- For extremely high throughput requirements, you might need alternative solutions

#### KV Methods
- `set(key: string, value: any): Promise<any>`
  - Sets a key-value pair. Accepts any JSON-serializable value.
  
- `get(key: string): Promise<any>`
  - Retrieves a value by key. Throws if key doesn't exist.
  
- `delete(key: string): Promise<void>`
  - Removes a key-value pair.

- `exists(key: string): Promise<boolean>`
  - Checks if a given key exists.
  
- `list(options: ListOptions): Promise<Array<Entry>>`
  - Returns filtered key-value pairs.
  
- `clear(): Promise<void>`
  - Removes all entries.
  
- `size(): number`
  - Returns current store size.

### ListOptions Interface
```typescript
interface ListOptions {
    prefix?: string;  // Filter by key prefix
    limit?: number;   // Max number of results
}
```

## Examples

A common use case for xk6-kv is sharing state between VUs for workflows such as producer-consumer patterns or rendez-vous points. The following example demonstrates a producer-consumer workflow where one VU produces tokens and another consumes them, coordinating through the shared key-value store.

```javascript
import { sleep } from "k6";
import { openKv } from "k6/x/kv";

export let options = {
  scenarios: {
    producer: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 10,
      exec: "producer",
    },
    consumer: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 10,
      startTime: "5s",
      exec: "consumer",
    },
  },
};

const kv = openKv({ backend: "memory" });

export async function producer() {
  let latestProducerID = 0;
  if (await kv.exists(`latest-producer-id`)) {
    latestProducerID = await kv.get(`latest-producer-id`);
  }

  console.log(`[producer]-> adding token ${latestProducerID}`);
  await kv.set(`token-${latestProducerID}`, "token-value");
  await kv.set(`latest-producer-id`, latestProducerID + 1);

  // Let's simulate a delay between producing tokens
  sleep(1);
}

export async function consumer() {
  console.log("[consumer]<- waiting for next token");

  // Let's list the existing tokens, and consume the first we find
  const entries = await kv.list({ prefix: "token-" });
  if (entries.length > 0) {
    await kv.get(entries[0].key);
    console.log(`[consumer]<- consumed token ${entries[0].key}`);
    await kv.delete(entries[0].key);
  } else {
    console.log("[consumer]<- no tokens available");
  }

  // Let's simulate a delay between consuming tokens
  sleep(1);
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request