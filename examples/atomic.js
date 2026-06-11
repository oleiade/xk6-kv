import { sleep } from "k6";
import { openKv } from "k6/x/kv";

export const options = {
  scenarios: {
    atomicCounter: {
      executor: "shared-iterations",
      vus: 10,
      iterations: 100,
    },
  },
};

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
  await kv.set("counter", 0);
}

async function incrementCounter(key) {
  for (;;) {
    const entry = await kv.get(key);
    const current = entry.value === null ? 0 : entry.value;

    const result = await kv.atomic()
      .check(entry)
      .set(key, current + 1)
      .commit();

    if (result.ok) {
      return current + 1;
    }

    // Another VU updated the key first. Yield briefly, then retry with a fresh entry.
    sleep(0.01);
  }
}

export default async function () {
  const value = await incrementCounter("counter");
  console.log(`VU ${__VU} incremented counter to ${value}`);
}

export async function teardown() {
  const counter = await kv.get("counter");
  console.log(`final counter value: ${counter.value}`);
}
