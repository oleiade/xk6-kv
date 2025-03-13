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