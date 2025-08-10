import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.3.0/index.js";

const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export default async function () {
  await kv.clear();

  // Insert multiple keys
  await kv.set("keyA", "alpha");
  await kv.set("keyB", "bravo");
  await kv.set("keyC", "charlie");

  // Call randomKey and assert it returns a valid key
  const random = await kv.randomKey();
  console.log(`Randomly picked key: ${random}`);
  expect(["keyA", "keyB", "keyC"]).toContain(random);

  // Fetch the value to ensure the key is accessible
  const value = await kv.get(random);
  expect(value).toBeDefined();
}
