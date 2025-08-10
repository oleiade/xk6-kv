import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.3.0/index.js";

const kv = openKv({
  backend: "disk",
  trackKeys: true,
});

export default async function () {
  await kv.clear();

  // Add a few entries
  await kv.set("hello", "world");
  await kv.set("foo", "bar");

  // Call rebuildKeyList to simulate manual recovery
  const result = await kv.rebuildKeyList();
  expect(result).toEqual(true);

  // List entries to confirm keys were correctly rebuilt
  const entries = await kv.list();
  expect(entries).toHaveLength(2);
}
