import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.3.0/index.js";

const kv = openKv({
	// The disk backend will use BoltDB as a storage engine and persist the key-value store on disk (".k6.kv" file).
    // It is slower than the memory backend, but offers durability, and increases the amount of data that can be stored.
	backend: "disk",
});

export default async function () {
	await kv.set("key", "value");
	const value = await kv.get("key");
	expect(value).toEqual("value");

	await kv.delete("key");
	const exists = await kv.exists("key");
	expect(exists).toEqual(false);

	const entries = await kv.list();
	expect(entries).toHaveLength(0);
}
