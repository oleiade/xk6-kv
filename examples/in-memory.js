import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.3.0/index.js";

const kv = openKv({
	// The "memory" backend offers the best performance.
    //
	// When selecting the "memory" backend, the key-value store is ephemeral
	// and will be lost when the test ends.
	backend: "memory",
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

