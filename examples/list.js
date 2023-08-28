import { openKv } from "k6/x/kv";

const kv = openKv();

export async function setup() {
    await kv.clear();
}

export default async function () {
    await kv.set("foo", "bar");
    await kv.set("abc", 123);
    await kv.set("easy as", [1, 2, 3]);
    await kv.set("a b c", { "123": "baby you and me girl"});
    console.log(`current size of the KV store: ${kv.size()}`)

    const entries = await kv.list({ prefix: "a" });
    for (const entry of entries) {
        console.log(`found entry: ${JSON.stringify(entry)}`);
    }
}