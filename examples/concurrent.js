import { openKv, DoesNotExistError } from "k6/x/kv";

export let options = {
  iterations: 1,
  vus: 1,
};

const kv = openKv();

export default async function () {
  await kv.set("foo", "bar");
  await kv.set("froufrou", 123);

  let val = await kv.get("foo");
  console.log(val);

  val = await kv.get("froufrou");
  console.log(val);

  try {
    await kv.get("does-not-exist");
  } catch (e) {
    console.log(JSON.stringify(e));
  }

  await kv.delete("foo");
}
