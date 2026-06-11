package kv

import (
	"fmt"
	"testing"

	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/modulestest"
)

const atomicTestPrelude = `
function assert(condition, message) {
	if (!condition) {
		throw new Error(message);
	}
}

function assertEqual(actual, expected, message) {
	if (actual !== expected) {
		throw new Error(message + ": got " + JSON.stringify(actual) + ", want " + JSON.stringify(expected));
	}
}

function assertDeepEqual(actual, expected, message) {
	const actualJSON = JSON.stringify(actual);
	const expectedJSON = JSON.stringify(expected);
	if (actualJSON !== expectedJSON) {
		throw new Error(message + ": got " + actualJSON + ", want " + expectedJSON);
	}
}

function assertEntry(entry, key, value, message) {
	assert(entry && typeof entry === "object", message + ": entry must be an object");
	assertEqual(entry.key, key, message + ": key");
	assertDeepEqual(entry.value, value, message + ": value");
	if (value === null) {
		assertEqual(entry.versionstamp, null, message + ": missing versionstamp");
	} else {
		assert(typeof entry.versionstamp === "string" && entry.versionstamp.length > 0, message + ": present versionstamp");
	}
}

function assertCommitOK(result, message) {
	assert(result && typeof result === "object", message + ": result must be an object");
	assertEqual(result.ok, true, message + ": ok");
	assert(typeof result.versionstamp === "string" && result.versionstamp.length > 0, message + ": versionstamp");
}

function assertCommitConflict(result, message) {
	assert(result && typeof result === "object", message + ": result must be an object");
	assertEqual(result.ok, false, message + ": ok");
	assertEqual(Object.prototype.hasOwnProperty.call(result, "versionstamp"), false, message + ": no versionstamp");
}

function yieldToEventLoop() {
	return new Promise((resolve) => setTimeout(resolve, 0));
}

async function atomicIncrement(kv, key) {
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

		await yieldToEventLoop();
	}
}
`

func TestKVAtomicGetReturnsVersionedEntries(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();

		assertEqual(typeof kv.atomic, "function", "kv.atomic is exposed");
		assertEqual(typeof kv.get, "function", "kv.get is exposed");
		assertEqual(typeof kv.getMany, "function", "kv.getMany is exposed");
		assertEqual(typeof kv.getEntry, "undefined", "kv.getEntry is not exposed");
		assertEqual(typeof kv.getManyEntries, "undefined", "kv.getManyEntries is not exposed");

		const missing = await kv.get("entry:missing");
		assertEntry(missing, "entry:missing", null, "missing entry");

		await kv.set("entry:present", { count: 1 });
		const present = await kv.get("entry:present");
		assertEntry(present, "entry:present", { count: 1 }, "present entry");

		const entries = await kv.getMany(["entry:present", "entry:missing"]);
		assertEqual(entries.length, 2, "getMany length");
		assertEntry(entries[0], "entry:present", { count: 1 }, "getMany present");
		assertEntry(entries[1], "entry:missing", null, "getMany missing");
	`)
}

func TestKVAtomicCommitAppliesSetDeleteAndReturnsVersionstamp(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();

		const create = await kv.atomic()
			.check({ key: "account:1", versionstamp: null })
			.set("account:1", { balance: 100 })
			.set("audit:1", "created")
			.commit();
		assertCommitOK(create, "create commit");

		const account = await kv.get("account:1");
		assertEntry(account, "account:1", { balance: 100 }, "created account");
		assert(account.versionstamp === create.versionstamp, "entry versionstamp should match commit versionstamp");

		const update = await kv.atomic()
			.check(account)
			.set("account:1", { balance: 125 })
			.delete("audit:1")
			.commit();
		assertCommitOK(update, "update commit");
		assert(update.versionstamp !== create.versionstamp, "new commit should produce a new versionstamp");

		assertEntry(await kv.get("account:1"), "account:1", { balance: 125 }, "updated account");
		assertEntry(await kv.get("audit:1"), "audit:1", null, "deleted audit entry");
	`)
}

func TestKVAtomicCheckFailureDoesNotMutate(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();

		await kv.set("inventory:item", 3);
		const stale = await kv.get("inventory:item");
		await kv.set("inventory:item", 2);

		const result = await kv.atomic()
			.check(stale)
			.set("inventory:item", 1)
			.set("inventory:reservation", "reserved")
			.delete("inventory:log")
			.commit();

		assertCommitConflict(result, "stale check commit");
		assertEntry(await kv.get("inventory:item"), "inventory:item", 2, "stale check leaves original key alone");
		assertEntry(await kv.get("inventory:reservation"), "inventory:reservation", null, "stale check skips set mutation");
	`)
}

func TestKVAtomicAbsentCheckFailsIfKeyAppearsBeforeCommit(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();

		const missing = await kv.get("lock:setup");
		assertEntry(missing, "lock:setup", null, "initially missing lock");

		await kv.set("lock:setup", "other-vu");

		const result = await kv.atomic()
			.check(missing)
			.set("lock:setup", "this-vu")
			.set("setup:payload", "initialized")
			.commit();

		assertCommitConflict(result, "absent check conflicts with concurrent create");
		assertEntry(await kv.get("lock:setup"), "lock:setup", "other-vu", "concurrent create survives");
		assertEntry(await kv.get("setup:payload"), "setup:payload", null, "failed initializer skips payload");
	`)
}

func TestKVAtomicAllOrNothingMultiKeyCommit(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();

		await kv.set("transfer:a", 10);
		await kv.set("transfer:b", 20);

		const accountA = await kv.get("transfer:a");
		const accountB = await kv.get("transfer:b");

		const ok = await kv.atomic()
			.check(accountA)
			.check(accountB)
			.set("transfer:a", 7)
			.set("transfer:b", 23)
			.set("transfer:receipt:1", { from: "a", to: "b", amount: 3 })
			.commit();
		assertCommitOK(ok, "balanced transfer commit");

		assertEntry(await kv.get("transfer:a"), "transfer:a", 7, "debit applied");
		assertEntry(await kv.get("transfer:b"), "transfer:b", 23, "credit applied");

		const staleA = accountA;
		const currentB = await kv.get("transfer:b");
		const failed = await kv.atomic()
			.check(staleA)
			.check(currentB)
			.set("transfer:a", 1)
			.set("transfer:b", 29)
			.set("transfer:receipt:2", { from: "a", to: "b", amount: 6 })
			.commit();
		assertCommitConflict(failed, "partially stale transfer commit");

		assertEntry(await kv.get("transfer:a"), "transfer:a", 7, "failed transfer keeps debit");
		assertEntry(await kv.get("transfer:b"), "transfer:b", 23, "failed transfer keeps credit");
		assertEntry(await kv.get("transfer:receipt:2"), "transfer:receipt:2", null, "failed transfer skips receipt");
	`)
}

func TestKVAtomicBuilderSurvivesAwaitBoundaries(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();

		await kv.set("await:counter", 0);
		const entry = await kv.get("await:counter");
		const operation = kv.atomic().check(entry);

		await yieldToEventLoop();

		const result = await operation
			.set("await:counter", entry.value + 1)
			.set("await:side-effect", true)
			.commit();
		assertCommitOK(result, "commit after await");

		assertEntry(await kv.get("await:counter"), "await:counter", 1, "mutation after await boundary");
		assertEntry(await kv.get("await:side-effect"), "await:side-effect", true, "second mutation after await boundary");
	`)
}

func TestKVAtomicPromiseAllRetryLoopWithinVU(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();
		await kv.set("counter:single-vu", 0);

		const increments = [];
		for (let i = 0; i < 50; i++) {
			increments.push(atomicIncrement(kv, "counter:single-vu"));
		}

		await Promise.all(increments);
		assertEntry(await kv.get("counter:single-vu"), "counter:single-vu", 50, "Promise.all increments are not lost");
	`)
}

func TestKVAtomicPromiseAllInitializesSharedFixtureOnce(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();

		const attempts = [];
		for (let i = 0; i < 20; i++) {
			attempts.push((async () => {
				const lock = await kv.get("fixture:promise-lock");
				if (lock.value !== null) {
					return { ok: false };
				}
				return await kv.atomic()
					.check(lock)
					.set("fixture:promise-lock", i)
					.set("fixture:promise-winner:" + i, true)
					.commit();
			})());
		}

		const results = await Promise.all(attempts);
		assertEqual(results.filter((result) => result.ok).length, 1, "only one Promise.all initializer wins");

		const winners = await kv.list({ prefix: "fixture:promise-winner:" });
		assertEqual(winners.length, 1, "only one Promise.all winner marker is written");
	`)
}

func TestKVAtomicMultipleVUsRetryLoop(t *testing.T) {
	t.Parallel()

	const vuCount = 8
	const incrementsPerVU = 25

	root := New()
	initRuntime := newAtomicRuntime(t, root, 0)
	runAtomicScript(t, initRuntime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();
		await kv.set("counter:multi-vu", 0);
	`)

	runAtomicVUScripts(t, root, vuCount, func(_ int) string {
		return fmt.Sprintf(`
			const kv = openKv({ backend: "memory" });
			for (let i = 0; i < %d; i++) {
				await atomicIncrement(kv, "counter:multi-vu");
			}
		`, incrementsPerVU)
	})
	if t.Failed() {
		return
	}

	verifyRuntime := newAtomicRuntime(t, root, 99)
	runAtomicScript(t, verifyRuntime, fmt.Sprintf(`
		const kv = openKv({ backend: "memory" });
		assertEntry(await kv.get("counter:multi-vu"), "counter:multi-vu", %d, "multi-VU increments are not lost");
	`, vuCount*incrementsPerVU))
}

func TestKVAtomicMultipleVUsInitializeSharedFixtureOnce(t *testing.T) {
	t.Parallel()

	const vuCount = 10

	root := New()
	initRuntime := newAtomicRuntime(t, root, 0)
	runAtomicScript(t, initRuntime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();
	`)

	runAtomicVUScripts(t, root, vuCount, func(_ int) string {
		return `
			const kv = openKv({ backend: "memory" });
			const lock = await kv.get("fixture:lock");
			assertEqual(lock.key, "fixture:lock", "fixture lock check key");
			if (lock.value !== null) {
				return;
			}
			const result = await kv.atomic()
				.check(lock)
				.set("fixture:lock", VU_ID)
				.set("fixture:created-by", VU_ID)
				.set("fixture:winner:" + VU_ID, true)
				.set("fixture:successes", 1)
				.commit();

			if (!result.ok) {
				return;
			}
		`
	})
	if t.Failed() {
		return
	}

	verifyRuntime := newAtomicRuntime(t, root, 99)
	runAtomicScript(t, verifyRuntime, fmt.Sprintf(`
		const kv = openKv({ backend: "memory" });
		const winner = await kv.get("fixture:created-by");
		assert(typeof winner.value === "number" && winner.value >= 1 && winner.value <= %d, "one VU recorded as setup owner");
		assertEntry(await kv.get("fixture:successes"), "fixture:successes", 1, "fixture initialized exactly once");
		const winners = await kv.list({ prefix: "fixture:winner:" });
		assertEqual(winners.length, 1, "only one VU won the fixture lock");
	`, vuCount))
}

func TestKVAtomicListEntriesAreCheckable(t *testing.T) {
	t.Parallel()

	runtime := newAtomicRuntime(t, New(), 1)
	runAtomicScript(t, runtime, `
		const kv = openKv({ backend: "memory" });
		await kv.clear();

		await kv.set("queue:1", "first");
		await kv.set("queue:2", "second");
		await kv.set("other:1", "ignored");

		const entries = await kv.list({ prefix: "queue:" });
		assertEqual(entries.length, 2, "list prefix length");
		assertEntry(entries[0], "queue:1", "first", "first listed entry");
		assertEntry(entries[1], "queue:2", "second", "second listed entry");

		const result = await kv.atomic()
			.check(entries[0])
			.delete(entries[0].key)
			.set("queue:claimed:1", entries[0].value)
			.commit();
		assertCommitOK(result, "claim listed entry");

		assertEntry(await kv.get("queue:1"), "queue:1", null, "claimed work removed");
		assertEntry(await kv.get("queue:claimed:1"), "queue:claimed:1", "first", "claim marker written");
	`)
}

func TestKVAtomicMultipleVUsClaimWorkExactlyOnce(t *testing.T) {
	t.Parallel()

	const vuCount = 5
	const workItems = 20

	root := New()
	initRuntime := newAtomicRuntime(t, root, 0)
	runAtomicScript(t, initRuntime, fmt.Sprintf(`
		const kv = openKv({ backend: "memory" });
		await kv.clear();
		for (let i = 0; i < %d; i++) {
			await kv.set("work:" + i, { item: i });
		}
	`, workItems))

	runAtomicVUScripts(t, root, vuCount, func(_ int) string {
		return `
			const kv = openKv({ backend: "memory" });

			for (;;) {
				const entries = await kv.list({ prefix: "work:" });
				if (entries.length === 0) {
					break;
				}

				let claimed = false;
				for (const entry of entries) {
					const result = await kv.atomic()
						.check(entry)
						.delete(entry.key)
						.set("claimed:" + entry.key, VU_ID)
						.commit();
					if (result.ok) {
						claimed = true;
						break;
					}
				}

				if (!claimed) {
					await yieldToEventLoop();
				}
			}
		`
	})
	if t.Failed() {
		return
	}

	verifyRuntime := newAtomicRuntime(t, root, 99)
	runAtomicScript(t, verifyRuntime, fmt.Sprintf(`
		const kv = openKv({ backend: "memory" });
		const remaining = await kv.list({ prefix: "work:" });
		assertEqual(remaining.length, 0, "all work was claimed");

		const claimed = await kv.list({ prefix: "claimed:work:" });
		assertEqual(claimed.length, %d, "every item has exactly one claim marker");

		const seen = new Set();
		for (const entry of claimed) {
			const workKey = entry.key.substring("claimed:".length);
			assert(!seen.has(workKey), "duplicate claim marker for " + workKey);
			seen.add(workKey);
			assert(typeof entry.value === "number" && entry.value >= 1 && entry.value <= %d, "claim records claiming VU");
		}
	`, workItems, vuCount))
}

func newAtomicRuntime(t *testing.T, root *RootModule, vuID int) *modulestest.Runtime {
	t.Helper()

	runtime := modulestest.NewRuntime(t)
	instance := root.NewModuleInstance(runtime.VU)
	exports := instance.Exports()

	if err := runtime.VU.Runtime().Set("openKv", exports.Named["openKv"]); err != nil {
		t.Fatalf("set openKv: %v", err)
	}
	if err := runtime.VU.Runtime().Set("VU_ID", vuID); err != nil {
		t.Fatalf("set VU_ID: %v", err)
	}

	return runtime
}

func runAtomicVUScripts(t *testing.T, root *RootModule, vuCount int, scriptForVU func(vuID int) string) {
	t.Helper()

	errs := make(chan error, vuCount)
	runtimes := make([]*modulestest.Runtime, vuCount)
	for vuID := 1; vuID <= vuCount; vuID++ {
		runtimes[vuID-1] = newAtomicRuntime(t, root, vuID)
	}

	for i, runtime := range runtimes {
		go func(vuID int, runtime *modulestest.Runtime) {
			_, err := runtime.RunOnEventLoop(wrapAtomicScript(scriptForVU(vuID)))
			if err != nil {
				errs <- fmt.Errorf("vu %d: %w", vuID, err)
				return
			}
			errs <- nil
		}(i+1, runtime)
	}

	for range vuCount {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

func runAtomicScript(t *testing.T, runtime *modulestest.Runtime, body string) sobek.Value {
	t.Helper()

	value, err := runtime.RunOnEventLoop(wrapAtomicScript(body))
	if err != nil {
		t.Fatalf("run atomic script: %v", err)
	}

	return value
}

func wrapAtomicScript(body string) string {
	return "(async () => {\n" + atomicTestPrelude + "\n" + body + "\n})()"
}
