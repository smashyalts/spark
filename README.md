<h1 align="center">spark (native memory fork)</h1>

<h3 align="center">
  A fork of <a href="https://github.com/lucko/spark">spark</a> that adds native memory and heap leak detection.
</h3>

---

## What this fork adds

Upstream spark profiles CPU time and Java allocation. It never looks at `malloc`, so
off-heap memory is invisible to it — and that is where a large class of real Minecraft
server memory problems live: zlib streams behind `Inflater`/`Deflater` that were never
`end()`ed, Netty direct buffers never `release()`d, image codec state never `dispose()`d,
memory allocated inside JNI libraries. The symptom is a server whose RSS climbs for days
while the Java heap graph stays flat, and then gets OOM-killed.

This fork adds native memory leak detection and folds it together with Java heap leak
detection into the existing profiler command.

```
/spark profiler start --leaks --timeout 3600
```

| Flag | What it adds |
|---|---|
| `--leaks` | both native and heap leak detection |
| `--native-leaks` | off-heap only |
| `--heap-leaks` | Java heap retention only |

Everything else behaves exactly like upstream spark. Same command, same permissions,
same viewer links. It is a drop-in replacement: remove `spark.jar`, add this jar.

## How it works

async-profiler's `event=` is single-valued, but `alloc=` and `nativemem=` are separate
additive engines, so one recording can carry execution samples, live-object allocation
samples and malloc/free events simultaneously. Verified against async-profiler 4.5: a
single run of `event=wall,interval=10ms,alloc=512k,live,nativemem` produced
`profiler.WallClockSample`, `profiler.Malloc`, `profiler.Free`, `profiler.LiveObject`
and `jdk.ObjectAllocationInNewTLAB` in one JFR, with native leak totals matching a
nativemem-only run of the same workload to within 4%.

A native leak is an allocation with no matching free. This fork correlates malloc
against free by address in-process — the same approach as async-profiler's
`jfrconv --leak`, but built in, so no conversion tool is needed on the server. Verified
against a real JFR it produces byte-identical totals to `jfrconv --leak`.

Allocations in the final 10% of the recording are discarded: they have not had a fair
chance to be freed yet, and without that correction a genuine slow leak is buried under
normal short-lived allocation.

## Things to know

**Window rotation is disabled during leak detection.** A leak is by definition an
allocation that outlives the window it was made in, so spark's usual 60-second rotation
would report almost everything as leaked while losing the long-lived allocations that
matter. Consequence: `--leaks` cannot be combined with `--only-ticks-over`, and says so
rather than producing wrong data.

**Leak detection requires async-profiler**, so Linux or macOS, not Windows. It fails
loudly rather than handing back a profile with no leak data in it.

**`nativemem` intercepts `malloc`/`realloc`/`calloc`/`free`, but not `mmap`.** Metaspace,
the JIT code cache, GC structures and `MappedByteBuffer` are largely invisible. For a
suspected classloader/metaspace leak from repeated `/reload`, use NMT instead.

**Not all native growth is a plugin's fault.** glibc arena retention can show over a
gigabyte of RSS with no leak at all, and Netty's pooled arenas grow to peak load and
stay there by design. A plateau is normal; monotonic growth is suspicious.

## Data format

Profiles keep the standard `application/x-spark-sampler` content type, so links still
open in the normal spark viewer — which renders the CPU profile and silently ignores the
leak data it doesn't know about (proto3 ignores unknown fields). Reading the leak data
needs a consumer that understands the extended schema; field numbers are in `CHANGES.md`.

## Building

```bash
./gradlew :spark-bukkit:build
```

Requires JDK 21. Output is `spark-<version>-bukkit.jar` — **use the bukkit jar**, on
Paper too. The `spark-paper` module is the library Paper bundles inside the server jar
and has no `plugin.yml`; it is not a drop-in plugin.

## Licence

**GPL-3.0-or-later**, same as spark — this cannot be relicensed. Original copyright
notices are intact. `CHANGES.md` lists every modification, as GPLv3 §5(a) requires.

spark is itself a fork of [WarmRoast](https://github.com/sk89q/WarmRoast) by sk89q, also
GPLv3. Bundled async-profiler is Apache-2.0.

Not affiliated with or endorsed by lucko. Please don't report this fork's bugs to the
spark issue tracker. Upstream's README is preserved as `README.spark.md`.
