# Changes from upstream spark

This is a modified version of [spark](https://github.com/lucko/spark) by lucko.

This file exists to satisfy GPLv3 §5(a), which requires that modified files carry
prominent notices stating that they were changed and the date of change. Every
modification below is additionally marked inline in the source with a `// fork`
comment so the provenance is obvious when reading the code itself.

This fork is licensed under the **GNU General Public License v3.0 or later**, the same
licence as spark. It cannot be relicensed. The original copyright notices are intact
and have been added to, not replaced.

Forked from spark commit `f181ccf` (upstream version 1.10-SNAPSHOT).

---

## What this fork adds

Upstream spark profiles CPU time and Java allocation. It has no way to see native
(off-heap) memory, which is where a large class of real Minecraft server memory
problems actually live: zlib streams behind `Inflater`/`Deflater`, Netty's pooled
direct arenas, image codec state, and memory allocated inside JNI libraries. The
symptom is a server whose RSS climbs for days while the Java heap stays perfectly
flat — and no existing spark mode can explain it.

This fork adds native memory leak detection, and combines it with CPU profiling and
Java heap leak detection into **one command producing one shareable profile**.

```
/spark profiler start --leaks
```

That single recording captures execution samples, native malloc/free pairs, and
surviving heap objects simultaneously.

---

## Changed files

Date of changes: 2026-08-02

### `spark-common/src/main/proto/spark/spark_sampler.proto`
- Added `SamplerData.native_memory_threads` (field **1100**), `heap_leak_threads`
  (**1101**) and `extended_contents` (**1102**), plus a new `ExtendedProfileContents`
  message.
- Added `SamplerMetadata.SamplerMode.NATIVE_MEMORY = 100` and `COMBINED = 101`.

Field numbers sit in a deliberately high, fenced band (`reserved 1000 to 1099` marks
the reservation). Upstream numbers its fields sequentially and is currently at 8, so
anything low would collide on a future rebase. Because proto3 parsers silently ignore
and preserve unknown fields, a profile carrying these still renders correctly in the
stock spark viewer — it shows the execution profile and simply never knows the rest
is there. That is what allows this fork to keep the standard
`application/x-spark-sampler` content type and a single share link.

### `spark-common/.../sampler/async/jfr/JfrReader.java`
- `MallocEvent` and `LiveObject` made `public` so they can be referenced from the
  parent package. No logic changed.

This file is vendored from async-profiler (Apache-2.0) and its provenance is
unchanged.

### `spark-common/.../sampler/async/AsyncProfilerAccess.java`
- Added `checkNativeMemoryProfilingSupported`. Native memory profiling is not an
  `event=` value and so cannot be feature-detected from the profiler's event list;
  the check is against async-profiler's major version (≥ 4.0), which is where the
  feature landed.

### `spark-common/.../sampler/async/SampleCollector.java`
- Added `SampleCollector.NativeMemory`, which contributes the standalone
  `nativemem=` engine argument. Deliberately does not pass `nofree`, which would
  suppress free events and make every allocation look leaked.

### `spark-common/.../sampler/async/AsyncProfilerJob.java`
- Added an `init` overload accepting extra collectors, whose profiler arguments are
  folded into the same `start` command.
- Added `readNativeMemoryLeakSegments`, which correlates malloc against free by
  address and reports only the survivors. This is the same idea as async-profiler's
  own `jfrconv --leak` / `MallocLeakAggregator`, reimplemented in-process so a
  profile can be produced and uploaded in one step with no conversion tool on the
  server.
- Added `LEAK_TAIL_RATIO` (0.10). Allocations in the final 10% of the recording are
  ignored because they have not had a fair chance to be freed yet; without this a
  genuine slow leak is buried under normal short-lived allocation.
- Added an `aggregate` overload routing each collector's events to its own
  aggregator.

### `spark-common/.../sampler/async/AsyncSampler.java`
- Holds a map of extra collectors to their own aggregators, so CPU milliseconds and
  leaked bytes never end up summed into the same tree.
- Window rotation is disabled whenever a leak engine is active. Rotation stops and
  restarts async-profiler, which discards in-flight malloc→free correlation and the
  live-object set — fatal for leak detection, since a leak is by definition an
  allocation that outlives the window it was made in. Upstream already does this for
  `--alloc-live-only`; this fork extends the same rule.
- Writes the extra trees and an `ExtendedProfileContents` summary.
- Added `isHeapLeakCollector`, used by both the rotation check and the proto export.
  `SampleCollector.HeapLeak` is a sibling of `SampleCollector.Allocation`, not a
  subclass, so the `instanceof Allocation` tests these two sites originally used never
  matched the collector `--heap-leaks` actually registers.

### `spark-common/.../sampler/AbstractSampler.java`
- Added `writeExtraDataToProto`. Kept separate from `writeDataToProto` because that
  method also writes profile-wide data (time windows, window statistics, class source
  mappings) which must be written exactly once, from the primary tree.

### `spark-common/.../sampler/SamplerMode.java`
- Added `NATIVE_MEMORY`. Default interval is 0 (record every malloc): leaks are
  frequently many small allocations rather than a few large ones, and a sampling
  interval silently drops exactly that shape.

### `spark-common/.../sampler/SamplerBuilder.java`
- Added `nativeMemoryLeaks` / `heapLeaks` flags, support checks, and attachment of
  the leak engines. Leak detection requires async-profiler and fails loudly rather
  than silently producing a profile with no leak data in it.

### `spark-common/.../command/modules/SamplerModule.java`
- Added `--leaks`, `--native-leaks` and `--heap-leaks` flags. `--leaks` enables both,
  so the common case ("something is eating memory and I don't know which kind") is
  one command rather than requiring the user to already know the answer.

### Branding
- Deliberately unchanged. The plugin name, command (`/spark`), permission nodes,
  upload User-Agent and jar naming all remain upstream's, so this is a drop-in
  replacement: remove `spark.jar`, add this jar, and everything a server already knows
  about spark keeps working. `plugin.yml` adds the fork author alongside upstream's and
  points `website` at the fork repo.

### `settings.gradle`
- Build scoped to `spark-api`, `spark-common`, `spark-bukkit`, `spark-paper`,
  `spark-velocity` and `spark-geyser`. The Fabric/Forge/NeoForge/Sponge/BungeeCord
  modules are excluded from the build because they drag in the full Minecraft modding
  toolchain and are not needed here. **Their source is untouched** and they can be
  re-enabled by restoring the lines.

---

## New files

Date of changes: 2026-08-07

### `spark-geyser/` (new module)
A Geyser platform module, implemented as a **Geyser extension**. Upstream spark has no
Geyser support at all, so on Geyser Standalone — where Geyser is its own JVM process
rather than a plugin inside a server — there was previously no way to profile the
process from the inside. This module adds one, which matters most for the native
memory work this fork exists for: Geyser Standalone is a long-lived proxy process that
does heavy protocol translation through Netty, exactly the shape of workload where
off-heap growth shows up and the Java heap graph stays flat.

- `GeyserSparkPlugin.java` — implements both Geyser's `Extension` and spark's
  `SparkPlugin`. Enables the platform on `GeyserPreInitializeEvent`, registers each of
  spark's own top-level commands as a Geyser extension command on
  `GeyserDefineCommandsEvent` (so `/spark profiler start --leaks` works as it does
  everywhere else, rather than a single catch-all subcommand), declares spark's
  permission nodes on `GeyserRegisterPermissionsEvent` so Geyser Standalone's
  permissions file can actually grant them, and tears down on `GeyserShutdownEvent`.
  Async work runs on a daemon thread pool owned by the extension, since the Geyser API
  exposes no scheduler.
- `GeyserSparkCommandSender.java` — Geyser's `CommandSource#sendMessage` takes a
  `String`, not an Adventure `Component`, so output is serialized here: ANSI for
  console, legacy section codes for Bedrock players.
- `GeyserPlatformInfo.java` — reports type `PROXY`, brand `Geyser (<platform>)` so the
  profile records which flavour of Geyser produced it, and reads the Geyser version
  reflectively from `GeyserImpl.VERSION` (it is not exposed on the extension API).
- `GeyserPlayerPingProvider.java` — Bedrock-side RakNet ping per connection.
- `GeyserClassSourceLookup.java` — attributes sampled classes to the Geyser extension
  that loaded them, via each extension's own classloader.
- `extension.yml`, `build.gradle` — Java 21 target (current Geyser requires it, unlike
  spark's Java 8 baseline); guava and Adventure are shaded and relocated because Geyser
  makes no guarantee about exposing its own copies to extension classloaders.

No tick hook or tick reporter is provided, because Geyser has no tick loop — so
`/spark tps` reports nothing, the same as on the Velocity and BungeeCord modules.
Everything else, including `--leaks`, `--native-leaks` and `--heap-leaks`, works.

Verified by loading the built jar into Geyser Standalone 2.11.1-b1209: the extension
enables, `/spark` lists its subcommands, `/spark health` and `/spark gc` return real
data, and `/spark profiler start --leaks` produces a profile that decodes as
`Platform: Geyser 2.11.1-b1209 / Profiler: spark + native-memory fork` carrying native
memory leak trees.

---

## Not changed

Everything user-facing. This fork identifies itself as spark because it *is* spark,
with one feature added. Keeping the name means Paper's bundled spark still defers to
it correctly, existing permission grants keep working, and clients can swap the jar
without relearning anything.

## Attribution

spark is itself a fork of [WarmRoast](https://github.com/sk89q/WarmRoast) by sk89q,
also GPLv3. This fork inherits that lineage.

async-profiler (Apache-2.0) is bundled, as in upstream spark.
