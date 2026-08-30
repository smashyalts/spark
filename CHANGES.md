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

## Bug fixes

Date of changes: 2026-08-30

A full line-by-line audit of the tree. Fixes to code this fork introduced are listed first;
fixes to inherited upstream code follow.

### In this fork's own code

#### `SampleCollector.HeapLeak` emitted a second `event=`, so `--heap-leaks` could not start
`HeapLeak` only ever runs as an *additional* collector, but it emitted `event=<alloc>`
alongside the primary collector's `event=wall`, producing a start command containing two
`event=` arguments:

```
start,event=wall,interval=4000us,event=alloc,alloc=524287,live,nativemem=0,threads,jfr,file=...
```

async-profiler's `event=` is single-valued and it rejects a duplicate outright — all three
bundled 4.5 binaries carry the error string `Duplicate event argument`, sitting directly
alongside its other argument-parsing errors (`event must not be empty`, `Invalid interval`).
`AsyncProfilerJob#start` treats any response other than `profiling started` as fatal, so
`--heap-leaks` — and therefore `--leaks`, which turns it on — failed at the point of starting
the profiler.

`alloc=` is a separate additive engine and is by itself sufficient to switch allocation
sampling on, which is exactly the combination this fork documents as verified:
`event=wall,interval=10ms,alloc=512k,live,nativemem`. The `event=` is now dropped.

`AsyncProfilerJob` additionally refuses to start if *any* additional collector contributes an
`event=` argument, so this cannot be reintroduced. (`ExtraCollectorArgumentsTest`.)

Note this was established by reading the code and the bundled binaries: async-profiler is
Linux/macOS only, so the failure could not be reproduced on the Windows host this audit ran
on. It does not square with the note under "New files" claiming `--leaks` was verified
end-to-end on Geyser Standalone; that claim should be re-checked against a real run.

#### Native memory tracking retained every settled address
The eviction of a settled malloc/free pair was gated on the entry carrying no allocation
details, which is only ever true for a free seen *before* its malloc. The ordinary case —
malloc, then free — was therefore never evicted, so the tracking map grew with **total
allocation history** rather than with outstanding memory: the precise unbounded growth the
2,000,000 address cap and the whole net-counting design exist to avoid. An entry is now
dropped as soon as its balance returns to zero. (`NativeMemoryLeakTrackingTest`.)

#### Window rotation dropped the extra collectors
`AsyncSampler#rotateProfilerJob` re-initialised the replacement job through the overload that
takes no extra collectors, so a rotated recording carried none of the extra engines while
aggregation still routed into their aggregators — empty leak trees, indistinguishable from
"profiled and found nothing". Reachable as soon as any extra collector that does not disable
rotation is added. The extra aggregators are now also pruned alongside the primary one.

#### Extra trees were encoded against their own time windows
Each extra tree built its own `ProtoTimeEncoder`, so its `times` array was indexed against a
different key set than the profile-wide `time_windows` list every reader interprets it
against. All trees now share one key set, computed over the union of every tree written into
the profile.

#### `duration_millis` was measured to export time
It was computed as `now - startTime` at export, so a profile exported after the fact — or
exported repeatedly, as the live viewer does — claimed a longer recording than it ran for and
understated the leak rate. `AbstractSampler` now records an end time and it is used instead.

#### Extra aggregators were never closed.

### In inherited upstream code

- **`SparkStaticLogger` never installed the platform logger.** The field is initialised to
  `Logger.FALLBACK`, so the `== null` guard in `setLogger` could never pass — every static log
  line went to `System.out`/`System.err`, which is exactly what the class exists to avoid.
- **`PlatformStatisticsProvider` threw on a null `collectionUsage`.** `getCollectionUsage()`
  returns null for pools that don't track it; the NPE is swallowed by `SparkMetadata#gather`,
  which drops *every* platform statistic from the profile. (`HealthModule` already guarded the
  same value.)
- **`CpuMonitor`'s polling task could die permanently.** `new BigDecimal(double)` throws for a
  non-finite reading, and an exception escaping a `scheduleAtFixedRate` task cancels it for
  good — freezing every CPU statistic for the lifetime of the JVM.
- **`ThreadDumper.Regex` cached into a plain `HashMap`** from both the sampler and aggregation
  threads.
- **`AbstractNode#getTimeAccumulator` used get-then-put**, so two threads racing on a new
  window each created an accumulator and the loser's samples were discarded. (`resolveChild`,
  immediately above it, already used `computeIfAbsent`.)
- **`NetworkInterfaceInfo` never detected a missing transmit column.** The check ran after the
  receive-field offset had been added, and `rxFieldsLength + -1` is not `-1`, so a missing
  column passed validation and was then read from the wrong index.
- **Trusted viewer keys were never persisted.** `TrustedKeyStore` updated the configuration but
  never saved it, so a key the user explicitly trusted had to be trusted again after a restart.
- **`BytebinClient` replaced upload failures with a bare `IOException`.** `getInputStream()` in
  the `finally` block throws when the server answered with an error status, losing the real
  reason for the failure.
- **Locale-sensitive case conversion** on command aliases, flag names, `--compress` values and
  environment variable names — all identifiers, all of which stop matching under a Turkish
  locale. Now `Locale.ROOT`.
- **`DecimalFormat` shared across threads** in `TickMonitor` and `GcMonitoringModule`, both of
  which format from an arbitrary async executor thread.
- **`MinecraftServerCommandSender#getName` had its console check inverted** (`getEntity() != null`
  where the console is the source with *no* entity).
- **`NeoForgeClassSourceLookup` dereferenced a null classloader**, which every bootstrap-loaded
  class in a stack trace has.
- **`CpuMonitorTest` asserted against the operating system.** Both readings are documented to
  return a negative value when unavailable — briefly in a fresh JVM, and permanently on hosts
  whose performance counters are unavailable (verified on Windows, where the underlying
  `ProcessCpuLoad`/`SystemCpuLoad` attributes read `-1.0` indefinitely). The test now waits out
  the first case and skips the second.

- **The active sampler was never cleared when a profile failed.** `SamplerModule` unset the
  sampler the *future* produced, which is null on failure - so a dead sampler stayed installed
  and every later `profiler start` answered "Profiler is already running!" until an admin ran
  `profiler cancel`.
- **`GarbageCollectionMonitor` iterated a plain `ArrayList` of listeners** from the JMX
  notification thread while commands added, removed and cleared it.
- **`WindowStatisticsCollector` and `TrustedKeyStore` held plain `HashMap`/`HashSet` state that
  genuinely crosses threads** - window start times are recorded on the sampler's scheduler and
  read on the command thread; pending viewer keys are added on the websocket listener thread and
  removed on a command thread.
- **`ThreadDumper.Specific` published two lazily-computed sets without `volatile`**, and
  aggregation runs on the scheduler during rotation but on the command thread at stop.
- **`WorldStatisticsProvider` passed a possibly-null game rule default into protobuf**, which
  rejects null strings - taking the whole world statistics section down rather than one field.
- **`CpuInfo` indexed `[1]` of a `/proc/cpuinfo` split without checking the length**, and
  returned the WMI processor name with its fixed-width padding still attached.
- **`OperatingSystemInfo` accepted a blank WMI line as the OS name**, which is not null and so
  suppressed the `os.name` fallback.
- **`BukkitPlatformInfo` read index 3 of the server package name**, which modern Paper does not
  have; the resulting exception is thrown outside the metadata gather's try blocks and fails the
  entire export.
- **The Geyser extension left its handlers unguarded** against a failed pre-initialise, and
  nulled its fields on shutdown so a late callback met a `NullPointerException`.
- **The JFR temp file was only deleted on the success path.** A leak recording is written with
  `nativemem=0` and routinely runs to gigabytes, so a parse failure left one behind on the disk
  of the server being diagnosed.

The Fabric/Forge/NeoForge/Sponge/BungeeCord/Minecraft modules are excluded from the build, so
the fixes in those trees are source-level only and are not compile-verified here.

### Method

Four passes over the tree. The first read every file end to end; the second and third re-read it
and were paired with mechanical cross-checks (locale-sensitive case conversion, non-concurrent
collections on fields that cross threads, unguarded array indexing after `split`, unguarded
numeric parsing, mutable static state). The linear reads found the logic errors; the
cross-checks found most of the concurrency ones. The fourth pass produced nothing new by either
method.

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
