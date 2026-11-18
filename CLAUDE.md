# CLAUDE.md — mirage

Local, self-grading incident-response training for **KCD** (Kafka Connect Decorator) running
on Strimzi Kafka Connect. Two real Kafka clusters, a real Connect worker, a real
MirrorSourceConnector between them, and 19 labs that break something and grade whether you
fixed it.

## Purpose

Preparation for on-call incidents, weighted toward **connector** problems (auth,
config, SMTs, offsets, poison records) rather than cluster administration. The labs are
practice for judgement under uncertainty, not a kubectl quiz.

### Source of truth for what is worth drilling

The official KCD Kafka Connect Troubleshooting Guide (internal wiki) is the triage index
on-call actually pages on. Its five symptom classes map onto the labs:

| Guide section | Labs |
|---|---|
| §1 connector with no tasks | 5 (pattern matches nothing), 9 (paused) |
| §2 connectors with failing tasks | 10 (missing SMT class), 16 (broker refuses), 17 (producer refuses), 18 (broker refuses on timestamp) |
| §3 connectors missing | 11 (`group.id` / storage topics renamed) |
| §4 message loss | 4 (offsets keyed by name), 8 (upstream outage) |
| §5 KCD and topic lag | 1 (single task), 3 (heap), 6 (quota) |
| auth / certs (via §1, §4) | 13 (SCRAM password), 14 (ACL), 15 (truststore) |
| not in the guide: change merged, never applied | 19 (Strimzi operator heap-OOM crashloop; pdm-eks-utilities #4541) |

Lab 19 breaks the **operator**, which every other lab depends on. `restore_operator` in
`lib/common.sh` runs first in `restore_baseline` and `recreate_connect_cluster`, so a reset or
repair after lab 19 cannot report a baseline that nothing reconciled. Its fault is
`JAVA_OPTS=-Xmx24m` added with `kubectl set env`; `kubectl apply` of the vendored Deployment does
**not** remove an out-of-band env var, so the restore removes it explicitly.


Before adding a lab from a doc, **verify the fault reproduces here**. Two documented
scenarios do not, and a lab teaching a fault this environment cannot produce is worse than
no lab:

- **Partition mismatch (§4).** Tested: target rebuilt at 3 partitions against 12 on source,
  2000 records produced, 2000 replicated, tasks green. The documented loss depends on
  `HashPartitionerAssignor` and key-based routing; `curl localhost:8083/connector-plugins`
  shows no `com.mirageduel` classes in the vanilla Strimzi image.
- **Network/security-group blocks (§1).** No way to express a VPC SG in kind.

---

## Layout

| Path | What it is |
|---|---|
| `mirage` | the CLI — all `cmd_*` functions and the dispatch table at the bottom |
| `lib/common.sh` | assertion engine, kubectl/Kafka helpers, `KUBECONFIG` pinning, REST-API helpers |
| `lib/provider.sh` | kind/minikube abstraction — the ONLY file that names a provider |
| `lib/debriefs.sh` | post-lab debriefs for `mirage why <n>`, one `case` branch per lab |
| `labs/NN-name.sh` | one lab each: metadata + 5 functions |
| `manifests/*.yaml` | the baseline environment (Kafka, Connect, connector, topic, users, ingress) |
| `run-all-labs.sh` | regression harness — applies the documented answers and grades them |
| `vendor/strimzi-0.47.0/` | pinned operator manifests, namespace rewritten to `mirage` |
| `vendor/ingress-nginx-1.11.3/` | pinned controller, baremetal variant, admission webhook stripped |
| `.state/` | gitignored: kubeconfig, provider, per-lab results, injection logs, scratch marks, grade logs |

### Anatomy of a lab

Every `labs/NN-*.sh` defines exactly this contract:

```bash
LAB_TITLE="..."        # short, shown in `list`
LAB_DIFFICULTY="..."   # easy | medium | hard | exam
LAB_ALERT="..."        # the real Datadog monitor this maps to

lab_brief()   # symptom, the job, where to start. No answers.
lab_break()   # inject the fault. MUST return non-zero if it cannot. Says nothing
              # about WHAT it injected — see "lab_break must not narrate the fault".
lab_check()   # assert_* calls. Ends with a throughput check.
lab_hint()    # blank-line-separated blocks, general -> specific
lab_solve()   # the full answer, as runnable commands
```

Then add a `case` branch to `lib/debriefs.sh` and the number to **both** `LAB_ORDER` in
`mirage` and the default `LABS=(...)` in `run-all-labs.sh`. Forgetting the second is how
lab 16 was initially absent from the regression run.

### `mirage stop` only grades the active lab

`cmd_stop` refuses any lab number that is not `.state/active`, and refuses outright when no
lab is active. Both exist because grading the wrong lab produces a report that looks like a
lab bug: `stop 3` while lab 4 was injected reported `autoRestart unset` and
`tasks running: 0` — lab 4 had deleted and renamed the connector lab 3 grades — next to
`records replicate 2000 >= 1600`, because lab 4's connector was doing the mirroring. Nothing
in that output points at the actual mistake. It also overwrote lab 3's recorded result with
a meaningless FAIL.

With nothing active the cluster is at its baseline, so a grade measures a healthy
environment; many assertions pass there (every throughput check does), which would report a
PASS nobody earned.

`run-all-labs.sh` sets **`MIRAGE_FORCE_STOP=1`** to bypass both guards. It grades twice per
lab on purpose, and `cmd_stop` calls `clear_active` on a PASS — so after a pre-fix *false*
pass the post-fix call would be refused for "nothing active" and the false pass, the exact
thing that check exists to catch, would be reported as a missing lab instead.

### `lab_break` must not narrate the fault

`lab_break` output is shown to the trainee, so naming what it injected hands over the
answer before they look at anything. `info "Setting -Xmx to 2048m against a 1024Mi
container"` is the whole of lab 3.

- Say `info "Injecting the fault"` — or `Injecting fault N/M` for an exam lab.
- Progress and timing are fine: `Generating load`, `Waiting for the stall to establish
  (~60s)`, `Waiting for the workers to roll (~60s)`. Phase names must not carry the cause,
  so `Letting the fault take hold`, not `Letting the recursion take hold`.
- Setup that is not the fault may be described (`Seeding a baseline and letting it
  replicate`), as may observable state the trainee could read themselves (`source=N
  target=M`).
- Numbers only `lab_break` knows, and that the fix depends on, belong in a `mark` for
  post-mortem debugging, not on stdout — see `lab6.sizing`.
- Comments in `lab_break` are for the reader of the source, not the trainee: explain the
  fault there freely.

---

## Environment facts worth knowing before changing anything

- Namespace `mirage` (NS in lib/common.sh; the vendored Strimzi RoleBindings are rewritten
  to match, so changing it means rewriting those too). Clusters `kafka-source` / `kafka-target`, pools `source-pool` /
  `target-pool` (**never share a pool name** — the second silently overwrites the first).
- `KafkaConnect` is `mirage`; the connector is `lab-orders-from-source-to-target`; the topic
  is `lab-orders` with **12 partitions** (lab 1 depends on that number).
- Target has two listeners: `:9092` plaintext and `:9094` TLS + SASL/SCRAM-512, with
  `authorization: simple` and `ANONYMOUS` as superuser — so the plaintext path keeps full
  access and non-security labs are unaffected.
- `KafkaUser`s `kcd-replicator` (can write) and `kcd-readonly` (cannot) back labs 13–15.
  Applied by `setup` **and** `restore_baseline`.
- Brokers need **1536Mi** each, set on the `Kafka` CR (not the `KafkaNodePool`). At 768Mi the
  target OOMKills under sustained load, which surfaces as a replication deficit with nothing
  pointing at the broker.
- Needs **8+ cores / 16GB** in colima. At 4 cores the API server returns `TLS handshake
  timeout` and it looks like broken labs. A colima restart **destroys** the kind cluster, so
  resize before `setup`.
- `lib/common.sh` exports `KUBECONFIG="$STATE_DIR/kubeconfig"`, so labs can never touch a
  real cluster. Do not remove that.
- The cluster is built by **kind or minikube**, chosen by `provider_detect` in
  `lib/provider.sh` (`MIRAGE_PROVIDER` > `.state/provider` > kind > minikube) and recorded in
  `.state/provider` so `teardown`/`repair` in a later shell act on the right one. Every
  `kind`/`minikube` invocation in the repo lives in that one file — keep it that way.
- The Connect REST API is exposed through **ingress-nginx** (vendored, baremetal variant,
  admission webhook stripped) and a Host-routed `Ingress` at `mirage.local`, reached
  by a harness-managed `kubectl port-forward` on `localhost:8080`. `api_curl`/`api_up` in
  `lib/common.sh`; `mirage api` re-establishes it.
- **The host's Kafka CLI reaches a broker on `localhost:9092`**, set up by `setup`,
  `start`, `repair` and `verify`, and switched with `mirage kafka [source|target|stop|
  status]`. It is a convenience: nothing in a lab or grader may depend on it.
  - It forwards the **broker pod** on the **same local port it advertises**, not
    `svc/..-kafka-bootstrap` on an arbitrary port. A client bootstraps once and then
    reconnects to the advertised listener — internal cluster DNS such as
    `kafka-source-source-pool-0.kafka-source-kafka-brokers.mirage.svc:9092`. Verified: a
    bootstrap-Service forward on 19092 bootstraps and then dies with
    `UnknownHostException` on every subsequent request.
  - That name must also resolve, which needs a one-time `/etc/hosts` line per side
    (`127.0.0.1 <advertised host>`). `/etc/hosts` needs root, so this is the one half the
    harness cannot do: `kafka_hosts_entry_ok` reports it and prints the `sudo tee` line
    rather than claiming success.
  - **One side at a time.** Both clusters advertise 9092, and a second loopback address
    needs `sudo ifconfig lo0 alias` on macOS (`127.0.0.2:9092` fails to bind), so
    `kafka_pf_start` switches sides instead of running two forwards.
  - **A second forward on another local port does NOT give you the other cluster — it
    silently gives you the wrong one.** Tested: with the source forwarded on 9092 and the
    target on 9093, `kafka-topics --bootstrap-server localhost:9093 --list` returned the
    **source's** topic list, no error. The client bootstraps on 9093, learns the advertised
    `...:9092`, and reconnects there — into the other cluster's forward. Same result when
    bootstrapping via the target's own advertised hostname on :9093. `mirage kafka both`
    therefore refuses and explains, rather than offering something that lies.
  - `advertisedPort` on an internal listener *does* work (verified: the broker listened on
    9092 and advertised 9192) but it **broke the mirror** — the connector's task counts went
    empty, since Connect reaches the broker by the advertised address too. Do not use it to
    free up a local port.
  - **`mirage kcli <source|target> <tool> [args]`** sidesteps all of this: it execs the
    broker's own `bin/kafka-<tool>.sh` inside the pod with the right
    `--bootstrap-server`, so BOTH clusters are queryable at once with no forward and no
    switching. It rejects a caller-supplied `--bootstrap-server`, because a stale one is
    exactly how you read the wrong cluster and believe the answer.
  - **Which cluster a forward serves is proved by `cluster.id`, never assumed.** Adopting a
    reachable forward without checking identity produced the worst possible output:
    `mirage kafka target` adopted the source's forward, wrote `side=target` plus the
    target's uid, and printed `ready -> target broker` while every query returned source
    data. `kafka_pf_serves_side` compares the id read through the forward
    (`kafka-cluster cluster-id --bootstrap-server 127.0.0.1:9092`) against the id read
    inside the broker pod, and `mirage kafka` / `kafka status` both print the verified id.
    Do **not** discriminate by topic list — labs create and delete topics, so
    "does it have `__consumer_offsets`" is lab-dependent and wrong half the time. The ids
    are immutable: source `2EJ366RyRQe9EzolnammHA`, target `aeKS_WQGRrWu7WC_8q_I6A` on the
    current cluster (they change on rebuild, so read them, do not hardcode).
  - **A forward dies with its pod, and "port is held" does not mean "forward works."**
    `kubectl port-forward pod/...` targets one pod instance; when a lab replaces the broker
    it logs `failed to find sandbox "<id>" in store: not found` / `error: lost connection to
    pod` and every client gets `Connection to node -1 (localhost/127.0.0.1:9092) could not
    be established`. The pidfile still names the dead process. So health is a real TCP
    connect (`kafka_pf_connects`, via bash `/dev/tcp`) plus the forwarded pod's **UID**
    recorded in `.state/kafka-pf.uid` — names are useless, the broker returns as
    `kafka-<cluster>-<pool>-0` either way.
  - **A healthy forward is adopted, never torn down.** Reuse is allowed when the port
    accepts a connection and our bookkeeping does not actively disagree: missing side/uid
    files mean incomplete records, not a broken forward. This matters because
    `kafka_pf_stop` deletes the pid/side files while `kill` is denied for a process the
    shell did not spawn — so a forward routinely outlives its own records, and refusing to
    adopt it reports failure while `kafka-topics` works fine. Only a uid that *disagrees*
    with the live pod forces a rebuild.
  - Match the hosts entry by **space-delimited fields, not one regex**. A dotted hostname
    is full of regex metacharacters, and the obvious
    `127\.0\.0\.1[[:space:]]+.*([[:space:]])$host` never matches the normal single-space
    form, because the `.*` demands a second separator before the hostname.
- **A port-forward outlives `teardown`.** `cmd_teardown` does `rm -rf "$STATE_DIR"`, which
  deletes `api-pf.pid` (and `kafka-pf.pid`) — so a forward from the previous cluster keeps
  listening on 8080 or 9092, routing to a namespace that no longer exists. `api_start` then
  fails to bind
  (`unable to listen on any of the requested ports`) while `api_up` also fails, which looks
  like a broken ingress. `api_port_holder`/`api_stop_orphan` reclaim the port, and only when
  it is both occupied and unreachable, so a working forward is never taken away.
  `kafka_port_holder`/`kafka_pf_stop_orphan` are the same pair for 9092.
- **Strimzi's NetworkPolicy blocks the ingress, and kindnet DOES enforce it.** Strimzi
  generates `mirage-connect` admitting 8083 only from the Connect pods and the cluster
  operator, which excludes the `ingress-nginx` namespace: nginx logs `upstream timed out
  (110: Operation timed out) while connecting to upstream` and the request hangs, while the
  API answers 200 from inside the pod and from its own Service. `manifests/ingress.yaml`
  adds a second, additive policy for the `ingress-nginx` namespace; `cmd_api` re-applies it
  if missing. It must stay a separate object — the operator owns `mirage-connect` via an
  ownerReference and reverts edits to it, and an unowned policy also survives
  `recreate_connect_cluster`. Verified by deleting the policy (request returns 200) and
  waiting ~3s for the operator to recreate it (timeout returns). Do not "simplify" this away
  on the belief that kindnet ignores NetworkPolicy — that claim was tested and is false
  here.

---

## How to work on this

### Always
- Run `bash -n` on every script you touch. There is no test suite but syntax errors are free
  to catch.
- After changing a lab, run `./run-all-labs.sh <n>` for it, plus its route variants
  (`ALL_VARIANTS` in the script; `N:name` runs `fix_N_name`, and `bad_*` must still FAIL).
  After changing `lib/common.sh`, run the full suite — the assertion engine is shared by
  all 19. `--all` runs every lab then every variant.
- `./run-all-labs.sh` holds a lock at `.state/run-all-labs.lock`. **Never run two at once**:
  they reset and patch the same cluster and silently invalidate each other.
- Prefer `mirage repair` over `teardown && setup`. Repair rebuilds Connect in ~7 minutes and
  keeps Kafka data; a full rebuild re-pulls images.

### Never
- Do not gate background work on `pgrep` — it cannot read the process list in this sandbox
  (`pgrep: Cannot get process list`, exit 3), so the guard falls through immediately. Chain
  with `&&`, or poll `.state/run-all-labs.lock`. `ps` is blocked the same way, and `kill`
  (including `kill -0`) is denied for processes the sandbox did not spawn — so
  `api_pf_alive` cannot be exercised from an agent session even though it is correct in a
  real terminal. Use `lsof -nP -iTCP:<port> -sTCP:LISTEN` to see what holds a port.
- Do not use `/tmp`; use `$TMPDIR`.
- Do not put `spec.tls.trustedCertificates` on the `KafkaConnect` CR. It reconfigures the
  *worker* for TLS, which then times out against the plaintext bootstrap. Mount the CA with
  `template.pod.volumes` instead.
- Do not delete `mirage-connect-{configs,offsets,status}` from the target. They are the
  worker's live internals; the Connect cluster stores its own state on `kafka-target`.
- Do not delete `lab-orders` on the **source** cluster, by any route. The `KafkaTopic`
  operator does **not** recreate it (tested: still absent after 60s), `mirage reset --fresh`
  then reports "Baseline restored but replication is NOT working", and the worker is left
  crashlooping. `mirage kcli` refuses this one delete and points at `mirage reset --fresh`,
  which rebuilds it at the load-bearing 12 partitions. Deleting the target copy is fine —
  the mirror recreates it.
- Do not read `Unexpected error from SyncGroup` in the worker as a Connect fault. It is a
  BROKER problem and survives a full `mirage repair`, because the corruption is in the
  target's group coordinator. The real error appears only in the broker log:
  `IllegalStateException: New last written offset 33 of __consumer_offsets-7 must be
  greater than 1030` — a rewound coordinator partition, caused here by restarting the
  broker mid-write. Fix: delete the target broker pod so the coordinator reloads from its
  log, then restart the Connect worker. When Connect keeps crashlooping after
  `recreate_connect_cluster`, read the broker log before rebuilding anything else.
- Do not reach the Connect REST API from a `lab_check`. `api_curl` depends on the ingress
  controller and a port-forward; graders must survive both being down, so the assertion
  engine uses `runtime_config_value`, which execs into the worker. A grader that goes
  through the ingress can fail a correct fix, or pass while the pipeline is dead.
- Do not add `-n` to the `kubectl apply` for the vendored ingress-nginx manifest. It carries
  its own `ingress-nginx` namespace, and kubectl errors when `-n` disagrees with it. The
  Strimzi apply is the opposite case and does need its `-n`.

---

## Writing a check that actually works

This is the hard part of this repo, and most of the bugs found so far were here. A grader
that passes while the fault is live is worse than no grader.

**The failure modes, all of which have happened:**

1. **Comparing cumulative `src_count` to `tgt_count`.** At 60k records a 2000-record gap is
   3% — under any tolerance, so the check can never fail. Use `replication_delta`.
   The inverse bites just as hard and reached a trainee: lab 17 asserted
   `src_count - tgt_count <= 20` to prove records were delivered, which is **unsatisfiable**
   on a topic that has been re-seeded. `truncate_topic` restarts the target at the current
   offset, so the deficit is permanent history — 3211 records in the session that caught it
   — and a *correct* fix graded FAIL. It passed every harness run because
   `run-all-labs.sh` does `reset --fresh` first, so the gap started near zero. Measure a
   DELTA over a batch you just produced; history then cancels on both sides.
   `mirage reset` says this out loud ("the N-record deficit is history the target will never
   get back"), which is the signal that an absolute gap threshold is wrong.
2. **Percentage tolerance with no absolute cap.** 2% is right at 5k records and swallows the
   entire gap at 1M. Cap it.
3. **"Target grew by at least 1."** One heartbeat satisfies it.
4. **Counting tasks without naming the connector.** A stray connector from a loop lab has
   perfectly healthy tasks.
5. **`assert_eq running total` when both can be zero.** `0 == 0` passes. Require `total >= 1`.
6. **No throughput check at all.** Green task state is not a working pipeline.
7. **Trusting the CR when the fault is in the runtime.** Connect rejects bad config with a 400
   and keeps running the old version. Use `patch_connector_runtime` / `runtime_config_value`.
8. **Injecting a fault built from an empty value.** Labs 13–15 read a `KafkaUser` password;
   when the Secret was missing they injected `password=""`, taught the wrong failure, and
   could not be passed. Assert inputs before breaking anything (`require_scram_users`).
9. **Whole-topic throughput when the fault is per-partition.** Lab 16 stalls ONE partition;
   the other 11 carry the load, so a topic-wide delta reads healthy. Compare per-partition
   offsets before and after.
10. **Reading the worker log without a time boundary.** The Connect pod outlives a lab, so
    its log still holds the PREVIOUS lab's errors. Labs 13/14/15 assert "no **fresh**
    `SaslAuthenticationException` / `TopicAuthorizationException` / `SSLHandshakeException`"
    — running 14 after 13 read 13's auth failure and failed a correct fix. Use
    `worker_log_matches` / `worker_log`, which scope to the epoch (below), never a bare
    `kubectl logs --tail` in a grader.
11. **Throughput measured on the topic, satisfied by someone else's connector.**
    `replication_delta` counts records on the target topic; it cannot tell WHICH connector
    moved them. A leftover connector from another lab keeps mirroring `lab-orders`, so the
    closing throughput assertion passes while this lab's own connector is absent or dead.
    Observed: lab 4's `lab-orders-renamed` outlived its lab and lab 3 then graded
    `tasks running: 0` and `records replicate: 2000 >= 1600` in the same report.
    `cmd_start` now deletes the known renames, and every lab whose fault does not involve a
    second connector calls `assert_no_foreign_connectors` before its delta. Labs 7 and 12
    are exempt: creating extra connectors IS their fault, and their own checks cover it.

**Rules that follow:** every `lab_check` ends with a throughput assertion; measure deltas not
totals; name the connector in task counts; when the fault is scoped narrower than the topic,
scope the check the same way; scope every log read to the lab that is running; and before
trusting a throughput delta, assert that only this lab's connector could have produced it.

### Each lab starts on a fresh worker log

Two mechanisms, because they solve the problem for different readers.

**1. The pod is recycled.** `cmd_start` calls `recycle_connect_worker` before
`settle_connector`, deleting the Connect pod so the lab begins on an empty log. This is what
makes a trainee's own `kubectl -n mirage logs mirage-connect-0` correct — nothing can
intercept a user's kubectl, so the log itself has to be clean.

- Safe because the worker keeps no state in the pod: `config.storage.topic`, offsets and
  status live on `kafka-target`, so the replacement replays them and the connectors return.
- ~50s, absorbed by the settle wait that follows it.
- Compare the pod by **UID, not name**. The worker is a StrimziPodSet member and comes back
  as `mirage-connect-0` either way, so a name comparison never sees the replacement and the
  wait always times out. This was the first version's bug.
- It does not disturb the restart-sensitive labs: lab 3 grades a `restartCount` **delta**
  measured during grading, and a fresh pod at 0 still deltas to 0; lab 11 rolls the workers
  itself.
- A recycled pod has no `--previous`, so the old container's log is gone for good. That is
  the intent.

**2. The log epoch.** `log_epoch_set` stamps a UTC RFC3339 time into `.state/log.epoch`
immediately before `lab_break`; `worker_log` passes it as `--since-time`. The recycle happens
before the baseline settles, so the worker still logs its own boot and baseline traffic
before the fault goes in — the epoch narrows graders to lines the *fault* produced.

- Never add a bare `kubectl logs` to a lab or grader — go through `worker_log`.
- `worker_log_matches` returning `0` for an empty window is correct — a worker with nothing
  to say since the epoch has no errors. It re-checks that the log is readable at all before
  calling it `0`, so a broken fetch still reports `unreadable`.
- `cmd_reset` and a failed injection clear the epoch; with none set, reads fall back to a
  plain tail.
- **No `--tail` when an epoch is set.** Lab 13's admin client spins "Rebootstrapping" at
  ~50 lines/s after the auth failure (32k lines in 5 minutes), so a 3000-line tail lost
  every `SaslAuthenticationException`: `mirage logs --errors` said "no known failure
  signatures" with the fault live.
- **"No FRESH errors" counts from `grade.start`, not the epoch.** `cmd_stop` stamps it
  before `lab_check`; `worker_log_matches` reads from there. From the epoch, the fault's
  own pre-fix errors are always in range and a correct fix can never pass — it only did
  because the tail cap happened to cut them off. `lab_break` (no `grade.start`) still
  counts from the epoch.

---

## The harness is not proof the answer works

Each `fix_N` in `run-all-labs.sh` is a *reimplementation* of `mirage solve N`, not the same
code. Where they drift, a lab passes while its documented answer is broken — labs 7 and 12
both had a cleanup-ordering bug in their solves, and only lab 7 failed, because `fix_12`
happened to order it correctly.

Lab 12 then drifted the other way, which is worth reading as a pair with the above.
`mirage solve 7` tells you to rebuild the Connect cluster, because a deleted loop
connector's config is replayed from the compacted `config.storage.topic` and keeps
recreating `mm2-offset-syncs.target2.internal`. **`solve 12` injects the same
`target2` loop and never said to rebuild**, and `fix_12` did not either — so neither the
documented answer nor the recipe actually cleaned up, and the grader was right to fail it.
Measured: after deleting `lab-stray`, `mirage-connect-configs` still held 6 records naming
`target2`; `recreate_connect_cluster` took that to 0 and the artefact stayed gone.

It had passed every previous run because the per-lab worker recycle rolled the worker at a
convenient moment. Setting `MIRAGE_SKIP_RECYCLE=1` removed that accident and exposed the
real gap — **a lab that only passes because of an unrelated restart is not passing**. When
two labs inject the same fault shape, diff their solves; the one that was written second
tends to be missing a step the first one learned.

**When you change a solve, mirror it in the recipe or read the solve directly.** A green run
is not evidence the written answer works.

**The harness also runs on a cleaner cluster than a trainee ever will.** Every lab is
preceded by `reset --fresh`, which truncates the topic, so counts start near zero and no
historical deficit exists. A check that depends on that — any absolute record-count
threshold — passes 17/17 in the harness and fails the first person who runs two labs in a
row without truncating. Lab 17 shipped that way. Before trusting a green run, ask what the
check would read on a cluster that has been in use for an hour, and test it there: inject
the fault by hand on the current cluster, run the assertion's logic, and confirm it fails;
then apply the fix and confirm it passes.

`run-all-labs.sh` keeps each injection's output at `.state/inject-<n>.log` and surfaces any
`!!` warning from `lab_break` before grading. Read that file first when a post-fix grade
fails: it holds the numbers the lab computed. This exists because a lab 6 FAIL was once
undiagnosable — the injection was piped to `/dev/null`, so whatever `lab_break` said about
it was gone, and the failure never reproduced.

---

## Traps the labs themselves teach (and that bit the implementation)

- **Config pushes are asynchronous.** Restarting a connector before the runtime carries the
  new value makes tasks retry the old one and exhaust `autoRestart` (maxRestarts: 5). Poll
  `runtime_config_value` first. This was wrong in three solves.
- **An unthrottled loop kills the whole cluster, not just the lab.** The same-cluster
  identity mirror in labs 7 and 12 re-reads what it writes; unbounded it put 52G on the
  target in ~8 minutes, filled the 98G kind disk, and BOTH brokers crashlooped with
  `No space left on device` (they share the node disk), then Connect followed — it looked
  like pods dying on their own. `throttle_loop_client` sets a broker producer quota on the
  loop's `producer.override.client.id` (`mm2-target-target2`) before the loop exists:
  ~500 rec/s, still an obvious loop. Any new loop lab must use it. Recovery from a full
  disk: delete the loop connector, delete both broker pods (ephemeral storage is wiped),
  `mirage repair`, `mirage reset --fresh`.
- **On Strimzi, renaming the Connect cluster's identity does NOT make connectors vanish.** The
  operator re-creates every `KafkaConnector` on the new group within a reconcile, tasks go
  RUNNING, records flow. What is lost is the offsets: the new group cold-starts at
  `latest`. Lab 11 originally broke the config with no load running, so nothing was skipped
  and the trainee saw a fully healthy pipeline. It now produces across the roll (~12k
  skipped, `lab11.skipped`). Restoring the names replays from the old offsets — the gap is
  delivered, plus duplicates of everything the renamed group sent.
- **A failing task under autoRestart makes the target look AHEAD.** Each restart rewinds to
  the last committed offset and re-sends records it already delivered after the poison
  record, so the frozen partition's target offset still moves (duplicates) and the target can
  exceed the source. Lab 18 showed `deficit 0 — in sync` and a 94% live delta while task 1
  was FAILED. `mirage counts` therefore checks task state and per-partition gap growth, not
  just totals. Target offsets advancing is not proof a partition is replicating.
- **Cleanup ordering.** Anything deleted while a producer runs is recreated. Stop *every*
  connector first — the baseline mirror emits heartbeats too, not just the injected loop.
- **Rebuilding Connect while the operator is still rolling it.** Lab 12's bad heap makes
  every new pod crash, and the operator waits ~5 min per pod for readiness while holding the
  resource's lock. A `recreate_connect_cluster` in that window had its ADDED event skipped,
  the old reconcile brought a worker back after the topic deletes, and the broker
  AUTO-created `mirage-connect-offsets` with `cleanup.policy=delete`. The new worker then
  crashlooped on `required to have 'cleanup.policy=compact'` and the lab could not pass
  (47 min, 0 tasks). The rebuild now restarts the operator, waits for every Connect pod to
  be gone before deleting topics, and repairs any internal topic that comes back without
  `compact`. Also: a stale `Ready=True` on the CR does not mean the new spec rolled out.
- **A connector's config outlives the connector.** Connect's `config.storage.topic` is
  compacted and replayed on worker start, so a deleted loop connector keeps recreating
  `mm2-offset-syncs.<alias>.internal`. Only `recreate_connect_cluster` clears it.
- **Topic deletion is asynchronous and auto-recreated.** The topic reappears empty within
  ~10s, so wait for the *count* to clear, not for the topic to vanish.
- **Kubernetes never resizes a running pod**, and Strimzi does not roll workers on a
  `resources` change. A quota lab must account for old-size pods coexisting with new ones.
- **`ready_connect_pods` can exceed what you asked for.** A quota admits full-size workers
  greedily until one does not fit, so "I asked for 1 replica" does not mean one is running.
- **`-Xmx` is a ceiling, not a reservation — lab 3 cannot force an OOM.** Measured on a
  rebuilt cluster: `-Xmx 1920m` in a `1280Mi` container with ~1GB of 250KB records pushed
  through it plateaued at 1181MB of 1342MB (88%) and held. No OOMKill, no restart, no JVM
  `OutOfMemoryError`. The JVM maps only what the workload demands, and a byte-array mirror
  with modest buffers never demands the ceiling. Lab 3 therefore grades the
  **configuration**, which is genuinely wrong and is what the Datadog monitors in its
  `LAB_ALERT` would catch, and its brief says there may be no reproduction. Raising the load
  does not help; that was tried. A lab whose title promises a symptom it cannot produce
  teaches the trainee to distrust the harness — the title and brief were corrected to say
  "latent" rather than "restarting every few minutes".

---

## Conventions

- Bash, `set -uo pipefail`, `bash -n` clean. No bashisms beyond bash 3.2 (macOS default).
- Output goes through `say` / `info` / `warn` / `err` / `hr` from `lib/common.sh`; honour
  `NO_COLOR`.
- Read dotted secret keys via `python3` + base64, never `kubectl -o jsonpath` in a shell —
  jsonpath mangles them.
- Kafka 4.0 moved `GetOffsetShell`: use `bin/kafka-get-offsets.sh`.
- Comments explain *why*, especially where a line exists to defeat one of the traps above.
  Several of those lines look redundant and are not.
- Run any Python that touches downloaded or untrusted input with `python3 -I`.
