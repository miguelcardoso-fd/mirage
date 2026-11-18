# Mirage — Kafka Connect Incident-Response Labs

A local trainer for Strimzi Kafka Connect on-call. Each lab **breaks
something**, you **fix it with `kubectl`**, and `mirage stop` **grades you** —
pass, or a list of exactly where you fell short and why it matters.

Everything runs in a `kind` cluster on your laptop. Nothing touches MirageDuel
infrastructure.

---

## Setup

You need a container runtime and either `kind` **or** `minikube`. Any of Docker
Desktop, Rancher Desktop, or colima works as the runtime.

```bash
# kind (either route)
brew install kind
# ...or, if brew cannot write to its prefix:
mkdir -p ~/.local/bin && curl -fsSL -o ~/.local/bin/kind \
  "https://kind.sigs.k8s.io/dl/v0.30.0/kind-darwin-$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')" \
  && chmod +x ~/.local/bin/kind

# ...or minikube instead
brew install minikube

# a container runtime, started
open -a Docker                              # Docker Desktop
colima start --cpu 8 --memory 16            # or colima (4/8 starves the API server)

# colima's socket is not /var/run/docker.sock, so point docker at it once:
docker context use colima

docker info >/dev/null && echo ready
```

Then:

```bash
git clone git@github.com:miguelcardoso-fd/mirage.git
cd mirage
./mirage setup
./mirage verify     # confirms the whole pipeline actually works
```

### Choosing kind or minikube

`kind` is used when it is installed. Override per command, or for a session:

```bash
MIRAGE_PROVIDER=minikube ./mirage setup
```

Whichever provider builds the cluster is recorded in `.state/provider`, so a
later `teardown` or `repair` acts on the right one even from a different shell —
installing the other tool afterwards will not make `teardown` silently miss the
running cluster. Nothing in the labs depends on the provider; the whole
dependency lives in `lib/provider.sh`.

Setup creates the cluster, installs Strimzi **0.47.0** and **ingress-nginx
1.11.3** from the vendored manifests in `vendor/` (no network needed), deploys
two **Kafka 4.0.0** KRaft clusters (source + target), a Connect cluster with a
JMX exporter on **:9404**, an Ingress for the Connect REST API, and a baseline
`MirrorSourceConnector` modelled on the real `mirror_connector_template.j2`. It
finishes with a smoke test proving records replicate.

The target cluster exposes **two listeners**, which is what makes the security
labs possible:

| Listener | Port | Security | Used by |
|---|---|---|---|
| `plain` | 9092 | none | the baseline connector and every non-security lab |
| `scram` | 9094 | TLS + SASL/SCRAM-SHA-512 | labs 13, 14, 15 |

It also enables ACL enforcement (`authorization: simple`) with `ANONYMOUS` as a
superuser, so the plaintext path keeps full access and the other labs are
completely unaffected. Two `KafkaUser`s exist for the security labs:
`kcd-replicator` (can write) and `kcd-readonly` (can authenticate but not
write). The target CA is mounted into the worker at `/mnt/target-ca/`, which is
where the TLS lab's truststore lives.

`setup` and every `reset` apply `manifests/users.yaml`, and labs 13–15 refuse to
inject unless they can actually read the generated passwords. If the user
operator has not written the Secrets yet you get a clear error instead of a lab
broken with an empty password. To check by hand:

```bash
kubectl -n mirage get kafkauser                      # both should be Ready
kubectl -n mirage get secret kcd-replicator kcd-readonly
```

First run pulls images — budget 10–15 minutes. After that, `start`/`stop`
cycles take a couple of minutes each.

**Size the VM properly — CPU matters as much as memory.** Two Kafka clusters,
Connect, the Strimzi cluster operator and two entity operators all share the
node. At 4 cores the node hits ~2600% CPU under load and the Kubernetes API
server starts timing out (`TLS handshake timeout`), which looks like broken labs
but is starvation. Recommended:

```bash
colima stop && colima start --cpu 8 --memory 16
# Docker Desktop: Settings -> Resources -> 8 CPUs, 16GB
```

Minimum workable is ~6GB and 4 cores, but expect slow grading and occasional API
timeouts. `mirage setup` warns if Docker reports under 5GB.

**The brokers need 1536Mi each.** At 768Mi the target broker OOMKills (exit 137)
under sustained replication load, and because labs 4 and 7 grade on sustained
throughput, a broker dying mid-grade surfaces as a *replication deficit* with
nothing pointing at the broker. If a throughput-based lab fails for no clear
reason, check the brokers for restarts first:

```bash
kubectl -n mirage get pods -l strimzi.io/broker-role=true   # RESTARTS should be 0
kubectl -n mirage get pod <broker> -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}'
```

`OOMKilled` there means the broker, not your fix. Correlate the termination
timestamp against which lab was grading at the time.

No broker heap is set explicitly, so Strimzi derives `-Xmx` from the memory
limit — raising the limit raises the heap with it.

Note a colima restart **destroys the lab cluster** (Kafka uses ephemeral
storage), so resize first, then `mirage setup`.

### Notes on this setup

- **Your real clusters are never touched.** The harness writes and uses its own
  kubeconfig at `.state/kubeconfig`, so `kubectl` inside the labs can only ever
  see the lab cluster. You do not need to export anything, and your normal
  `~/.kube/config` context is left alone.
- `kind` is picked up from `~/.local/bin` automatically if it is not on `PATH`.

Optional, so you can type `mirage` from anywhere:

```bash
ln -s "$PWD/mirage" /usr/local/bin/mirage
```

---

## The loop

```bash
mirage intro           # how the labs work (first time)
eval "$(mirage env --export)"   # point your kubectl at the lab cluster
mirage next            # what to do next, in the recommended order
mirage start 1         # inject the fault
                        # ... investigate and fix with kubectl ...
mirage stop 1          # graded
mirage why 1           # debrief: what was really happening
```

`mirage next` walks the difficulty-ordered path and skips what you have already
passed, so you can just repeat `next` → `start N` → `stop N` until done.
`mirage list` shows the whole set with your results.

> **The labs are not obfuscated.** `labs/NN-*.sh` contains the fault, the
> assertions and the answer. Treat it as an honour system: diagnose from the
> cluster, not from the script. (Reading them afterwards is a good way to learn
> how the checks work.)

Pass looks like:

```
  [PASS] tasksMax raised above 1 (12 >= 2)
  [PASS] connector is running multiple tasks (12 >= 2)
  [PASS] all tasks are RUNNING, none failed
  [PASS] no connector other than lab-orders-from-source-to-target is replicating (none)
  [PASS] replication still healthy after the change (2000 >= 1600)
------------------------------------------------------------
  PASS    lab 1 — 5/5 checks
```

Fail tells you where and why:

```
  [FAIL] target.cluster.bootstrap.servers points at the real broker port
------------------------------------------------------------
  FAIL    lab 2 — 1 of 5 checks failed

Where you fell short

1. target.cluster.bootstrap.servers points at the real broker port
   why it matters: A restart cannot fix a wrong address. Config faults need
                   a config fix.
   look at:        kubectl -n mirage get kafkaconnector ... -o jsonpath='{.spec.config}'
```

Stuck? `mirage hint <n>` for a nudge, `mirage solve <n>` for the answer.
Re-run `stop` as many times as you like.

---

## The labs

Connector-layer faults first — those are what you actually get paged for.
Cluster/infra faults are included because they *present* as connector problems
and you need to recognise the difference.

### Connector faults

| # | What you are paged with | Level |
|---|---|---|
| 1 | Consumer lag climbing on one topic | easy |
| 9 | Connector idle, every task green | easy |
| 5 | Tasks RUNNING, zero records moving | medium |
| 2 | Replication stopped, no deploy went out | medium |
| 10 | Connector will not start after a config change | medium |
| 16 | One partition behind, eleven healthy | hard |
| 18 | A few partitions frozen, the rest flowing | hard |
| 17 | Records rejected before leaving the connector | hard |
| 4 | Target re-filling from the beginning | hard |
| 7 | Topic count and log volume climbing | hard |

### Security faults (connector-level)

| # | What you are paged with | Level |
|---|---|---|
| 15 | Connector will not start after a cert change | medium |
| 13 | Replication stopped after a credential rotation | medium |
| 14 | One topic stopped replicating | medium |

All three present identically — RUNNING, zero throughput — and have three
different owners and three different fixes. Telling them apart from the log is
the single most valuable skill in this set.

### Cluster / infrastructure faults

| # | What you are paged with | Level |
|---|---|---|
| 3 | Heap claim exceeds the container | medium |
| 6 | Scale-up requested, pods never appeared | medium |
| 8 | Replication deficit, nothing unhealthy | hard |
| 11 | Records missing after a naming tidy-up | hard |
| 19 | Resume merged, connector still paused | hard |
| 12 | **EXAM** — several alerts at once | exam |

The tables deliberately do not say what each lab teaches. The point is to work
that out from the symptom, as you would at 3am. `mirage why <n>` gives the full
mechanism **after** you have graded.

Recommended order (what `mirage next` follows):

```
1 → 9 → 5 → 2 → 10 → 15 → 13 → 14 → 3 → 6 → 8 → 16 → 18 → 17 → 4 → 7 → 11 → 19 → 12
```

Strictly by difficulty: easy → medium → hard → exam.

- **Easy (1, 9):** one field, one obvious signal.
- **Medium connector (5, 2, 10):** the signal misleads (green with no data;
  connector and task states disagree; a 400 that leaves the old config running),
  but the fix is a single config change.
- **Security block (15 → 13 → 14):** in the order a client negotiates — TLS
  handshake, then SASL authentication, then ACL authorization. Each layer has to
  pass before the next one can fail.
- **Medium cluster (3, 6):** no failure to chase; you read configuration and
  quota arithmetic.
- **Hard (8 first):** the fix is trivial, but you must *decide it is not KCD*
  while everything reads healthy.
- **Hard, per-partition (16 → 18 → 17):** topic-wide metrics hide them. 17 is the
  hardest: it is the producer, not the broker, that refuses.
- **Hard, state (4 → 7 → 11):** offsets, internal topics, compacted config; the
  cleanup order matters.
- **19 last before the exam:** the fault is below the layer every other lab
  trusts — the CR is correct and nothing reconciles it.

Labs 8 and 12 ask you to record a triage conclusion:

```bash
mirage answer 8 broker
```

---

## Inspection commands

The harness gives you shortcuts; everything else is plain `kubectl`.

```bash
mirage status            # cluster, pods, CRs, quota, lab state, REST API reachability
mirage verify            # full health check: clusters, worker, metrics, replication
mirage counts            # counts, task state, live delta, per-partition stalls
mirage diff              # baseline vs running: connector CR, CR vs worker runtime,
                         #   Connect identity (group.id, storage topics), anomalies
mirage topics            # topics on both clusters
mirage logs [lines]      # tail a Connect worker
mirage logs --errors     # known failure signatures since the lab started, explained
mirage metrics [pattern] # scrape the JMX exporter on :9404
mirage api [path]        # Connect REST API through the ingress
mirage kcli <source|target> <tool> [args]   # a Kafka CLI inside a broker
mirage kafka [source|target|stop|status]    # one broker on localhost:9092
mirage env [--export]    # the KUBECONFIG line for raw kubectl
```

`mirage counts` is the one you will use most. It answers the question that
matters in nearly every lab: *is data actually moving — on every partition?*
Besides the totals it reports tasks that are not RUNNING, partitions whose gap
grew during a live test batch, and a target AHEAD of the source (duplicates
from a task that keeps failing and being auto-restarted).

Two traps it exists to catch: a FAILED task freezes only *its* partitions, so
whole-topic throughput reads healthy; and the `KafkaConnector` CR status lags
the REST API — after each autoRestart it shows RUNNING for minutes while the
task is FAILED again. When they disagree, believe
`mirage api /connectors/<name>/status`.

`mirage verify` is what to run if a lab behaves strangely — it tells you
whether the environment or your fix is at fault.

### The Connect REST API

Exposed the same way production exposes it: a real `Ingress` whose backend is
the `mirage-connect-api` Service on its named `rest-api` port, routed by
ingress-nginx on the `Host` header. In production that host is
`kcd-<vertical>-<dc>.<env>.<region>.sb.fndlint.net`; here it is
`mirage.local`.

That name deliberately does not resolve, so you pass it as a header rather than
editing `/etc/hosts` (which would need sudo):

```bash
curl -H 'Host: mirage.local' localhost:8080/connectors
curl -H 'Host: mirage.local' localhost:8080/connectors/lab-orders-from-source-to-target/status
```

Or let the harness supply the header:

```bash
mirage api                      # show the URL and whether it is up
mirage api /connectors
mirage api /connectors/lab-orders-from-source-to-target/config
mirage api /connector-plugins   # what the running image actually contains
```

`localhost:8080` is a `kubectl port-forward` to the ingress controller, managed
by the harness (PID in `.state/api-pf.pid`). It is started by `setup` and
re-established by `mirage api`; `status` and `verify` report whether it is
alive. This avoids making the labs depend on host port mappings, which `kind`
can only accept at cluster-creation time — so enabling the API never requires
recreating your cluster, and the same mechanism works on minikube.

**Reading the failure modes.** They are distinguishable, and worth recognising
because the same three show up in production:

| Response | Meaning |
|---|---|
| connection refused / curl `000` | the port-forward is dead — `mirage api` |
| nginx `404` | your `Host` did not match the Ingress rule |
| nginx `504`, or a hang | nginx cannot reach port 8083 — almost always a NetworkPolicy |
| nginx `503` | nginx has no backend — no Connect pod behind the Service |

**The `504`/hang.** Strimzi auto-generates a NetworkPolicy (`mirage-connect`)
on the Connect pods admitting 8083 from only two peers: other pods of the same
Connect cluster, and the cluster operator. The ingress controller lives in the
`ingress-nginx` namespace and matches neither, so nginx's upstream TCP connect
never completes:

```
upstream timed out (110: Operation timed out) while connecting to upstream,
  server: mirage.local, upstream: "http://10.244.0.11:8083/connectors"
```

Meanwhile the REST API answers 200 from inside the pod and from its own
Service, so every backend signal reads healthy — the shape of incident that
sends people to look at Connect instead of at the policy.
`manifests/ingress.yaml` ships a second, additive policy granting that one
peer. It is deliberately a separate object: the operator owns `mirage-connect`
via an ownerReference and reverts edits to it.

This contradicts the common claim that kindnet does not enforce NetworkPolicy.
On this cluster it does — deleting `mirage-connect` makes the request return
200, and the operator recreates it ~3s later, at which point the timeout
returns.

**The `503`** is different, and usually transient: it appears for a few seconds
whenever Connect restarts or is rebuilt (`mirage repair`, or any lab that
patches the worker), because the `mirage-connect-api` Service briefly has no
endpoints. It means *the API is unreachable*, not *the connector is broken* —
`kubectl exec ... curl localhost:8083` and `mirage counts` stay authoritative.

It tracks the worker's readiness exactly, and clears itself:

```bash
# while a lab rebuilds the worker
kubectl -n mirage get pods -l strimzi.io/cluster=mirage   # 0/1 Running
kubectl -n mirage get endpoints mirage-connect-api        # no addresses -> 503
# the moment the pod reports ready, the same request returns 200
```

An empty `endpoints` list is the thing to check first, and it tells you to wait
rather than to start fixing something.

**The API is read-only here, as it is in PRD by convention.** `PUT /resume` or
`PUT /config` will succeed and then be reconciled away by the operator —
that is the whole point of lab 9. Change connectors through the
`KafkaConnector` CR.

No lab grades through the ingress: the assertion engine reaches Connect by
`kubectl exec` into the worker, so a dead controller or dropped forward can
never fail a lab or make one pass.

```bash
# the real tools — these are what you would use in PRD
kubectl -n mirage get kafkaconnector
kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target
kubectl -n mirage get kafkaconnect mirage -o yaml
kubectl -n mirage logs <pod> --previous        # essential after an OOMKill
kubectl -n mirage describe quota
```

Metrics carry **the same names as production**, so practising here transfers:

```bash
mirage metrics 'connector_status|task_count'
mirage metrics rebalance_epoch     # the rebalance-loop detector
```

---

## Housekeeping

```bash
mirage reset             # restore baseline, keep the cluster (fast)
mirage reset --fresh     # also empty the topic, so counts start from zero
mirage repair            # rebuild the Connect cluster when it is wedged
mirage teardown          # delete the cluster entirely
```

`start` auto-resets if another lab is still active, so you can jump around.
`reset` now **verifies** that replication actually works before reporting
success, and rebuilds the Connect cluster once if it does not.

Results persist in `.state/` — `mirage list` shows your pass/fail record, and
re-running a lab you have already passed does not erase that.

---

## What this environment does and does not reproduce

**Faithful to production:** Strimzi 0.47.0, Kafka 4.0.0,
`strimzi.io/use-connector-resources: "true"` and its reconciliation behaviour,
`autoRestart {enabled: true, maxRestarts: 5}`, `-XX:ExitOnOutOfMemoryError`,
`scheduled.rebalance.max.delay.ms: 300000` (still live in int/stg; the
`sb-kafka-connect-infra` repo moved to 120000 in Oct 2026), the stateful `group.id` + three
storage topics, `topics.exclude` loop guards, ByteArray converters, JMX metric
names, the restart-annotation workflow, and a Host-routed `Ingress` in front of
the Connect REST API with the same Service/named-port shape.

**Not reproduced:**

- **MirageDuel custom SMTs.** `ByteArrayInsertHeader`, `HashPartitionerAssignor`,
  `CustomIdentityReplicationPolicy` live in a prebuilt ECR image this lab cannot
  pull. Lab 10 turns that limitation into the lesson. Upstream
  `MirrorSourceConnector` only.
- **Scale.** One single-broker cluster per side, 768m heap, 1 worker. Production
  runs 5–15 workers with 6–8g heaps. See the note under **Layout**.
- **Partition mismatch.** Not reproducible: `MirrorSourceConnector` runs a
  "creating downstream topic-partitions" task that auto-widens a too-narrow
  target topic, so the mismatch heals itself within seconds. Worth knowing —
  it means a partition-count incident in production points at something
  blocking that task (ACLs on Create/Alter), not at the count itself.
- **Multi-cluster topology.** No outposts, no betting-state/super-state split, no
  cross-region.
- **Ingress authentication and TLS.** Production reaches the REST API over an
  *internal* ingress on HTTPS with a real DNS name. Here it is HTTP via a
  port-forward with the host supplied as a header — the routing shape matches,
  the transport and network boundary do not.
- **TLS/SASL — partially reproduced.** The target cluster has a real TLS +
  SASL/SCRAM listener on :9094 with ACL enforcement, which labs 13-15 use. What
  is *not* reproduced: mTLS client certificates, AWS MSK IAM auth, and IRSA —
  production KCD uses TLS + IRSA rather than SCRAM, so the mechanism differs
  even though the failure signatures (`SaslAuthenticationException`,
  `TopicAuthorizationException`, `SSLHandshakeException`) are identical.
- **PRD guardrails.** Here you can `kubectl patch` freely. **In PRD you cannot** —
  config changes are PRs to `sb-kafka-connectors-management`, and PRD roles are
  read-only. Every lab's solution notes which actions would be allowed live.

That last point is the one to keep in mind: the labs teach you to *diagnose* like
production and *act* in ways production would not always permit. The grading
explains the difference each time.

---

## Regression-testing the labs themselves

`run-all-labs.sh` exercises every lab end to end: inject, confirm the pre-fix
grade **fails**, apply the known-good fix, confirm the post-fix grade **passes**.

```bash
./run-all-labs.sh            # all 19, in the recommended order
./run-all-labs.sh 15 13 14   # just the security labs
./run-all-labs.sh 16:skip    # an alternate fix route (runs fix_16_skip)
./run-all-labs.sh 13:bad_plaintext   # a tempting WRONG fix: must still FAIL
./run-all-labs.sh --variants # every route variant
./run-all-labs.sh --all      # every lab, then every variant
```

**Route variants.** Most labs have more than one legitimate answer, and a grader
that only ever saw one of them can fail a correct fix nobody tested. `N:name`
runs `fix_N_name` instead of `fix_N`. Variants named `bad_*` are the tempting
wrong fixes from each lab's solve and hint (restart-only, `errors.tolerance=all`
where it loses data, back to the plaintext port, cleanup without the rebuild…):
they must still **FAIL** on every grade attempt, or the grader passes something
the lab exists to teach against. The list is `ALL_VARIANTS` in the script; the
summary prints the expected result next to the real one.

It applies the answers for you, so it is **not** a training tool — use
`mirage` for practice. Its value is catching the two failure modes that matter
when editing labs:

- a lab that **passes with the fault still live** (reported as
  `PASSED-UNFIXED`), which means a check cannot detect what it claims to
- a lab whose documented solve **no longer works** (reported as post-fix `FAIL`,
  with the failing assertions printed)
- a lab that could not be injected at all (`INJECT-FAILED`, `NO-CREDENTIALS`),
  which would otherwise look like a lab you simply cannot pass

Each run keeps its injection output at `.state/inject-<n>.log` and its grade
output at `.state/grade-<n>.log` (`grade-<n>-<variant>.log` for a variant), and any `!!` warning from `lab_break` is
surfaced before grading. **Read the injection log first when a post-fix grade
fails** — it holds the numbers the lab computed for its fault, which is usually
what distinguishes "the solve is broken" from "the fault was never built
properly".

**It only validates a solve to the extent the recipe mirrors it.** Each `fix_N`
in `run-all-labs.sh` is a reimplementation of `mirage solve N`, not the same
code — so where the two drift apart, a lab can keep passing while its *written
answer* is wrong. That is exactly what happened with labs 7 and 12: both solves
deleted topics while a connector was still running (which recreates them), yet
`fix_12` happened to stop the connectors first and so passed regardless. When
you change a solve, either mirror it in the recipe or read the solve directly —
a green run is not evidence that the documented answer works.

Budget roughly 2 hours for the 19 base labs, and several more for
`--variants`; it resets between labs and several labs wait on broker or
cluster rollouts.

**A correct fix often does not grade green on the first try.** The harness
retries the grade up to 3 times with 60s gaps, and labs where tasks recover from
a failed state (rather than just taking new config) routinely need the second
attempt — a connector coming back from `UNASSIGNED` takes around two minutes.
When practising with `mirage`, if `stop N` fails on throughput alone, wait a
minute and grade again before assuming the fix was wrong.

> Editing `run-all-labs.sh` while a run is in progress has no effect on that
> run — bash has already parsed the file. Let it finish, then re-run the
> specific labs.

**Only one run at a time.** Every lab resets and patches the *same* cluster, so
two overlapping runs silently invalidate each other's results — one run's
`reset --fresh` lands in the middle of the other's grading. The script holds a
lock (`.state/run-all-labs.lock`) and refuses to start a second run with exit 2.

Do not gate a queued run on `pgrep`: it cannot read the process list in some
sandboxed environments (`pgrep: Cannot get process list`, exit 3), so a
`while pgrep ...; do sleep; done` guard falls through immediately and starts the
very overlap it was meant to prevent. Chain runs in one command instead:

```bash
./run-all-labs.sh 1 2 9 && ./run-all-labs.sh 13 14
```

If a run is killed, the trap releases the lock; if it dies hard, remove it with
`rmdir .state/run-all-labs.lock`.

---

## Adding a new lab (Claude Code skill)

The repo ships a Claude Code skill, `.claude/skills/new-lab/`, that builds a lab
from a plain-language scenario. Open Claude Code in this folder and describe
the failure:

```
/new-lab create a lab where the connector cannot reach the producer (target) broker
```

or just say "add a lab where …" — the skill triggers on that too. It:

1. **Checks overlap first.** It compares the scenario with what every existing
   lab injects (`.claude/skills/new-lab/references/lab-catalog.md`, confirmed
   against `labs/*.sh`) and opens with a verdict: *already covered by lab N*,
   *partially covered by lab N*, or *not covered*. If already covered it stops
   and offers a distinct variant instead of building a duplicate.
2. **Scopes** the scenario: fault, symptom, correct fix, tempting wrong fix,
   and whether it can reproduce in kind (it refuses ones that cannot, such as
   VPC security groups).
3. **Spikes** the fault by hand on the live cluster before writing anything.
4. **Writes** `labs/NN-*.sh` following the lab contract and the grading rules
   in `CLAUDE.md`.
5. **Registers** it everywhere: `lib/debriefs.sh`, `LAB_ORDER` in `mirage`,
   `LABS` and `fix_NN` in `run-all-labs.sh`, the README tables,
   and the `CLAUDE.md` TSG table.
6. **Tests** it three ways, one at a time: `./run-all-labs.sh NN` (fails before
   the fix, passes after), a cheat test (the wrong fix must fail), and a run on
   a used cluster. If `lib/common.sh` changed, it also runs the full suite (~2h).
7. **Places** it in the recommended order by difficulty and reports what it did.

It resets the shared cluster while testing, so finish or abandon any lab you
have running first. Nothing is committed unless you ask.

---

## If something goes wrong

Run `mirage verify` first — it isolates environment problems from your fix.
If the environment itself is wedged, `mirage repair` rebuilds the Connect
cluster without touching your Kafka data or re-downloading images.

| Symptom | Cause | Fix |
|---|---|---|
| `setup` says Docker not reachable, but it is running | Your docker context points at a socket that does not exist — common with colima, whose socket is under `~/.colima/`, not `/var/run/docker.sock` | `docker context ls`, find the endpoint that exists, then `docker context use colima` |
| Kafka or Connect pods OOMKilled during setup | VM has under ~6GB | Raise Docker/colima memory, then `mirage teardown && mirage setup` |
| Connect stuck `0/1 Running`, restarting | Worker cannot finish reading its internal topics before the liveness probe fires | `mirage repair` |
| **Connector RUNNING but zero tasks, and a restart does not help** | The internal status/offset topic was recreated under the running worker, so its view of task state is corrupt | `mirage repair` |
| **No Connect pod at all after deleting one** | The StrimziPodSet went with it and the operator timed out | `mirage repair` |
| Connect crashloops with a clean shutdown and no error | Internal topics auto-created without `cleanup.policy=compact` | `mirage repair` (it deletes them so Connect recreates them correctly) |
| A Kafka cluster never goes Ready, broker logs show `UnknownHostException` for a pod that no longer exists | Stale KRaft quorum after a node-pool rename | Delete that `Kafka` + its `KafkaNodePool` + PVCs, then re-apply |
| `mirage counts` shows 0 for both sides after producing | Broker not reachable, or the topic does not exist | `mirage verify` |
| Record counts in the tens of thousands, checks feel sluggish | Labs produce on every start and stop | `mirage reset --fresh` empties the topic |
| A lab's `stop` hangs for minutes | Normal — checks produce records and wait up to 2 minutes for replication | Let it finish |
| `curl localhost:8080` refuses the connection | The port-forward died (laptop slept, or `kubectl` was killed) | `mirage api` re-establishes it |
| `mirage api` says port 8080 is held by a PID "not serving this cluster" | A port-forward left over from a cluster you tore down — `teardown` removes `.state/`, so its pidfile went with it and nothing could clean it up | `mirage api` reclaims it automatically; if that fails it prints the exact `kill <pid>` to run |
| REST API returns nginx's `404` for every path | The Ingress is missing, or its Host does not match | `mirage api` re-applies it; check `kubectl -n mirage describe ingress mirage-connect-api` |
| REST API returns `503` or `504` while the connector looks healthy | nginx has no backend — Connect is restarting, or its Service has no endpoints yet | Wait a few seconds; `kubectl -n mirage get svc,pods`; `mirage repair` if the worker is gone |
| REST API returns nginx's `404` and you did pass the Host header | Wrong host — the Ingress rule matches `mirage.local` exactly | `mirage api /connectors` supplies it for you |
| Both brokers CrashLoopBackOff with `No space left on device` | A self-mirroring loop (labs 7, 12) filled the node disk. The labs now throttle the loop, but an old checkout or a hand-made loop connector does not | Delete the loop connector, delete both broker pods (ephemeral storage is wiped), then `mirage repair` and `mirage reset --fresh` |
| `teardown` says it cannot tell which provider owns the cluster | `.state/` was deleted while the cluster still exists | `MIRAGE_PROVIDER=kind mirage teardown` (or `minikube`) |

Nuclear option, ~15 minutes:

```bash
mirage teardown
mirage setup && mirage verify
```

---

## Layout

```
mirage/
├── mirage               # the CLI
├── run-all-labs.sh      # regression harness (base fixes + route variants)
├── lib/common.sh        # assertion engine + kubectl/Kafka helpers
├── lib/provider.sh      # kind/minikube abstraction — the only file naming either
├── lib/debriefs.sh      # `mirage why <n>` debriefs, one per lab
├── manifests/           # baseline: kafka, connect (+JMX rules), topic, connector, ingress
├── vendor/strimzi-*/    # pinned Strimzi 0.47.0 install manifests (offline setup)
├── vendor/ingress-nginx-*/  # pinned ingress-nginx, webhook stripped (see its header)
├── labs/NN-*.sh         # one file per lab: break / check / hint / solve
├── .claude/skills/new-lab/  # Claude Code skill that builds and tests a new lab
└── .state/              # kubeconfig, provider, active lab, results (gitignored)
```

**Scale differences from production, deliberately.** The baseline runs **1**
Connect worker at 1280Mi / 768m heap, and Kafka brokers at 1536Mi (at 768Mi the
target OOMKills under sustained load), so everything fits one node. Production runs 5–15 workers at 8–10Gi with 6–8g heaps.
`offset.storage.partitions` is 5 here versus 25 in production, so a single small
broker can be read to the end before the liveness probe fires. The labs that
scale up or break the heap compute their numbers from the live baseline rather
than hardcoding, so they stay correct if you resize anything.

Adding a lab: copy any `labs/NN-*.sh`, set `LAB_TITLE` / `LAB_DIFFICULTY` /
`LAB_ALERT`, and implement `lab_brief`, `lab_break`, `lab_check`, `lab_hint`,
`lab_solve`. It is picked up automatically. In `lab_check`, use
`assert` / `assert_eq` / `assert_ge` — each takes a *why it matters* and a
*where to look*, which is what makes a failure useful rather than just red.

Helpers worth knowing:

| Helper | Use |
|---|---|
| `replication_delta <n> <timeout>` | Produce `n` records, return how many reached the target |
| `src_count` / `tgt_count` | Cumulative record counts (see warning below) |
| `conn_task_count RUNNING [name]` | Tasks in a state, for a named connector |
| `conn_state_field [name]` | Connector's own state (RUNNING / PAUSED / FAILED) |
| `runtime_config_value <key>` | What the WORKER is running, via REST — not the CR |
| `api_curl <path>` / `api_up` | Connect REST API via the ingress. **Never use in a `lab_check`** |
| `patch_connector_runtime` | Patch and wait for the runtime to reflect it |
| `patch_connector_verified` | Patch and confirm it stuck on the CR |
| `settle_connector` | Wait for reconciliation to finish before injecting |
| `broker_pod <cluster>` | The actual broker pod, not the entity-operator |
| `connect_worker_mem_mi` / `ns_mem_requests_mi` | Live sizing, for quota/heap labs |
| `recreate_connect_cluster` / `truncate_topic` | Recovery primitives |
| `metric <pattern>` | Scrape the JMX exporter |

### Eight ways a lab check can lie to you

Every one of these produced a false pass during testing, so they are worth
knowing before writing a new lab:

1. **Comparing cumulative `src_count` to `tgt_count`.** Labs produce records on
   every start and stop, so at 60k records a 2000-record gap is 3% — under any
   tolerance. Such a check can never fail. **Use `replication_delta`.**
2. **Percentage tolerances with no absolute cap.** Lab 4 allowed 2% deficit,
   which is correct at 5k records and silently swallows its entire 5000-record
   gap at 1M. **Cap the tolerance absolutely.**
3. **"Target grew by at least 1."** A single heartbeat satisfies it. **Require
   most of what you produced to arrive.**
4. **Counting tasks without naming the connector.** A stray connector from a
   loop lab has perfectly healthy tasks. **Pass the connector name.**
5. **Asserting `running == total` when both can be zero.** `0 == 0` passes.
   **Require `total >= 1` as well.**
6. **No throughput check at all.** Stable pods and green task state are not a
   working pipeline — a heap fix that starves the fetch buffers passes every
   structural check and moves no data. **Every lab ends with a delta check.**
7. **Trusting the CR when the fault must be in the runtime.** Connect rejects
   invalid config with a 400 and keeps running the old version, and during a
   rebalance it refuses config pushes entirely. **Use
   `patch_connector_runtime` when the worker itself must be broken.**
8. **Injecting a fault built from a value that is empty.** The inverse failure:
   not a false pass but a lab that can never be passed. Labs 13–15 build their
   fault from a `KafkaUser` password; when the Secret was missing the password
   read as `""`, so the lab injected `password=""`, taught the wrong failure and
   no fix could satisfy it. **Assert your inputs exist before breaking anything,
   and make a refused injection abort rather than print the brief** —
   `require_scram_users` and lab 8's "is the broker actually gone?" check.

Two more of the same shape, outside the assertions:

- **Counting pods by cluster label.** The entity-operator carries
  `strimzi.io/cluster` too, so "a pod is Running" is satisfied with no broker at
  all. Select on `strimzi.io/broker-role=true`.
- **Caching a pod name across a lab that deletes brokers.** `src_pod` / `tgt_pod`
  now validate the cached pod is still Running before returning it, so a stale
  name cannot turn into "replication is dead".

`replication_delta` also distinguishes *could not produce* from *produced and
nothing arrived* — otherwise a downed source broker reads identically to a dead
pipeline, and the lab blames the wrong layer.

