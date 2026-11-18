#!/usr/bin/env bash
# Post-lab debriefs for `mirage why <n>`.
#
# This exists because the teaching in each lab lives inside lab_solve, which you
# never see if you fix the lab yourself — exactly the people who most deserve the
# explanation. `why` closes that gap: what the fault actually was, the mechanism
# underneath it, how it presents in production, and the transferable rule.
#
# Four fields per lab, all optional but keep them short:
#   WHY_MECHANISM  what was really happening under the symptom
#   WHY_SIGNAL     how it shows up on a dashboard / in an alert
#   WHY_PROD       what the MirageDuel-specific equivalent is
#   WHY_RULE       the one sentence worth remembering on call

why_lab() {
  WHY_MECHANISM=""; WHY_SIGNAL=""; WHY_PROD=""; WHY_RULE=""
  case "$1" in

  1)
  WHY_MECHANISM="A MirrorSourceConnector divides work by PARTITION, so tasksMax is
a ceiling on parallelism, never a target. One task owning 12 partitions reads
them sequentially: throughput is bounded by one thread regardless of load, and
lag grows without anything being unhealthy."
  WHY_SIGNAL="records_lag_max climbs steadily while task state stays RUNNING and
error counts stay zero. replication.record_age_ms rising alongside lag is the
confirmation — age distinguishes 'behind' from 'idle'."
  WHY_PROD="There is no per-connector lag monitor any more: dataform #1798
removed the record_lag thresholds, so this is found downstream, not paged.
tasksMax lives in the connector declaration in sb-kafka-connectors-management,
so raising it is a PR, not a live edit."
  WHY_RULE="Lag with healthy tasks is a capacity problem, not a fault. Check
parallelism against partition count before you look for errors."
  ;;

  2)
  WHY_MECHANISM="A task that cannot reach its broker fails, and Strimzi's
autoRestart retries it up to maxRestarts (5) before giving up. The connector can
read RUNNING while individual tasks are FAILED or UNASSIGNED — those are separate
state machines."
  WHY_SIGNAL="total_task_count minus running_task_count is the real signal; the
failing-tasks monitor is a weighted subtraction for exactly this reason.
connector_status alone will not show it."
  WHY_PROD="autoRestart {enabled: true, maxRestarts: 5} is deliberate: finite, so
a recurring fault surfaces instead of looping forever. If a connector has burnt
its five restarts, re-annotating is not a fix — find the cause."
  WHY_RULE="Grade task counts, not connector state. RUNNING is an aggregate that
hides dead tasks."
  ;;

  3)
  WHY_MECHANISM="Heap exhaustion has three distinct shapes and each is defended
differently: a container OOMKill (exit 137, pod restarts), a JVM
OutOfMemoryError on a task thread (pod keeps running, task dies, nothing
self-heals), and scheduling starvation (pods never created at all)."
  WHY_SIGNAL="Exit 137 in kubectl describe = container limit. 'OutOfMemoryError'
in logs with a Running pod = heap too small for the workload. Pods Pending with
nothing unhealthy = quota or node capacity."
  WHY_PROD="Two independent defences exist because neither covers the other:
spec.autoRestart on the connector, and -XX:ExitOnOutOfMemoryError on the worker.
The second exists because a task-thread OOM otherwise leaves the task FAILED
while the worker keeps running, so nothing restarts it."
  WHY_RULE="Keep -Xmx at 50-70% of the container. The JVM needs metaspace,
direct buffers and thread stacks outside the heap."
  ;;

  4)
  WHY_MECHANISM="Connector identity IS offset identity. Offsets are stored keyed
by connector name in the offsets topic, so a renamed connector finds no offsets
and starts from consumer.auto.offset.reset — 'latest' means it silently skips
everything produced while it was gone."
  WHY_SIGNAL="NONE. This is the important part: every health signal reads green.
Tasks are RUNNING, there are no errors, throughput looks normal. Only comparing
source and target record counts reveals the gap, and only if you know to look."
  WHY_PROD="group.id and the config/offset/status storage topic names carry a
Connect cluster's live state. The in-repo warning is explicit: changing them for
an existing deploy orphans its connector state. This is why renames go through
an offset migration, not a rename."
  WHY_RULE="Silent data loss is the failure with no alert. Any change to
connector name, group.id or storage topics needs an offset plan first."
  ;;

  5)
  WHY_MECHANISM="A connector subscribed to a topic pattern that matches nothing
has no work to assign, so it reports RUNNING with zero tasks doing anything.
Healthy state, zero throughput — the pipeline is dead and nothing says so."
  WHY_SIGNAL="replication record_rate flatlining at zero while task state stays
green. The VM-era monitor description says it best: 'stale connectors, where we
have tasks running, but no data being replicated'."
  WHY_PROD="refresh.topics.enabled with refresh.topics.interval.seconds means a
pattern that matches nothing today may match later — so this can also appear as
a connector that silently stops covering a renamed topic."
  WHY_RULE="Always confirm throughput, never just state. Green tasks with zero
records is a failure mode, not a healthy idle."
  ;;

  6)
  WHY_MECHANISM="A ResourceQuota rejection happens at ADMISSION: the pod is never
created, so there is nothing Pending to inspect. The operator logs 'exceeded
quota' and the replica count simply never reaches its target."
  WHY_SIGNAL="Desired replicas exceed actual pods, nothing is Pending, no pod is
unhealthy. It presents as a memory incident and genuinely is one — but every
minute spent reading heap graphs is wasted."
  WHY_PROD="Quotas live with the namespace definitions in pdm-eks-utilities, so
raising one is a PR. Note the interaction with KEDA: if quota blocks scale-up,
the autoscaler cannot respond to memory pressure, so you get the alert AND no
remediation."
  WHY_RULE="When pods will not schedule, check quota before heap. 'exceeded
quota' means the quota; 'Insufficient memory' means the node, and only shrinking
helps."
  ;;

  7)
  WHY_MECHANISM="A mirror whose source and target are the same cluster re-reads
what it just wrote, so records multiply without bound. topics.exclude and
distinct cluster aliases are the loop guards; an identity replication policy
removes the name-based protection that would otherwise stop it."
  WHY_SIGNAL="Target record count growing far beyond source, topic count
exploding, log volume spiking. The 'unusual amount of logs' monitor (>150M/day)
often fires first."
  WHY_PROD="The real template ships topics.exclude='.*\\.heartbeats,
mm2.*\\.internal, .*\\.replica, __.*' precisely to prevent this, and
source.cluster.alias must differ from target.cluster.alias. Stopping the loop
does not undo the duplicates — that needs a deliberate re-seed."
  WHY_RULE="Order matters in cleanup: stop every producer first, or anything you
delete is recreated. And a connector's config outlives the connector — Connect
replays its compacted config topic on every worker start."
  ;;

  8)
  WHY_MECHANISM="A broker outage is upstream of the connector. The connector is
behaving correctly by failing — the fault is not in anything you own, and
restarting connectors adds rebalance churn to an already-degraded cluster."
  WHY_SIGNAL="Mass disconnects across MANY connectors simultaneously, not one.
'socket connection setup timeout' or 'Cancelled in-flight FETCH' across
unrelated namespaces is the tell that this is infrastructure, not config."
  WHY_PROD="The real incident showed simultaneous disconnects across prdmi,
prdnc and prdpr — three environments at once. One connector failing is yours;
every connector failing is the platform's."
  WHY_RULE="Blast radius identifies the layer. Before fixing, ask how many
things are broken — then escalate rather than restart."
  ;;

  9)
  WHY_MECHANISM="With strimzi.io/use-connector-resources: true the CRD is the
single source of truth, and the operator reconciles continuously. A pause or
resume issued through the REST API is reverted on the next reconcile, usually
within seconds, leaving no trace of who did what."
  WHY_SIGNAL="connector_status reads 2 (paused) or 6 (stopped) — a numeric gauge
where 1 is running. A connector that flaps back to paused after you resume it
via the API is this, not a fault."
  WHY_PROD="spec.state is the only legitimate control, and it is a config change,
so pausing in production is a PR. A connector stopped in DEV and PRD at the same
moment was traced to human API action — which is exactly why the CRD wins."
  WHY_RULE="If your change reverts itself, you are fighting the reconcile loop.
Change the desired state, not the runtime."
  ;;

  10)
  WHY_MECHANISM="SMTs are applied per record in a chain, and a transform that
emits a schema the converter cannot serialise fails at RUNTIME, not at config
validation. The CR goes Ready, then every task dies on the first record."
  WHY_SIGNAL="connector_status 3 (failed) shortly after a config change, with
DataException in the logs naming the converter and schema type."
  WHY_PROD="Upstream InsertHeader emits a STRING-schema header that
ByteArrayConverter rejects: 'Invalid schema type for ByteArrayConverter: STRING'.
MirageDuel ships ByteArrayInsertHeader for exactly this reason. Also expect both
com.mirageduel.* and com.flutter.* package names — that is version drift between
two repos, not a typo to fix."
  WHY_RULE="Ready=True after an SMT change proves nothing. Validate task state
AND throughput, because validation and execution are different gates."
  ;;

  19)
  WHY_MECHANISM="KafkaConnector and KafkaConnect are desired state; only the
Strimzi cluster operator turns them into REST calls on the worker. With the
operator crashlooping, the API server still accepts every CR edit, the deploy
pipeline still goes green, and nothing acts on any of it. The worker keeps
running the last config it was given. The JVM exited with code 3 because the
image runs it with -XX:+ExitOnOutOfMemoryError: a heap that is too small, not a
container that is too small."
  WHY_SIGNAL="metadata.generation ahead of status.observedGeneration on the CR,
a CR status block frozen at the last reconcile, and an operator pod whose last
termination is Error / exit 3 rather than OOMKilled / 137. No KCD alert covers
it: the crashloop and restart monitors are scoped to kube_app_name:kafka-connect."
  WHY_PROD="pdm-eks-utilities #4541: on racing-int-use2 the operator heap-OOM'd
and crashlooped 88 times with reason Error, so VPA, which reacts to OOMKilled,
never raised it. The floor was raised from 256Mi to 1Gi. The operator lives in
the strimzi namespace and belongs to compute-operations: from KCD on-call you
escalate with generation vs observedGeneration and the exit reason."
  WHY_RULE="When a merged change did not take effect, compare generation with
observedGeneration before touching the connector. If they differ, the problem
is the operator."
  ;;

  11)
  WHY_MECHANISM="group.id plus the config/offset/status storage topics ARE the
Connect cluster's identity. Change them and the worker starts a brand new empty
cluster: it cannot see the old connectors, and their offsets are orphaned
rather than deleted. On Strimzi the operator re-creates each connector from its
KafkaConnector CR within a reconcile, so they barely vanish — but they come back
with NO offsets, and consumer.auto.offset.reset=latest skips everything produced
in between."
  WHY_SIGNAL="A large, constant deficit that appeared at a deploy, while every
task is RUNNING and new records flow. connector_count dips to zero only briefly
(on VMs without an operator it stays at zero). Two sets of *-connect-* topics on
the target is the giveaway."
  WHY_PROD="Topic names are computed:
GROUP_ID=connect-cluster_\${prefix}-\${vertical}-\${DC}. The in-repo warning is
blunt — these names carry live state, and changing them for an existing deploy
orphans it. Recovery is restoring the names, not recreating connectors."
  WHY_RULE="Treat storage topic names and group.id as immutable for a live
deployment. They are identity, not configuration."
  ;;

  12)
  WHY_MECHANISM="Real incidents arrive as several faults at once, and the order
you fix them in decides whether you succeed. Workers that cannot start cannot
run connectors; cleaning while a producer runs recreates what you deleted;
restarting before config lands burns the autoRestart budget."
  WHY_SIGNAL="Several alerts firing together. The trap is treating them as
independent — some are consequences of others, and fixing a symptom first wastes
the time you need."
  WHY_PROD="Triage order that generalises: make the platform able to run
(quota, memory), then stop anything actively making it worse (loops), then
correct config, then clean up residue, then verify throughput."
  WHY_RULE="Sequence beats speed. Ask what is CAUSING what before you change
anything, and always verify data is moving at the end."
  ;;

  13)
  WHY_MECHANISM="Kafka negotiates TLS, then SASL, then authorization — so the
earliest failing stage is the only one you see, and it masks the rest. Bad
credentials produce SaslAuthenticationException only after the TLS handshake has
already succeeded."
  WHY_SIGNAL="'Authentication failed ... with SASL mechanism SCRAM-SHA-512'.
It appears first on the mirror's ADMIN client thread (replication-target-admin),
which then spins "Rebootstrapping" at ~50 lines/s and buries the real error
under thousands of INFO lines — a tail of the log can miss it entirely. Tasks
die on the same failure, but autoRestart revives them, so the CR status reads
RUNNING for minutes at a time; the REST status trace has the cause."
  WHY_PROD="Credentials belong in a mounted secret referenced through
FileConfigProvider (\${file:...}), never pasted into a connector config, which
would put them in Git and in every rendered manifest."
  WHY_RULE="Config pushes are asynchronous. Confirm the WORKER has the new value
(mirage api /connectors/<n>/config) before restarting, or tasks retry the old
one and exhaust autoRestart."
  ;;

  14)
  WHY_MECHANISM="Authentication and authorization are different questions.
TopicAuthorizationException means the identity was accepted and then refused an
operation — the credentials are valid and rotating them changes nothing."
  WHY_SIGNAL="'Not authorized to access topics: [...]'. Read the topic list and
the operation: a mirror needs Write AND Create/Describe on the target, so
read-only credentials authenticate perfectly and then fail on first produce."
  WHY_PROD="This is usually not yours to fix. An ACL grant is a platform/cluster
team action, so the right move is to gather evidence — failing principal, topic,
operation, exception — and hand off rather than retry."
  WHY_RULE="SaslAuthenticationException = who you are. TopicAuthorizationException
= what you may do. The second needs a grant, not a credential."
  ;;

  15)
  WHY_MECHANISM="TLS is negotiated before SASL, so a truststore problem fails the
handshake before credentials are ever presented. The symptom therefore looks
like an auth failure while the credentials are entirely correct."
  WHY_SIGNAL="SSLHandshakeException, 'unable to find valid certification path',
or 'keystore password was incorrect'. Fixing trust can then REVEAL an auth
failure that was always there — two different exceptions in sequence is
progress, not a new incident."
  WHY_PROD="Certificate material is mounted at /mnt/kcd-service-ssl/ with
passwords resolved through FileConfigProvider. Note
ssl.endpoint.identification.algorithm is deliberately empty: hostname
verification is off because brokers are reached by names that do not match
their certificates. That is not a misconfiguration to fix."
  WHY_RULE="Work the negotiation order: trust, then identity, then permission.
The earliest failing stage is the only one you can see."
  ;;

  16)
  WHY_MECHANISM="The TARGET broker refuses the record (RecordTooLargeException,
not retriable), so the task that owns its partition fails rather than silently
drop it. autoRestart revives it, it re-reads the same offset and fails again —
so that task's partitions wedge (3 of 12 with 4 tasks) while the rest keep
flowing. Aggregate throughput therefore looks survivable."
  WHY_SIGNAL="One task FAILED with 'Unrecoverable exception from producer send
callback' caused by RecordTooLargeException — in the REST status; the CR reads
RUNNING for minutes after each autoRestart. Per-partition: that task's offsets
stop advancing while the rest move. Whole-topic throughput hides it, and
autoRestart re-sends can even put the target AHEAD of the source."
  WHY_PROD="Topics are declared in sbv2_data_dependencies, so max.message.bytes
is a PR and a conversation with the owning team, not a kubectl change. The
asymmetric case — a limit raised on one cluster and not the other — is how this
happens in practice, and it only surfaces when a big enough record arrives."
  WHY_RULE="When the fault is scoped narrower than the topic, measure narrower
than the topic. And restarting a task that is refusing a record just spends an
autoRestart attempt: widen first, restart second."
  ;;

  17)
  WHY_MECHANISM="max.request.size is enforced by the connector's OWN producer,
before anything touches the network. The record is serialized, measured, and
rejected in-process — so it never reaches a broker, and no broker log mentions
it. Tasks stay RUNNING because a rejected send is an error the producer reports,
not a crash."
  WHY_SIGNAL="RecordTooLargeException naming max.request.size specifically, with
zero FAILED tasks and both clusters' topic configs clean. The absence of
broker-side evidence is the signal: if comparing cluster limits finds nothing,
the refusal is local to the worker."
  WHY_PROD="producer.override.max.request.size lives in the connector
declaration in sb-kafka-connectors-management — config-as-code, so the fix is
your PR rather than the topic owner's. Templates moving large records carry a
handle_high_message_payload block raising it to 34603008. Note the
troubleshooting guide's rule: if the fix is NOT a connector config change, you
need restart?includeTasks=true, because restarting a connector does not restart
its tasks."
  WHY_RULE="Same exception can come from either end of a write. Ask who refused
it — the broker that would store it, or the producer that would send it —
because the answer decides whose repository the fix lives in."
  ;;

  18)
  WHY_MECHANISM="MirrorMaker preserves each record's CreateTime. The target topic
had a message.timestamp.before.max.ms the source did not, so the source stored a
record stamped two hours ago and the target refused it with a non-retriable
InvalidTimestampException. The task owning that partition fails, autoRestart
re-reads the same offset, and its partitions freeze while the rest flow."
  WHY_SIGNAL="InvalidTimestampException naming the record's timestamp and the
allowed window, under 'Unrecoverable exception from producer send callback'. The
frozen partitions are exactly one task's share — p, p+4, p+8 with four tasks."
  WHY_PROD="Upstream producers set event time, and replays or backfills stamp
it in the past. Topic configs live in sbv2_data_dependencies, so the window is a
PR to the owning team; hand them the exact timestamp from the exception."
  WHY_RULE="Size is not the only thing a broker validates. When a record is
refused, read the Caused-by before reasoning about bytes. And errors.tolerance
against a POLICY (a window, a limit) drops every future violator, not just this
one: widen first, restart second."
  ;;

  esac
}
