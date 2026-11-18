#!/usr/bin/env bash
# Exercise every lab end to end: inject, confirm the pre-fix grade FAILS,
# apply the known-good fix, confirm the post-fix grade PASSES.
#
# This is a regression harness for the labs themselves, not a training tool —
# it applies the answers for you. Use `mirage` for actual practice.
#
#   ./run-all-labs.sh            # all labs, in LAB_ORDER
#   ./run-all-labs.sh 13 14 15   # only these
#   ./run-all-labs.sh 16:skip    # an alternate route (fix_16_skip); bad_* must FAIL
#   ./run-all-labs.sh --variants # every route variant
#   ./run-all-labs.sh --all      # every lab, then every variant

set -uo pipefail
# Resolve symlinks so this works when invoked through one (see mirage).
_ral_self="${BASH_SOURCE[0]}"
while [ -L "$_ral_self" ]; do
  _ral_link="$(readlink "$_ral_self")"
  case "$_ral_link" in
    /*) _ral_self="$_ral_link" ;;
    *)  _ral_self="$(dirname "$_ral_self")/$_ral_link" ;;
  esac
done
cd "$(dirname "$_ral_self")"
unset _ral_self _ral_link
export PATH="$HOME/.local/bin:$PATH"

# The per-lab worker recycle (~50s x 17 = ~14min) exists so a HUMAN running
# `kubectl logs mirage-connect-0` sees only the current lab. There is no human
# here, and every grader reads the log through worker_log, which scopes to the
# lab's epoch regardless. Skip it.
export MIRAGE_SKIP_RECYCLE=1

# `mirage stop` refuses to grade a lab that is not the active one, so a human
# cannot grade lab 3 while lab 4 is injected. This harness grades deliberately
# and twice per lab, and a PASS clears the active marker — so the post-fix call
# would be refused. It knows which lab it injected; bypass the guard.
export MIRAGE_FORCE_STOP=1
source lib/common.sh

DEFAULT_LABS=(1 9 5 2 10 15 13 14 3 6 8 16 18 17 4 7 11 19 12)
LABS=("$@")
if [ "${#LABS[@]}" -eq 0 ]; then
  LABS=("${DEFAULT_LABS[@]}")
fi

RESULTS=()
started=$(date +%s)
attempt=0
pre=""
post=""
gradelog=""

# Only one run at a time: concurrent runs reset and patch the same cluster, so
# their results are meaningless. `pgrep` is not reliable in every environment
# (it cannot read the process list under some sandboxes), so hold a real lock.
LOCKDIR="${STATE_DIR:-.state}/run-all-labs.lock"
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  err "another run-all-labs.sh is already running (lock: $LOCKDIR)"
  say "  Results from concurrent runs are invalid — they share one cluster."
  say "  Wait for it to finish, or if it died: rmdir $LOCKDIR"
  exit 2
fi
printf '%s\n' "$$ $(date -u +%FT%TZ)" > "$LOCKDIR/owner" 2>/dev/null
trap 'rm -rf "$LOCKDIR"' EXIT INT TERM

# ---------------------------------------------------------------- fix recipes
# One function per lab applying the documented solve. Kept here rather than in
# the labs so the labs stay honest about being unsolved.

ca_password() { target_ca_password; }
user_password() { scram_password "$1"; }
jaas_patch() {   # $1=user $2=password -> writes merge patch to stdout
  python3 -I -c "
import json,sys
u,pw=sys.argv[1],sys.argv[2]
jaas='org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";' % (u,pw)
print(json.dumps({'spec':{'config':{
  'target.cluster.sasl.jaas.config':jaas,
  'producer.override.sasl.jaas.config':jaas}}}))" "$1" "$2"
}
patch_conn() { k patch kafkaconnector "$CONNECTOR" --type merge -p "$1" >/dev/null 2>&1; }
restart_conn() { k annotate kafkaconnector "$CONNECTOR" strimzi.io/restart=true --overwrite >/dev/null 2>&1; }

# Put the correct SCRAM credentials on both entities, then restart — in that
# order. Config pushes are asynchronous: restarting before the runtime carries
# the new password makes the tasks retry the OLD one and burn their autoRestart
# budget (maxRestarts: 5), after which nothing recovers them. This is the
# restart-ordering trap the labs' solves warn about.
apply_creds_then_restart() {   # $1 = KafkaUser whose password to use
  local user="$1" pw n=0 want
  pw="$(user_password "$user")"
  if [ -z "$pw" ]; then
    err "could not read password for $user — is the KafkaUser secret present?"
    return 1
  fi

  patch_conn "$(jaas_patch "$user" "$pw")"

  # Wait until the WORKER reports the corrected JAAS, not just the CR.
  want="password=\"$pw\""
  while [ "$n" -lt 180 ]; do
    case "$(runtime_config_value 'producer.override.sasl.jaas.config')" in
      *"$want"*) break ;;
    esac
    sleep 6; n=$((n+6))
  done
  if [ "$n" -ge 180 ]; then
    warn "runtime never reported the new credentials; restarting anyway"
  fi

  restart_conn
  sleep 20
  # A task that failed on the old password may still be FAILED; one more nudge
  # now that the runtime definitely has the right one.
  if [ "$(conn_task_count FAILED)" != "0" ]; then
    restart_conn
    sleep 20
  fi
}

fix_1() { patch_conn '{"spec":{"tasksMax":12}}'; }

fix_2() {
  patch_conn '{"spec":{"config":{
    "source.cluster.bootstrap.servers":"kafka-source-kafka-bootstrap:9092",
    "target.cluster.bootstrap.servers":"kafka-target-kafka-bootstrap:9092",
    "producer.override.bootstrap.servers":"kafka-target-kafka-bootstrap:9092"}}}'
  restart_conn
}

fix_3() {
  k patch kafkaconnect "$CONNECT" --type merge -p '{"spec":{
    "jvmOptions":{"gcLoggingEnabled":false,"-Xms":"768m","-Xmx":"768m",
                  "-XX":{"ExitOnOutOfMemoryError":"true"}}}}' >/dev/null 2>&1
  patch_conn '{"spec":{"config":{
    "consumer.max.poll.records":"500","consumer.fetch.max.bytes":"52428800",
    "consumer.max.partition.fetch.bytes":"1048576",
    "producer.override.buffer.memory":"33554432",
    "producer.override.max.request.size":"2097152"}}}'
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=420s >/dev/null 2>&1
}

fix_4() {
  # Re-create from the beginning of the log so the gap is replayed.
  kq delete kafkaconnector --all --wait=true >/dev/null 2>&1
  sed 's/consumer.auto.offset.reset: "latest"/consumer.auto.offset.reset: "earliest"/' \
    manifests/connector.yaml | kubectl apply -f - >/dev/null 2>&1
}

fix_5() { patch_conn '{"spec":{"config":{"topics":"lab-orders"}}}'; restart_conn; }

fix_6() {
  k patch resourcequota mirage-quota --type merge \
    -p '{"spec":{"hard":{"requests.memory":"8Gi","requests.cpu":"6","pods":"12"}}}' >/dev/null 2>&1
  # Raising the quota only lets the pods be ADMITTED; they still have to pull,
  # start and pass readiness. Wait for the requested replica count.
  local want n=0
  want="$(connect_get '.spec.replicas')"
  [ -z "$want" ] && want=3
  while [ "$n" -lt 420 ]; do
    [ "$(ready_connect_pods)" -ge "$want" ] 2>/dev/null && break
    sleep 15; n=$((n+15))
  done
}

fix_7() {
  # Ordering matters, and getting it wrong is the whole trap: anything deleted
  # while a connector is still running gets recreated. Heartbeats in particular
  # reappear within seconds. So stop EVERY producer first, then clean, then
  # re-seed — the same ordering fix_12 needs.
  kq delete kafkaconnector --all --wait=true >/dev/null 2>&1

  # The loop connector's config survives its own deletion. Connect's
  # config.storage.topic is compacted and replayed on every worker start, and
  # lab 7's loop used aliases target->target2, so the worker keeps recreating
  # mm2-offset-syncs.target2.internal on the target no matter how often you
  # delete the topic. Verified: the configs topic still holds 5 records naming
  # target2 after `delete kafkaconnector --all`.
  #
  # Only rebuilding the Connect cluster (which recreates the internal topics)
  # clears that, so do it here rather than deleting the symptom in a loop.
  recreate_connect_cluster >/dev/null 2>&1

  local tp n=0 attempt
  tp="$(tgt_pod)"

  # Clean, then CONFIRM. Retry, because a straggler write can still resurrect a
  # topic between the delete and the check.
  for attempt in 1 2 3; do
    _kafka_in "$tp" 'for t in $(bin/kafka-topics.sh --bootstrap-server '"$TGT_BOOT"' --list \
        | grep -E "\.heartbeats$|^mm2-|\.internal$|\.replica$"); do
        bin/kafka-topics.sh --bootstrap-server '"$TGT_BOOT"' --delete --topic "$t" 2>/dev/null; done' >/dev/null 2>&1
    sleep 10
    if [ "$(_kafka_in "$tp" "bin/kafka-topics.sh --bootstrap-server $TGT_BOOT --list" 2>/dev/null \
            | grep -cE '\.heartbeats$|^mm2-|\.internal$|\.replica$')" = "0" ]; then
      break
    fi
  done

  # Drop the polluted target topic. The delete is asynchronous and the topic is
  # auto-recreated empty, so wait for the COUNT to clear rather than for the
  # topic to disappear — tgt_count reads stale offsets until it does, which is
  # what made the inflation assertion fire on data that no longer existed.
  _kafka_in "$tp" "bin/kafka-topics.sh --bootstrap-server $TGT_BOOT --delete --topic $TOPIC" >/dev/null 2>&1
  n=0
  while [ "$n" -lt 90 ]; do
    [ "$(topic_count "$tp" "$TGT_BOOT" "$TOPIC")" -le 0 ] 2>/dev/null && break
    sleep 5; n=$((n+5))
  done

  # Re-seed with a correctly scoped connector: narrow topic pattern plus the
  # exclude guard, which is what stops the loop recurring.
  kubectl apply -f manifests/connector.yaml >/dev/null 2>&1
  patch_conn '{"spec":{"config":{"topics":"lab-orders",
    "topics.exclude":".*\\.heartbeats, mm2.*\\.internal, .*\\.replica, __.*"}}}'

  # The grader measures a replication delta, which is 0 until the mirror has
  # recreated the target topic and assigned tasks.
  settle_connector 180 >/dev/null 2>&1
  n=0
  while [ "$n" -lt 120 ]; do
    topic_exists "$tp" "$TGT_BOOT" "$TOPIC" && break
    sleep 5; n=$((n+5))
  done
}

fix_8() {
  mark lab8.answer broker
  k patch kafkanodepool source-pool --type json \
    -p '[{"op":"remove","path":"/spec/template/pod/affinity"}]' >/dev/null 2>&1
  forget_broker_pods
  local n=0
  while [ "$n" -lt 300 ]; do
    [ -n "$(broker_pod kafka-source)" ] && break
    sleep 15; n=$((n+15))
  done
  restart_conn
}

fix_9() { patch_conn '{"spec":{"state":"running"}}'; }

fix_10() {
  patch_conn '{"spec":{"config":{
    "transforms":"StripInternal",
    "transforms.StripInternal.type":"org.apache.kafka.connect.transforms.DropHeaders",
    "transforms.StripInternal.headers":"__internal_debug"}}}'
  restart_conn
}

fix_11() {
  k patch kafkaconnect "$CONNECT" --type merge -p '{"spec":{"config":{
    "group.id":"connect-cluster_mirage",
    "config.storage.topic":"mirage-connect-configs",
    "offset.storage.topic":"mirage-connect-offsets",
    "status.storage.topic":"mirage-connect-status"}}}' >/dev/null 2>&1
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=420s >/dev/null 2>&1
}

# Mirrors `mirage solve 19`: fix the operator and touch nothing else. The CR
# already says running; the connector resumes once the operator reconciles it.
fix_19() {
  k set env deploy/"$OPERATOR_DEPLOY" JAVA_OPTS- >/dev/null 2>&1
  k rollout status deploy/"$OPERATOR_DEPLOY" --timeout=180s >/dev/null 2>&1
}

fix_12() {
  # This recipe deliberately mirrors `mirage solve 12` step for step, so a pass
  # here is evidence the DOCUMENTED answer works. If you change one, change the
  # other — a recipe that diverges from its solve validates nothing about it.

  # Step 1 — stop the loop before it creates more junk.
  kq delete kafkaconnector lab-stray --wait=true >/dev/null 2>&1

  # Step 2 — make the workers able to run, and stable: widen the quota, then
  # size heap at 50-70% of a container that fits the node.
  k patch resourcequota mirage-quota --type merge \
    -p '{"spec":{"hard":{"requests.memory":"8Gi","requests.cpu":"6","pods":"12"}}}' >/dev/null 2>&1
  k patch kafkaconnect "$CONNECT" --type merge -p '{"spec":{
    "jvmOptions":{"gcLoggingEnabled":false,"-Xms":"512m","-Xmx":"512m",
                  "-XX":{"ExitOnOutOfMemoryError":"true"}},
    "resources":{"requests":{"cpu":"100m","memory":"896Mi"},
                 "limits":{"memory":"896Mi"}}}}' >/dev/null 2>&1
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=600s >/dev/null 2>&1

  # Step 3 — stop EVERY connector before touching topics. The baseline mirror
  # emits heartbeats too, so cleaning up while anything runs recreates it.
  kq delete kafkaconnector lab-orders-from-source-to-target --wait=true >/dev/null 2>&1
  kq delete kafkaconnector --all --wait=true >/dev/null 2>&1

  # Step 3b — rebuild the Connect cluster, exactly as fix_7 does and for the
  # same reason. The stray connector's config outlives its deletion: Connect's
  # config.storage.topic is compacted and replayed on worker start, so the
  # worker keeps recreating mm2-offset-syncs.target2.internal on the target no
  # matter how often the topic is deleted.
  #
  # This was missing and lab 12 failed the scan on exactly that assertion
  # ("no replication artefacts left on the target (got '1')") while all 15 other
  # checks passed. Measured: after deleting lab-stray, mirage-connect-configs
  # still held 6 records naming target2; recreate_connect_cluster took it to 0
  # and the artefact stayed gone.
  #
  # It passed in earlier runs only because the per-lab worker recycle happened
  # to roll the worker at the right moment — MIRAGE_SKIP_RECYCLE=1 removed that
  # accident and exposed the real gap. Do not rely on a restart you did not ask
  # for.
  recreate_connect_cluster >/dev/null 2>&1
  kq delete kafkaconnector --all --wait=true >/dev/null 2>&1

  local tp n=0
  tp="$(tgt_pod)"

  # Now that nothing is writing, remove the loop's artefacts.
  _kafka_in "$tp" 'for t in $(bin/kafka-topics.sh --bootstrap-server '"$TGT_BOOT"' --list \
      | grep -E "\.heartbeats$|^mm2-|\.internal$|\.replica$"); do
      bin/kafka-topics.sh --bootstrap-server '"$TGT_BOOT"' --delete --topic "$t" 2>/dev/null; done' >/dev/null 2>&1

  # Drop the polluted target topic and wait for the delete to land.
  _kafka_in "$tp" "bin/kafka-topics.sh --bootstrap-server $TGT_BOOT --delete --topic $TOPIC" >/dev/null 2>&1
  while [ "$n" -lt 60 ]; do
    topic_exists "$tp" "$TGT_BOOT" "$TOPIC" || break
    sleep 3; n=$((n+3))
  done

  # Step 4 — bring the connector back with every correction applied at once.
  kubectl apply -f manifests/connector.yaml >/dev/null 2>&1
  patch_conn '{"spec":{"tasksMax":12,"config":{"topics":"lab-orders",
    "topics.exclude":".*\\.heartbeats, mm2.*\\.internal, .*\\.replica, __.*"}}}'
  settle_connector 180 >/dev/null 2>&1
}

fix_13() { apply_creds_then_restart kcd-replicator; }

fix_14() { apply_creds_then_restart kcd-replicator; }

# Build the truststore-password patch. Kept as a named function with the python
# in a quoted heredoc: inlining it in `python3 -c "..."` inside a command
# substitution lets the shell mangle the quoting, which silently produced a
# SyntaxError and a patch that was never applied.
truststore_patch() {   # $1 = password -> merge patch on stdout
  python3 -I - "$1" <<'PY'
import json,sys
pw = sys.argv[1]
print(json.dumps({'spec': {'config': {
    'target.cluster.ssl.truststore.password': pw,
    'producer.override.ssl.truststore.password': pw,
}}}))
PY
}

fix_15() {  local pw n patch
  pw="$(ca_password)"
  if [ -z "$pw" ]; then warn "could not read CA password"; return 1; fi

  patch="$(truststore_patch "$pw")"
  if [ -z "$patch" ]; then err "failed to build the truststore patch"; return 1; fi
  patch_conn "$patch"

  # Same ordering trap as the credential labs: wait for the runtime to carry the
  # corrected truststore password before restarting, or the tasks just retry the
  # broken one and burn their autoRestart budget.
  n=0
  while [ "$n" -lt 150 ]; do
    [ "$(runtime_config_value 'producer.override.ssl.truststore.password')" = "$pw" ] && break
    sleep 6; n=$((n+6))
  done

  restart_conn
  sleep 20
  if [ "$(conn_task_count FAILED)" != "0" ]; then
    restart_conn
    sleep 20
  fi
}

fix_16() {
  # Mirrors `mirage solve 16` route (a): make the target able to accept the
  # record, which is the lossless fix. The alternative (errors.tolerance=all)
  # also passes the grader but drops the record silently, so the recipe takes
  # the route the solve recommends.
  mark lab16.answer accept

  local tp
  tp="$(tgt_pod)"
  _kafka_in "$tp" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
    --entity-type topics --entity-name $TOPIC \
    --alter --add-config max.message.bytes=2097152" >/dev/null 2>&1

  # Widening the limit does NOT revive a task that already exhausted its
  # autoRestart budget retrying the oversized record: it stays FAILED with
  # "Unrecoverable exception from producer send callback". Restart it only once
  # the target can actually accept the write — restarting first just spends
  # another attempt.
  sleep 20
  if [ "$(conn_task_count FAILED)" != "0" ]; then
    restart_conn
    sleep 30
  fi
  settle_connector 180 >/dev/null 2>&1

  # A restarted task has to rejoin, be assigned partitions, and work through the
  # backlog that built up behind the pill. Grading before that finishes burned
  # all three retry attempts, so wait for the recovery here instead: no FAILED
  # tasks, and every partition actually moving.
  local n=0
  while [ "$n" -lt 240 ]; do
    if [ "$(conn_task_count FAILED)" = "0" ] && [ "$(conn_task_count RUNNING)" -ge 1 ] 2>/dev/null; then
      break
    fi
    restart_conn
    sleep 20; n=$((n+20))
  done

  # Let the backlog drain before the grader samples per-partition offsets.
  produce 600 1000 2000 >/dev/null 2>&1
  sleep 45
}

fix_17() {
  # Mirrors `mirage solve 17` route (5a): raise the producer limit so the
  # oversized records are DELIVERED. errors.tolerance=all would also clear the
  # alert, but the grader asserts the records arrived, so the lossy route
  # correctly fails — that asymmetry is the lesson.
  #
  # patch_connector_runtime, not patch_conn: Connect applies config
  # asynchronously and a restart issued before the runtime carries the new value
  # makes tasks retry the old one and spend the autoRestart budget. Wait for the
  # runtime, then restart only if something actually died.
  patch_connector_runtime \
    '{"spec":{"config":{"producer.override.max.request.size":"2097152"}}}' \
    'producer.override.max.request.size' '2097152' 180 >/dev/null 2>&1

  sleep 20
  if [ "$(conn_task_count FAILED)" != "0" ]; then
    restart_conn
    sleep 30
  fi
  settle_connector 180 >/dev/null 2>&1

  # The records refused during the fault are still on the source, unreplicated.
  # Give the mirror time to work through that backlog before grading.
  #
  # Waits for the gap to STOP MOVING, not for it to reach an absolute number:
  # a topic that has been re-seeded carries a permanent historical deficit, so
  # `gap -le 20` never becomes true and this loop would always burn its full
  # timeout. Same cumulative-vs-delta trap the grader itself fell into.
  local n=0 prev=-1 gap
  while [ "$n" -lt 180 ]; do
    gap=$(( $(src_count) - $(tgt_count) ))
    [ "$gap" = "$prev" ] && break     # drained: two reads agree
    prev="$gap"
    sleep 15; n=$((n+15))
  done
}

fix_18() {
  # Mirrors `mirage solve 18` route (4a): remove the target's timestamp window so
  # the pill is DELIVERED. The grader asserts the pill's marker is on the target,
  # so errors.tolerance=all would not satisfy the lossless branch.
  mark lab18.answer accept

  _kafka_in "$(tgt_pod)" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
    --entity-type topics --entity-name $TOPIC \
    --alter --delete-config message.timestamp.before.max.ms" >/dev/null 2>&1

  # Same order as lab 16: the cause is gone before any restart, otherwise the
  # restarted task re-reads the pill and spends another autoRestart attempt.
  sleep 20
  restart_conn
  sleep 30
  settle_connector 180 >/dev/null 2>&1

  local n=0
  while [ "$n" -lt 240 ]; do
    if [ "$(conn_task_count FAILED)" = "0" ] && [ "$(conn_task_count RUNNING)" -ge 1 ] 2>/dev/null; then
      break
    fi
    restart_conn
    sleep 20; n=$((n+20))
  done

  produce 600 1000 2000 >/dev/null 2>&1
  sleep 45
}


# ------------------------------------------------------------- route variants
# `./run-all-labs.sh 16:skip 13:bad_plaintext` runs fix_<n>_<variant> instead of
# fix_<n>. A lab usually has more than one legitimate answer, and a grader that
# only ever saw one of them can fail a correct fix nobody tested. Variants named
# bad_* are the tempting WRONG fixes: they must still FAIL after being applied,
# or the grader passes something the lab exists to teach against.
#
# Each mirrors an option written in that lab's solve/hint/WHY NOT section.

# --- alternate routes: must PASS ---

fix_3_b() {    # Option B: keep the heap, give the container room
  k patch kafkaconnect "$CONNECT" --type merge -p '{"spec":{
    "resources":{"requests":{"cpu":"250m","memory":"2Gi"},"limits":{"memory":"2Gi"}}}}' >/dev/null 2>&1
  patch_conn '{"spec":{"config":{
    "consumer.max.poll.records":"500","consumer.fetch.max.bytes":"52428800",
    "consumer.max.partition.fetch.bytes":"1048576",
    "producer.override.buffer.memory":"33554432",
    "producer.override.max.request.size":"2097152"}}}'
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=420s >/dev/null 2>&1
}

fix_6_b() {    # Option B: shrink each worker to fit the existing quota
  k patch kafkaconnect "$CONNECT" --type merge -p '{"spec":{
    "resources":{"requests":{"cpu":"100m","memory":"768Mi"},"limits":{"memory":"768Mi"}},
    "jvmOptions":{"-Xms":"448m","-Xmx":"448m","-XX":{"ExitOnOutOfMemoryError":"true"}}}}' >/dev/null 2>&1
  # Strimzi does not roll workers on a resources change and Kubernetes never
  # resizes a running pod, so old-size pods must be replaced to free quota.
  kq delete pod -l "strimzi.io/cluster=$CONNECT" --wait=false >/dev/null 2>&1
  local want n=0
  want="$(connect_get '.spec.replicas')"; [ -z "$want" ] && want=3
  while [ "$n" -lt 420 ]; do
    [ "$(ready_connect_pods)" -ge "$want" ] 2>/dev/null && break
    sleep 15; n=$((n+15))
  done
}

fix_9_remove() {   # remove spec.state, let the Strimzi default apply
  k patch kafkaconnector "$CONNECTOR" --type json \
    -p '[{"op":"remove","path":"/spec/state"}]' >/dev/null 2>&1
}

fix_10_remove() {  # Option A: remove the transform entirely
  k patch kafkaconnector "$CONNECTOR" --type json -p '[
    {"op":"remove","path":"/spec/config/transforms"},
    {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.type"},
    {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.header"},
    {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.value.literal"}]' >/dev/null 2>&1
  restart_conn
}

fix_14_b() {   # Option B: grant the rights to the identity in use
  k patch kafkauser kcd-readonly --type merge -p '{"spec":{"authorization":{
    "type":"simple","acls":[
      {"resource":{"type":"topic","name":"*","patternType":"literal"},
       "operations":["Read","Write","Create","Describe","DescribeConfigs"],"host":"*"},
      {"resource":{"type":"group","name":"*","patternType":"literal"},
       "operations":["Read","Describe"],"host":"*"},
      {"resource":{"type":"cluster"},
       "operations":["Describe","DescribeConfigs","Create"],"host":"*"}]}}}' >/dev/null 2>&1
  sleep 30    # the user operator has to write the ACLs to the broker
  restart_conn
  sleep 20
  [ "$(conn_task_count FAILED)" != "0" ] && { restart_conn; sleep 20; }
}

fix_16_skip() {    # route (b): errors.tolerance=all — passes WITH silent loss
  mark lab16.answer skip
  patch_connector_runtime '{"spec":{"config":{"errors.tolerance":"all",
    "errors.log.enable":"true","errors.log.include.messages":"false"}}}' \
    'errors.tolerance' 'all' 180 >/dev/null 2>&1
  sleep 20
  [ "$(conn_task_count FAILED)" != "0" ] && { restart_conn; sleep 30; }
  settle_connector 180 >/dev/null 2>&1
  produce 600 1000 2000 >/dev/null 2>&1
  sleep 45
}

fix_16_escalate() {  # route (c): escalate, and still see the recovery through
  fix_16
  mark lab16.answer escalate
}

fix_17_remove() {  # 5b: drop the override and inherit the 1MB default
  k patch kafkaconnector "$CONNECTOR" --type json \
    -p '[{"op":"remove","path":"/spec/config/producer.override.max.request.size"}]' >/dev/null 2>&1
  local n=0
  while [ "$n" -lt 180 ]; do
    [ -z "$(runtime_config_value 'producer.override.max.request.size')" ] && break
    sleep 6; n=$((n+6))
  done
  sleep 20
  [ "$(conn_task_count FAILED)" != "0" ] && { restart_conn; sleep 30; }
  settle_connector 180 >/dev/null 2>&1
  sleep 60
}

fix_18_widen() {   # widen the window past the record's age instead of deleting it
  mark lab18.answer accept
  _kafka_in "$(tgt_pod)" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
    --entity-type topics --entity-name $TOPIC \
    --alter --add-config message.timestamp.before.max.ms=86400000" >/dev/null 2>&1
  sleep 20; restart_conn; sleep 30
  settle_connector 180 >/dev/null 2>&1
  local n=0
  while [ "$n" -lt 240 ]; do
    [ "$(conn_task_count FAILED)" = "0" ] && [ "$(conn_task_count RUNNING)" -ge 1 ] 2>/dev/null && break
    restart_conn; sleep 20; n=$((n+20))
  done
  produce 600 1000 2000 >/dev/null 2>&1
  sleep 45
}

fix_18_skip() {    # route (b): errors.tolerance=all — passes WITH silent loss
  mark lab18.answer skip
  patch_connector_runtime '{"spec":{"config":{"errors.tolerance":"all",
    "errors.log.enable":"true","errors.log.include.messages":"false"}}}' \
    'errors.tolerance' 'all' 180 >/dev/null 2>&1
  sleep 20
  [ "$(conn_task_count FAILED)" != "0" ] && { restart_conn; sleep 30; }
  settle_connector 180 >/dev/null 2>&1
  produce 600 1000 2000 >/dev/null 2>&1
  sleep 45
}

# --- tempting wrong fixes: must still FAIL ---

fix_1_bad_restart()  { restart_conn; sleep 30; }   # restart is not parallelism
fix_2_bad_restart()  { restart_conn; sleep 30; }   # restart without fixing the addresses
fix_8_bad_restart()  { mark lab8.answer connector; restart_conn; sleep 60; }  # broker still down
fix_13_bad_restart() { restart_conn; sleep 30; }   # same bogus password
fix_16_bad_restart() { mark lab16.answer accept; restart_conn; sleep 60; }    # target still refuses
fix_18_bad_restart() { mark lab18.answer accept; restart_conn; sleep 60; }

fix_9_bad_rest_resume() {   # resume via REST: works, then the operator reverts it
  kq exec "$(connect_pod)" -- curl -s -XPUT \
    "localhost:8083/connectors/$CONNECTOR/resume" >/dev/null 2>&1
  sleep 150   # > one operator reconcile, so the revert has happened
}

fix_10_bad_insertheader() {  # validates, goes Ready, then every task dies on DataException
  k patch kafkaconnector "$CONNECTOR" --type json -p '[
    {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.type"},
    {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.header"},
    {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.value.literal"}]' >/dev/null 2>&1
  patch_conn '{"spec":{"config":{"transforms":"Ins",
    "transforms.Ins.type":"org.apache.kafka.connect.transforms.InsertHeader",
    "transforms.Ins.header":"locale","transforms.Ins.value.literal":"en-US"}}}'
  restart_conn; sleep 60
}

fix_11_bad_partial() {   # restore group.id only; storage topics stay renamed
  k patch kafkaconnect "$CONNECT" --type merge \
    -p '{"spec":{"config":{"group.id":"connect-cluster_mirage"}}}' >/dev/null 2>&1
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=420s >/dev/null 2>&1
}

fix_13_bad_plaintext() {   # "just put it back on :9092" — the brief forbids it
  patch_connector_runtime '{"spec":{"config":{
    "target.cluster.bootstrap.servers":"kafka-target-kafka-bootstrap:9092",
    "producer.override.bootstrap.servers":"kafka-target-kafka-bootstrap:9092",
    "target.cluster.security.protocol":"PLAINTEXT",
    "producer.override.security.protocol":"PLAINTEXT"}}}' \
    'target.cluster.bootstrap.servers' 'kafka-target-kafka-bootstrap:9092' 180 >/dev/null 2>&1
  restart_conn; sleep 30
}

fix_17_bad_tolerance() {   # clears the alert, drops the records — must FAIL
  patch_connector_runtime '{"spec":{"config":{"errors.tolerance":"all"}}}' \
    'errors.tolerance' 'all' 180 >/dev/null 2>&1
  sleep 20
  [ "$(conn_task_count FAILED)" != "0" ] && { restart_conn; sleep 30; }
  settle_connector 180 >/dev/null 2>&1
}

fix_7_bad_no_rebuild() {   # every cleanup step EXCEPT the Connect rebuild
  kq delete kafkaconnector --all --wait=true >/dev/null 2>&1
  local tp n=0
  tp="$(tgt_pod)"
  _kafka_in "$tp" 'for t in $(bin/kafka-topics.sh --bootstrap-server '"$TGT_BOOT"' --list \
      | grep -E "\.heartbeats$|^mm2-|\.internal$|\.replica$"); do
      bin/kafka-topics.sh --bootstrap-server '"$TGT_BOOT"' --delete --topic "$t" 2>/dev/null; done' >/dev/null 2>&1
  _kafka_in "$tp" "bin/kafka-topics.sh --bootstrap-server $TGT_BOOT --delete --topic $TOPIC" >/dev/null 2>&1
  while [ "$n" -lt 90 ]; do
    [ "$(topic_count "$tp" "$TGT_BOOT" "$TOPIC")" -le 0 ] 2>/dev/null && break
    sleep 5; n=$((n+5))
  done
  kubectl apply -f manifests/connector.yaml >/dev/null 2>&1
  patch_conn '{"spec":{"config":{"topics":"lab-orders",
    "topics.exclude":".*\\.heartbeats, mm2.*\\.internal, .*\\.replica, __.*"}}}'
  settle_connector 180 >/dev/null 2>&1
  # A worker restart replays the compacted config topic — the loop's config
  # comes back with it. This is what makes skipping the rebuild wrong.
  kq delete pod -l "strimzi.io/cluster=$CONNECT" --wait=true >/dev/null 2>&1
  settle_connector 240 >/dev/null 2>&1
  sleep 30
}

# Every variant, for `./run-all-labs.sh --variants`.
ALL_VARIANTS=(3:b 6:b 9:remove 10:remove 14:b 16:skip 16:escalate 17:remove 18:widen 18:skip
  1:bad_restart 2:bad_restart 8:bad_restart 13:bad_restart 16:bad_restart 18:bad_restart
  9:bad_rest_resume 10:bad_insertheader 11:bad_partial 13:bad_plaintext 17:bad_tolerance
  7:bad_no_rebuild)

# -------------------------------------------------------------------- driver

# --variants = every route variant; --all = base fixes then every variant.
# Either flag may sit anywhere among explicit labs: `13 18 --variants`.
_expanded=()
for _a in "${LABS[@]}"; do
  case "$_a" in
    --variants) _expanded+=("${ALL_VARIANTS[@]}") ;;
    --all)      _expanded+=("${DEFAULT_LABS[@]}" "${ALL_VARIANTS[@]}") ;;
    *)          _expanded+=("$_a") ;;
  esac
done
LABS=("${_expanded[@]}")

for spec in "${LABS[@]}"; do
  n="${spec%%:*}"; variant=""
  [ "$spec" != "$n" ] && variant="${spec#*:}"
  fixfn="fix_$n${variant:+_$variant}"
  expect="PASS"
  case "$variant" in bad*) expect="FAIL" ;; esac
  hr
  say "${C_B}### LAB $spec ###${C_0}  ${C_DIM}(fix: $fixfn, expect post-fix $expect)${C_0}"
  hr

  if ! NO_COLOR=1 ./mirage reset --fresh >/dev/null 2>&1; then
    warn "reset failed before lab $n — attempting repair"
    NO_COLOR=1 ./mirage repair >/dev/null 2>&1
  fi

  # The security labs build their fault from a KafkaUser password. If the user
  # operator has not written the Secret yet, they inject password="" and become
  # unwinnable, so make sure the credentials exist before injecting.
  if ! ensure_scram_users; then
    RESULTS+=("$spec|NO-CREDENTIALS|-|$expect")
    err "lab $n: SCRAM users never became available — skipping"
    continue
  fi

  info "injecting"
  # Keep the injection output instead of discarding it. lab_break prints the
  # numbers it computed and warns when it cannot build a clean fault (lab 6's
  # "no quota window fits both fixes" is the example), and a post-fix failure is
  # undiagnosable without it: a lab 6 FAIL was once left unexplainable because
  # this was >/dev/null 2>&1 and the warning, if any, was gone.
  injectlog="$STATE_DIR/inject-$n.log"
  if ! NO_COLOR=1 ./mirage start "$n" >"$injectlog" 2>&1; then
    RESULTS+=("$spec|INJECT-FAILED|-|$expect")
    err "lab $n: injection failed"
    say "      ${C_DIM}(output — $injectlog)${C_0}"
    tail -5 "$injectlog" 2>/dev/null | sed 's/^/      /'
    continue
  fi
  # Surface a warning even when injection "succeeded": a lab that warns here can
  # still be unwinnable, which otherwise shows up only as a confusing FAIL.
  if grep -qE '^!!' "$injectlog" 2>/dev/null; then
    warn "lab $n: injection warned — the fault may not behave as documented"
    grep -E '^!!' "$injectlog" | sed 's/^/      /'
  fi

  info "grading BEFORE fix (expect FAIL)"
  if NO_COLOR=1 ./mirage stop "$n" >/dev/null 2>&1; then
    pre="PASSED-UNFIXED"      # bad: the lab grades green with the fault live
    err "lab $n: pre-fix grade PASSED — false pass"
  else
    pre="failed-ok"
    say "  pre-fix: correctly FAILED"
  fi

  info "applying $fixfn"
  if declare -F "$fixfn" >/dev/null; then
    "$fixfn"
  else
    RESULTS+=("$spec|$pre|NO-RECIPE|$expect")
    err "no fix recipe $fixfn"
    continue
  fi

  # Recovery is not instant and varies by lab: a connector coming back from
  # UNASSIGNED can take ~2 minutes, and a Connect rollout longer. A fixed sleep
  # here produced false post-fix FAILures, so wait for the cluster to settle and
  # then retry the grade until a real deadline.
  info "waiting for recovery to settle"
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=420s >/dev/null 2>&1
  settle_connector 180 >/dev/null 2>&1

  info "grading AFTER fix (expect $expect, retrying up to ~8min)"
  post="FAIL"
  attempt=0
  gradelog="$STATE_DIR/grade-${spec/:/-}.log"
  # A wrong fix must fail EVERY attempt — a pass on a later retry is still a
  # false pass — so it gets the same three attempts as a right one.
  while [ "$attempt" -lt 3 ]; do
    # Keep the output of EVERY attempt. Re-grading afterwards to find out what
    # failed runs minutes later against a cluster that has moved on — it can
    # report different failures, or none, and tell you nothing about the run.
    if NO_COLOR=1 ./mirage stop "$n" >"$gradelog" 2>&1; then
      post="PASS"
      say "  ${C_GRN}post-fix: PASS${C_0} (attempt $((attempt+1)))"
      break
    fi
    attempt=$((attempt+1))
    [ "$attempt" -lt 3 ] && { say "  retrying grade in 60s"; sleep 60; }
  done
  if [ "$post" = "FAIL" ] && [ "$expect" = "FAIL" ]; then
    say "  ${C_GRN}post-fix: correctly FAILED${C_0} (wrong fix rejected)"
    grep -E '\[FAIL\]' "$gradelog" 2>/dev/null | head -3 | sed 's/^/      /'
  elif [ "$post" = "PASS" ] && [ "$expect" = "FAIL" ]; then
    err "lab $spec: a WRONG fix PASSED — false pass"
  elif [ "$post" = "FAIL" ]; then
    err "lab $spec: post-fix grade still FAILING after $attempt attempts"
    say "      ${C_DIM}(assertions from the final attempt — $gradelog)${C_0}"
    if grep -qE '\[FAIL\]' "$gradelog" 2>/dev/null; then
      grep -E '\[FAIL\]' "$gradelog" | sed 's/^/      /'
    else
      # No assertion failed, so the grade died before or during the checks.
      warn "      no [FAIL] lines — the grade itself errored. Last output:"
      tail -15 "$gradelog" 2>/dev/null | sed 's/^/      /'
    fi
  fi

  RESULTS+=("$spec|$pre|$post|$expect")
done

elapsed=$(( $(date +%s) - started ))
hr
say "${C_B}SUMMARY${C_0}  (${elapsed}s)"
hr
printf '  %-22s %-16s %-10s %s\n' "lab" "pre-fix" "post-fix" "expected"
bad=0
for r in "${RESULTS[@]}"; do
  IFS='|' read -r n pre post expect <<<"$r"
  expect="${expect:-PASS}"
  ok="ok"
  { [ "$pre" = "failed-ok" ] && [ "$post" = "$expect" ]; } || { ok="BAD"; bad=$((bad+1)); }
  printf '  %-22s %-16s %-10s %-8s %s\n' "$n" "$pre" "$post" "$expect" "$ok"
done
hr
if [ "$bad" -eq 0 ]; then
  say "${C_GRN}${C_B}  All ${#RESULTS[@]} labs behaved correctly.  ${C_0}"
else
  err "$bad of ${#RESULTS[@]} labs misbehaved (see above)"
fi
exit "$bad"
