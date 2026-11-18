#!/usr/bin/env bash
# Lab 11 — Orphaned Connect cluster: changed group.id / storage topics

LAB_TITLE="Records missing after a naming tidy-up"
LAB_DIFFICULTY="hard"
LAB_ALERT="connector_count drops to 0"

lab_brief() {
cat <<'EOF'
SYMPTOM
  A deploy went out that "tidied up some naming". Everything LOOKS fine: the
  Connect cluster is Ready, the pod is Running, the connector is RUNNING and
  records are flowing right now.

  No error. No alert. But the target is missing a large block of records that
  the source has — produced around the time of the deploy — and they are not
  catching up.

  Compare what the cluster is using now against what it was using before the
  deploy.

YOUR JOB
  Work out what changed, undo it, and get replication healthy again.

WHEN DONE
  mirage stop 11
EOF
}

lab_break() {
  local before_src before_tgt
  before_src="$(src_count)"; before_tgt="$(tgt_count)"

  # Load runs ACROSS the roll. Strimzi re-creates the connector from its
  # KafkaConnector CR on the renamed group within a reconcile, so the connector
  # never visibly vanishes — the real cost is the offsets: the new group has
  # none, consumer.auto.offset.reset=latest, and everything produced while no
  # worker owned the connector is skipped. Without load there is nothing to
  # skip and the fault has no symptom at all.
  info "Generating load"
  produce 30000 1000 250 >/dev/null 2>&1 &
  local load=$!

  if ! k patch kafkaconnect "$CONNECT" --type merge -p '{"spec":{"config":{
      "group.id":"connect-cluster_mirage-tidied",
      "config.storage.topic":"mirage-tidied-connect-configs",
      "offset.storage.topic":"mirage-tidied-connect-offsets",
      "status.storage.topic":"mirage-tidied-connect-status"
  }}}' >/dev/null 2>&1; then
    wait "$load" 2>/dev/null
    err "could not inject the fault (KafkaConnect patch rejected)"
    return 1
  fi
  mark lab11.broken 1
  info "Waiting for the workers to roll (~60s)"
  k wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=300s >/dev/null 2>&1 || true
  wait "$load" 2>/dev/null
  info "Waiting for the connector to come back (~60s)"
  settle_connector 180 >/dev/null 2>&1 || true
  sleep 20
  # Post-mortem only: how many records the cold start skipped.
  mark lab11.skipped "$(( ($(src_count) - before_src) - ($(tgt_count) - before_tgt) ))"
}

lab_check() {
  local gid cfg off sts

  gid="$(connect_get '.spec.config.group\.id')"
  cfg="$(connect_get '.spec.config.config\.storage\.topic')"
  off="$(connect_get '.spec.config.offset\.storage\.topic')"
  sts="$(connect_get '.spec.config.status\.storage\.topic')"

  assert_eq "group.id restored" "$gid" "connect-cluster_mirage" \
    "group.id is half of the Connect cluster's identity. Change it and the workers join a different group, see an empty config topic, and believe no connectors exist." \
    "kubectl -n mirage get kafkaconnect $CONNECT -o jsonpath='{.spec.config.group\\.id}'"

  assert_eq "config.storage.topic restored" "$cfg" "mirage-connect-configs" \
    "Holds every connector's configuration. Pointing at a new topic means the cluster starts with no connectors at all." \
    "mirage topics — look for the topic that already holds data"

  assert_eq "offset.storage.topic restored" "$off" "mirage-connect-offsets" \
    "Holds source-connector offsets. A new topic means every connector cold-starts, and with auto.offset.reset=latest that is SILENT DATA LOSS." \
    "This is the same mechanism as Lab 4, but at cluster scope — it loses every connector's position at once."

  assert_eq "status.storage.topic restored" "$sts" "mirage-connect-status" \
    "Holds connector and task state plus error traces." \
    "kubectl -n mirage get kafkaconnect $CONNECT -o jsonpath='{.spec.config}'"

  assert_ge "Connect pods Running" "$(ready_connect_pods)" 1 \
    "The cluster must be healthy before connectors can be reconciled onto it." \
    "kubectl -n mirage get pods -l strimzi.io/cluster=$CONNECT"

  # The decisive evidence: the offsets topic in use must be the one that
  # actually holds committed offsets, not a freshly-created empty one.
  local used_off_count stray_count
  used_off_count="$(topic_count "$(tgt_pod)" "$TGT_BOOT" "${off:-mirage-connect-offsets}")"
  stray_count="$(topic_count "$(tgt_pod)" "$TGT_BOOT" mirage-tidied-connect-offsets)"
  assert_ge "the offsets topic in use actually holds offsets" "${used_off_count:-0}" 1 \
    "An empty offsets topic means every connector cold-starts. With consumer.auto.offset.reset=latest that is SILENT DATA LOSS — the connector resumes at the END of its source topics and skips everything in between. The old topic still holds the real offsets." \
    "in-use topic '${off}' holds ${used_off_count:-0} records; the stray 'mirage-tidied-connect-offsets' holds ${stray_count:-0}"

  info "Waiting for the connector to be reconciled back (up to 150s)"
  local n=0
  while [ "$n" -lt 150 ]; do
    [ "$(conn_tasks_total)" -ge 1 ] 2>/dev/null && break
    sleep 5; n=$((n+5))
  done

  assert_ge "connector has tasks again" "$(conn_tasks_total)" 1 \
    "Once the cluster regains its identity, the operator reconciles the KafkaConnector CRs back onto it." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  assert_ge "tasks RUNNING" "$(conn_task_count RUNNING)" 1 \
    "Identity restored but tasks dead means a second fault." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # A bare "grew by at least 1" is far too weak — a single heartbeat satisfies
  # it. Require most of what was produced to actually arrive.
  info "Confirming replication resumed"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 150)"
  assert_ge "data flows again at the expected volume" "${delta:-0}" 1600 \
    "Full recovery is throughput, not green status. A trickle may just be heartbeats." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.config}' | tr ',' '\n' | grep -E 'group.id|storage.topic'
  mirage topics

  Compare the storage topic names in the CR against the topics that exist — and
  crucially, against which of them actually HOLD DATA.

  List the topics on the target cluster and look for TWO sets of Connect
  internal topics:

    mirage topics

  You should see both mirage-connect-* and mirage-tidied-connect-*. One set
  holds your real connector state; the other is empty and freshly created.

  Prove which is which by counting records in each:

    for t in mirage-connect-offsets mirage-tidied-connect-offsets; do
      echo -n "$t: "
      kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$t" 2>/dev/null \
        | awk -F: '{s+=$3} END {print s+0}'
    done

  The one with millions of records is the real one.

  Then read what the cluster is currently pointed at:

    kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.config}' | tr ',' '\n' | grep -E 'group.id|storage.topic'

  Four settings form a Connect cluster's identity:
      group.id
      config.storage.topic
      offset.storage.topic
      status.storage.topic

  Point any of them somewhere new and the cluster comes up against empty topics
  with all connector state orphaned. The repo says so verbatim:

    "These names carry the Connect cluster's live config/offset/status topics:
     changing them for an existing deploy orphans its connector state."

  Restore all four to the original names. The old topics still hold the data.
EOF
}

lab_solve() {
cat <<'EOF'
  kubectl -n mirage patch kafkaconnect mirage --type merge -p '{"spec":{"config":{
    "group.id":"connect-cluster_mirage",
    "config.storage.topic":"mirage-connect-configs",
    "offset.storage.topic":"mirage-connect-offsets",
    "status.storage.topic":"mirage-connect-status"
  }}}'

  kubectl -n mirage wait kafkaconnect/mirage --for=condition=Ready --timeout=600s
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target

  # Back on the old offsets the connector resumes where it was BEFORE the deploy:
  # the skipped block is delivered, and so is everything the renamed group
  # already delivered — duplicates. `mirage counts` shows the target AHEAD.
  # At-least-once: consumers must be idempotent; tell the owning team.
  mirage counts --quick

  # Optional cleanup of the stray empty topics
  for t in mirage-tidied-connect-configs mirage-tidied-connect-offsets \
           mirage-tidied-connect-status; do
    kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t"
  done

WHY THIS IS THE WORST CHANGE AVAILABLE
  Connect keeps ALL of its own state in Kafka, not on disk. That is what makes
  workers disposable — and it makes these four names load-bearing. Repoint them
  and:
    - every connector disappears (empty config topic)
    - every offset is lost (empty offset topic)
    - with consumer.auto.offset.reset=latest, connectors resume at the END of
      their topics and silently skip everything produced in between

  No error. No alert on the data loss itself. You would notice only the
  connector count dropping.

AT MIRAGEDUEL
  These values are COMPUTED at deploy time, not hand-written:
    GROUP_ID="connect-cluster_${prefix}-${vertical}-${DC}"
    <prefix>-<vertical>-<dc>-connect-{configs,offsets,status}
  where prefix = <service>fd. So rt-catalog in dc ar0 gives
  connect-cluster_kcdfd-rt-catalog-ar0.

  The V2 -> Strimzi migration had to match all four EXACTLY between the old VM
  cluster and the new one, sourced from the Ansible cookbooks. Never change them
  as an incident action.
EOF
}
