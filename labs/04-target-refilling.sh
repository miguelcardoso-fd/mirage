#!/usr/bin/env bash
# Lab 4 — Offset inconsistency after a connector rename (silent data loss)

LAB_TITLE="Target re-filling from the beginning"
LAB_DIFFICULTY="hard"
LAB_ALERT="NONE - no monitor covers this. Every health signal reads green."

lab_brief() {
cat <<'EOF'
SYMPTOM
  A change went through earlier. Everything looks healthy now: the connector is
  RUNNING, tasks are RUNNING, there are no errors, and throughput looks normal.

  No alert has fired. No monitor covers what is actually wrong.

  But the target is not holding what the source holds, and nothing in the
  health signals will tell you that.

YOUR JOB
  1. Work out whether the target holds everything the source holds.
  2. If it does not, recover what is missing.
  3. Leave exactly ONE connector running, mirroring correctly.

WHEN DONE
  mirage stop 4
EOF
}

lab_break() {
  info "Seeding a baseline and letting it replicate"
  produce 3000 1000 3000
  local n=0
  while [ "$n" -lt 60 ]; do
    [ "$(tgt_count)" -ge 2500 ] 2>/dev/null && break
    sleep 5; n=$((n+5))
  done

  local pre_src pre_tgt
  pre_src="$(src_count)"; pre_tgt="$(tgt_count)"
  info "Replicated baseline: source=$pre_src target=$pre_tgt"

  info "Injecting the fault"
  k patch kafkaconnector "$CONNECTOR" --type merge \
    -p '{"spec":{"state":"stopped"}}' >/dev/null
  sleep 15

  produce 5000 1000 5000
  mark lab4.gap_records 5000
  mark lab4.pre_tgt "$pre_tgt"

  info "Reconfiguring the connector"
  k delete kafkaconnector "$CONNECTOR" --wait=true >/dev/null 2>&1
  sed "s/name: $CONNECTOR/name: lab-orders-renamed/; s/mirageduel.com\/connector-name: .*/mirageduel.com\/connector-name: lab_orders_renamed/" \
    "$LAB_HOME/manifests/connector.yaml" | kubectl apply -f - >/dev/null
  mark lab4.renamed 1

  sleep 20
  info "Source now holds $(src_count); target holds $(tgt_count)"
}

lab_check() {
  local src tgt conns count

  conns="$(kq get kafkaconnector -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -c . || true)"
  assert_eq "exactly one connector exists" "${conns:-0}" "1" \
    "Two connectors mirroring one topic double-write to the target and fight over the same work." \
    "kubectl -n mirage get kafkaconnector"

  local name running total
  name="$(kq get kafkaconnector -o jsonpath='{.items[0].metadata.name}')"
  running="$(conn_task_count RUNNING "$name")"
  total="$(conn_tasks_total "$name")"

  assert_ge "the surviving connector has running tasks" "${running:-0}" 1 \
    "Recovery is not complete until replication is actually flowing again." \
    "kubectl -n mirage describe kafkaconnector $name"

  assert_eq "no failed tasks on it" "$(conn_task_count FAILED "$name")" "0" \
    "A failed task means the recovery config is wrong somewhere." \
    "kubectl -n mirage describe kafkaconnector $name"

  info "Comparing source and target record counts"
  src="$(src_count)"
  tgt="$(tgt_count)"

  local deficit=$(( ${src:-0} - ${tgt:-0} ))
  [ "$deficit" -lt 0 ] && deficit=0

  # Tolerance must stay well below the 5000-record gap this lab creates.
  # A pure percentage silently false-passes on a large topic: at 1M records a
  # 2% allowance is 20000, which swallows the whole gap. Cap it absolutely.
  local tolerance=$(( ${src:-0} / 50 ))
  [ "$tolerance" -lt 50 ] && tolerance=50
  [ "$tolerance" -gt 500 ] && tolerance=500

  assert "target caught up with source (deficit <= ${tolerance})" \
    "$([ "$deficit" -le "$tolerance" ] && echo 0 || echo 1)" \
    "The 5000 records produced while the connector was stopped are the whole point of this lab. consumer.auto.offset.reset=latest means a freshly-named connector starts at the END of the log and never sees them." \
    "source=$src target=$tgt missing=$deficit — run: mirage counts"

  if [ "$deficit" -gt "$tolerance" ]; then
    say ""
    warn "You are still missing ~$deficit records."
    warn "A connector with no stored offsets + auto.offset.reset=latest skips history silently."
  fi
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage get kafkaconnector
  mirage counts

  Think about WHERE a source connector's offsets are keyed, and what
  consumer.auto.offset.reset does when no offset is found.

  Count both sides first:
    mirage counts

  A source connector's offsets live in the Connect offset storage topic, keyed
  by CONNECTOR NAME. Rename the connector and it finds no offsets at all — a
  cold start. What it does then is decided by:

    consumer.auto.offset.reset

  The baseline sets that to "latest", i.e. start at the end, skip everything
  that already exists. That is why the gap is invisible: the consumer is caught
  up by its own reckoning.

  To replay history you need the connector to start from the beginning instead.
  One of two routes:

    a) recreate it with consumer.auto.offset.reset=earliest, or
    b) migrate the old connector's offsets onto the new name

  Option (a) is what you can do locally. Note the target may end up with some
  duplicates — Connect is at-least-once, and that is an acceptable trade
  against silent loss.
EOF
}

lab_solve() {
cat <<'EOF'
  # Inspect the damage
  mirage counts

  # Recreate from the beginning of the log so the gap is replayed
  kubectl -n mirage delete kafkaconnector --all

  kubectl -n mirage apply -f - <<'YAML'
  apiVersion: kafka.strimzi.io/v1beta2
  kind: KafkaConnector
  metadata:
    name: lab-orders-recovered
    namespace: mirage
    labels:
      strimzi.io/cluster: mirage
  spec:
    class: org.apache.kafka.connect.mirror.MirrorSourceConnector
    tasksMax: 4
    autoRestart:
      enabled: true
      maxRestarts: 5
    config:
      topics: "lab-orders"
      replication.factor: "1"
      clusters: "source,target"
      source.cluster.alias: source
      target.cluster.alias: target
      source.cluster.bootstrap.servers: kafka-source-kafka-bootstrap:9092
      target.cluster.bootstrap.servers: kafka-target-kafka-bootstrap:9092
      producer.override.bootstrap.servers: kafka-target-kafka-bootstrap:9092
      replication.policy.class: org.apache.kafka.connect.mirror.IdentityReplicationPolicy
      key.converter: org.apache.kafka.connect.converters.ByteArrayConverter
      value.converter: org.apache.kafka.connect.converters.ByteArrayConverter
      header.converter: org.apache.kafka.connect.converters.ByteArrayConverter
      emit.heartbeats.interval.seconds: "1"
      heartbeats.replication.enabled: "false"
      refresh.topics.enabled: "true"
      connector.client.config.override.policy: All
      topics.exclude: ".*\\.heartbeats, mm2.*\\.internal, .*\\.replica, __.*"
      consumer.auto.offset.reset: "earliest"
  YAML

WHY THIS IS THE MOST DANGEROUS FAILURE
  No task failed. No alert fired. Commit success 100%. Lag zero — because the
  consumer really is at the end of the log. Every health signal says fine while
  data is quietly missing.

AT MIRAGEDUEL
  - Offsets must be MIGRATED, not abandoned: use datastream-migrator, which
    dumps a connector's offsets and republishes them under the new name.
  - The same mechanism at CLUSTER scope is worse: changing group.id or any of
    config/offset/status.storage.topic orphans EVERY connector's state. The
    V2 -> Strimzi migration had to match all four exactly.
  - The manual pipeline's --offset-reset accepts ONLY "latest"; "earliest" is
    rejected, because replaying a production mirror from the beginning would
    flood the target. So the fix you just applied locally is deliberately NOT
    available in production — which is why the rename is prevented by process
    instead.
EOF
}
