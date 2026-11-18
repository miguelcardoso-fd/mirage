#!/usr/bin/env bash
# Lab 7 — Replication loop / internal-topic recursion

LAB_TITLE="Topic count and log volume climbing"
LAB_DIFFICULTY="hard"
LAB_ALERT="Unusual amount of logs (>150M/day); topic count explosion; throughput spike"

lab_brief() {
cat <<'EOF'
SYMPTOM
  The target cluster is growing topics it should never contain. Log volume has
  spiked.

  Throughput looks enormous, but the target's record count is climbing far
  faster than the source's.

  Left alone in production this is a self-amplifying outage.

YOUR JOB
  Restore correct replication of lab-orders, and clean up anything that should
  not be on the target.

WHEN DONE
  mirage stop 7
EOF
}

lab_break() {
  # The baseline connector mirrors source->target. Here we make a SECOND
  # connector that mirrors the TARGET cluster back into itself with a
  # wide-open pattern and no exclusions: instant recursion.
  k patch kafkaconnector "$CONNECTOR" --type merge -p '{"spec":{"config":{
      "topics":".*",
      "topics.exclude":""
  }}}' >/dev/null || { err "could not inject the fault (baseline connector patch)"; return 1; }

  # Bound the loop before it exists — see throttle_loop_client. Without it
  # this lab fills the node disk and kills both brokers.
  if ! throttle_loop_client; then
    err "could not inject the fault (loop throttle not applied; refusing an unbounded loop)"
    return 1
  fi
  kubectl apply -f - >/dev/null <<'EOF'
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: lab-loopback
  namespace: mirage
  labels:
    strimzi.io/cluster: mirage
spec:
  class: org.apache.kafka.connect.mirror.MirrorSourceConnector
  tasksMax: 2
  autoRestart:
    enabled: true
    maxRestarts: 5
  config:
    topics: ".*"
    topics.exclude: ""
    replication.factor: "1"
    clusters: "target,target2"
    source.cluster.alias: target
    target.cluster.alias: target2
    source.cluster.bootstrap.servers: kafka-target-kafka-bootstrap:9092
    target.cluster.bootstrap.servers: kafka-target-kafka-bootstrap:9092
    producer.override.bootstrap.servers: kafka-target-kafka-bootstrap:9092
    replication.policy.class: org.apache.kafka.connect.mirror.IdentityReplicationPolicy
    key.converter: org.apache.kafka.connect.converters.ByteArrayConverter
    value.converter: org.apache.kafka.connect.converters.ByteArrayConverter
    header.converter: org.apache.kafka.connect.converters.ByteArrayConverter
    emit.heartbeats.interval.seconds: "1"
    heartbeats.replication.enabled: "true"
    refresh.topics.enabled: "true"
    refresh.topics.interval.seconds: "10"
    connector.client.config.override.policy: All
    consumer.auto.offset.reset: "earliest"
    producer.override.client.id: mm2-target-target2
EOF
  [ $? -eq 0 ] || { err "could not inject the fault (loop connector rejected)"; return 1; }

  mark lab7.loopback 1
  produce 2000 1000 2000
  info "Letting the fault take hold (~45s)"
  sleep 45
}

lab_check() {
  local topics conns

  topics="$(list_target_topics)"

  # The loopback connector itself must be gone.
  conns="$(kq get kafkaconnector -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
  assert "the loopback connector is removed" \
    "$(printf '%s\n' "$conns" | grep -qx 'lab-loopback' && echo 1 || echo 0)" \
    "A connector whose source and target are the same cluster with an identity policy is a guaranteed loop: it reads a topic and writes the same name straight back." \
    "kubectl -n mirage get kafkaconnector"

  local topics_cfg excl
  topics_cfg="$(conn_get '.spec.config.topics')"
  excl="$(conn_get '.spec.config.topics\.exclude')"

  assert "topic pattern is no longer a bare wildcard" \
    "$([ "$topics_cfg" != ".*" ] && echo 0 || echo 1)" \
    "'.*' sweeps in Connect's own config/offset/status topics and every replication artefact, which is how the recursion starts." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config.topics}'"

  assert "topics.exclude guard is restored" \
    "$([ -n "$excl" ] && echo 0 || echo 1)" \
    "The exclusion list is the second loop-prevention mechanism once an identity replication policy removes name-based protection. Production value blocks .*\\.heartbeats, mm2.*\\.internal, .*\\.replica and __.*" \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config.topics\\.exclude}'"

  if [ -n "$excl" ]; then
    assert "exclusion covers __* internal topics" \
      "$(printf '%s' "$excl" | grep -q '__' && echo 0 || echo 1)" \
      "Mirroring __consumer_offsets is a classic self-inflicted outage." \
      "current topics.exclude: $excl"
    assert "exclusion covers heartbeats" \
      "$(printf '%s' "$excl" | grep -q 'heartbeats' && echo 0 || echo 1)" \
      "Heartbeat topics replicate endlessly if not excluded." \
      "current topics.exclude: $excl"
  fi

  # Source and target aliases must differ — the explicit loop guard.
  local sa ta
  sa="$(conn_get '.spec.config.source\.cluster\.alias')"
  ta="$(conn_get '.spec.config.target\.cluster\.alias')"
  assert "source and target cluster aliases differ" \
    "$([ -n "$sa" ] && [ "$sa" != "$ta" ] && echo 0 || echo 1)" \
    "Identical aliases defeat MirrorMaker2's explicit loop detection." \
    "source.cluster.alias=$sa target.cluster.alias=$ta"

  # Replication artefacts that should never be on the target.
  # NOTE: mirage-connect-{configs,offsets,status} legitimately live on the
  # target — this Connect cluster uses kafka-target as its backing store — so
  # their presence is NOT a loop signal. Heartbeat and mm2 topics are.
  local junk
  junk="$(printf '%s\n' "$topics" | grep -cE '\.heartbeats$|^mm2-|\.internal$|\.replica$' || true)"
  assert_eq "no replication artefacts left on the target" "${junk:-0}" "0" \
    "Heartbeat / mm2-internal / .replica topics on the target are what the loop produced. Stopping the loop does not remove what already exists — delete them." \
    "mirage topics"

  # The clearest loop signal: records multiplying on the target.
  local src tgt
  src="$(src_count)"; tgt="$(tgt_count)"
  local inflated=0
  if [ "${src:-0}" -gt 0 ] && [ "${tgt:-0}" -gt $(( src * 3 / 2 )) ]; then inflated=1; fi
  assert "target record count is not inflated by re-replication" "$inflated" \
    "A self-mirroring connector re-reads what it just wrote, so the same records accumulate without bound. Stopping the loop does NOT undo the duplicates already written — the polluted target topic has to be dropped and re-replicated. See 'mirage solve 7' step 4." \
    "source=$src target=$tgt — mirage counts"

  # Business data must still flow — and by a meaningful amount. "grew by at
  # least 1" is satisfied by a single heartbeat record.
  info "Confirming legitimate replication still works"
  local delta
  delta="$(replication_delta 1500 120)"
  assert_ge "lab-orders still replicates after the cleanup" "${delta:-0}" 1200 \
    "Stopping the loop must not stop the real pipeline. An over-broad exclusion can block your actual topic." \
    "target grew by ${delta:-0} of 1500 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  mirage topics
  mirage counts
  kubectl -n mirage get kafkaconnector -o yaml | grep -E 'name:|topics|alias'

  Think about WHY MirrorMaker2 normally cannot do this, and what two mechanisms
  replace that protection when an identity replication policy is used.

  List what is on the target and look for things that have no business there:

    mirage topics
    mirage counts

  Look for *.heartbeats, mm2-* and *.replica topics — those are replication
  artefacts being mirrored. Also compare the record counts: if the target holds
  far more than the source, records are being re-read and re-written.

  NOTE: mirage-connect-configs / -offsets / -status DO belong on the target —
  this Connect cluster keeps its own state on kafka-target. Do not delete them;
  doing so orphans the worker.

  Two separate problems to fix:

    1. A connector whose source cluster and target cluster are the SAME, using
       an identity replication policy. DefaultReplicationPolicy would prefix
       replicated topics (us-east.foo) which is how MM2 recognises and refuses
       already-replicated data. Identity policies throw that away, so the only
       remaining guards are (a) aliases differing and (b) topics.exclude.

    2. A topic pattern of ".*" with an empty exclusion list on the main
       connector.

  The production exclusion value is:
    .*\.heartbeats, mm2.*\.internal, .*\.replica, __.*

  After stopping the loop you still need to clean up what it already wrote:

    a) the junk topics (*.heartbeats, mm2-*, *.internal, *.replica), and
    b) the DUPLICATES inside lab-orders itself on the target

  For (b), compare the counts: `mirage counts`. If the target holds far more
  than the source, those extra records are duplicates the loop produced. There
  is no way to un-write them selectively — the polluted target topic has to be
  dropped and re-replicated from the source.
EOF
}

lab_solve() {
cat <<'EOF'
  # 1. Stop EVERY connector first. The loopback is the obvious culprit, but the
  #    baseline mirror emits heartbeats too — clean the junk topics while
  #    anything is still running and they simply reappear within seconds, while
  #    re-replication keeps inflating the target as you work.
  kubectl -n mirage delete kafkaconnector lab-loopback
  kubectl -n mirage delete kafkaconnector lab-orders-from-source-to-target

  # 2. Rebuild the Connect cluster. This is the step people miss: deleting the
  #    loop connector does NOT remove its config. Connect's config.storage.topic
  #    is compacted and replayed on every worker start, so the worker keeps
  #    recreating the loop's offset-syncs topic (mm2-offset-syncs.<alias>.internal)
  #    on the target however many times you delete it. Check for yourself:
  #
  #      kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  #        --topic mirage-connect-configs --from-beginning --max-messages 200 \
  #        --timeout-ms 15000 | grep target2
  #
  #    In this lab environment, rebuild it with:
  #      mirage repair
  #    In production you would instead delete the connector through its CR and
  #    let the operator reconcile; you do not hand-edit Connect's internals.

  # 3. Now that nothing is writing and the worker has no memory of the loop,
  #    delete the junk that was already replicated.
  #    CAREFUL: do NOT delete mirage-connect-{configs,offsets,status} — this
  #    Connect cluster stores its own state on kafka-target, so those are its
  #    live internals. Deleting them orphans the worker.
  for t in $(kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
               | grep -E '\.heartbeats$|^mm2-|\.internal$|\.replica$'); do
    kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t"
  done

  # 3. The duplicates already on the target are still there — stopping the loop
  #    does not remove them. Drop the polluted topic and let it re-replicate.
  #    (In production this is a deliberate, approved re-seed, not a casual step.)
  kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic lab-orders

  # Wait for ALL the deletes to actually land. Kafka topic deletion is
  # asynchronous: a topic you just deleted still appears in --list for a few
  # seconds. Check the junk patterns as well as lab-orders, or you declare it
  # clean too early and recreate the connector against a half-removed topic.
  until ! kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
      | grep -qE '\.heartbeats$|^mm2-|\.internal$|\.replica$|^lab-orders$'; do
    sleep 3
  done

  # 4. Re-seed with a sane pattern AND the exclusion guard — the guard is what
  #    stops the loop recurring, so apply it as you bring the connector back.
  kubectl -n mirage apply -f manifests/connector.yaml   # from the mirage dir
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"config":{
      "topics":"lab-orders",
      "topics.exclude":".*\\.heartbeats, mm2.*\\.internal, .*\\.replica, __.*"
    }}}'

  # 5. Confirm business data flows again, at the right volume
  mirage counts

WHY THIS IS POSSIBLE AT ALL
  DefaultReplicationPolicy prefixes replicated topics with the source alias
  (orders -> us-east.orders). That prefix is not cosmetic: it is how MM2 knows a
  topic is already replicated and refuses to mirror it again.

  KCD uses IdentityReplicationPolicy (same name) and CustomIdentityReplicationPolicy
  (arbitrary target name). Both discard that protection. Two mechanisms replace it,
  and BOTH must hold:
    1. source.cluster.alias != target.cluster.alias
    2. topics.exclude blocking heartbeats, mm2 internals, .replica and __*

AT MIRAGEDUEL
  If you are ever asked to add a topic pattern to a KCD connector, check it
  against topics.exclude before approving. Note the real connector template also
  sets heartbeats.replication.enabled=false as a third guard.
EOF
}
