#!/usr/bin/env bash
# Lab 2 — Connector task crash / FAILED state

LAB_TITLE="Replication stopped, no deploy went out"
LAB_DIFFICULTY="medium"
LAB_ALERT="Missing running tasks (< 1); failing tasks (total - running > 1)"

lab_brief() {
cat <<'EOF'
SYMPTOM
  Replication has stopped. A config change went out shortly before.

  The health signals do not agree with each other. Read the connector's state
  and its tasks' states separately before concluding anything.

YOUR JOB
  Get the connector RUNNING with healthy tasks, and records flowing again.

WHEN DONE
  mirage stop 2
EOF
}

lab_break() {
  # Break the SOURCE as well as the target. Target-only is not enough: existing
  # task threads keep producing to the old address as zombies, so data keeps
  # flowing and the fault is not observable from throughput.
  k patch kafkaconnector "$CONNECTOR" --type merge -p '{"spec":{"config":{
      "source.cluster.bootstrap.servers":"kafka-source-kafka-bootstrap:9999",
      "target.cluster.bootstrap.servers":"kafka-target-kafka-bootstrap:9999",
      "producer.override.bootstrap.servers":"kafka-target-kafka-bootstrap:9999"
  }}}' >/dev/null

  # Recreate so tasks are built fresh against the dead addresses rather than
  # lingering with the previous good config.
  k annotate kafkaconnector "$CONNECTOR" strimzi.io/restart=true --overwrite >/dev/null 2>&1
  sleep 20
  produce 3000 1000 3000
}

lab_check() {
  local src tgt prod failed running total cstate

  src="$(conn_get '.spec.config.source\.cluster\.bootstrap\.servers')"
  tgt="$(conn_get '.spec.config.target\.cluster\.bootstrap\.servers')"
  prod="$(conn_get '.spec.config.producer\.override\.bootstrap\.servers')"

  assert_eq "source.cluster.bootstrap.servers points at the real broker port" \
    "$src" "kafka-source-kafka-bootstrap:9092" \
    "With the source unreachable the connector cannot even list topics, so it never produces usable task configs." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config}'"

  assert_eq "target.cluster.bootstrap.servers points at the real broker port" \
    "$tgt" "kafka-target-kafka-bootstrap:9092" \
    "A restart cannot fix a wrong address. Config faults need a config fix." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config}'"

  assert_eq "producer.override.bootstrap.servers also corrected" \
    "$prod" "kafka-target-kafka-bootstrap:9092" \
    "producer.override.* wins over target.cluster.* (connector.client.config.override.policy=All), so fixing only one leaves the producer broken." \
    "Three keys carry addresses here. Miss one and the pipeline stays broken."

  cstate="$(conn_state_field)"
  assert_eq "connector state is RUNNING" "${cstate:-UNKNOWN}" "RUNNING" \
    "A connector stuck UNASSIGNED or FAILED is not serving traffic even if task entries still exist. This fault makes the connector unresponsive rather than failing its tasks — read the connector state, not just the task list." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.status.connectorStatus.connector.state}'"

  failed="$(conn_task_count FAILED)"
  running="$(conn_task_count RUNNING)"
  total="$(conn_tasks_total)"

  assert_eq "no tasks left in FAILED" "${failed:-0}" "0" \
    "Fixing config does not auto-clear a FAILED task if autoRestart exhausted its 5 attempts — restart it." \
    "kubectl -n mirage annotate kafkaconnector $CONNECTOR strimzi.io/restart=true --overwrite"

  assert_ge "tasks are running" "${running:-0}" 1 \
    "A connector with zero running tasks moves no data, even when it reports RUNNING itself." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Requires total >= 1 too: a bare running==total passes vacuously at 0 == 0.
  assert "every task is running" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "Partial recovery still loses throughput on the unassigned partitions. With zero tasks this cannot pass by default." \
    "running=${running:-0} of total=${total:-0} — kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Throughput measured as a DELTA, so a zombie task pointed at the old address
  # or a stale cumulative total cannot fake a pass.
  info "Verifying fresh data actually reaches the target"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 120)"
  assert_ge "fresh records replicate to the target" "${delta:-0}" 1600 \
    "Task state can read RUNNING while no data moves. Only a measured increase proves the pipeline works." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target
  kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
    -o jsonpath='{.status.connectorStatus.connector.state}{"\n"}'
  kubectl -n mirage logs <connect-pod> --tail=100 | grep -iE 'error|timed out|unresponsive'

  Read the config before you touch anything. Compare every bootstrap address
  against the port the Kafka clusters actually listen on.

  First, read the CONNECTOR state, not just the task list:

    kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
      -o jsonpath='{.status.connectorStatus.connector.state}{"\n"}'

  UNASSIGNED means the connector could not be placed and is not doing its job.
  Worker logs say more:

    kubectl -n mirage logs <connect-pod> --tail=200 | grep -iE 'timed out|unresponsive|fetchMetadata'

  A MirrorSourceConnector pointed at a dead address does NOT neatly fail its
  tasks — it hangs fetching metadata. So do not wait for a FAILED task and a
  tidy stack trace; they may never arrive.

  Compare every address in the config against the port the clusters listen on:

    kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
      -o jsonpath='{.spec.config}' | tr ',' '\n' | grep bootstrap
    kubectl -n mirage get kafka kafka-target -o jsonpath='{.spec.kafka.listeners}{"\n"}'

  THREE keys carry bootstrap addresses, not one:
    source.cluster.bootstrap.servers
    target.cluster.bootstrap.servers
    producer.override.bootstrap.servers      <- wins over target.cluster.*

  Because connector.client.config.override.policy is All, missing the override
  leaves the producer pointed at nothing.

  After correcting config, the connector may need a restart to pick it up.
EOF
}

lab_solve() {
cat <<'EOF'
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"config":{
      "source.cluster.bootstrap.servers":"kafka-source-kafka-bootstrap:9092",
      "target.cluster.bootstrap.servers":"kafka-target-kafka-bootstrap:9092",
      "producer.override.bootstrap.servers":"kafka-target-kafka-bootstrap:9092"
    }}}'

  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite

  # Confirm recovery with throughput, not status
  mirage counts     # run twice, a few seconds apart

WHY THE SYMPTOMS LOOK ODD
  You might expect FAILED tasks. Instead the connector goes UNASSIGNED and the
  worker logs "failed to properly shut down, has become unresponsive". The
  MirrorSourceConnector blocks in fetchMetadata against the dead address rather
  than throwing, so autoRestart never engages — there is no FAILED task to
  restart. That is the lesson: match your diagnosis to the observed state, not
  to the state you assumed.

RESTART OPTIONS COMPARED
  strimzi.io/restart=true            connector + all tasks
  strimzi.io/restart-task=0          one task, least disruptive
  POST /connectors/<n>/restart?includeTasks=true&onlyFailed=true
                                     failed tasks only — preferred via REST

AT MIRAGEDUEL
  The restart annotation is allowed live, including PRD — it does not express
  desired state. The config patch is NOT: that is a PR to
  sb-kafka-connectors-management.
EOF
}
