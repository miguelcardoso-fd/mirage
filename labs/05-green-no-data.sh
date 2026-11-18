#!/usr/bin/env bash
# Lab 5 — Stale connector: RUNNING but moving no data

LAB_TITLE="Tasks RUNNING, zero records moving"
LAB_DIFFICULTY="medium"
LAB_ALERT="Missing running tasks (<1); replication record_rate at zero"

lab_brief() {
cat <<'EOF'
SYMPTOM
  A downstream team says they stopped receiving data an hour ago.

  Nothing is obviously broken: the connector reports RUNNING, there are no
  failures, no restarts, no errors in the log. Nothing is alerting on failure.

  And yet the target is not growing.

YOUR JOB
  Find why nothing is flowing and restore replication of lab-orders.

WHEN DONE
  mirage stop 5
EOF
}

lab_break() {
  # A topic pattern matching nothing produces NO task assignments at all, so the
  # connector sits RUNNING with zero tasks and zero throughput. Nothing fails,
  # nothing alerts on failure, and the pipeline is dead.
  k patch kafkaconnector "$CONNECTOR" --type merge -p '{"spec":{"config":{
      "topics":"lab-orders-v2"
  }}}' >/dev/null
  k annotate kafkaconnector "$CONNECTOR" strimzi.io/restart=true --overwrite >/dev/null 2>&1
  sleep 10
  produce 3000 1000 3000
}

lab_check() {
  local topics running total

  topics="$(conn_get '.spec.config.topics')"
  assert_eq "connector is subscribed to the real topic" "$topics" "lab-orders" \
    "A topic pattern matching nothing yields NO task assignments, so the connector sits RUNNING with zero tasks. Status is green because nothing went wrong — there is simply no work." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config.topics}'"

  wait_connector_tasks 1 120 || true
  running="$(conn_task_count RUNNING)"
  total="$(conn_tasks_total)"

  assert_ge "tasks exist again" "${total:-0}" 1 \
    "With nothing to replicate the connector has no tasks at all. Task count is the fastest tell." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  assert_ge "tasks running" "${running:-0}" 1 \
    "No running tasks, no replication." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  assert_eq "no failed tasks" "$(conn_task_count FAILED)" "0" \
    "A failed task means the repair introduced a new fault." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Delta, not cumulative totals: the only trustworthy signal.
  info "Proving data actually moves now"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 120)"
  assert_ge "target record count is increasing" "${delta:-0}" 1600 \
    "This is the lesson of the lab: task state is not a throughput signal. Confirm with record counts or replication_record_rate." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  mirage counts                 # run twice — is the target moving at all?
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target
  kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target -o yaml

  Pay attention to the TASK COUNT as well as the connector state, then compare
  what the connector is configured to replicate against what actually exists.

  Ask two separate questions:

    1. What is the connector configured to replicate?
         kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
           -o jsonpath='{.spec.config.topics}{"\n"}'

    2. What topics actually exist on the source?
         kubectl -n mirage exec kafka-source-source-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

  If (1) names something absent from (2), the connector has no work to do. It
  reports RUNNING forever with ZERO tasks, because nothing has failed — there is
  just nothing to consume. Check the task count:

    kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
      -o jsonpath='{.status.connectorStatus.tasks}{"\n"}'

  This is why "is the connector running?" is a useless health check on its own,
  and why MirageDuel alerts on commit percentage and record rate instead.
EOF
}

lab_solve() {
cat <<'EOF'
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"config":{"topics":"lab-orders"}}}'

  # refresh.topics.enabled=true means it picks the topic up within the
  # refresh interval; restart only if you are impatient:
  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite

HOW TO SPOT THIS FAST IN PRODUCTION
  Do not trust state. Trust throughput:
    kafka_connect_mirror_replication_record_rate      -> near zero?
    kafka_connect_mirror_replication_record_age_ms    -> climbing?
    kafka_consumer_fetch_records_lag_max              -> flat at zero is suspicious

  Zero lag AND zero rate usually means "consuming nothing", not "caught up".

OTHER CAUSES OF THE SAME SHAPE
  - a topics.exclude pattern that accidentally matches your real topic
  - broker connectivity dropped but the task has not errored yet (Lab 8)
  - the source topic genuinely went quiet upstream (check the producer side)
EOF
}
