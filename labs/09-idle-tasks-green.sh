#!/usr/bin/env bash
# Lab 9 — Paused/stopped connector and the reconciliation trap

LAB_TITLE="Connector idle, every task green"
LAB_DIFFICULTY="easy"
LAB_ALERT="Missing running tasks; connector_status not 1"

lab_brief() {
cat <<'EOF'
SYMPTOM
  Replication stopped. No errors anywhere. No failed tasks. The connector is
  simply not doing anything, and task counts have gone to zero.

  A teammate says "I already tried restarting it through the REST API and it
  worked for a minute, then went back."

YOUR JOB
  Get replication running again, in a way that SURVIVES the operator's next
  reconciliation loop.

WHEN DONE
  mirage stop 9
EOF
}

lab_break() {
  patch_connector_verified '{"spec":{"state":"paused"}}' '.spec.state' 'paused'
  sleep 15
  produce 2000 1000 2000
}

lab_check() {
  local state running total paused

  state="$(conn_get .spec.state)"

  # Running is expressed either by state: running or by omitting the field.
  assert "spec.state is running (or omitted, which defaults to running)" \
    "$([ -z "$state" ] || [ "$state" = "running" ] && echo 0 || echo 1)" \
    "This is DESIRED state, held in the CR. Resuming via the REST API appears to work and is reverted by the operator on its next reconciliation. The only durable fix is the CR." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.state}'"

  assert_eq "connector reports RUNNING" "$(conn_state_field)" "RUNNING" \
    "The connector's own state must be RUNNING, not PAUSED or STOPPED." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  wait_connector_tasks 1 120 || true
  running="$(conn_task_count RUNNING)"
  total="$(conn_tasks_total)"
  paused="$(conn_task_count PAUSED)"

  assert_ge "tasks exist" "${total:-0}" 1 \
    "A connector with no tasks moves no data." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Counting entries is not enough: a paused connector can retain PAUSED task
  # entries, which would satisfy a naive total-count check.
  assert_eq "no tasks left PAUSED" "${paused:-0}" "0" \
    "Task entries can persist in PAUSED state, so counting tasks is not proof of recovery. Their STATE is what matters." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Requires total >= 1 too: a bare running==total passes vacuously at 0 == 0.
  assert "every task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "Resumed but not running means something else is also wrong. With zero tasks this cannot pass by default." \
    "running=${running:-0} of total=${total:-0} — kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Measure a DELTA. Comparing lifetime source/target totals cannot fail once
  # the topic holds tens of thousands of records, so it proves nothing.
  info "Confirming replication actually resumed"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 120)"
  assert_ge "fresh records replicate after resuming" "${delta:-0}" 1600 \
    "A paused connector keeps its offsets, so on resume it replays from where it left off rather than skipping ahead. That is the key difference from a renamed connector (Lab 4)." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target -o yaml
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target

  Look at spec, not just status.

  Read the SPEC, not the status:

    kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
      -o jsonpath='{.spec.state}{"\n"}'

  spec.state is DESIRED state. Valid values: running, paused, stopped. The field
  is omitted when running, because Strimzi defaults to running.

  Why your teammate's REST call did not stick: the cluster CR carries
  strimzi.io/use-connector-resources=true, so the operator treats the
  KafkaConnector CR as the source of truth and reconciles toward it
  continuously. A REST resume succeeds, genuinely changes behaviour, and is then
  reverted on the next loop. The API does not reject you — it succeeds and then
  evaporates.

  Fix the CR instead. Either set state back to running, or remove the field.

  Good news for this lab: because the connector KEPT its name, it also kept its
  offsets. Resuming replays the gap. Contrast with Lab 4, where a rename lost
  them entirely.
EOF
}

lab_solve() {
cat <<'EOF'
  # Either of these works
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"state":"running"}}'

  # or remove the field and let the Strimzi default apply
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type json -p '[{"op":"remove","path":"/spec/state"}]'

WHAT WILL NOT WORK
  Resuming through the REST API, the way you would reach for it on the VMs.
  `mirage api` only does GETs, so issue the PUT yourself through the ingress:

    curl -XPUT -H 'Host: mirage.local' \
      localhost:8080/connectors/lab-orders-from-source-to-target/resume

    -> succeeds, then the operator reverts it on the next reconcile

THE ALLOWED/NOT-ALLOWED LINE
  Allowed live, including PRD (these do not express desired state):
    kubectl annotate kafkaconnector <n> strimzi.io/restart=true
    kubectl annotate kafkaconnector <n> strimzi.io/restart-task=<id>
    POST /connectors/<n>/restart[?includeTasks=true&onlyFailed=true]

  NOT allowed live — these are config, so PR only:
    pause / resume / stop          (spec.state)
    tasksMax, any spec.config key
    create / delete a connector

AT MIRAGEDUEL
  Pausing and resuming is a PR to sb-kafka-connectors-management setting
  spec.state. If you find a connector paused with no deploy explaining it,
  suspect a human API mutation (see the runbook for that) — a connector stopped
  in DEV and PRD at the same moment really happened, and was traced to exactly
  that.
EOF
}
