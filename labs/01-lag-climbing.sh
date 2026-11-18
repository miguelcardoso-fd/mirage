#!/usr/bin/env bash
# Lab 1 — High consumer lag / task bottleneck

LAB_TITLE="Consumer lag climbing on one topic"
LAB_DIFFICULTY="easy"
LAB_ALERT="None today: per-connector lag monitors were removed (dataform #1798), so lag is reported by the downstream team"

lab_brief() {
cat <<'EOF'
SYMPTOM
  Replication is falling behind. Lag climbs and never recovers. Record age
  grows steadily. Nothing is FAILED — the connector and its task are RUNNING.

YOUR JOB
  Restore replication so the target keeps up with the source.

WHEN DONE
  mirage stop 1
EOF
}

lab_break() {
  k patch kafkaconnector "$CONNECTOR" --type merge \
    -p '{"spec":{"tasksMax":1}}' >/dev/null
  info "Generating load into $TOPIC (this takes ~30s)"
  produce 40000 2000 20000
  mark lab1.seeded 1
}

lab_check() {
  local tmax total running
  tmax="$(conn_get .spec.tasksMax)"
  total="$(conn_tasks_total)"
  running="$(conn_task_count RUNNING)"

  assert_ge "tasksMax raised above 1" "${tmax:-0}" 2 \
    "One task cannot drain a 12-partition topic. tasksMax is the parallelism ceiling." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.tasksMax}'"

  # 12 partitions is the real ceiling; reward reaching it without demanding it.
  if [ "${tmax:-0}" -gt 12 ]; then
    warn "tasksMax=$tmax exceeds the 12 partitions available."
    warn "Not wrong, but it buys nothing: task count is bounded by available work."
  fi

  assert_ge "connector is running multiple tasks" "${total:-0}" 2 \
    "Raising tasksMax only helps once the connector actually spawns the tasks." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Requires total >= 1 too: a bare running==total passes vacuously at 0 == 0.
  assert "all tasks are RUNNING, none failed" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "Extra tasks that fail on startup leave you no better off. With zero tasks there is nothing running, so this cannot pass by default." \
    "running=${running:-0} of total=${total:-0} — kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Regression guard: raising tasksMax must not break the pipeline. Measured as
  # a delta, because comparing lifetime source/target totals cannot fail once
  # the topic holds tens of thousands of records.
  info "Checking replication is still healthy after your change"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 120)"
  assert_ge "replication still healthy after the change" "${delta:-0}" 1600 \
    "More tasks must not break what already worked. If fresh records are not arriving, the new tasks are not replicating." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target
  kubectl -n mirage get kafkatopic lab-orders -o jsonpath='{.spec.partitions}'

  Count the partitions on the source topic. That number is the maximum useful
  parallelism for a mirror connector — one task can own one or more partitions,
  but a partition is never split across tasks.

  tasksMax is a CEILING, not a target. Setting it to 50 on a 12-partition topic
  gets you 12 tasks. Setting it to 1 gets you 1, no matter how much load arrives.

  The field is spec.tasksMax on the KafkaConnector.
EOF
}

lab_solve() {
cat <<'EOF'
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"tasksMax":12}}'

  Then watch tasks appear and the backlog drain:
    kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target

AT MIRAGEDUEL
  tasksMax is configuration, so in PRD this is a PR to
  sb-kafka-connectors-management — never a live kubectl patch. The operator
  would revert a patch anyway. Real connectors use values like 10, 18, 50.
EOF
}
