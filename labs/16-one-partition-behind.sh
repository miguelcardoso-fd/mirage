#!/usr/bin/env bash
# Lab 16 — poison pill: one oversized record stalls a partition

LAB_TITLE="One partition behind, eleven healthy"
LAB_DIFFICULTY="hard"
LAB_ALERT="Cannot process record: 'Received exception when fetching the next record from'; partial throughput loss"

lab_brief() {
cat <<'EOF'
SYMPTOM
  Replication is not dead — it is PARTIALLY dead. Most records are arriving.
  Throughput dropped but did not go to zero, and it is not recovering.

  The connector reports RUNNING. Most tasks report RUNNING; one keeps dying
  and being revived by autoRestart, so depending on when and where you look
  (the KafkaConnector CR lags the REST API) it may read RUNNING too.

  Whole-topic throughput looks survivable. Look narrower than the topic.

YOUR JOB
  Get replication fully healthy again WITHOUT losing business data silently.

WHEN DONE
  mirage stop 16
EOF
}

lab_break() {
  local sp tp
  sp="$(src_pod)"; tp="$(tgt_pod)"
  if [ -z "$sp" ] || [ -z "$tp" ]; then
    err "need both brokers running to inject this lab"
    return 1
  fi

  # The mirror path uses ByteArrayConverter for key, value and header, so no
  # record can "fail to convert" — the bytes are never interpreted. A realistic
  # poison pill therefore has to be a record the TARGET refuses, not one the
  # converter chokes on. A target-side max.message.bytes lower than the source's
  # does exactly that, and is a real incident: limits drift between clusters
  # after one side is retuned.
  if ! _kafka_in "$tp" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
        --entity-type topics --entity-name $TOPIC \
        --alter --add-config max.message.bytes=65536" >/dev/null 2>&1; then
    err "could not set max.message.bytes on the target"
    return 1
  fi
  mark lab16.limit 65536

  info "Generating load"
  produce 1500 1000 2000 >/dev/null 2>&1

  info "Generating more load"
  if ! _kafka_in "$sp" "bin/kafka-producer-perf-test.sh --topic $TOPIC \
        --num-records 1 --record-size 204800 --throughput 10 \
        --producer-props bootstrap.servers=$SRC_BOOT max.request.size=2097152" >/dev/null 2>&1; then
    err "could not produce the oversized record"
    return 1
  fi
  mark lab16.injected 1

  info "Generating load behind it"
  produce 1500 1000 2000 >/dev/null 2>&1

  info "Waiting for the stall to establish (~60s)"
  sleep 60
}

lab_check() {
  local tp limit answer
  tp="$(tgt_pod)"
  answer="$(marked lab16.answer)"

  # Did they reason about it at all? This lab is about the decision, so not
  # recording one is itself worth calling out — but do not fail them for it.
  if [ -z "$answer" ]; then
    warn "You did not record a decision (mirage answer 16 accept|skip|escalate)."
    warn "In a real incident the reasoning is the part that gets reviewed."
  else
    _record_pass "decision recorded ${C_DIM}($answer)${C_0}"
  fi

  # The escalate path is legitimate, but it has to be an informed escalation:
  # you must have found the actual cause, not just given up.
  if [ "$answer" = "escalate" ]; then
    info "You chose to escalate. Checking you identified the cause first."
    limit="$(_kafka_in "$tp" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
        --entity-type topics --entity-name $TOPIC --describe" 2>/dev/null \
        | grep -oE 'max\.message\.bytes=[0-9]+' | head -1 | cut -d= -f2)"
    assert "escalation is informed, not a guess" \
      "$([ -n "$(marked lab16.answer)" ] && echo 0 || echo 1)" \
      "Escalating is a valid choice here, but only with evidence attached: the failing topic, the target's limit, and the size of the record being refused." \
      "mirage answer 16 escalate"
    say ""
    info "Escalation accepted as a decision. For the lab to PASS you must also"
    info "restore replication — in production that would be the other team's"
    info "change, but you still need to see it through."
    say ""
  fi

  # Whichever route, the pipeline must actually be healthy again.
  wait_connector_tasks 1 120 || true
  local running total
  running="$(conn_task_count RUNNING)"; total="$(conn_tasks_total)"

  assert_ge "tasks exist" "${total:-0}" 1 \
    "A stalled partition does not remove tasks — if they are gone, something else broke." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  assert "every task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "The task that owns the pill's partition fails on it (RecordTooLargeException is not retriable). Once the target accepts the record, a task that exhausted autoRestart stays FAILED until you restart it — widen first, restart second." \
    "running=${running:-0} of total=${total:-0}"

  # Which fix did they actually apply? Read the end state rather than trusting
  # the recorded answer: the answer is their reasoning, this is their action.
  limit="$(_kafka_in "$tp" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
      --entity-type topics --entity-name $TOPIC --describe" 2>/dev/null \
      | grep -oE 'max\.message\.bytes=[0-9]+' | head -1 | cut -d= -f2)"
  local tolerance
  tolerance="$(conn_get '.spec.config.errors\.tolerance')"

  if [ -n "$limit" ] && [ "${limit:-0}" -ge 262144 ] 2>/dev/null; then
    _record_pass "target now accepts the record ${C_DIM}(max.message.bytes=$limit)${C_0}"
  elif [ -z "$limit" ]; then
    _record_pass "target limit reset to the broker default ${C_DIM}(no topic override)${C_0}"
  elif [ "$tolerance" = "all" ]; then
    warn "You set errors.tolerance=all. That is a REAL option, and it works —"
    warn "but understand what you chose: the oversized record is now dropped"
    warn "silently and permanently. Nothing alerts on it. For betting data that"
    warn "is usually the wrong trade; see 'mirage why 16'."
    _record_pass "pipeline unblocked via errors.tolerance ${C_DIM}(with silent loss)${C_0}"
  else
    _record_fail "the target can accept the oversized record, or you chose tolerance deliberately" \
      "The target still refuses records above ${limit:-?} bytes and errors.tolerance is not set, so the pill is still stuck. Either raise max.message.bytes on the target topic (no data loss) or set errors.tolerance=all (accepts silent loss)." \
      "kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name $TOPIC --describe"
  fi

  # The real test: does the backlog behind the pill drain? A stalled partition
  # means fresh records keep arriving on the OTHER 11 partitions, so a
  # whole-topic delta reads healthy while one partition is wedged — the exact
  # shape that makes this lab hard, and it would make a naive check useless.
  # Measure per-partition progress instead.
  info "Checking every partition advances (producing 1500 across all of them)"
  local before after stalled=0 pcount=0 p off_b off_a
  before="$(_kafka_in "$tp" "bin/kafka-get-offsets.sh --bootstrap-server $TGT_BOOT --topic $TOPIC" 2>/dev/null)"
  produce 1500 1000 2000 >/dev/null 2>&1
  sleep 45
  after="$(_kafka_in "$tp" "bin/kafka-get-offsets.sh --bootstrap-server $TGT_BOOT --topic $TOPIC" 2>/dev/null)"

  if [ -n "$before" ] && [ -n "$after" ]; then
    while IFS= read -r line; do
      p="$(printf '%s' "$line" | awk -F: '{print $2}')"
      off_a="$(printf '%s' "$line" | awk -F: '{print $3}')"
      off_b="$(printf '%s\n' "$before" | awk -F: -v part="$p" '$2==part {print $3}')"
      [ -z "$off_b" ] && continue
      pcount=$((pcount+1))
      [ "${off_a:-0}" -le "${off_b:-0}" ] 2>/dev/null && stalled=$((stalled+1))
    done <<<"$after"
  fi

  assert_eq "no partition is stalled" "${stalled:-99}" "0" \
    "A partition whose offset does not move while the others do is the poison pill still being retried. The whole-topic throughput looks fine because the other partitions carry the load — that is why this incident is easy to misread as healthy." \
    "compare: kafka-get-offsets.sh --topic $TOPIC on the target, twice, 30s apart"

  if [ "${pcount:-0}" -gt 0 ] && [ "${stalled:-0}" -eq 0 ]; then
    _record_pass "all ${pcount} partitions advancing ${C_DIM}(pill cleared)${C_0}"
  fi

  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 800 120)"
  assert_ge "replication healthy overall" "${delta:-0}" 600 \
    "Throughput must also be healthy in aggregate, not just unstalled." \
    "target grew by ${delta:-0} of 800 produced — mirage counts"

  # And did the poison record itself make it, if they chose the lossless route?
  if [ -n "$limit" ] && [ "${limit:-0}" -ge 262144 ] 2>/dev/null; then
    local src_total tgt_total gap
    src_total="$(src_count)"; tgt_total="$(tgt_count)"
    gap=$(( ${src_total:-0} - ${tgt_total:-0} ))
    [ "$gap" -lt 0 ] && gap=0
    if [ "$gap" -le 50 ]; then
      _record_pass "no records lost ${C_DIM}(source and target within $gap)${C_0}"
    else
      info "Source leads target by $gap — if you re-seeded the topic that is"
      info "expected; if not, the backlog may still be draining."
    fi
  fi
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  mirage counts
  mirage logs --errors

  Then compare the two sides. The record exists on the source and not on the
  target, so ask what the TARGET thinks the limit is:

    kubectl -n mirage exec kafka-target-target-pool-0 -- \
      bin/kafka-configs.sh --bootstrap-server localhost:9092 \
        --entity-type topics --entity-name lab-orders --describe

  A per-partition stall with ONE task failing is the signature. Find which
  partitions stopped advancing — they are that task's share (3 of 12 with 4
  tasks). Ask the REST API, not the CR, which lags behind autoRestart:

    mirage api /connectors/lab-orders-from-source-to-target/status

  Throughput dropped but did not stop. That shape matters: a dead pipeline is
  zero, a stalled PARTITION is "most things still work". Only one partition is
  blocked, and it is blocked on one record.

  Find the error. Each autoRestart attempt fails on the same record, so it
  logs again every cycle:

    mirage logs --errors

  Expect a producer-side rejection naming a size — RecordTooLargeException, or
  a message about the request being larger than the server will accept.

  The record exists on the source and cannot be written to the target, so the
  two clusters disagree about what is allowed. Compare them:

    kubectl -n mirage exec kafka-source-source-pool-0 -- \
      bin/kafka-configs.sh --bootstrap-server localhost:9092 \
        --entity-type topics --entity-name lab-orders --describe

    kubectl -n mirage exec kafka-target-target-pool-0 -- \
      bin/kafka-configs.sh --bootstrap-server localhost:9092 \
        --entity-type topics --entity-name lab-orders --describe

  One side has a max.message.bytes override the other does not.

  Now the actual decision, which is the point of this lab. You can:

    a) raise the target's limit so the record fits
         -> no data loss; the record arrives. Needs the target cluster's owner
            to be comfortable with larger messages.

    b) set errors.tolerance=all on the connector
         -> unblocks immediately, and DROPS the record silently, forever. No
            alert fires for it. Fast, and lossy.

    c) escalate without fixing
         -> correct if the limit is not yours to change, but only with evidence:
            which topic, which limit, how big the record is.

  Before choosing, ask the question that decides it: is this record's content
  something the business cannot lose? KCD mirrors betting-adjacent data.

  Note what is NOT the fix here: restarting the task on its own. It will retry
  the same record and stall again, and you will have spent your autoRestart
  budget.

  But note the converse too, because it catches people: once you HAVE widened
  the limit, a task that already exhausted its restarts will not come back by
  itself. It sits FAILED with "Unrecoverable exception from producer send
  callback". Check task state after your fix, and restart it if needed —
  widening first, restarting second.
EOF
}

lab_solve() {
cat <<'EOF'
  # 1. Confirm the shape: partial throughput, one task FAILED (or cycling)
  mirage counts
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target | grep -A4 tasks

  # 2. Find the rejection
  POD=mirage-connect-0
  kubectl -n mirage logs "$POD" --tail=2000 | grep -iE 'too large|RecordTooLarge|MESSAGE_TOO_LARGE'

  # 3. Compare the two clusters' limits — they disagree
  kubectl -n mirage exec kafka-target-target-pool-0 -- \
    bin/kafka-configs.sh --bootstrap-server localhost:9092 \
      --entity-type topics --entity-name lab-orders --describe

  # 4a. PREFERRED — make the target able to accept it. No data loss.
  kubectl -n mirage exec kafka-target-target-pool-0 -- \
    bin/kafka-configs.sh --bootstrap-server localhost:9092 \
      --entity-type topics --entity-name lab-orders \
      --alter --add-config max.message.bytes=2097152

  #     Or remove the override entirely and inherit the broker default:
  #       --alter --delete-config max.message.bytes
  #
  #     The producer must also be willing to send it. The real template raises
  #     producer.override.max.request.size to 34603008 in its
  #     handle_high_message_payload block for exactly this case.

  # 4a-ii. IMPORTANT — widening the limit does not revive a task that already
  #        died retrying the record. If a task exhausted its autoRestart budget
  #        while the limit was still too low, it stays FAILED with
  #        "Unrecoverable exception from producer send callback" and no amount
  #        of waiting fixes it. Restart it ONCE the target can accept the write:
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target | grep -A3 tasks
  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite
  #        Order matters: restart BEFORE widening and it just fails again,
  #        spending another attempt.

  # 4b. ALTERNATIVE — skip bad records. Unblocks instantly, loses the record.
  #     Only choose this knowing the record is disposable.
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"config":{
      "errors.tolerance":"all",
      "errors.log.enable":"true",
      "errors.log.include.messages":"false"
    }}}'

  # 5. Verify the stall cleared, not just that tasks are green
  mirage counts

WHY RESTARTING DOES NOT HELP
  The task is not broken. It is correctly refusing to silently drop a record it
  cannot deliver. Restart it and it re-reads the same offset, hits the same
  record, and stalls again — with one less autoRestart attempt in the budget.

THE DECISION, WHICH IS THE REAL LESSON
  Connect gives you errors.tolerance (none|all),
  errors.deadletterqueue.topic.name and retry settings. They let a pipeline skip
  or divert bad records instead of failing. Whether you SHOULD use them depends
  entirely on the data.

  KCD largely does not use them, deliberately. The mirror path uses
  ByteArrayConverter for key, value and header — records are opaque bytes, never
  deserialised, so almost nothing can "fail to convert". There is nothing to
  tolerate, and a tolerance setting would only mask delivery failures like this
  one.

  The instructive exception is AvroToProtoTransform, a MirageDuel SMT that does
  interpret record content. Its contract is the opposite of tolerance:

      "Any per-record failure throws ConnectException
       (halt-and-page; no DLQ, no silent drop)."

  That is the correct choice for financial-adjacent data: a silently dropped
  record is worse than a paged engineer. Know which philosophy a connector
  follows BEFORE you "fix" a failing task by making it tolerant.

AT MIRAGEDUEL
  Topic configuration is not a kubectl change — topics are declared in
  sbv2_data_dependencies, so widening max.message.bytes is a PR and usually a
  conversation with the owning team about why a record got that large.

  The matching producer settings live in the connector declaration
  (producer.override.max.request.size), so both sides are config-as-code. In PRD
  you have read-only access anyway: your job is diagnosis and handoff, with the
  evidence attached.

  Watch for the asymmetric case: a limit raised on one cluster and not the other
  is how this happens in practice, and it only shows up when a record large
  enough to matter actually arrives — often long after the change.
EOF
}
