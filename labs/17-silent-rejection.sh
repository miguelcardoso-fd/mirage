#!/usr/bin/env bash
# Lab 17 — producer-side size rejection: the connector refuses its own writes
#
# Maps to §2 "Connectors With Failing Tasks" / max.request.size in the official
# [KCD] Kafka Connect Troubleshooting Guide.
#
# Deliberately NOT a duplicate of lab 16. Both surface RecordTooLargeException,
# and that is the point — same exception, different side, different fix:
#
#   lab 16  the TARGET BROKER refuses the write (topic max.message.bytes).
#           Fix lives on the cluster: a topic config, i.e. someone else's PR.
#           One partition stalls; the other 11 carry the load.
#
#   lab 17  the CONNECTOR'S OWN PRODUCER refuses to send it
#           (producer.override.max.request.size). Fix lives in the connector
#           declaration, i.e. your PR. The record never reaches the network, so
#           no broker log mentions it and comparing cluster limits finds nothing
#           — both clusters agree, and the connector is the one disagreeing.
#
# Verified before building: with max.request.size=16384 the worker logged 19
# RecordTooLargeException hits while 4 tasks stayed RUNNING and 0 FAILED.

LAB_TITLE="Records rejected before leaving the connector"
LAB_DIFFICULTY="hard"
LAB_ALERT="RecordTooLargeException in connector logs; partial throughput loss with no task failures"

lab_brief() {
cat <<'EOF'
SYMPTOM
  Throughput has dropped and is not recovering. Some records arrive, some do
  not.

  The connector reports RUNNING. Every task reports RUNNING. Nothing is FAILED,
  nothing is restarting, and no broker on either cluster is complaining.

  The gap is not random: small records get through, and the bigger ones do not.

YOUR JOB
  Restore full replication. The oversized records must arrive, not be dropped.

WHEN DONE
  mirage stop 17
EOF
}

lab_break() {
  local sp
  sp="$(src_pod)"
  if [ -z "$sp" ]; then
    err "need the source broker running to inject this lab"
    return 1
  fi

  # 16384 is chosen so ordinary lab traffic (1000-byte records) is unaffected
  # and only the oversized batch below is refused. A limit low enough to block
  # everything would read as "replication is dead", which is a different and
  # much easier incident.
  local limit=16384

  # patch_connector_runtime, not a bare patch: Connect applies config
  # asynchronously, and asserting on the CR would pass while the worker still
  # runs the old value. If the push does not land there is no fault to find, so
  # fail the injection rather than present a lab that cannot be solved.
  if ! patch_connector_runtime \
      "{\"spec\":{\"config\":{\"producer.override.max.request.size\":\"$limit\"}}}" \
      'producer.override.max.request.size' "$limit" 150; then
    err "could not push max.request.size to the worker runtime"
    return 1
  fi
  mark lab17.limit "$limit"

  info "Generating load"
  produce 1200 1000 2000 >/dev/null 2>&1

  # The records the connector will refuse. Produced with an explicit
  # max.request.size on the PRODUCER-PERF side so the source accepts them —
  # the source must hold records the mirror cannot forward, or there is nothing
  # to diagnose.
  info "Generating oversized load"
  if ! _kafka_in "$sp" "bin/kafka-producer-perf-test.sh --topic $TOPIC \
        --num-records 40 --record-size 65536 --throughput 20 \
        --producer-props bootstrap.servers=$SRC_BOOT max.request.size=2097152" >/dev/null 2>&1; then
    err "could not produce the oversized records"
    return 1
  fi
  mark lab17.injected 1

  info "Generating load behind it"
  produce 1200 1000 2000 >/dev/null 2>&1

  info "Waiting for the rejections to establish (~60s)"
  sleep 60

  # Confirm the fault is actually live. A silent no-op injection is worse than
  # a failed one: the trainee hunts a fault that is not there and no fix can
  # pass the grade.
  local hits
  hits="$(worker_log_matches 'RecordTooLargeException|max\.request\.size')"
  if [ "$hits" = "unreadable" ] || [ "${hits:-0}" -lt 1 ] 2>/dev/null; then
    warn "no size rejection appeared in the worker log (hits=$hits)"
    warn "the fault may not behave as described"
  fi
}

lab_check() {
  local runtime_limit running total

  # The fix must be in the RUNTIME, not just the CR. Connect rejects an invalid
  # config with a 400 and keeps serving the old one, so a CR that looks correct
  # proves nothing about what the worker is doing.
  runtime_limit="$(runtime_config_value 'producer.override.max.request.size')"

  wait_connector_tasks 1 120 || true
  running="$(conn_task_count RUNNING)"; total="$(conn_tasks_total)"

  assert_ge "tasks exist" "${total:-0}" 1 \
    "A size rejection does not remove tasks — if they are gone, something else broke." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # total >= 1 is asserted above, so this cannot pass on 0 == 0.
  assert "every task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "This fault normally leaves tasks RUNNING — that is the trap. If tasks are now FAILED, your change made it worse." \
    "running=${running:-0} of total=${total:-0}"

  # Accept any of: raised well above the oversized record, or the override
  # removed entirely (inheriting Kafka's 1MB default). Both are correct fixes.
  if [ -z "$runtime_limit" ] || [ "$runtime_limit" = "unreachable" ]; then
    _record_pass "producer size override removed ${C_DIM}(inherits the default)${C_0}"
  elif [ "${runtime_limit:-0}" -ge 131072 ] 2>/dev/null; then
    _record_pass "producer can send the oversized records ${C_DIM}(max.request.size=$runtime_limit)${C_0}"
  else
    _record_fail "the connector's producer accepts records above 64KB ${C_DIM}(runtime max.request.size=$runtime_limit)${C_0}" \
      "The worker is still refusing its own writes at ${runtime_limit} bytes, so the oversized records can never leave the connector. Raise producer.override.max.request.size, or delete the override to inherit the 1MB default. Check the RUNTIME value, not the CR — Connect keeps serving the old config if it rejects a new one." \
      "mirage api /connectors/$CONNECTOR/config | grep max.request.size"
  fi

  # No FRESH rejections. Scoped to this lab's log epoch, so a rejection from an
  # earlier lab or an earlier attempt cannot fail a correct fix.
  local hits
  hits="$(worker_log_matches 'RecordTooLargeException')"
  assert_eq "no fresh size rejections in the worker log" "${hits:-unreadable}" "0" \
    "A recurring RecordTooLargeException means the producer limit is still below the record size. 'unreadable' means the worker log could not be fetched at all." \
    'mirage logs --errors'

  # The oversized records must have actually ARRIVED. This is the assertion that
  # separates a real fix from errors.tolerance=all, which unblocks the pipeline
  # by discarding them — green tasks, healthy throughput, data silently gone.
  #
  # Measured as a DELTA over a fresh batch of oversized records, never as
  # src_count vs tgt_count. The cumulative comparison is failure mode #1 in
  # CLAUDE.md and it broke this lab in the field: a session that has re-seeded
  # the topic carries a permanent historical deficit (3211 records in the
  # report that caught this), so the gap can never reach any absolute
  # threshold and a correct fix is graded FAIL. It only passed in the harness
  # because run-all-labs.sh truncates the topic first.
  #
  # History cancels out in a delta: if the producer can send them the gap is
  # unchanged, and if it still refuses them the gap grows by the batch size.
  # Verified on a cluster carrying a 3211-record deficit — 40 oversized
  # records replicated and the gap stayed at 3211.
  #
  # Counting only the large records on the target would be more direct, but
  # reading record sizes needs DumpLogSegments and on-disk segment paths, which
  # differ across Kafka versions.
  info "Checking fresh oversized records reach the target"
  local sp before_gap after_gap grew n=0
  sp="$(src_pod)"
  before_gap=$(( $(src_count) - $(tgt_count) ))

  if ! _kafka_in "$sp" "bin/kafka-producer-perf-test.sh --topic $TOPIC \
        --num-records 40 --record-size 65536 --throughput 20 \
        --producer-props bootstrap.servers=$SRC_BOOT max.request.size=2097152" >/dev/null 2>&1; then
    warn "could not produce the verification batch — skipping the delivery check"
    before_gap=""
  fi

  if [ -n "$before_gap" ]; then
    # Wait for the batch to drain rather than sampling once: 40 records at
    # 64KB take a moment, and an impatient read looks identical to a drop.
    while [ "$n" -lt 150 ]; do
      after_gap=$(( $(src_count) - $(tgt_count) ))
      grew=$(( after_gap - before_gap ))
      [ "$grew" -le 10 ] && break
      sleep 10; n=$((n+10))
    done

    assert "fresh oversized records are delivered, not dropped" \
      "$([ "${grew:-9999}" -le 10 ] && echo 0 || echo 1)" \
      "40 oversized records were produced to the source and the source/target gap grew by ${grew:-?}, so they are not arriving. If tasks are green and the log is quiet, the likely cause is errors.tolerance=all: that unblocks the pipeline by DISCARDING oversized records — no alert, no recovery. For betting-adjacent data that is the wrong trade; raise producer.override.max.request.size so the records are delivered instead." \
      "gap went from ${before_gap} to ${after_gap:-?} over 40 oversized records"
  fi

  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  info "Confirming replication is healthy end to end"
  local delta
  delta="$(replication_delta 2000 150)"
  assert_ge "records replicate after the fix" "${delta:-0}" 1600 \
    "Raising a limit must leave a working pipeline. Green tasks with no throughput is a different failure, not a fix." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  mirage counts
  mirage logs --errors

  Note the shape before you touch anything: throughput dropped but did not stop,
  and every task is RUNNING. Something is being refused, not failing.

  The error names a size and a setting. Read it carefully — it tells you which
  side of the write is refusing:

    mirage logs --errors

  Expect RecordTooLargeException. Now the question that decides this lab: WHO
  is refusing? There are two possibilities and they have different fixes.

    a) the target BROKER refuses to store it   -> a topic config
                                                  (max.message.bytes)
    b) the connector's own PRODUCER refuses
       to send it                              -> a connector config
                                                  (max.request.size)

  Distinguish them by checking the clusters first:

    kubectl -n mirage exec kafka-target-target-pool-0 -- \
      bin/kafka-configs.sh --bootstrap-server localhost:9092 \
        --entity-type topics --entity-name lab-orders --describe

  If both clusters agree and neither has a restrictive override, the broker is
  not the one complaining. The record never reached it.

  So look at what the connector is actually running. Not the CR — the RUNTIME:

    mirage api /connectors/lab-orders-from-source-to-target/config | grep -i size

  Compare that number to the size in the exception.

  A note on why the CR is not enough: Connect applies config asynchronously and
  rejects invalid config with a 400 while continuing to serve the old version.
  The CR can say one thing and the worker run another. The REST API is the
  worker's own answer.

  When you fix it, there are two correct routes:

    a) raise producer.override.max.request.size above the record size
    b) delete the override and inherit Kafka's 1MB default

  And one route that LOOKS like a fix and is not:

    errors.tolerance=all

  That unblocks the pipeline by discarding the oversized records. Tasks go
  green, throughput recovers, and the records are gone — permanently, with no
  alert. This lab grades whether the records ARRIVED, so that route fails here,
  deliberately.

  Finally, check task state after your change. If a task exhausted its
  autoRestart budget while the limit was still too low it stays FAILED and will
  not come back on its own:

    kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
      strimzi.io/restart=true --overwrite

  Order matters: widen first, restart second. Restarting into an unchanged limit
  just spends another attempt.
EOF
}

lab_solve() {
cat <<'EOF'
  # 1. Confirm the shape: partial throughput, every task healthy
  mirage counts
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target | grep -A4 tasks

  # 2. Find the rejection
  mirage logs --errors
  #    Expect, repeatedly:
  #      org.apache.kafka.common.errors.RecordTooLargeException: The message is
  #      65536 bytes when serialized which is larger than 16384, which is the
  #      value of the max.request.size configuration.

  # 3. Rule out the broker. Both clusters agree — neither limits the record.
  kubectl -n mirage exec kafka-target-target-pool-0 -- \
    bin/kafka-configs.sh --bootstrap-server localhost:9092 \
      --entity-type topics --entity-name lab-orders --describe
  #    No max.message.bytes override. The record never reached the broker, so
  #    nothing on the cluster side can explain it.

  # 4. Read what the WORKER is running, not what the CR says
  mirage api /connectors/lab-orders-from-source-to-target/config | grep -i size

  # 5a. PREFERRED — raise the producer limit so the records are delivered
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"config":{
      "producer.override.max.request.size":"2097152"
    }}}'

  # 5b. EQUIVALENT — drop the override and inherit Kafka's 1MB default
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type json -p '[{"op":"remove","path":"/spec/config/producer.override.max.request.size"}]'

  # 6. Wait for the RUNTIME to carry the new value before restarting anything.
  #    Config pushes are asynchronous; restarting first makes tasks retry the
  #    old value and burn the autoRestart budget (maxRestarts: 5).
  until mirage api /connectors/lab-orders-from-source-to-target/config \
          | grep -q '"producer.override.max.request.size":"2097152"'; do sleep 5; done

  # 7. Only now, if a task died retrying while the limit was too low:
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target | grep -A3 tasks
  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite

  # 8. Verify the records ARRIVED — not merely that tasks are green
  mirage counts

WHAT MAKES THIS DIFFERENT FROM LAB 16
  Same exception, opposite side of the write.

  Lab 16: the target broker refuses to STORE the record. The write leaves the
  connector, crosses the network, and is rejected. The broker's limit is the
  problem and the fix is a topic config — at MirageDuel, a PR to
  sbv2_data_dependencies and a conversation with the topic's owner.

  Lab 17: the connector's own producer refuses to SEND it. The record never
  reaches the network. No broker log mentions it, and comparing the two
  clusters' limits finds nothing wrong, because nothing is wrong there. The fix
  is in the connector declaration — your PR, not someone else's.

  Diagnostically that distinction is the whole lab: the first instinct on
  RecordTooLargeException is to compare cluster limits, and here that
  investigation correctly finds nothing and has to be abandoned.

WHY NOT errors.tolerance=all
  It works, in the sense that the alert clears. The oversized records are
  dropped silently and permanently; no alert fires for a tolerated record.

  KCD's mirror path uses ByteArrayConverter for key, value and header, so
  records are opaque bytes and almost nothing can "fail to convert". There is
  nothing legitimate to tolerate, and a tolerance setting would only mask
  delivery failures like this one. The instructive counter-example is
  AvroToProtoTransform, which does interpret content and whose contract is the
  opposite: "Any per-record failure throws ConnectException (halt-and-page; no
  DLQ, no silent drop)." For financial-adjacent data a paged engineer beats a
  vanished record.

AT MIRAGEDUEL
  producer.override.max.request.size lives in the connector declaration in
  sb-kafka-connectors-management, so this is config-as-code and the fix is a PR.
  The real templates carry a handle_high_message_payload block that raises it to
  34603008 for connectors known to move large records.

  The troubleshooting guide's §2 note matters here: if the fix is NOT a connector
  config change you must restart the connector AND its tasks —
  POST /connectors/<name>/restart?includeTasks=true — because restarting the
  connector alone does not restart its tasks. A connector config change does
  roll the tasks by itself, which is why route 5a needs no explicit restart
  unless a task already died.
EOF
}
