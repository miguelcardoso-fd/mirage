#!/usr/bin/env bash
# Lab 18 — poison pill: one record the target refuses on its TIMESTAMP
#
# Maps to §2 "Connectors With Failing Tasks" in the official [KCD] Kafka Connect
# Troubleshooting Guide.
#
# Deliberately NOT a duplicate of lab 16. Both are a single record the target
# broker refuses, and both put the same decision in front of you (widen the
# target, or drop the record with errors.tolerance). What differs is everything
# you have to read to get there:
#
#   lab 16  size.      RecordTooLargeException, max.message.bytes.
#   lab 18  time.      InvalidTimestampException, message.timestamp.before.max.ms.
#                      The record is SMALL — size-based reasoning finds nothing,
#                      and the bytes look like every other record. Only its
#                      CreateTime is wrong: an upstream producer stamped event
#                      time from a replay/backfill, which MM2 preserves verbatim.
#
# Verified before building (Kafka 4.0, this cluster): target window 1h, one
# record stamped now-2h on the source. Worker logged
#   InvalidTimestampException: Timestamp <ts> of message with offset 0 is out of
#   range. The timestamp should be within [<lo>, <hi>]
# The owning task cycled through autoRestart and settled FAILED; the 3
# partitions it owns (p, p+4, p+8 with 4 tasks) stopped, the other 9 advanced.
# Deleting the override + restarting drained all 12; errors.tolerance=all also
# unblocked them and the record was silently dropped.
#
# The console producer cannot set a timestamp (LineMessageReader only knows
# parse.key / parse.headers), so the pill is sent by a tiny Java producer run
# through the JDK source launcher inside the broker pod — jdk.compiler ships in
# the Strimzi image.

LAB_TITLE="A few partitions frozen, the rest flowing"
LAB_DIFFICULTY="hard"
LAB_ALERT="Unrecoverable exception from producer send callback; FAILED task; partial throughput loss"

# Age of the pill vs the target window. The pill must be older than the window
# at DELIVERY time, and ordinary lab traffic (stamped now, delivered in seconds)
# must sit comfortably inside it.
LAB18_WINDOW_MS=3600000     # 1h
LAB18_AGE_MS=7200000        # 2h

_lab18_producer_src() {
cat <<'EOF'
import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

// args: bootstrap topic ageMillis marker
public class Pill {
    public static void main(String[] a) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", a[0]);
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        long ts = System.currentTimeMillis() - Long.parseLong(a[2]);
        try (KafkaProducer<byte[], byte[]> prod = new KafkaProducer<>(p)) {
            RecordMetadata m = prod.send(new ProducerRecord<>(a[1], null, ts, null,
                a[3].getBytes("UTF-8"))).get();
            System.out.println("partition=" + m.partition() + " offset=" + m.offset());
        }
    }
}
EOF
}

_lab18_window() {
  _kafka_in "$(tgt_pod)" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
      --entity-type topics --entity-name $TOPIC --describe" \
    | grep -oE 'message\.timestamp\.before\.max\.ms=[0-9]+' | head -1 | cut -d= -f2
}

lab_brief() {
cat <<'EOF'
SYMPTOM
  Throughput dropped and is not recovering, but it did not go to zero. Most
  records still arrive.

  At least one task has been restarting and may now be FAILED. Restarting it
  buys a few seconds and then it fails again.

  Nothing about the records looks unusual: no big payloads, no traffic spike.
  The SAME records go through on the source cluster without complaint.

YOUR JOB
  Get every partition moving again WITHOUT silently losing business data.
  Record your decision: mirage answer 18 accept|skip|escalate

WHEN DONE
  mirage stop 18
EOF
}

lab_break() {
  local sp tp marker out b64
  sp="$(src_pod)"; tp="$(tgt_pod)"
  if [ -z "$sp" ] || [ -z "$tp" ]; then
    err "need both brokers running to inject this lab"
    return 1
  fi

  # The target's acceptance window is tightened; the source keeps the default
  # (before.max.ms = Long.MAX), so the source happily stores a record the
  # target will refuse. Same "limits drift between clusters" shape as lab 16.
  if ! _kafka_in "$tp" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
        --entity-type topics --entity-name $TOPIC \
        --alter --add-config message.timestamp.before.max.ms=$LAB18_WINDOW_MS" >/dev/null 2>&1; then
    err "could not inject the fault (target topic config)"
    return 1
  fi
  mark lab18.window "$LAB18_WINDOW_MS"

  info "Generating load"
  produce 1500 1000 2000 >/dev/null 2>&1

  # A unique marker so the grader can prove the pill itself was delivered
  # (lossless route) rather than skipped (errors.tolerance route).
  marker="kcd-pill-$(date +%s)"
  b64="$(_lab18_producer_src | base64 | tr -d '\n')"
  out="$(_kafka_in "$sp" "mkdir -p /tmp/lab18 && echo $b64 | base64 -d > /tmp/lab18/Pill.java \
      && java -cp '/opt/kafka/libs/*' /tmp/lab18/Pill.java $SRC_BOOT $TOPIC $LAB18_AGE_MS $marker 2>/dev/null")"
  case "$out" in
    *partition=*) ;;
    *) err "could not inject the fault (producer did not confirm the write)"
       return 1 ;;
  esac
  mark lab18.marker "$marker"
  mark lab18.partition "$(printf '%s' "$out" | grep -oE 'partition=[0-9]+' | cut -d= -f2)"
  mark lab18.injected 1

  info "Generating load behind it"
  produce 1500 1000 2000 >/dev/null 2>&1

  info "Waiting for the stall to establish (~60s)"
  sleep 60
}

lab_check() {
  local tp answer window tolerance
  tp="$(tgt_pod)"
  answer="$(marked lab18.answer)"

  if [ -z "$answer" ]; then
    warn "You did not record a decision (mirage answer 18 accept|skip|escalate)."
    warn "In a real incident the reasoning is the part that gets reviewed."
  else
    _record_pass "decision recorded ${C_DIM}($answer)${C_0}"
  fi

  if [ "$answer" = "escalate" ]; then
    info "Escalation accepted as a decision. For the lab to PASS you must also"
    info "restore replication — in production that would be the other team's"
    info "change, but you still need to see it through."
  fi

  wait_connector_tasks 1 120 || true
  local running total
  running="$(conn_task_count RUNNING)"; total="$(conn_tasks_total)"

  assert_ge "tasks exist" "${total:-0}" 1 \
    "A refused record does not remove tasks — if they are gone, something else broke." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  assert "every task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "A task that exhausted autoRestart while the pill was still refused stays FAILED even after you remove the cause. Fix the cause first, THEN restart the connector." \
    "running=${running:-0} of total=${total:-0}"

  # Read the end state, not the recorded answer: the answer is reasoning, this
  # is action.
  window="$(_lab18_window)"
  tolerance="$(conn_get '.spec.config.errors\.tolerance')"
  local route=""

  if [ -z "$window" ]; then
    route=lossless
    _record_pass "target window override removed ${C_DIM}(broker default)${C_0}"
  elif [ "${window:-0}" -gt "$LAB18_AGE_MS" ] 2>/dev/null; then
    route=lossless
    _record_pass "target window now covers the record ${C_DIM}(before.max.ms=$window)${C_0}"
  elif [ "$tolerance" = "all" ]; then
    route=lossy
    warn "You set errors.tolerance=all. That is a REAL option, and it works —"
    warn "but the refused record is now dropped silently and permanently, and so"
    warn "will be every future record outside the window. See 'mirage why 18'."
    _record_pass "pipeline unblocked via errors.tolerance ${C_DIM}(with silent loss)${C_0}"
  else
    _record_fail "the target accepts the record, or you chose tolerance deliberately" \
      "The target still refuses records older than ${window:-?}ms and errors.tolerance is not set, so the pill is still stuck. Either widen/remove message.timestamp.before.max.ms on the target topic (no data loss) or set errors.tolerance=all (accepts silent loss)." \
      "kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name $TOPIC --describe"
  fi

  # Per-partition, not topic-wide: the failed task owns only some partitions,
  # so a whole-topic delta reads healthy while they stay frozen.
  info "Checking every partition advances (producing 1500 across all of them)"
  local before after stalled=0 pcount=0 p off_b off_a line
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
    "A partition whose offset does not move while the others do belongs to the task still stuck on the pill. Whole-topic throughput hides it." \
    "kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $TOPIC   (twice, 30s apart)"
  if [ "${pcount:-0}" -gt 0 ] && [ "${stalled:-0}" -eq 0 ]; then
    _record_pass "all ${pcount} partitions advancing ${C_DIM}(pill cleared)${C_0}"
  fi

  # Lossless route: prove the pill itself arrived, by its marker, on its own
  # partition. Not an absolute count comparison — history on a re-seeded topic
  # would make that unsatisfiable (see lab 17).
  local marker part found
  marker="$(marked lab18.marker)"; part="$(marked lab18.partition)"
  if [ "$route" = "lossless" ] && [ -n "$marker" ] && [ -n "$part" ]; then
    found="$(_kafka_in "$tp" "bin/kafka-console-consumer.sh --bootstrap-server $TGT_BOOT \
        --topic $TOPIC --partition $part --offset earliest --timeout-ms 15000 2>/dev/null \
        | grep -c '$marker'")"
    assert_ge "the refused record was delivered, not skipped" "${found:-0}" 1 \
      "The window is fixed but the pill is not on the target. If errors.tolerance=all was set at any point, the record was dropped before you widened the window — that loss is permanent." \
      "kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --partition $part --offset earliest --timeout-ms 15000 | grep $marker"
  fi

  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 800 120)"
  assert_ge "replication healthy overall" "${delta:-0}" 600 \
    "Throughput must also be healthy in aggregate, not just unstalled." \
    "target grew by ${delta:-0} of 800 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  mirage counts
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target

  Some partitions move, some do not. Which ones are frozen, and which task owns
  them? A task is the unit that fails, so the frozen set is one task's share.

  Read the FAILED task's root cause: the last "Caused by" under
  "Unrecoverable exception from producer send callback". The describe output
  has the trace; the worker log has it too, repeating:

    kubectl -n mirage logs mirage-connect-0 | grep 'Caused by' | tail -3

  It is not a size problem. The exception names a timestamp and a window. The
  record's CreateTime came from whoever produced it upstream, and the mirror
  preserves it exactly.

  The source stored the record; the target refuses it. Compare what each
  cluster allows:

    kubectl -n mirage exec kafka-source-source-pool-0 -- bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name lab-orders --describe
    kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name lab-orders --describe

  And to see which partitions are frozen, run this twice ~30s apart:

    kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic lab-orders

  The decision, same as lab 16:
    a) widen the target's window -> record delivered, no loss
    b) errors.tolerance=all      -> unblocked, record DROPPED silently, and so
                                    is every future one outside the window
    c) escalate                  -> with evidence: topic, window, record ts

  Then remember: a task that already exhausted autoRestart stays FAILED after
  the cause is gone. Fix first, restart second.
EOF
}

lab_solve() {
cat <<'EOF'
  # 1. Shape: partial throughput, one task FAILED (or cycling)
  mirage counts
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target

  # 2. Find the refusal
  kubectl -n mirage logs mirage-connect-0 | grep 'Caused by' | tail -3
  #    InvalidTimestampException: Timestamp T of message ... is out of range.
  #    The timestamp should be within [LO, HI]
  #    T is ~2h old; the window is 1h each side.

  # 3. Compare limits — the target has an override the source does not
  kubectl -n mirage exec kafka-source-source-pool-0 -- bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name lab-orders --describe
  kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name lab-orders --describe
  #    target: message.timestamp.before.max.ms=3600000   source: none

  # 4a. PREFERRED — let the target accept it. No data loss.
  mirage answer 18 accept
  kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name lab-orders \
    --alter --delete-config message.timestamp.before.max.ms

  #     THEN restart — the task that died on the pill does not come back alone.
  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite

  # 4b. ALTERNATIVE — skip it. Unblocks, and loses the record forever.
  #     mirage answer 18 skip
  #     kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
  #       --type merge -p '{"spec":{"config":{"errors.tolerance":"all",
  #         "errors.log.enable":"true","errors.log.include.messages":"false"}}}'

  # 5. Verify: every task RUNNING, every partition moving
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target
  kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic lab-orders    # twice, ~30s apart
  mirage counts

WHY RESTARTING ALONE DOES NOT HELP
  The task resumes from its last committed offset, re-reads the same record,
  and is refused again — one less autoRestart attempt each time.

WHY NOT errors.tolerance=all HERE
  A timestamp window is a POLICY, not a one-off. Tolerance does not skip one
  record; it skips every record that ever falls outside the window — a
  backfill, a replay, a producer with a bad clock — silently, with no alert.
  For betting-adjacent data a paged engineer beats a silent drop.

AT MIRAGEDUEL
  Topic configs are declared in sbv2_data_dependencies, so widening the window
  is a PR and a conversation with the owning team. The upstream cause (who is
  stamping event time hours in the past, and is it a replay?) is the question
  to hand them, with the exact timestamp from the exception.
EOF
}
