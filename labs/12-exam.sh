#!/usr/bin/env bash
# Lab 12 — The exam: multiple simultaneous faults, no hints about which

LAB_TITLE="EXAM - several alerts at once"
LAB_DIFFICULTY="exam"
LAB_ALERT="Several at once. Triage order is part of the test."

lab_brief() {
cat <<'EOF'
  ############################################################
  #  EXAM — this lab injects SEVERAL faults at once.         #
  #  You are not told which, or how many.                    #
  ############################################################

SYMPTOM
  Replication is broken. Alerts are firing across the board. A change window
  went badly and more than one thing is wrong.

YOUR JOB
  Restore full, correct replication of lab-orders from source to target, and
  leave the cluster's safety settings as you found them.

WHEN DONE
  mirage stop 12
EOF
}

lab_break() {
  # Order matters. The Connect-cluster patch triggers a rolling restart, and any
  # connector patch issued during that churn can be silently dropped. So inject
  # the CONNECTOR faults first, verify they stuck, then break the cluster.

  info "Injecting fault 1/4"
  # Must land in the RUNTIME, not just the CR, and must go FIRST. Creating the
  # stray connector (and later the quota-blocked scale-up) triggers rebalances,
  # during which Connect answers "500 ... a rebalance is expected" and the
  # operator cannot push connector config at all.
  if ! patch_connector_runtime \
    '{"spec":{"tasksMax":1,"config":{"topics":"lab-orders-renamed-v3"}}}' \
    'topics' 'lab-orders-renamed-v3' 180; then
    err "could not inject fault 1 (connector runtime never took the new config)"
    return 1
  fi

  info "Injecting fault 2/4"
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
  name: lab-stray
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
  [ $? -eq 0 ] || { err "could not inject fault 2"; return 1; }

  info "Injecting fault 3/4"
  local cur per hard
  cur="$(ns_mem_requests_mi)"
  per="$(connect_worker_mem_mi)"
  [ "${per:-0}" -lt 128 ] && per=1280
  hard=$(( cur + per + (per / 2) ))
  kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ResourceQuota
metadata:
  name: mirage-quota
  namespace: $NS
spec:
  hard:
    pods: "12"
    requests.cpu: "6"
    requests.memory: ${hard}Mi
EOF
  [ $? -eq 0 ] || { err "could not inject fault 3"; return 1; }

  info "Injecting fault 4/4"
  k patch kafkaconnect "$CONNECT" --type merge -p '{"spec":{
      "replicas":3,
      "jvmOptions":{"gcLoggingEnabled":false,"-Xms":"2g","-Xmx":"2g",
                    "-XX":{"ExitOnOutOfMemoryError":"true"}}
  }}' >/dev/null || { err "could not inject fault 4"; return 1; }

  mark lab12.quota 1

  produce 3000 1000 3000
  info "Letting the mess settle"
  sleep 45
}

lab_check() {
  # --- workers ---
  local want have pending
  want="$(connect_get '.spec.replicas')"
  have="$(ready_connect_pods)"
  pending="$(kq get pods -l "strimzi.io/cluster=$CONNECT" \
    -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | grep -c '^Pending$' || true)"

  assert_eq "no Connect pods Pending" "${pending:-0}" "0" \
    "FAULT: namespace ResourceQuota too tight for the requested workers. Check quota before heap when pods will not schedule." \
    "kubectl -n mirage describe quota; kubectl -n mirage describe pod <pending-pod> | tail -20"

  assert_ge "all requested replicas Running" "${have:-0}" "${want:-1}" \
    "Workers must be stable before connector work means anything." \
    "kubectl -n mirage get pods -l strimzi.io/cluster=$CONNECT"

  # --- heap sanity ---
  local xmx lim
  xmx="$(connect_get '.spec.jvmOptions.-Xmx')"
  lim="$(connect_get '.spec.resources.limits.memory')"
  to_mb() {
    local v="${1:-0}" n; n="$(printf '%s' "$v" | tr -cd '0-9.')"
    [ -z "$n" ] && { echo 0; return; }
    case "$v" in
      *Gi|*g|*G) printf '%.0f' "$(echo "$n * 1024" | bc -l 2>/dev/null || echo 0)" ;;
      *Mi|*m|*M) printf '%.0f' "$n" ;;
      *) printf '%.0f' "$(echo "$n / 1048576" | bc -l 2>/dev/null || echo 0)" ;;
    esac
  }
  local xmb lmb
  xmb="$(to_mb "$xmx")"; lmb="$(to_mb "$lim")"
  assert "heap fits inside the container limit" \
    "$([ "${xmb:-0}" -gt 0 ] && [ "${lmb:-0}" -gt 0 ] && [ "$xmb" -lt "$lmb" ] && echo 0 || echo 1)" \
    "FAULT: -Xmx exceeded limits.memory, guaranteeing OOMKill. Production runs 67-80%." \
    "-Xmx=$xmx limit=$lim"

  # --- defences intact ---
  assert_eq "ExitOnOutOfMemoryError still enabled" \
    "$(connect_get '.spec.jvmOptions.-XX.ExitOnOutOfMemoryError')" "true" \
    "Do not disable a defence to silence a symptom." \
    "kubectl -n mirage get kafkaconnect $CONNECT -o jsonpath='{.spec.jvmOptions}'"

  assert_eq "autoRestart still enabled" "$(conn_get '.spec.autoRestart.enabled')" "true" \
    "The connector-side OOM defence." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.autoRestart}'"

  # --- cluster identity untouched ---
  assert_eq "group.id unchanged" "$(connect_get '.spec.config.group\.id')" \
    "connect-cluster_mirage" \
    "Cluster identity must survive your repair. Changing it orphans all connector state." \
    "kubectl -n mirage get kafkaconnect $CONNECT -o jsonpath='{.spec.config.group\\.id}'"

  assert_eq "offset.storage.topic unchanged" \
    "$(connect_get '.spec.config.offset\.storage\.topic')" "mirage-connect-offsets" \
    "Same reason. A new offset topic means silent data loss." \
    "kubectl -n mirage get kafkaconnect $CONNECT -o jsonpath='{.spec.config}'"

  # --- exactly one connector, correct ---
  local conns
  conns="$(kq get kafkaconnector -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -c . || true)"
  assert_eq "exactly one connector remains" "${conns:-0}" "1" \
    "FAULT: a stray connector mirrored the target cluster into itself with an identity policy and no exclusions — a replication loop." \
    "kubectl -n mirage get kafkaconnector"

  assert_eq "connector subscribed to the real topic" "$(conn_get '.spec.config.topics')" "lab-orders" \
    "FAULT: the topic pattern named a topic that does not exist, so tasks ran but idled. RUNNING is not a throughput signal." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config.topics}'"

  assert_ge "tasksMax raised for a 12-partition topic" "$(conn_get .spec.tasksMax)" 2 \
    "FAULT: tasksMax was 1 against 12 partitions — a throughput ceiling." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.tasksMax}'"

  local excl
  excl="$(conn_get '.spec.config.topics\.exclude')"
  assert "topics.exclude guard present" "$([ -n "$excl" ] && echo 0 || echo 1)" \
    "Identity replication policies have no name-based loop protection, so the exclusion list is load-bearing." \
    "production value: .*\\.heartbeats, mm2.*\\.internal, .*\\.replica, __.*"

  # Scope task checks to the REAL connector by name. A stray loopback connector
  # also has running tasks, so an unscoped count passes while the pipeline that
  # matters is still broken.
  local running total
  wait_connector_tasks 2 120 || true
  running="$(conn_task_count RUNNING "$CONNECTOR")"
  total="$(conn_tasks_total "$CONNECTOR")"
  assert_ge "the real connector has multiple tasks running" "${running:-0}" 2 \
    "Parallelism must take effect on lab-orders-from-source-to-target specifically. A stray connector's tasks do not count." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"
  assert_eq "no failed tasks on the real connector" "$(conn_task_count FAILED "$CONNECTOR")" "0" \
    "Clean recovery means nothing left in FAILED." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # --- target cleanliness ---
  # mirage-connect-* legitimately lives on kafka-target (Connect's own backing
  # store), so only unambiguous replication artefacts count as junk.
  local topics junk
  topics="$(list_target_topics)"
  junk="$(printf '%s\n' "$topics" | grep -cE '\.heartbeats$|^mm2-|\.internal$|\.replica$' || true)"
  assert_eq "no replication artefacts left on the target" "${junk:-0}" "0" \
    "Leftovers from the loop must be deleted; stopping production does not remove what already exists." \
    "mirage topics"

  # Same requirement as Lab 7: killing the loop does not un-write the duplicates
  # it already produced. A target holding far more than the source is not
  # recovered, however healthy everything else looks.
  local src_n tgt_n inflated=0
  src_n="$(src_count)"; tgt_n="$(tgt_count)"
  if [ "${src_n:-0}" -gt 0 ] && [ "${tgt_n:-0}" -gt $(( src_n * 3 / 2 )) ]; then inflated=1; fi
  assert "target is not still inflated by the loop" "$inflated" \
    "The stray connector re-wrote lab-orders on the target. Stopping it leaves those duplicates in place — the polluted target topic has to be dropped and re-replicated (see Lab 7)." \
    "source=$src_n target=$tgt_n — mirage counts"

  # --- throughput, the only trustworthy signal ---
  # A self-mirroring stray connector re-writes lab-orders on the target, so a
  # raw delta can be hugely POSITIVE while the real pipeline is dead. Require
  # the delta to track what was produced, not merely exceed it.
  info "Final check: is lab-orders flowing at the right rate?"
  local delta
  delta="$(replication_delta 3000 150)"
  local sane=1
  if [ "${delta:-0}" -ge 2400 ] && [ "${delta:-0}" -le 6000 ]; then sane=0; fi
  assert "fresh lab-orders records replicated, without duplication" "$sane" \
    "Expect roughly the 3000 records produced. Far more means something is re-replicating them (a loop); far fewer means the real pipeline is not moving data. Both are failures." \
    "target grew by ${delta:-0}, expected 2400-6000 — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage get pods
  kubectl -n mirage get kafkaconnect,kafkaconnector
  kubectl -n mirage describe quota
  mirage counts
  mirage topics

  There is no hint for this lab. `mirage solve 12` lists the faults if you get
  truly stuck, but try to find them yourself first.

  No hints for the exam.

  But a triage ORDER worth following, because faults mask each other:

    1. Can workers even run?      fewer pods than replicas -> quota (a quota
                                  refuses at admission: no Pending pod, just
                                  'exceeded quota' in events / operator log);
                                  OOMKilled -> limits
    2. Are workers stable?        restarts, heap vs container limit
    3. Is cluster identity sane?  group.id and the three storage topics
    4. Is the right connector set present?  strays, duplicates
    5. Is the connector configured correctly?  topics, tasksMax, exclusions
    6. Is data actually moving?    counts, not status

  Going straight to step 5 while step 1 is broken wastes the most time.
EOF
}

lab_solve() {
cat <<'EOF'
FOUR FAULTS WERE INJECTED

  1. Real connector: tasksMax=1 and topics="lab-orders-renamed-v3" (does not
     exist) -> no work, so no tasks, and no parallelism even once repointed
  2. Stray connector "lab-stray": target cluster mirrored into itself,
     topics=".*", no exclusions, identity policy
       -> replication loop polluting the target
  3. ResourceQuota requests.memory set just below what 3 workers need
       -> the third worker is refused at admission (not Pending — never created)
  4. -Xmx 2g inside a container limit well under it
       -> guaranteed OOMKill if that worker ever starts

A WORKING RECOVERY, IN ORDER

  # 4 first — stop the loop before it creates more junk
  # ORDER MATTERS: delete the looping connector FIRST. If you clean the junk
  # topics while it is still running, it simply recreates them — and the
  # re-replication keeps inflating the target while you work.
  kubectl -n mirage delete kafkaconnector lab-stray

  # 1 + 2 — make the workers able to run, and stable.
  #   Read the container limit, then set a heap at 50-70% of it.
  kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.resources}{"\n"}'

  kubectl -n mirage patch resourcequota mirage-quota --type merge \
    -p '{"spec":{"hard":{"requests.memory":"8Gi","requests.cpu":"6","pods":"12"}}}'

  #   3 workers must also FIT the node, which is shared with both Kafka
  #   clusters. 3 x 896Mi = 2688Mi works. Do not go below ~768Mi
  #   per container: Connect 4.0 needs room for metaspace and direct buffers
  #   outside the heap, and a 640Mi container OOMKills even with a 384m heap.
  kubectl -n mirage patch kafkaconnect mirage --type merge -p '{"spec":{
    "jvmOptions":{"gcLoggingEnabled":false,"-Xms":"512m","-Xmx":"512m",
                  "-XX":{"ExitOnOutOfMemoryError":"true"}},
    "resources":{"requests":{"cpu":"100m","memory":"896Mi"},
                 "limits":{"memory":"896Mi"}}
  }}'

  kubectl -n mirage wait kafkaconnect/mirage --for=condition=Ready --timeout=600s

  # 3 + 5 — stop EVERY connector before touching topics. The baseline mirror
  #   emits heartbeats as well as the stray one, so cleaning up while it still
  #   runs just recreates what you delete. Do not patch it and leave it running:
  #   take it down, clean, then bring it back correctly configured.
  kubectl -n mirage delete kafkaconnector lab-orders-from-source-to-target

  # 3b — REBUILD THE CONNECT CLUSTER. This is the step that is easy to miss, and
  #   skipping it is why the cleanup below looks like it fails: deleting the
  #   stray connector does NOT remove its config. Connect's config.storage.topic
  #   is compacted and replayed on every worker start, so the worker keeps
  #   recreating the loop's mm2-offset-syncs.target2.internal on the target
  #   however many times you delete the topic. Verified: after deleting
  #   lab-stray, mirage-connect-configs still held 6 records naming target2 and
  #   the topic came straight back.
  #
  #   Check for yourself:
  #     kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  #       --topic mirage-connect-configs --from-beginning --max-messages 300 \
  #       --timeout-ms 20000 | grep target2
  #
  #   In this lab environment:
  mirage repair
  #   In production you delete the connector through its CR and let the operator
  #   reconcile; you do not hand-edit Connect's internals.
  #
  #   Then stop the baseline connector again — repair brings it back.
  kubectl -n mirage delete kafkaconnector --all

  # Clean up what the loop created, now that nothing is writing.
  # CAREFUL: never delete mirage-connect-{configs,offsets,status} — they are
  # this Connect cluster's live internals, stored on kafka-target.
  for t in $(kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
               | grep -E '\.heartbeats$|^mm2-|\.internal$|\.replica$'); do
    kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t"
  done

  # The loop's duplicates are STILL on the target — stopping it does not
  # un-write them. Drop the polluted topic and let it re-replicate.
  # (Same remediation as Lab 7. In production this is a deliberate re-seed.)
  kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic lab-orders

  # Wait for the delete to land, or the mirror adopts a half-removed topic.
  until ! kubectl -n mirage exec kafka-target-target-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx lab-orders; do sleep 3; done

  # Bring the connector back with every correction applied at once: narrow
  # topic pattern, the exclude guard, and enough tasks for 12 partitions.
  kubectl -n mirage apply -f manifests/connector.yaml   # from the mirage dir
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"tasksMax":12,"config":{
      "topics":"lab-orders",
      "topics.exclude":".*\\.heartbeats, mm2.*\\.internal, .*\\.replica, __.*"
    }}}'

  # 6. Verify: counts should now track, not balloon
  mirage counts
EOF
}
