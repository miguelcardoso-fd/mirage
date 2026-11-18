#!/usr/bin/env bash
# Lab 3 — OOMKilled / heap exhaustion

LAB_TITLE="Heap claim exceeds the container"
LAB_DIFFICULTY="medium"
LAB_ALERT="Heap saturation (>90%, 10m); OutOfMemoryError logs (>0); container memory (>95%); restarts (>=2/15m)"

lab_brief() {
cat <<'EOF'
SYMPTOM
  A change window closed an hour ago. Nothing is on fire. Replication is
  flowing, tasks are RUNNING, and the pod has not restarted.

  An audit flagged the Connect cluster as misconfigured anyway.

  This is a LATENT fault: the worker is one busy hour away from dying, and
  today's load is not that hour. Do not wait for a reproduction — there may
  never be one on this traffic. Read the configuration and decide whether it
  is survivable.

YOUR JOB
  Make the worker's memory configuration safe, and keep replication healthy.

WHEN DONE
  mirage stop 3
EOF
}

lab_break() {
  # This lab injects a LATENT fault and cannot reliably produce a restart.
  #
  # Measured on a rebuilt cluster: -Xmx 1920m in a 1280Mi container, then ~1GB
  # of 250KB records pushed through it. Container usage plateaued at 1181MB of
  # 1342MB (88%) and held — no OOMKill, no restart, no JVM OutOfMemoryError,
  # over several minutes. The reason is that -Xmx is a CEILING, not a
  # reservation: the JVM maps only what the workload demands, and a mirror
  # moving byte arrays with modest buffers never demands 1920m. Reaching the
  # ceiling needs a heap-retention pattern this workload does not have.
  #
  # So the lab grades the CONFIGURATION, which is genuinely wrong and is what an
  # audit or a production alert would flag, and the brief says plainly that
  # there may be no reproduction. Do not try to "fix" this by raising the load:
  # that was tried and the ceiling is not reachable this way. If a future change
  # makes a real OOM reproducible, update the title and brief with it.
  #
  # Break the heap:container relationship relative to whatever the baseline is,
  # so the lab holds on any machine.
  local lim_mi
  lim_mi="$(connect_worker_mem_mi)"
  [ "${lim_mi:-0}" -lt 256 ] && lim_mi=1280
  local bad_heap=$(( lim_mi + (lim_mi / 2) ))   # 150% of the container

  k patch kafkaconnect "$CONNECT" --type merge -p "{\"spec\":{
      \"jvmOptions\":{\"gcLoggingEnabled\":false,\"-Xms\":\"${bad_heap}m\",\"-Xmx\":\"${bad_heap}m\",
                    \"-XX\":{\"ExitOnOutOfMemoryError\":\"true\"}}
  }}" >/dev/null

  info "Adjusting the connector profile"
  k patch kafkaconnector "$CONNECTOR" --type merge -p '{"spec":{"config":{
      "consumer.max.poll.records":"30000",
      "consumer.fetch.max.bytes":"524288000",
      "consumer.max.partition.fetch.bytes":"16777216",
      "producer.override.buffer.memory":"268435456",
      "producer.override.max.request.size":"25165824"
  }}}' >/dev/null

  info "Generating load (this takes a moment)"
  produce 3000 60000 1500 "$TOPIC" "max.request.size=25165824 buffer.memory=134217728"
}

lab_check() {
  local xmx_raw lim_raw xmx_mb lim_mb ratio running total phase_ok

  xmx_raw="$(connect_get '.spec.jvmOptions.-Xmx')"
  lim_raw="$(connect_get '.spec.resources.limits.memory')"

  to_mb() {
    local v="${1:-0}" n
    n="$(printf '%s' "$v" | tr -cd '0-9.')"
    [ -z "$n" ] && { echo 0; return; }
    case "$v" in
      *Gi|*g|*G) printf '%.0f' "$(echo "$n * 1024" | bc -l 2>/dev/null || echo 0)" ;;
      *Mi|*m|*M) printf '%.0f' "$n" ;;
      *)         printf '%.0f' "$(echo "$n / 1048576" | bc -l 2>/dev/null || echo 0)" ;;
    esac
  }

  xmx_mb="$(to_mb "$xmx_raw")"
  lim_mb="$(to_mb "$lim_raw")"

  assert "heap (-Xmx) is smaller than the container memory limit" \
    "$([ "${xmx_mb:-0}" -gt 0 ] && [ "${lim_mb:-0}" -gt 0 ] && [ "$xmx_mb" -lt "$lim_mb" ] && echo 0 || echo 1)" \
    "Heap larger than the container guarantees an OOMKill: the JVM will try to use memory the cgroup will not grant. Raising heap makes this worse, not better." \
    "-Xmx=$xmx_raw limit=$lim_raw"

  if [ "${lim_mb:-0}" -gt 0 ] && [ "${xmx_mb:-0}" -gt 0 ]; then
    ratio=$(( xmx_mb * 100 / lim_mb ))
    assert "heap:container ratio leaves room for non-heap (<= 85%)" \
      "$([ "$ratio" -le 85 ] && echo 0 || echo 1)" \
      "Metaspace, thread stacks, direct buffers and native memory all live outside the heap but inside the limit. Production KCD runs 67-80%." \
      "current ratio ${ratio}% (-Xmx=$xmx_raw of limit=$lim_raw)"

    if [ "$ratio" -lt 25 ]; then
      warn "ratio ${ratio}% is very conservative — stable, but wasting most of the container."
    fi
  fi

  local oom_flag
  oom_flag="$(connect_get '.spec.jvmOptions.-XX.ExitOnOutOfMemoryError')"
  assert_eq "ExitOnOutOfMemoryError still enabled" "${oom_flag:-unset}" "true" \
    "This is the worker-side OOM defence. Without it a task-thread OOM leaves the task FAILED while the pod stays Running — nothing restarts it." \
    "kubectl -n mirage get kafkaconnect $CONNECT -o jsonpath='{.spec.jvmOptions}'"

  local ar
  ar="$(conn_get '.spec.autoRestart.enabled')"
  assert_eq "autoRestart still enabled on the connector" "${ar:-unset}" "true" \
    "The connector-side half of the pair. autoRestart recovers one task without disturbing the others." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.autoRestart}'"

  # Let the rollout finish before measuring stability, otherwise we count
  # restarts that are part of a legitimate resize.
  info "Waiting for the Connect cluster to finish rolling (up to 300s)"
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=300s >/dev/null 2>&1 || true

  info "Watching for restarts over 90s"
  local r1 r2
  r1="$(connect_pod_restarts)"
  sleep 90
  r2="$(connect_pod_restarts)"

  assert_eq "no new pod restarts while settling" "$(( ${r2:-0} - ${r1:-0} ))" "0" \
    "A worker still cycling means memory pressure is unresolved, not merely reconfigured." \
    "kubectl -n mirage get pods -l strimzi.io/cluster=$CONNECT"

  assert_ge "at least one Connect pod Running" "$(ready_connect_pods)" 1 \
    "No worker, no replication." \
    "kubectl -n mirage get pods -l strimzi.io/cluster=$CONNECT"

  wait_connector_tasks 1 90 || true
  running="$(conn_task_count RUNNING)"
  total="$(conn_tasks_total)"
  assert_ge "tasks running again" "${running:-0}" 1 \
    "Stable workers with dead tasks still move no data." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  local oom_lines pod
  pod="$(connect_pod)"
  oom_lines="$(kq logs "$pod" --tail=400 2>/dev/null | grep -c -i "OutOfMemoryError" || true)"
  assert_eq "no fresh OutOfMemoryError in current worker logs" "${oom_lines:-0}" "0" \
    "A recurring OOM means the heap is still too small for the configured fetch/produce buffers." \
    "kubectl -n mirage logs $pod | grep -i -A20 outofmemory"

  # Stable workers are not the same as a working pipeline. Without this, the lab
  # passes with the heap fixed and replication still dead — e.g. if the fetch
  # settings were cut so far back that nothing moves, or tasks never recovered.
  info "Confirming replication actually works again"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 150)"
  assert_ge "records replicate after the fix" "${delta:-0}" 1600 \
    "Resizing the heap must leave a working pipeline. Stable pods with no throughput is a different failure, not a fix." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage get pods -l strimzi.io/cluster=mirage
  kubectl -n mirage describe pod <pod> | grep -A8 "Last State"
  kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.jvmOptions}'
  kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.resources}'

  Three failure shapes look similar. Learn to tell them apart:
    (a) container OOMKill      exit 137, pod restarts
    (b) JVM OutOfMemoryError   pod Running, zero restarts, task FAILED
    (c) quota starvation       pods Pending, nothing unhealthy  (that is Lab 6)

  Here the configuration guarantees (a) or (b) under enough load, even if
  neither has fired yet.

  Compare two numbers directly:

    kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.jvmOptions.-Xmx}{"\n"}'
    kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.resources.limits.memory}{"\n"}'

  The heap must fit INSIDE the container with room left over. Metaspace, thread
  stacks, direct byte buffers and native allocations all count against the
  container limit but are not part of the heap. Production KCD sits at 67-80%.

  Two valid directions: lower -Xmx, or raise limits.memory. Since request equals
  limit here (Guaranteed QoS), raising the limit also raises the request.

  Also ask what made this connector hungry in the first place — look at
  consumer.max.poll.records and producer.override.buffer.memory. A high-throughput
  profile is a heap multiplier.

  Do NOT disable ExitOnOutOfMemoryError or autoRestart to make symptoms quieter.
  Those are the two defences that make an OOM survivable.
EOF
}

lab_solve() {
cat <<'EOF'
  # Read the two numbers that matter
  kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.jvmOptions.-Xmx}{"\n"}'
  kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.resources.limits.memory}{"\n"}'

  # Option A — bring the heap inside the container (aim for 50-70%)
  #   e.g. a 1280Mi container -> 768m heap (60%)
  kubectl -n mirage patch kafkaconnect mirage --type merge -p '{"spec":{
    "jvmOptions":{"gcLoggingEnabled":false,"-Xms":"768m","-Xmx":"768m",
                  "-XX":{"ExitOnOutOfMemoryError":"true"}}
  }}'

  # Option B — keep the heap, give the container room (watch node capacity)
  kubectl -n mirage patch kafkaconnect mirage --type merge -p '{"spec":{
    "resources":{"requests":{"cpu":"250m","memory":"2Gi"},"limits":{"memory":"2Gi"}}
  }}'

  # Either way, also reduce what the connector buffers:
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"config":{
      "consumer.max.poll.records":"500",
      "consumer.fetch.max.bytes":"52428800",
      "consumer.max.partition.fetch.bytes":"1048576",
      "producer.override.buffer.memory":"33554432",
      "producer.override.max.request.size":"2097152"
    }}}'

THE DEFENCE MATRIX
  autoRestart  ExitOnOOM   outcome of a task-thread OOM
  off          off         task FAILED forever, pod looks healthy — SILENT STALL
  on           off         operator restarts the task, heap stays degraded
  off          on          JVM exits, pod restarts clean, tasks reassigned
  on           on          production default — each covers the other

AT MIRAGEDUEL
  prd: 8Gi container / 6g heap. prd rt-catalog: 10Gi / 8g. Ratios 67-80%.
  KEDA triggers scale-up at 75% in prd, deliberately below 90%, because heap
  OOMs at Xmx (~83-90% of the container) before a 90% trigger could add a worker.
  Resourcing is config: PRD changes are a PR, and you cannot exec in to take a
  heap dump.
EOF
}
