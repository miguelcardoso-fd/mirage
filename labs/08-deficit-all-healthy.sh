#!/usr/bin/env bash
# Lab 8 — Broker outage / disconnect storm: deciding it is NOT your layer

LAB_TITLE="Replication deficit, nothing unhealthy"
LAB_DIFFICULTY="hard"
LAB_ALERT="Timeout connecting to Kafka (>50/5m); disconnect rate (>1000/5m)"

lab_brief() {
cat <<'EOF'
SYMPTOM
  Pager fires. Connector tasks are erroring. Logs are full of:
    "Disconnecting from node N due to socket connection setup timeout"

YOUR JOB
  Two parts:
    1. TRIAGE — establish the blast radius and decide whether this is a KCD
       fault or an upstream one. Write your conclusion:
           mirage answer 8 broker      (if upstream/broker problem)
           mirage answer 8 connector   (if a connector misconfiguration)
    2. RECOVER — once the underlying cause is addressed, get replication
       flowing again.

  Think before you restart anything: consider what a restart costs on a system
  that is already degraded.

WHEN DONE
  mirage stop 8
EOF
}

lab_break() {
  # Scaling a KRaft dual-role pool to 0 is refused (it is also the controller),
  # so instead make the broker unschedulable and remove it. The pod cannot come
  # back until the selector is cleared, which is what you will undo.
  k patch kafkanodepool source-pool --type merge -p '{"spec":{
      "template":{"pod":{"affinity":{"nodeAffinity":{
        "requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[
          {"matchExpressions":[{"key":"mirage/absent","operator":"In","values":["yes"]}]}
        ]}}}}}
  }}' >/dev/null 2>&1

  local p
  p="$(src_pod)"
  [ -n "$p" ] && k delete pod "$p" --force --grace-period=0 >/dev/null 2>&1
  forget_broker_pods

  # Confirm the outage actually happened. If the affinity patch were rejected
  # the broker would come straight back and you would be hunting a fault that
  # is not there.
  local n=0 gone=1
  while [ "$n" -lt 120 ]; do
    if [ -z "$(broker_pod kafka-source)" ]; then gone=0; break; fi
    sleep 5; n=$((n+5))
  done
  if [ "$gone" -ne 0 ]; then
    err "the source broker is still Running — the outage was not injected"
    say "  Check: kubectl -n $NS get kafkanodepool source-pool -o yaml | grep -A8 affinity"
    return 1
  fi

  mark lab8.broker_down 1
  info "Waiting for the connector to notice (~45s)"
  sleep 45
}

lab_check() {
  local ans
  ans="$(marked lab8.answer)"

  assert_eq "triage conclusion recorded as 'broker'" "${ans:-none}" "broker" \
    "Simultaneous failures across both consumers and producers, on every connector, is the signature of a broker or network event — not a connector fault. Restarting connectors cannot fix an absent broker and adds rebalance churn. Record it with: mirage answer 8 broker" \
    "mirage answer 8 broker|connector"

  # The source BROKER specifically must be back. Counting pods by cluster label
  # is not enough: the entity-operator carries the same label and stays Running
  # even with no broker at all. And Strimzi can still report the Kafka CR as
  # Ready while its broker pod is Pending, so neither signal alone is reliable.
  forget_broker_pods
  local src_broker src_ready
  src_broker="$(broker_pod kafka-source)"
  assert "source broker pod is Running again" \
    "$([ -n "$src_broker" ] && echo 0 || echo 1)" \
    "Replication cannot resume while the source cluster has no running broker. Note the entity-operator pod keeps running regardless, so 'some pod is up' proves nothing — check for strimzi.io/broker-role=true. In production this fix belongs to the Kafka owners; here you play both roles." \
    "kubectl -n mirage get pods -l strimzi.io/cluster=kafka-source,strimzi.io/broker-role=true"

  # Prove the broker actually serves requests, not just that a pod exists.
  src_ready=1
  if [ -n "$src_broker" ]; then
    _kafka_in "$src_broker" "bin/kafka-topics.sh --bootstrap-server $SRC_BOOT --list" \
      >/dev/null 2>&1 && src_ready=0
  fi
  assert "source broker answers API requests" "$src_ready" \
    "A broker pod that is Running but not yet serving still refuses connections. Strimzi may report the Kafka CR as Ready before this is true." \
    "kubectl -n mirage exec kafka-source-source-pool-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"

  wait_connector_tasks 1 120 || true
  local running failed
  running="$(conn_task_count RUNNING)"
  failed="$(conn_task_count FAILED)"

  assert_ge "connector tasks running" "${running:-0}" 1 \
    "After connectivity returns, tasks generally recover on their own. Restart only those still FAILED afterwards." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Deliberately AFTER the throughput check in importance: during this outage
  # tasks keep reporting RUNNING while moving nothing, which is the whole point
  # of the "stale connector" failure mode. Task state is not a health signal.
  assert_eq "no tasks left FAILED" "${failed:-0}" "0" \
    "Tasks that failed during the outage may need a restart once the broker is healthy — that is the legitimate use of the restart annotation. Note tasks can also read RUNNING throughout while replicating nothing." \
    "kubectl -n mirage annotate kafkaconnector $CONNECTOR strimzi.io/restart=true --overwrite"

  # The only trustworthy signal, measured as a delta.
  # Threshold is deliberately lower than other labs. After an outage the
  # connector resumes from its last committed offset and, with
  # consumer.auto.offset.reset=latest, legitimately skips part of what was
  # produced while the broker was away. Demanding ~100% would fail a correct
  # recovery. We only need proof the pipeline is moving real volume again.
  info "Confirming replication resumed"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 150)"
  assert_ge "data flows again after recovery" "${delta:-0}" 500 \
    "Full recovery means throughput, not green status. Throughout this outage the tasks read RUNNING while replicating nothing — exactly what the production monitor calls a stale connector. Some loss is expected here: auto.offset.reset=latest skips records produced during the outage." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage get pods
  kubectl -n mirage logs <connect-pod> --tail=200 | grep -i disconnect
  mirage counts

  Blast radius is the whole diagnosis. Is one connector affected, or everything?
  Is it one side of the pipe, or both?

  First, look at who is actually failing:

    kubectl -n mirage get pods
    kubectl -n mirage get kafka

  If a BROKER pod is missing or Pending, no amount of connector work will help.
  Note the entity-operator pod carries the same cluster label and stays Running
  regardless — select the broker specifically:

    kubectl -n mirage get pods -l strimzi.io/cluster=kafka-source,strimzi.io/broker-role=true

  Also note Strimzi may still report the Kafka CR as Ready while its broker pod
  is Pending, so neither signal alone is trustworthy.

  Blast-radius reasoning, which is the real skill here:

    many connectors, consumers AND producers, several environments
        -> broker or network event. Not KCD. Escalate.
    one connector only
        -> that connector's source/target config
    one pod only
        -> that worker's networking

  In the real production storm this lab is modelled on, prdmi, prdnc and prdpr
  all failed at the same moment, hitting both replication-consumer and
  connector-producer clients. Three independent environments failing together
  is not a connector fault.

  Confirm with throughput rather than state:
    kafka_connect_mirror_replication_record_rate   -> near zero?
    kafka_connect_worker_rebalance_epoch           -> climbing? (membership churn)

  To record your conclusion:  mirage answer 8 broker

  To restore the broker here, look at what is stopping it from scheduling:
    kubectl -n mirage get kafkanodepool source-pool -o jsonpath='{.spec.template.pod.affinity}{"\n"}'
EOF
}

lab_solve() {
cat <<'EOF'
  # 1. Record the triage conclusion
  mirage answer 8 broker

  # 2. Restore the source broker (in production: the Kafka owners do this).
  #    The pool was given an impossible node affinity; clear it.
  kubectl -n mirage patch kafkanodepool source-pool --type json \
    -p '[{"op":"remove","path":"/spec/template/pod/affinity"}]'

  kubectl -n mirage wait kafka/kafka-source --for=condition=Ready --timeout=600s

  # 3. Restart only tasks still FAILED afterwards
  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite

  # 4. Verify throughput, not status
  mirage counts

WHAT NOT TO DO
  Do not restart connectors while the broker is down. It cannot help, and each
  restart triggers rebalance churn in a cluster that is already struggling.

  Note scheduled.rebalance.max.delay.ms=300000 is deliberately absorbing pod
  churn for you here (production's repo value is now 120000, two minutes). On the legacy VMs this was 0, and the Ansible template
  said so explicitly: instant reassignment on every pod death is
  "incompatible with a cloud-native setup ... a heavy operation".

AT MIRAGEDUEL
  The fix is at the broker/network layer. Escalate to the Kafka owners WITH your
  blast-radius evidence — which connectors, which directions, which environments,
  and the fact that record_rate is zero while task state reads RUNNING.

  Remember the production monitor's own words about this failure:
    "This might cause stale connectors, where we have tasks running,
     but no data being replicated."
EOF
}
