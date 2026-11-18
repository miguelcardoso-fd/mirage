#!/usr/bin/env bash
# Lab 19 — The Strimzi cluster operator is crashlooping, so nothing reconciles
#
# Real shape: pdm-eks-utilities #4541 (Oct 2026). On racing-int-use2 the cluster
# operator's heap was exhausted, the JVM exited on ExitOnOutOfMemoryError, and
# the pod crashlooped 88 times with exit reason `Error` — NOT `OOMKilled`, because
# the container never hit its memory limit. VPA keys off OOMKilled, so it never
# raised the operator's memory. Workers kept running; every CR change sat
# unreconciled.
#
# The operator image's launch_java.sh prepends $JAVA_OPTS and always adds
# -XX:+ExitOnOutOfMemoryError, so -Xmx24m reproduces that exactly: measured
# `Error/3`, "Terminating due to java.lang.OutOfMemoryError: Java heap space",
# CrashLoopBackOff, within ~40s. Shrinking the container instead (128Mi) was
# tried and alternates OOMKilled/137 with Error/3 — a different, flakier lesson.
#
# The vendored Deployment uses the Recreate strategy, so the healthy operator pod
# is gone before the broken one starts: there is no overlap that reconciles.

LAB_TITLE="Resume merged, connector still paused"
LAB_DIFFICULTY="hard"
LAB_ALERT="No KCD monitor pages for this: connector_status stays 2 (paused) and the crashloop/restart monitors are scoped to kube_app_name:kafka-connect, which the operator is not"

lab_brief() {
cat <<'EOF'
SYMPTOM
  Yesterday the connector was paused for a maintenance window, through the
  normal route: a PR setting spec.state: paused. This morning the resume PR
  merged and the deploy pipeline went green. The KafkaConnector resource now
  says spec.state: running.

  An hour later the downstream team reports nothing new has arrived. There are
  no FAILED tasks, no errors from the Connect worker, and the worker pod has not
  restarted.

YOUR JOB
  Find out why a correct, merged change never took effect, and get replication
  running again in a way that will still hold after the next deploy.

  Resuming the connector through the REST API is not the answer here — work out
  why, and what that would leave broken.

WHEN DONE
  mirage stop 19
EOF
}

lab_break() {
  local n=0 state

  # 1. The maintenance window: paused through the CR, with a healthy operator,
  #    so the runtime genuinely reaches PAUSED.
  patch_connector_verified '{"spec":{"state":"paused"}}' '.spec.state' 'paused' || return 1
  while [ "$n" -lt 90 ]; do
    state="$(runtime_connector_state)"
    [ "$state" = "PAUSED" ] && break
    sleep 3; n=$((n+3))
  done
  if [ "$state" != "PAUSED" ]; then
    warn "the connector never reached PAUSED (runtime says '${state:-unknown}')"
    return 1
  fi

  # 2. The fault: starve the operator's heap. ExitOnOutOfMemoryError makes the
  #    JVM exit with code 3, so the container reports Error, not OOMKilled.
  info "Letting the fault take hold (~60s)"
  k set env deploy/"$OPERATOR_DEPLOY" JAVA_OPTS=-Xmx24m >/dev/null 2>&1
  n=0
  while [ "$n" -lt 180 ]; do
    if [ "$(operator_restarts)" -ge 1 ] 2>/dev/null && ! operator_ready; then
      break
    fi
    sleep 5; n=$((n+5))
  done
  if operator_ready || [ "$(operator_restarts)" -lt 1 ] 2>/dev/null; then
    warn "the fault did not take hold; restoring and aborting"
    restore_operator
    return 1
  fi

  # 3. The resume PR lands on a CR that nothing is reconciling.
  k patch kafkaconnector "$CONNECTOR" --type merge -p '{"spec":{"state":"running"}}' >/dev/null 2>&1
  sleep 20
  if [ "$(conn_get .spec.state)" != "running" ] || [ "$(runtime_connector_state)" != "PAUSED" ]; then
    warn "injection did not produce the expected CR/runtime split; restoring and aborting"
    restore_operator
    return 1
  fi

  # Records the downstream team is waiting for.
  produce 2000 1000 2000
  mark lab19.injected
}

lab_check() {
  local before after gen observed state spec running total failed

  assert "Strimzi cluster operator is Ready" \
    "$(operator_ready && echo 0 || echo 1)" \
    "Every KafkaConnector and KafkaConnect change is applied by the operator. With it down, the API server accepts your CR edits and nothing ever acts on them." \
    "kubectl -n $NS get pods -l name=$OPERATOR_DEPLOY ; kubectl -n $NS logs deploy/$OPERATOR_DEPLOY --previous"

  # Ready is not stable: with a starved heap the operator can pass readiness and
  # then die on its first real reconcile. Watch it across a window.
  before="$(operator_restarts)"
  info "Watching the operator for restarts (60s)"
  sleep 60
  after="$(operator_restarts)"
  assert_eq "operator did not restart during the grading window" \
    "$(( ${after:-0} - ${before:-0} ))" "0" \
    "A crashlooping operator reconciles in bursts between crashes, if at all. 'Error' with exit code 3 and 'Terminating due to java.lang.OutOfMemoryError' in --previous is a heap problem, not a container-limit problem." \
    "kubectl -n $NS describe pod -l name=$OPERATOR_DEPLOY | grep -A6 'Last State'"

  gen="$(conn_get .metadata.generation)"
  observed="$(conn_get .status.observedGeneration)"
  assert_eq "operator has reconciled the latest connector spec" \
    "observed ${observed:-none} of ${gen:-?}" "observed ${gen:-?} of ${gen:-?}" \
    "observedGeneration lagging metadata.generation is the one field that proves nobody has acted on your change. The status block below it is stale until then — it still describes the last reconcile." \
    "kubectl -n $NS get kafkaconnector $CONNECTOR -o jsonpath='{.metadata.generation} {.status.observedGeneration}'"

  spec="$(conn_get .spec.state)"
  assert "desired state is running (spec.state running or omitted)" \
    "$([ -z "$spec" ] || [ "$spec" = "running" ] && echo 0 || echo 1)" \
    "The resume was already correct in the CR. The fix belongs to the operator, not the connector." \
    "kubectl -n $NS get kafkaconnector $CONNECTOR -o jsonpath='{.spec.state}'"

  state="$(runtime_connector_state)"
  assert_eq "worker reports the connector RUNNING" "${state:-unknown}" "RUNNING" \
    "This is what the worker is actually doing. With the operator down, the CR's status block cannot tell you this." \
    "kubectl -n $NS exec mirage-connect-0 -- curl -s localhost:8083/connectors/$CONNECTOR/status"

  wait_connector_tasks 1 120 || true
  total="$(conn_tasks_total)"
  running="$(conn_task_count RUNNING)"
  failed="$(conn_task_count FAILED)"
  assert "every task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "Read from the CR status, which only refreshes once the operator reconciles again." \
    "running=${running:-0} of total=${total:-0}, failed=${failed:-0} — kubectl -n $NS describe kafkaconnector $CONNECTOR"

  info "Confirming replication resumed"
  assert_no_foreign_connectors
  local delta
  delta="$(replication_delta 2000 120)"
  assert_ge "fresh records replicate" "${delta:-0}" 1600 \
    "A paused connector moves nothing; green tasks are not proof." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  The CR says running. What does the WORKER say? Ask it directly:

    kubectl -n mirage exec mirage-connect-0 -- \
      curl -s localhost:8083/connectors/lab-orders-from-source-to-target/status

  When the desired state and the actual state disagree, something between them
  is not doing its job. What applies a KafkaConnector change to the worker?

  Compare these two numbers on the connector resource:

    kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
      -o jsonpath='{.metadata.generation} {.status.observedGeneration}{"\n"}'

  generation is bumped by every spec change. observedGeneration is written by
  the operator when it has acted on one. If they differ, nobody has. The whole
  status block is stale too — it still describes the last reconcile.

  Look at the operator itself:

    kubectl -n mirage get pods -l name=strimzi-cluster-operator
    kubectl -n mirage describe pod -l name=strimzi-cluster-operator | grep -A6 'Last State'
    kubectl -n mirage logs deploy/strimzi-cluster-operator --previous | tail -20

  Note the exit reason. It is NOT OOMKilled. What does exit code 3 mean for a
  JVM started with -XX:+ExitOnOutOfMemoryError, and where is its heap set?

  The operator's heap is too small to start. Check the Deployment's environment:

    kubectl -n mirage get deploy strimzi-cluster-operator \
      -o jsonpath='{.spec.template.spec.containers[0].env}' | tr ',' '\n' | grep -A1 JAVA

  Remove the override (or give the heap real room), wait for the rollout, and
  watch observedGeneration catch up. The connector resumes on its own — you do
  not touch it.
EOF
}

lab_solve() {
cat <<'EOF'
  # 1. Prove the change was never applied
  kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
    -o jsonpath='{.metadata.generation} {.status.observedGeneration}{"\n"}'
  kubectl -n mirage exec mirage-connect-0 -- \
    curl -s localhost:8083/connectors/lab-orders-from-source-to-target/status

  # 2. Find the reason: the operator is crashlooping on heap exhaustion
  kubectl -n mirage get pods -l name=strimzi-cluster-operator
  kubectl -n mirage describe pod -l name=strimzi-cluster-operator | grep -A6 'Last State'
  #   Reason: Error, Exit Code: 3  (not OOMKilled/137)
  kubectl -n mirage logs deploy/strimzi-cluster-operator --previous | grep -i outofmemory
  #   Terminating due to java.lang.OutOfMemoryError: Java heap space

  # 3. Fix the operator, not the connector
  kubectl -n mirage set env deploy/strimzi-cluster-operator JAVA_OPTS-
  kubectl -n mirage rollout status deploy/strimzi-cluster-operator

  # 4. Watch it catch up — no connector action needed
  kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
    -o jsonpath='{.metadata.generation} {.status.observedGeneration}{"\n"}'

WHY NOT RESUME THROUGH THE REST API?
  With the operator dead, nothing would revert it, so it would appear to work.
  But the operator is still down: the NEXT change to any connector or to the
  Connect cluster — a credential rotation, a tasksMax bump, a rollback — will
  also silently never apply. You would have hidden the incident, not fixed it.

EXIT CODE 3 vs 137
  137 / OOMKilled  the kernel killed the container for exceeding limits.memory
  3 / Error        the JVM exited itself on OutOfMemoryError (ExitOnOutOfMemoryError)
                   — the heap is too small, the container may be fine
  The operator's heap is a percentage of its container (MaxRAMPercentage=20 in
  this image) unless JAVA_OPTS overrides it. Raising the container raises the
  heap; a JAVA_OPTS -Xmx override defeats that.

IN PRODUCTION
  The operator runs in the `strimzi` namespace and is owned by compute-operations
  via pdm-eks-utilities. You cannot fix it from KCD on-call: escalate with the
  evidence (generation vs observedGeneration, exit code 3, the OOM line).
  pdm-eks-utilities #4541 is the real precedent: VPA never raised the operator's
  memory because the pod never reported OOMKilled.
EOF
}
