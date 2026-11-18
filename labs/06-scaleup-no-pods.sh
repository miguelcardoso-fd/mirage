#!/usr/bin/env bash
# Lab 6 — ResourceQuota starvation (the incident that looks like a memory problem)

LAB_TITLE="Scale-up requested, pods never appeared"
LAB_DIFFICULTY="medium"
LAB_ALERT="Heap saturation and container memory alerts. No pod is unhealthy."

lab_brief() {
cat <<'EOF'
SYMPTOM
  A scale-up was requested and did not happen. The KafkaConnect CR asks for 3
  replicas; fewer pods than that exist. The missing pod is not Pending, not
  CrashLoopBackOff — it simply is not there.

  No pod has OOMKilled. No restarts. Heap is fine. Nothing is crashing.

YOUR JOB
  Get all requested Connect workers Running. Whatever is refusing them, treat
  it as a real constraint: fit within it, or adjust it deliberately.

WHEN DONE
  mirage stop 6
EOF
}

lab_break() {
  # The fault must be the QUOTA, not the node. The kind node's memory is shared
  # with the two Kafka clusters, and on a small VM 3 big workers would be
  # unschedulable for lack of node memory — an unwinnable lab. So: shrink each
  # worker to something that genuinely fits, then set the quota just below what
  # the scale-up needs, computed from live usage rather than a fixed number.
  local want=3 per=896 cur hard
  mark lab6.want "$want"
  mark lab6.per_worker "$per"

  # 896Mi / 512m heap is the smallest size verified to start reliably: Connect
  # 4.0 needs headroom outside the heap, and a 640Mi container OOMKills even
  # with a 384m heap. The fault must be the QUOTA, not an unstartable worker.
  info "Preparing the environment (resizing workers to ${per}Mi each)"
  k patch kafkaconnect "$CONNECT" --type merge -p "{\"spec\":{
      \"replicas\":1,
      \"jvmOptions\":{\"gcLoggingEnabled\":false,\"-Xms\":\"512m\",\"-Xmx\":\"512m\",
                    \"-XX\":{\"ExitOnOutOfMemoryError\":\"true\"}},
      \"resources\":{\"requests\":{\"cpu\":\"100m\",\"memory\":\"${per}Mi\"},
                   \"limits\":{\"memory\":\"${per}Mi\"}}
  }}" >/dev/null
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=360s >/dev/null 2>&1
  settle_connector 150 >/dev/null 2>&1

  # Ready can be true while a worker from a previous attempt is still
  # terminating. Wait for the scale-down to actually complete, or the namespace
  # measurement below counts a pod that is going away.
  local w=0
  while [ "$w" -lt 180 ]; do
    [ "$(ready_connect_pods)" -le 1 ] 2>/dev/null && break
    sleep 5; w=$((w+5))
  done

  # The ceiling must satisfy two competing requirements, or one of the two
  # documented remediations becomes impossible:
  #   A. 3 x $per must NOT fit, or there is no incident to diagnose.
  #   B. shrinking to $small MUST let the missing workers in, or "fit within the
  #      existing quota" is unwinnable.
  #
  # Sizing it is subtler than "below what 3 full workers need", because
  # of two behaviours that only show up at runtime:
  #
  #   1. When replicas goes to $want, the quota admits full-size workers
  #      GREEDILY until one does not fit. So the steady state is k full-size
  #      pods, where k is however many the ceiling allows - not the 1 that
  #      lab_break asked for.
  #   2. Kubernetes never resizes a running pod, and Strimzi does not roll
  #      workers because `resources` changed. So an Option B patch leaves those
  #      k pods at $per and only admits the remaining ones at $small.
  #
  # Option B therefore needs:  baseline + k*per + (want-k)*small
  # and the fault needs:       baseline + want*per  >  ceiling
  # Search for a ceiling satisfying both instead of deriving it in one step and
  # hoping - that guess was wrong three times.
  local small=768 baseline workers k need probe
  mark lab6.small "$small"
  cur="$(ns_mem_requests_mi)"

  workers="$(ready_connect_pods)"
  [ -z "$workers" ] || [ "$workers" -lt 1 ] 2>/dev/null && workers=1
  baseline=$(( cur - (workers * per) ))
  [ "$baseline" -lt 0 ] && baseline=0

  hard=0
  probe=$(( baseline + want * small ))
  while [ "$probe" -lt $(( baseline + want * per )) ]; do
    # How many full-size workers would this ceiling admit?
    k=0
    while [ $(( baseline + (k + 1) * per )) -le "$probe" ] && [ "$k" -lt "$want" ]; do
      k=$((k+1))
    done
    need=$(( baseline + k * per + (want - k) * small ))
    if [ "$need" -le "$probe" ]; then hard="$probe"; break; fi
    probe=$(( probe + 64 ))
  done

  if [ "$hard" -eq 0 ]; then
    # No ceiling satisfies both; keep the fault real and say so rather than ship
    # a lab whose Option B silently cannot pass.
    hard=$(( baseline + want * per - 64 ))
    warn "no quota window fits both fixes: Option B needs the workers rolled first"
  fi

  info "Injecting the fault"
  mark lab6.sizing "ns=${cur}Mi baseline=${baseline}Mi ceiling=${hard}Mi per=${per}Mi small=${small}Mi"
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

  info "Requesting $want Connect workers"
  k patch kafkaconnect "$CONNECT" --type merge \
    -p "{\"spec\":{\"replicas\":$want}}" >/dev/null
  mark lab6.quota 1
  sleep 30
}

lab_check() {
  local want have pending
  want="$(connect_get '.spec.replicas')"
  local target_want
  target_want="$(marked lab6.want)"; [ -z "$target_want" ] && target_want=3

  assert_ge "still asking for a scaled-up cluster (>=$target_want replicas)" \
    "${want:-0}" "$target_want" \
    "Scaling back down dodges the exercise rather than solving it. The ask was to fit $target_want workers, or to widen the quota so they fit." \
    "kubectl -n mirage get kafkaconnect $CONNECT -o jsonpath='{.spec.replicas}'"

  info "Waiting for pods to schedule (up to 180s)"
  local n=0
  while [ "$n" -lt 180 ]; do
    have="$(ready_connect_pods)"
    [ "${have:-0}" -ge "${want:-99}" ] && break
    sleep 5; n=$((n+5))
  done
  have="$(ready_connect_pods)"

  pending="$(kq get pods -l "strimzi.io/cluster=$CONNECT" \
    -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | grep -c '^Pending$' || true)"

  # Work out which layer is refusing, so the failure message blames the right
  # one. A quota refusal happens at admission: the pod is never created, so
  # there is nothing Pending to inspect — the evidence is in the operator log.
  local why=""
  if [ "${have:-0}" -lt "${want:-0}" ]; then
    # Quota rejections repeat on every reconcile, but the operator is chatty;
    # look back far enough to catch one.
    if kq logs -l name=strimzi-cluster-operator --tail=2000 2>/dev/null \
         | grep -qi 'exceeded quota'; then
      why="The namespace quota is refusing pod creation at admission — the pod is never created, so nothing shows as Pending. Raise requests.memory, or make each worker small enough to fit the existing ceiling."
    elif kq get pods -l "strimzi.io/cluster=$CONNECT" -o json 2>/dev/null \
           | grep -qi 'Insufficient memory'; then
      why="The NODE is out of memory, not the quota — only shrinking the workers (requests.memory and -Xmx together) helps. Raising the quota changes nothing."
    else
      why="Check both the quota and the node: kubectl -n mirage describe quota, then kubectl -n mirage describe pod <pod>."
    fi
  fi

  assert_eq "no Connect pods left Pending" "${pending:-0}" "0" \
    "A Pending pod with nothing unhealthy means the scheduler could not place it — usually node capacity. ${why}" \
    "kubectl -n mirage describe pod <pending-pod> | tail -20"

  assert "all requested workers were admitted" \
    "$([ "${have:-0}" -ge "${want:-99}" ] && echo 0 || echo 1)" \
    "Workers missing with nothing Pending means the quota rejected them at admission. ${why}" \
    "kubectl -n mirage describe quota mirage-quota; kubectl -n mirage logs -l name=strimzi-cluster-operator --tail=100 | grep -i 'exceeded quota'"

  assert_ge "all requested replicas are Running" "${have:-0}" "${want:-3}" \
    "Partial scale-up means the quota still does not accommodate the workload." \
    "kubectl -n mirage get pods -l strimzi.io/cluster=$CONNECT"

  # Did they reason about it, or just nuke the quota?
  if kq get resourcequota mirage-quota >/dev/null 2>&1; then
    local hard_mem used_mem
    hard_mem="$(kq get resourcequota mirage-quota -o jsonpath='{.status.hard.requests\.memory}')"
    used_mem="$(kq get resourcequota mirage-quota -o jsonpath='{.status.used.requests\.memory}')"
    _record_pass "quota kept in place and worked within/widened ${C_DIM}(used $used_mem of $hard_mem)${C_0}"
  else
    warn "You deleted the quota rather than fitting within it or raising it."
    warn "That works locally but is not the production move — quotas exist to protect"
    warn "shared cluster capacity. At MirageDuel a quota change is a PR to pdm-eks-utilities."
  fi

  wait_connector_tasks 1 90 || true
  assert_ge "connector still has running tasks after the change" \
    "$(conn_task_count RUNNING)" 1 \
    "Resizing workers should not leave the connector broken." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Shrinking workers to fit a quota is a legitimate fix, but it can starve the
  # heap and kill replication. Stable pods are not proof the pipeline survived.
  info "Confirming replication survived the resize"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 150)"
  assert_ge "replication still working after the resize" "${delta:-0}" 1600 \
    "Fitting the work into quota must not break throughput. If you shrank the workers, check you left enough heap — see Lab 3." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.replicas}{"\n"}'
  kubectl -n mirage get pods -l strimzi.io/cluster=mirage
  kubectl -n mirage describe quota
  kubectl -n mirage logs -l name=strimzi-cluster-operator --tail=100 | grep -i quota

  When desired replicas exceed actual pods and nothing is Pending, the pod was
  rejected at ADMISSION — it never got as far as scheduling. The operator log
  holds the reason.

  Check quota BEFORE you investigate heap. That ordering is the lesson.

  Read the quota's used-versus-hard numbers:

    kubectl -n mirage describe quota mirage-quota

  Then find out why the pod was refused. A quota rejection happens at ADMISSION,
  so no pod object is ever created and there is nothing Pending to describe.
  The reason lands in the operator log instead:

    kubectl -n mirage logs -l name=strimzi-cluster-operator --tail=200 | grep -i 'exceeded quota'

  It will read like:
    pods "mirage-connect-2" is forbidden: exceeded quota: mirage-quota,
    requested: requests.memory=384Mi, used: 2688Mi, limited: 2880Mi

  Note the POD COUNT is not the binding constraint — memory requests are.

  Do the arithmetic yourself:

    replicas:          kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.replicas}{"\n"}'
    per-worker memory: kubectl -n mirage get kafkaconnect mirage -o jsonpath='{.spec.resources.requests.memory}{"\n"}'
    quota ceiling:     kubectl -n mirage get resourcequota mirage-quota -o jsonpath='{.status.hard.requests\.memory}{"\n"}'
    already used:      kubectl -n mirage get resourcequota mirage-quota -o jsonpath='{.status.used.requests\.memory}{"\n"}'

  replicas x per-worker must fit inside (ceiling - everything else in the namespace).

  Two legitimate routes:
    a) raise the quota so the workers you want actually fit
    b) make each worker smaller so the replicas fit the existing quota

  But FIRST read the Pending pod's events to see which layer is refusing you:
    "exceeded quota"      -> the quota. Either route works.
    "Insufficient memory" -> the NODE is full. Only (b) helps; raising the quota
                             changes nothing. Check the node's allocatable memory:
                             kubectl get node -o jsonpath='{.items[0].status.allocatable.memory}'

  requests.memory is what counts against quota, and in this chart request equals
  limit (Guaranteed QoS), so lowering one lowers both. If you shrink the worker,
  shrink -Xmx too or you will swap this incident for Lab 3's.

  Note how this interacts badly with autoscaling: if quota blocks scale-up, KEDA
  cannot respond to memory pressure, so you get the memory alert AND no
  remediation.
EOF
}

lab_solve() {
cat <<'EOF'
  # See the numbers first
  kubectl -n mirage describe quota mirage-quota
  kubectl -n mirage get kafkaconnect mirage \
    -o jsonpath='replicas={.spec.replicas} per-worker={.spec.resources.requests.memory}{"\n"}'

  # Option A — widen the quota (the usual production fix)
  kubectl -n mirage patch resourcequota mirage-quota --type merge \
    -p '{"spec":{"hard":{"requests.memory":"4Gi","requests.cpu":"6","pods":"12"}}}'

  # Option B — shrink each worker so the replicas fit the existing ceiling.
  #   Do the arithmetic FIRST, do not guess a size:
  #     ceiling   = .status.hard.requests.memory
  #     baseline  = used - (current_workers x current_per_worker)
  #     per-worker budget = (ceiling - baseline) / desired_replicas
  #   768Mi x 3 is sized to fit this lab's ceiling. Do not go below ~768Mi:
  #   Connect 4.0 needs room outside the heap and a 640Mi container OOMKills
  #   even with a 384m heap — you would trade this incident for Lab 3's.
  #   Keep -Xmx at 50-70% of the container.
  kubectl -n mirage patch kafkaconnect mirage --type merge -p '{"spec":{
    "resources":{"requests":{"cpu":"100m","memory":"768Mi"},"limits":{"memory":"768Mi"}},
    "jvmOptions":{"-Xms":"448m","-Xmx":"448m","-XX":{"ExitOnOutOfMemoryError":"true"}}
  }}'

  # Verify
  kubectl -n mirage describe quota mirage-quota
  kubectl -n mirage get pods -l strimzi.io/cluster=mirage

WATCH WHICH LAYER IS REFUSING YOU
  Read the Pending pod's events before choosing:

    kubectl -n mirage describe pod <pending-pod> | tail -20

    "exceeded quota: ..."      -> the namespace quota. Raise it, or shrink pods.
    "Insufficient memory"      -> the NODE is full. Only shrinking helps; raising
                                  the quota changes nothing.

  On this one-node kind cluster the two Kafka clusters hold a large share of the
  node, so on a small VM the workers must stay small. In production the node pool
  is elastic and the quota is usually the real ceiling.

THE DIAGNOSTIC ORDER THAT MATTERS
  When pods will not schedule, check quota FIRST. This incident presents as a
  memory problem and genuinely is one — but no pod is unhealthy, so every minute
  spent reading heap graphs is wasted.

    pods Pending + nothing unhealthy  ->  quota, or node capacity
    pods OOMKilled (exit 137)         ->  container limit too small
    task FAILED, pod Running          ->  JVM heap too small

AT MIRAGEDUEL
  Quotas live with the namespace definitions in pdm-eks-utilities, so a quota
  increase is a PR. Short term, reducing per-pod requests or replica count can
  fit the work into existing quota — also a PR, since both are config.

  Note how this interacts with KEDA: if quota blocks scale-up, the autoscaler
  cannot respond to memory pressure, so you get the memory alert AND no
  remediation. That pairing is the real production trap.
EOF
}
