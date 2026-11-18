#!/usr/bin/env bash
# Shared helpers for the KCD lab harness.
# Sourced by mirage and by every labs/*.sh definition.

NS=mirage
CONNECT=mirage
CONNECTOR=lab-orders-from-source-to-target
# Broker pod names depend on Strimzi-assigned node IDs, so discover them
# rather than hardcoding an index. Cached per shell to avoid repeat lookups.
_SRC_POD_CACHE=""
_TGT_POD_CACHE=""

broker_pod() {
  # $1 = cluster name (kafka-source | kafka-target)
  # strimzi.io/broker-role=true selects the actual broker, excluding the
  # entity-operator and any other pod carrying the cluster label.
  kubectl -n "$NS" get pods \
    -l "strimzi.io/cluster=$1,strimzi.io/broker-role=true" \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}' 2>/dev/null \
    | awk '$2=="Running" {print $1; exit}'
}

src_pod() {
  # Validate the cache before trusting it. Labs delete and recreate brokers, and
  # a stale pod name makes every exec fail in a way that looks like dead
  # replication rather than a bad lookup.
  if [ -n "$_SRC_POD_CACHE" ] \
     && kubectl -n "$NS" get pod "$_SRC_POD_CACHE" \
          -o jsonpath='{.status.phase}' 2>/dev/null | grep -q '^Running$'; then
    printf '%s' "$_SRC_POD_CACHE"
    return
  fi
  _SRC_POD_CACHE="$(broker_pod kafka-source)"
  printf '%s' "$_SRC_POD_CACHE"
}

tgt_pod() {
  if [ -n "$_TGT_POD_CACHE" ] \
     && kubectl -n "$NS" get pod "$_TGT_POD_CACHE" \
          -o jsonpath='{.status.phase}' 2>/dev/null | grep -q '^Running$'; then
    printf '%s' "$_TGT_POD_CACHE"
    return
  fi
  _TGT_POD_CACHE="$(broker_pod kafka-target)"
  printf '%s' "$_TGT_POD_CACHE"
}

# Invalidate when a lab scales a broker away and back.
forget_broker_pods() { _SRC_POD_CACHE=""; _TGT_POD_CACHE=""; }
SRC_BOOT=kafka-source-kafka-bootstrap:9092
TGT_BOOT=kafka-target-kafka-bootstrap:9092
TOPIC=lab-orders

# Honour a LAB_HOME the caller already resolved (mirage walks symlinks before
# sourcing this), and fall back to deriving it from this file's own location for
# scripts that source lib/common.sh directly.
LAB_HOME="${LAB_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
STATE_DIR="$LAB_HOME/.state"
mkdir -p "$STATE_DIR"

# The lab owns its own kubeconfig so it can never touch your real clusters,
# and so you never have to export anything. Honour an explicit KUBECONFIG
# only if the caller set one deliberately.
if [ -z "${KCD_LAB_RESPECT_KUBECONFIG:-}" ]; then
  export KUBECONFIG="$STATE_DIR/kubeconfig"
fi

# Cluster-provider verbs (kind | minikube). Sourced after STATE_DIR and
# KUBECONFIG exist, because the provider functions read both.
# shellcheck source=provider.sh
source "$LAB_HOME/lib/provider.sh"

# ---------- output ----------

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'
  C_BLU=$'\033[34m'; C_DIM=$'\033[2m'; C_B=$'\033[1m'; C_0=$'\033[0m'
else
  C_RED=; C_GRN=; C_YEL=; C_BLU=; C_DIM=; C_B=; C_0=
fi

say()  { printf '%s\n' "$*"; }
info() { printf '%s==>%s %s\n' "$C_BLU" "$C_0" "$*"; }
warn() { printf '%s!!%s  %s\n' "$C_YEL" "$C_0" "$*"; }
err()  { printf '%sxx%s  %s\n' "$C_RED" "$C_0" "$*" >&2; }
hr()   { printf '%s%s%s\n' "$C_DIM" "------------------------------------------------------------" "$C_0"; }

# ---------- assertion engine ----------
# Each lab's check() calls assert_* repeatedly. Failures accumulate with
# explanation text so `stop` can tell the user exactly where they went wrong.

ASSERT_PASS=0
ASSERT_FAIL=0
FAIL_LOG=()

_record_pass() { ASSERT_PASS=$((ASSERT_PASS+1)); printf '  %s[PASS]%s %s\n' "$C_GRN" "$C_0" "$1"; }
_record_fail() {
  ASSERT_FAIL=$((ASSERT_FAIL+1))
  printf '  %s[FAIL]%s %s\n' "$C_RED" "$C_0" "$1"
  FAIL_LOG+=("$1|$2|$3")
}

# assert <description> <condition-exit-code> <why-it-matters> <where-to-look>
assert() {
  local desc="$1" ok="$2" why="${3:-}" where="${4:-}"
  if [ "$ok" -eq 0 ]; then _record_pass "$desc"; else _record_fail "$desc" "$why" "$where"; fi
}

# assert_eq <description> <actual> <expected> <why> <where>
assert_eq() {
  local desc="$1" actual="$2" expected="$3" why="${4:-}" where="${5:-}"
  if [ "$actual" = "$expected" ]; then
    _record_pass "$desc ${C_DIM}($actual)${C_0}"
  else
    _record_fail "$desc ${C_DIM}(got '$actual', expected '$expected')${C_0}" "$why" "$where"
  fi
}

# assert_ge <description> <actual> <minimum> <why> <where>
assert_ge() {
  local desc="$1" actual="${2:-0}" min="$3" why="${4:-}" where="${5:-}"
  [[ "$actual" =~ ^-?[0-9]+$ ]] || actual=0
  if [ "$actual" -ge "$min" ]; then
    _record_pass "$desc ${C_DIM}($actual >= $min)${C_0}"
  else
    _record_fail "$desc ${C_DIM}(got $actual, need >= $min)${C_0}" "$why" "$where"
  fi
}

reset_assertions() { ASSERT_PASS=0; ASSERT_FAIL=0; FAIL_LOG=(); }

# Fail the grade if a connector other than this lab's is mirroring lab-orders.
#
# replication_delta measures the TOPIC's count on the target, so it cannot tell
# which connector moved the data — any healthy mirror satisfies it. That turns
# the throughput assertion every lab_check ends with into a check that can pass
# on another lab's leftover: lab 4's `lab-orders-renamed` survived its lab and
# lab 3 then graded "tasks running: 0" and "records replicate: 2000 >= 1600" in
# the same run.
#
# Call this BEFORE the throughput assertion, so the report names the real
# problem instead of leaving a contradictory pair of results.
assert_no_foreign_connectors() {
  local found
  found="$(kq get kafkaconnector -o name 2>/dev/null \
    | sed 's|.*/||' | grep -v "^$CONNECTOR\$" | tr '\n' ' ' | sed 's/ *$//')"
  assert_eq "no connector other than $CONNECTOR is replicating" \
    "${found:-none}" "none" \
    "A leftover connector from another lab keeps mirroring lab-orders, so a throughput check can pass while THIS lab's connector is absent or dead. Delete it, or run 'mirage reset'." \
    "kubectl -n $NS get kafkaconnector"
}

report_result() {
  local lab="$1" label
  # Numeric ids read as "lab 7"; named scopes (e.g. "environment") read as-is.
  case "$lab" in
    ''|*[!0-9]*) label="$lab" ;;
    *)           label="lab $lab" ;;
  esac
  hr
  if [ "$ASSERT_FAIL" -eq 0 ]; then
    printf '%s%s  PASS  %s  %s — %s/%s checks\n' "$C_GRN" "$C_B" "$C_0" "$label" "$ASSERT_PASS" "$((ASSERT_PASS+ASSERT_FAIL))"
    hr
    return 0
  fi

  printf '%s%s  FAIL  %s  %s — %s of %s checks failed\n' "$C_RED" "$C_B" "$C_0" "$label" "$ASSERT_FAIL" "$((ASSERT_PASS+ASSERT_FAIL))"
  hr

  # Say how close this is, and in which direction. "2 of 7 failed" does not tell
  # you whether the config is wrong or the config is right and the pipeline is
  # still dead — and those need different next moves.
  local cfg_fail=0 flow_fail=0 entry desc
  for entry in "${FAIL_LOG[@]}"; do
    desc="$(printf '%s' "${entry%%|*}" | sed 's/\x1b\[[0-9;]*m//g')"
    case "$desc" in
      *replicat*|*throughput*|*records*|*caught\ up*|*deficit*|*inflat*|*artefact*|*stalled*|*partition*|*advancing*|*accept\ the\ oversized*)
        flow_fail=$((flow_fail+1)) ;;
      *) cfg_fail=$((cfg_fail+1)) ;;
    esac
  done

  say ""
  if [ "$cfg_fail" -gt 0 ] && [ "$flow_fail" -eq 0 ]; then
    say "${C_YEL}Shape:${C_0} configuration is still wrong. Data movement was not reached."
    say "       Fix the config first — the throughput checks run after it."
  elif [ "$cfg_fail" -eq 0 ] && [ "$flow_fail" -gt 0 ]; then
    say "${C_YEL}Shape:${C_0} ${C_B}the config looks right but data is not moving.${C_0}"
    say "       This is the harder half. Green task state is not a working pipeline:"
    say "       check heap/buffers (lab 3), credentials (labs 13-15), or whether a"
    say "       task is RUNNING while its client silently fails to connect."
  else
    say "${C_YEL}Shape:${C_0} $cfg_fail configuration problem(s) and $flow_fail data-flow problem(s)."
    say "       Work the configuration ones first; several flow checks depend on them."
  fi

  say ""
  say "${C_B}Where you fell short${C_0}"
  local i=1
  for entry in "${FAIL_LOG[@]}"; do
    local desc why where
    IFS='|' read -r desc why where <<<"$entry"
    say ""
    printf '%s%d.%s %s\n' "$C_B" "$i" "$C_0" "$(printf '%s' "$desc" | sed 's/\x1b\[[0-9;]*m//g')"
    [ -n "$why" ]   && printf '   %swhy it matters:%s %s\n' "$C_DIM" "$C_0" "$why"
    [ -n "$where" ] && printf '   %slook at:%s       %s\n' "$C_DIM" "$C_0" "$where"
    i=$((i+1))
  done
  case "$lab" in
    ''|*[!0-9]*) : ;;   # named scope (e.g. environment) — no hint/solve exists
    *)
      say ""
      say "Run ${C_B}mirage hint $lab${C_0} for a nudge, or ${C_B}mirage solve $lab${C_0} to see the answer."
      say "Re-run ${C_B}mirage stop $lab${C_0} after another attempt."
      ;;
  esac
  return 1
}

# ---------- kubectl helpers ----------

k()  { kubectl -n "$NS" "$@"; }
kq() { kubectl -n "$NS" "$@" 2>/dev/null; }

cluster_up() { kubectl cluster-info >/dev/null 2>&1; }

require_cluster() {
  if ! cluster_up; then
    err "No reachable Kubernetes cluster. Run: mirage setup"
    exit 1
  fi
  if ! kq get ns "$NS" >/dev/null; then
    err "Namespace '$NS' not found. Run: mirage setup"
    exit 1
  fi
}

# Read a field off the connector CR.
conn_get() { kq get kafkaconnector "$CONNECTOR" -o "jsonpath={$1}"; }
conn_exists() { kq get kafkaconnector "$CONNECTOR" >/dev/null; }

# Any connector in the namespace, by name.
cr_get() { kq get kafkaconnector "$1" -o "jsonpath={$2}"; }

connect_get() { kq get kafkaconnect "$CONNECT" -o "jsonpath={$1}"; }

# Connector readiness from status.conditions[type=Ready].
conn_ready() {
  local name="${1:-$CONNECTOR}"
  kq get kafkaconnector "$name" -o json 2>/dev/null \
    | py_json 'c=[x for x in d.get("status",{}).get("conditions",[]) if x.get("type")=="Ready"]; print(c[0]["status"] if c else "Unknown")'
}

conn_state_field() {
  local name="${1:-$CONNECTOR}"
  kq get kafkaconnector "$name" -o json 2>/dev/null \
    | py_json 'print(d.get("status",{}).get("connectorStatus",{}).get("connector",{}).get("state","UNKNOWN"))'
}

# Count tasks in a given state from status.connectorStatus.tasks.
conn_task_count() {
  local want="$1" name="${2:-$CONNECTOR}"
  kq get kafkaconnector "$name" -o json 2>/dev/null \
    | py_json "t=d.get('status',{}).get('connectorStatus',{}).get('tasks',[]);
print(len([x for x in t if str(x.get('state','')).upper()=='${want}'.upper()]) if '${want}' else len(t))"
}

conn_tasks_total() {
  local name="${1:-$CONNECTOR}"
  kq get kafkaconnector "$name" -o json 2>/dev/null \
    | py_json 'print(len(d.get("status",{}).get("connectorStatus",{}).get("tasks",[])))'
}

# python3 is guaranteed on macOS; avoids a hard jq dependency.
py_json() {
  python3 -I -c "
import json,sys
raw=sys.stdin.read().strip()
if not raw:
    print('')
    sys.exit(0)
try:
    d=json.loads(raw)
except Exception:
    print('')
    sys.exit(0)
$1
" 2>/dev/null
}

# Sum of requests.memory (in Mi) across all non-terminated pods in the namespace.
# Used by the quota labs so their numbers adapt to whatever the node is running
# instead of hardcoding totals that only hold on one machine.
ns_mem_requests_mi() {
  kubectl -n "$NS" get pods -o json 2>/dev/null | py_json '
import re
def mi(v):
    if not v: return 0
    v=str(v)
    m=re.match(r"^([0-9.]+)\s*(Gi|Mi|Ki|G|M|K|)$", v)
    if not m: return 0
    n=float(m.group(1)); u=m.group(2)
    return int(n*1024) if u in ("Gi","G") else int(n) if u in ("Mi","M") else int(n/1024) if u in ("Ki","K") else int(n/1048576)
tot=0
for p in d.get("items",[]):
    if p.get("status",{}).get("phase") in ("Succeeded","Failed"): continue
    for c in p.get("spec",{}).get("containers",[]):
        tot+=mi(c.get("resources",{}).get("requests",{}).get("memory"))
print(tot)'
}

# Per-worker memory request of the Connect cluster, in Mi.
connect_worker_mem_mi() {
  local v
  v="$(connect_get '.spec.resources.requests.memory')"
  python3 -I -c "
import re,sys
v='''${v}'''.strip()
m=re.match(r'^([0-9.]+)\s*(Gi|Mi|Ki|G|M|K|)\$', v)
if not m: print(0); sys.exit()
n=float(m.group(1)); u=m.group(2)
print(int(n*1024) if u in ('Gi','G') else int(n) if u in ('Mi','M') else int(n/1024) if u in ('Ki','K') else int(n/1048576))
" 2>/dev/null || echo 0
}

ready_connect_pods() {
  kq get pods -l "strimzi.io/cluster=$CONNECT" \
    -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | grep -c '^Running$' || true
}

connect_pod() {
  kq get pods -l "strimzi.io/cluster=$CONNECT" \
    -o jsonpath='{.items[0].metadata.name}'
}

connect_pod_restarts() {
  kq get pods -l "strimzi.io/cluster=$CONNECT" -o json 2>/dev/null \
    | py_json 'print(sum(cs.get("restartCount",0) for p in d.get("items",[]) for cs in p.get("status",{}).get("containerStatuses",[])))'
}

# ---------- kafka helpers (exec in the broker pod; no ephemeral pods) ----------

_kafka_in() {
  local pod="$1"; shift
  kq exec "$pod" -c kafka -- bash -c "$*" 2>/dev/null
}

# Total messages across all partitions of a topic.
# Kafka 4.0 moved GetOffsetShell to org.apache.kafka.tools and ships the
# kafka-get-offsets.sh wrapper; the old kafka.tools class no longer exists.
topic_count() {
  local pod="$1" boot="$2" topic="$3"
  _kafka_in "$pod" "bin/kafka-get-offsets.sh \
      --bootstrap-server $boot --topic $topic 2>/dev/null" \
    | awk -F: 'NF==3 {s+=$3} END {print s+0}'
}

src_count() { topic_count "$(src_pod)" "$SRC_BOOT" "${1:-$TOPIC}"; }
tgt_count() { topic_count "$(tgt_pod)" "$TGT_BOOT" "${1:-$TOPIC}"; }

topic_exists() {
  local pod="$1" boot="$2" topic="$3"
  _kafka_in "$pod" "bin/kafka-topics.sh --bootstrap-server $boot --list 2>/dev/null" \
    | grep -qx "$topic"
}

topic_partitions() {
  local pod="$1" boot="$2" topic="$3"
  _kafka_in "$pod" "bin/kafka-topics.sh --bootstrap-server $boot --describe --topic $topic 2>/dev/null" \
    | grep -c 'Partition:' || true
}

# "partition gap" per line: source end offset minus target end offset for
# $TOPIC. Only meaningful as a CHANGE between two reads — history makes the
# absolute gap nonzero on a healthy re-seeded topic.
partition_gaps() {
  local s t
  s="$(_kafka_in "$(src_pod)" "bin/kafka-get-offsets.sh --bootstrap-server $SRC_BOOT --topic $TOPIC 2>/dev/null")"
  t="$(_kafka_in "$(tgt_pod)" "bin/kafka-get-offsets.sh --bootstrap-server $TGT_BOOT --topic $TOPIC 2>/dev/null")"
  { printf '%s\n' "$s" | sed 's/^/S:/'; printf '%s\n' "$t" | sed 's/^/T:/'; } \
    | awk -F: 'NF==4 && $1=="S" {s[$3]=$4} NF==4 && $1=="T" {t[$3]=$4}
               END {for (p in s) print p, s[p]-(t[p]+0)}' | sort -n
}

# Partitions whose gap grew between two partition_gaps reads, space-separated.
# A task that FAILED freezes only its own partitions while the rest carry the
# load, so a whole-topic delta reads healthy (labs 16, 18).
gaps_grew() {
  { printf '%s\n' "$1" | sed 's/^/B /'; printf '%s\n' "$2" | sed 's/^/A /'; } \
    | awk 'NF==3 && $1=="B" {b[$2]=$3} NF==3 && $1=="A" && ($2 in b) && $3>b[$2] {printf "%s%s", sep, $2; sep=" "}'
}

list_target_topics() {
  _kafka_in "$(tgt_pod)" "bin/kafka-topics.sh --bootstrap-server $TGT_BOOT --list 2>/dev/null"
}

# Produce n records of a given size into the source topic.
produce() {
  local n="${1:-2000}" size="${2:-1000}" tput="${3:-2000}" topic="${4:-$TOPIC}" extra="${5:-}"
  local pod
  pod="$(src_pod)"
  if [ -z "$pod" ]; then
    warn "no running source broker — cannot produce"
    return 1
  fi
  _kafka_in "$pod" "bin/kafka-producer-perf-test.sh \
      --topic $topic --num-records $n --record-size $size --throughput $tput \
      --producer-props bootstrap.servers=$SRC_BOOT $extra" >/dev/null
}

# Prove replication is live RIGHT NOW by producing a marked batch and watching
# the target grow by at least that much. Immune to lifetime-total inflation,
# which is why no lab should compare cumulative source/target counts.
#
# Usage: replication_delta <records> <timeout_seconds> [producer props]
# Echoes the observed target delta.
replication_delta() {
  local n="${1:-1500}" timeout="${2:-90}" props="${3:-}"
  local before after waited=0

  before="$(tgt_count)"
  if ! [[ "${before}" =~ ^[0-9]+$ ]]; then
    warn "could not read target count (broker unreachable?) — reporting 0"
    echo 0
    return 1
  fi

  # If producing fails, the delta would read 0 and look exactly like dead
  # replication. Say so instead of silently blaming the pipeline.
  if ! produce "$n" 1000 "$n" "$TOPIC" "$props"; then
    warn "producing to the source failed — the source broker may be down"
  fi

  while [ "$waited" -lt "$timeout" ]; do
    after="$(tgt_count)"
    [[ "${after}" =~ ^[0-9]+$ ]] || after="$before"
    [ "$after" -ge $(( before + (n * 80 / 100) )) ] && break
    sleep 5; waited=$((waited+5))
  done

  after="$(tgt_count)"
  [[ "${after}" =~ ^[0-9]+$ ]] || after="$before"
  local delta=$(( after - before ))
  [ "$delta" -lt 0 ] && delta=0     # topic truncated mid-measure
  echo "$delta"
}

# Replace the Connect worker pod so its log starts empty for the next lab.
#
# The point is the LOG, not the worker: the pod outlives a lab, so a plain
# `kubectl logs` shows the previous lab's errors and reads as evidence about
# the current one. Deleting the pod is the only way to make the raw command
# correct, since nothing can intercept a user's own kubectl.
#
# Safe because the worker keeps NO state in the pod — config.storage.topic,
# offsets and status all live on kafka-target, so the replacement replays them
# and comes back with the same connectors. ~45s, and it runs where cmd_start
# already waits for the baseline to settle, so it costs almost nothing extra.
#
# It also does not disturb the two labs that care about restarts: lab 3 grades a
# restartCount DELTA measured during grading (a fresh pod at 0 still deltas to
# 0), and lab 11 rolls the workers itself.
recycle_connect_worker() {
  local pod old_uid new_uid n=0
  pod="$(connect_pod)"
  [ -z "$pod" ] && return 0     # nothing running; the next start creates one

  # Identify the pod by UID, not name. The worker belongs to a StrimziPodSet,
  # which re-creates it under the SAME name (mirage-connect-0), so comparing
  # names never sees the replacement and the wait always times out.
  old_uid="$(kq get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null)"

  kq delete pod "$pod" --wait=false >/dev/null 2>&1

  while [ "$n" -lt 240 ]; do
    new_uid="$(kq get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null)"
    if [ -n "$new_uid" ] && [ "$new_uid" != "$old_uid" ]; then
      kq wait pod "$pod" --for=condition=Ready --timeout=180s >/dev/null 2>&1 && return 0
    fi
    sleep 5; n=$((n+5))
  done
  return 1
}

# ---------- log epoch ----------
# The Connect worker pod outlives a lab. Its log therefore still holds the
# errors the PREVIOUS lab injected, and three graders assert "no FRESH <error>
# in the worker log" — so running 14 after 13 would read 13's
# SaslAuthenticationException and fail a correct fix. `mirage logs --errors`
# has the same problem for the trainee, who gets handed a signature belonging
# to a lab they already finished.
#
# So: `mirage start` stamps the moment the fault goes in, and every log read
# scopes itself to lines after that stamp. Deliberately NOT a pod restart —
# labs 3 and 11 are about the worker rolling, and forcing a roll at injection
# would destroy what they teach.
log_epoch_set() {
  # UTC, second precision: --since-time wants RFC3339 and the Connect log has
  # second granularity anyway.
  mark log.epoch "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  unmark grade.start
}
log_epoch_clear() { unmark log.epoch; unmark grade.start; }

# Stamped by `mirage stop` right before lab_check. "No FRESH errors" means
# none since grading began: counting from the epoch includes the errors the
# fault itself produced BEFORE the fix, so a correct fix could never pass.
# It only ever did because a --tail cap happened to cut those lines off.
grade_start_set() { mark grade.start "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; }
log_epoch()       { marked log.epoch; }

# Fetch the worker log from the current lab's epoch onward. With no epoch (a
# bare `mirage logs` outside a lab) it falls back to the tail, which is the old
# behaviour.
# Usage: worker_log [tail-lines]
worker_log() {
  local lines="${1:-3000}" pod epoch
  pod="$(connect_pod)"
  [ -z "$pod" ] && return 1
  epoch="${WORKER_LOG_SINCE:-$(log_epoch)}"
  if [ -n "$epoch" ]; then
    # No --tail here: --since-time already bounds it, and a failing client can
    # log ~50 lines/s (lab 13's admin client spins "Rebootstrapping" after the
    # auth failure), which pushed the few real SaslAuthenticationException
    # lines out of a 3000-line tail within a minute — `mirage logs --errors`
    # then reported no failure signatures while the fault was live.
    kubectl -n "$NS" logs "$pod" --since-time="$epoch" 2>/dev/null
  else
    kubectl -n "$NS" logs "$pod" --tail="$lines" 2>/dev/null
  fi
}

# Count matches in a Connect worker's log, since the current lab's epoch.
# Returns the count on success, or "unreadable" if the log could not be fetched
# — a silent empty result would otherwise read as "no errors found" and pass a
# check that should fail.
#
# NOTE: "no output" is ambiguous here. A worker with nothing to say since the
# epoch legitimately prints nothing, and that is a 0, not "unreadable". So the
# pod and the fetch are what get verified, not the emptiness of the result.
# Usage: worker_log_matches <extended-regex> [tail-lines]
worker_log_matches() {
  local pat="$1" lines="${2:-3000}" pod out rc
  pod="$(connect_pod)"
  if [ -z "$pod" ]; then echo "unreadable"; return 1; fi
  # Graders count from grading start (see grade_start_set); a lab_break
  # confirming its own fault has no grade.start and counts from the epoch.
  out="$(WORKER_LOG_SINCE="$(marked grade.start)" worker_log "$lines")"; rc=$?
  if [ "$rc" -ne 0 ]; then echo "unreadable"; return 1; fi
  if [ -z "$out" ]; then
    # Empty since the epoch. Confirm the log is readable at all before calling
    # it zero, so a broken fetch is still reported as unreadable.
    if [ -z "$(kubectl -n "$NS" logs "$pod" --tail=1 2>/dev/null)" ]; then
      echo "unreadable"; return 1
    fi
    echo "0"; return 0
  fi
  printf '%s\n' "$out" | grep -ciE "$pat" || true
}

# ---------- metrics ----------

metric() {
  local pat="$1" pod
  pod="$(connect_pod)"
  [ -n "$pod" ] || { echo ""; return; }
  kq exec "$pod" -- curl -s --max-time 10 localhost:9404/metrics 2>/dev/null | grep -E "$pat" || true
}

metric_sum() {
  metric "^$1" | awk '{s+=$NF} END {printf "%d", s+0}'
}

# ---------- waiting ----------

# spin_until <seconds> <description> <command...>
spin_until() {
  local timeout="$1" desc="$2"; shift 2
  local n=0
  printf '  %s...%s %s' "$C_DIM" "$C_0" "$desc"
  while [ "$n" -lt "$timeout" ]; do
    if "$@" >/dev/null 2>&1; then printf ' %sok%s\n' "$C_GRN" "$C_0"; return 0; fi
    sleep 2; n=$((n+2)); printf '.'
  done
  printf ' %stimeout%s\n' "$C_YEL" "$C_0"
  return 1
}

wait_connector_tasks() {
  local want="${1:-1}" timeout="${2:-120}" n=0
  while [ "$n" -lt "$timeout" ]; do
    [ "$(conn_tasks_total)" -ge "$want" ] 2>/dev/null && return 0
    sleep 2; n=$((n+2))
  done
  return 1
}

# ---------- SCRAM credentials (labs 13-15) ----------

# Read a KafkaUser's generated password. kubectl jsonpath mangles dotted keys
# in a shell, so decode via python instead of `-o jsonpath=...| base64 -d`.
secret_key() {   # $1=secret $2=key
  kq get secret "$1" -o json 2>/dev/null \
    | py_json "import base64; print(base64.b64decode(d['data']['$2']).decode())"
}
scram_password()   { secret_key "$1" password; }
target_ca_password() { secret_key kafka-target-cluster-ca-cert 'ca.password'; }

# Labs 13-15 build their fault out of a password. If the KafkaUser secret is
# missing, the password reads as empty, the lab injects password="" and grades
# an entirely different failure than the one it claims to teach — and no fix
# can pass it. Refuse to inject rather than run an unwinnable lab.
#
# This is exactly how labs 13/14 silently became unsolvable: users.yaml was not
# applied by setup, so the secrets never existed.
require_scram_users() {
  local u missing=""
  for u in "$@"; do
    [ -n "$(scram_password "$u")" ] || missing="$missing $u"
  done
  if [ -n "$(target_ca_password)" ]; then :; else missing="$missing kafka-target-cluster-ca-cert"; fi

  if [ -n "$missing" ]; then
    err "Cannot inject this lab: missing credentials for$missing"
    say "  The security labs need the KafkaUser secrets created from manifests/users.yaml."
    say "  Create them with:"
    say "    ${C_B}kubectl -n $NS apply -f $LAB_HOME/manifests/users.yaml${C_0}"
    say "  then wait for the user operator to write the secrets:"
    say "    ${C_B}kubectl -n $NS get secret kcd-replicator kcd-readonly${C_0}"
    say "  Or just re-run ${C_B}mirage repair${C_0}, which now reapplies them."
    return 1
  fi
  return 0
}

ensure_scram_users() {
  if require_scram_users kcd-replicator kcd-readonly >/dev/null 2>&1; then return 0; fi
  kubectl apply -f "$LAB_HOME/manifests/users.yaml" >/dev/null 2>&1
  local n=0
  while [ "$n" -lt 180 ]; do
    require_scram_users kcd-replicator kcd-readonly >/dev/null 2>&1 && return 0
    sleep 5; n=$((n+5))
  done
  return 1
}

# ---------- state tracking ----------

set_active()   { printf '%s' "$1" > "$STATE_DIR/active"; }
get_active()   { cat "$STATE_DIR/active" 2>/dev/null || echo ""; }
clear_active() { rm -f "$STATE_DIR/active"; }

mark()      { printf '%s' "${2:-1}" > "$STATE_DIR/$1"; }
marked()    { cat "$STATE_DIR/$1" 2>/dev/null || echo ""; }
unmark()    { rm -f "$STATE_DIR/$1"; }

record_pass() { printf '%s' "pass" > "$STATE_DIR/lab$1.result"; }
record_fail() { printf '%s' "fail" > "$STATE_DIR/lab$1.result"; }
lab_result()  { cat "$STATE_DIR/lab$1.result" 2>/dev/null || echo "-"; }

# ---------- the Strimzi cluster operator (lab 19) ----------

OPERATOR_DEPLOY=strimzi-cluster-operator

operator_ready() {
  local r
  r="$(kq get deploy "$OPERATOR_DEPLOY" -o jsonpath='{.status.readyReplicas}')"
  [ "${r:-0}" -ge 1 ] 2>/dev/null
}

# Summed across every operator pod, so a rollout that replaces the pod does not
# read as "restarts went back to zero".
operator_restarts() {
  kq get pods -l "name=$OPERATOR_DEPLOY" -o json 2>/dev/null \
    | py_json 'print(sum(cs.get("restartCount",0) for p in d.get("items",[]) for cs in p.get("status",{}).get("containerStatuses",[])))'
}

# Put the operator back to its vendored Deployment. Must run before anything
# that needs reconciling: with the operator dead, restore_baseline's applies are
# accepted by the API server and then never acted on, and a reset reports a
# baseline that does not exist.
#
# `kubectl apply` alone is not enough. Lab 19 injects JAVA_OPTS with `set env`,
# and a field added out-of-band is not in last-applied-configuration, so a
# three-way merge leaves it in place. Remove it explicitly; the apply then
# restores resources if a trainee changed them. Both are no-ops on a healthy
# operator, so this costs nothing when nothing is wrong.
restore_operator() {
  local manifest
  manifest="$LAB_HOME/vendor/strimzi-${STRIMZI_VERSION:-0.47.0}/060-Deployment-strimzi-cluster-operator.yaml"
  if [ -n "$(kq get deploy "$OPERATOR_DEPLOY" \
        -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].name}')" ]; then
    k set env deploy/"$OPERATOR_DEPLOY" JAVA_OPTS- >/dev/null 2>&1
  fi
  [ -f "$manifest" ] && kubectl apply -n "$NS" -f "$manifest" >/dev/null 2>&1
  k rollout status deploy/"$OPERATOR_DEPLOY" --timeout=180s >/dev/null 2>&1
  operator_ready
}

# The connector's state as the WORKER reports it. While the operator is down the
# CR's status block is frozen at the last reconcile, so it cannot answer this.
runtime_connector_state() {
  local pod
  pod="$(connect_pod)"
  [ -z "$pod" ] && { echo ""; return; }
  kq exec "$pod" -- curl -s --max-time 15 \
    "localhost:8083/connectors/$CONNECTOR/status" 2>/dev/null \
    | py_json "print(d.get('connector',{}).get('state',''))"
}

# Restore the pristine connector + connect CR from the manifests.
restore_baseline() {
  restore_operator
  kq delete kafkaconnector --all --wait=true >/dev/null 2>&1
  kubectl apply -f "$LAB_HOME/manifests/connect.yaml"  >/dev/null 2>&1
  kubectl apply -f "$LAB_HOME/manifests/topic.yaml"    >/dev/null 2>&1
  # The security labs read passwords out of these Secrets. Reapply every reset:
  # a lab that cannot read a password injects password="" and becomes unwinnable.
  kubectl apply -f "$LAB_HOME/manifests/users.yaml"    >/dev/null 2>&1
  kubectl apply -f "$LAB_HOME/manifests/connector.yaml" >/dev/null 2>&1
  clear_target_topic_overrides
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=300s >/dev/null 2>&1
}

# A same-cluster identity mirror (the loop in labs 7 and 12) re-reads every
# record it writes. Unbounded, lab 12's filled the 98G kind disk in ~8 minutes
# (8.5G per target lab-orders partition) and crashed BOTH brokers with
# "No space left on device" — they share the node disk — taking Connect down
# after them. A broker-side producer quota on the loop's client.id (set by the
# loop connector's producer.override.client.id, shared by all its tasks) caps
# it at ~0.5MB/s: still an obvious loop, never a dead cluster.
# Keyed to that one client.id, so it touches nothing else; left in place across
# resets on purpose (harmless, and re-applied idempotently).
LOOP_CLIENT_ID=mm2-target-target2
throttle_loop_client() {
  local tp
  tp="$(tgt_pod)"; [ -z "$tp" ] && return 1
  _kafka_in "$tp" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT --alter \
      --entity-type clients --entity-name $LOOP_CLIENT_ID \
      --add-config producer_byte_rate=524288" >/dev/null
}

# Drop every dynamic config override on the TARGET copy of the topic. Labs 16
# and 18 inject theirs there (max.message.bytes, message.timestamp.before.max.ms)
# and trainees fix by adding more; only `reset --fresh` used to clear them, by
# recreating the topic. A leftover 64KB limit failed all four tasks of the next
# lab with RecordTooLargeException and nothing pointing back at lab 16.
#
# Clearing ALL overrides, not a list, is safe: the mirror runs with
# sync.topic.configs.enabled=false, so nothing legitimate ever sets one here.
# The source copy is left alone — its KafkaTopic CR owns it.
clear_target_topic_overrides() {
  local tp keys
  tp="$(tgt_pod)"
  [ -z "$tp" ] && return 0
  # Dynamic-only --describe prints "  key=value sensitive=... synonyms=..." per
  # override under a "Dynamic configs for topic" header.
  keys="$(_kafka_in "$tp" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
      --entity-type topics --entity-name $TOPIC --describe" \
    | awk '/^ +[a-z.]+=/ { sub(/^ +/, ""); split($1, kv, "="); printf "%s%s", sep, kv[1]; sep="," }')"
  [ -z "$keys" ] && return 0
  _kafka_in "$tp" "bin/kafka-configs.sh --bootstrap-server $TGT_BOOT \
      --entity-type topics --entity-name $TOPIC --alter --delete-config $keys" >/dev/null 2>&1
}

# Recreate the source topic to drop accumulated records. Labs produce on every
# start and stop, so the topic grows without bound across a session; that is
# what made absolute-count assertions meaningless (and is why checks measure
# deltas). Truncating keeps counts readable and speeds up drain checks.
truncate_topic() {
  local pod tp n=0 parts
  pod="$(src_pod)"
  [ -z "$pod" ] && return 1

  kq delete kafkaconnector --all --wait=true >/dev/null 2>&1
  kq delete kafkatopic "$TOPIC" --wait=true >/dev/null 2>&1

  # Delete on the broker too, then WAIT for it to actually disappear. Re-applying
  # the KafkaTopic while the old one still exists gets a 1-partition topic
  # adopted instead of the 12-partition one the labs need.
  _kafka_in "$pod" "bin/kafka-topics.sh --bootstrap-server $SRC_BOOT --delete --topic $TOPIC" >/dev/null 2>&1
  tp="$(tgt_pod)"
  [ -n "$tp" ] && _kafka_in "$tp" "bin/kafka-topics.sh --bootstrap-server $TGT_BOOT --delete --topic $TOPIC" >/dev/null 2>&1

  while [ "$n" -lt 60 ]; do
    topic_exists "$pod" "$SRC_BOOT" "$TOPIC" || break
    sleep 3; n=$((n+3))
  done

  kubectl apply -f "$LAB_HOME/manifests/topic.yaml" >/dev/null 2>&1

  # Confirm the operator created it at the right width before returning.
  n=0
  while [ "$n" -lt 90 ]; do
    parts="$(topic_partitions "$pod" "$SRC_BOOT" "$TOPIC")"
    [ "${parts:-0}" -ge 12 ] && return 0
    sleep 5; n=$((n+5))
  done
  warn "topic $TOPIC came back with ${parts:-0} partitions, expected 12"
  return 1
}

# Full rebuild of the Connect cluster. Needed when a lab (or a student deleting
# a worker pod) leaves the StrimziPodSet gone, or when the internal topics get
# recreated underneath a running worker and its view of task state corrupts.
#
# Deleting the internal topics matters: if a broker auto-creates them WITHOUT
# cleanup.policy=compact, Connect shuts down cleanly on startup and crashloops
# with no obvious error. Letting Connect create them itself gets the policy right.
recreate_connect_cluster() {
  # The rebuild below waits for the operator to make Connect Ready again.
  restore_operator
  kq delete kafkaconnector --all --wait=true >/dev/null 2>&1
  kq delete kafkaconnect "$CONNECT" --wait=true >/dev/null 2>&1
  kq delete pod -l "strimzi.io/cluster=$CONNECT" --force --grace-period=0 >/dev/null 2>&1
  sleep 8

  local pod t n
  pod="$(tgt_pod)"
  if [ -n "$pod" ]; then
    for t in mirage-connect-configs mirage-connect-status mirage-connect-offsets; do
      _kafka_in "$pod" "bin/kafka-topics.sh --bootstrap-server $TGT_BOOT --delete --topic $t" >/dev/null 2>&1
    done
    # Wait for the deletes to land. Re-applying while they still exist lets a
    # stale topic be adopted — notably `status` at 1 partition instead of 5,
    # which makes the worker spin forever on "unknown topic or partition" for
    # partitions 1-4 while reporting its tasks as RUNNING.
    n=0
    while [ "$n" -lt 60 ]; do
      topic_exists "$pod" "$TGT_BOOT" mirage-connect-status || break
      sleep 3; n=$((n+3))
    done
  fi
  sleep 5

  kubectl apply -f "$LAB_HOME/manifests/connect.yaml" >/dev/null 2>&1
  kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=420s >/dev/null 2>&1

  # Verify Connect rebuilt `status` at the width its config asks for. If a
  # broker auto-created it narrower, the worker is silently broken.
  pod="$(tgt_pod)"
  if [ -n "$pod" ]; then
    local parts
    parts="$(topic_partitions "$pod" "$TGT_BOOT" mirage-connect-status)"
    if [ "${parts:-0}" -lt 5 ]; then
      warn "mirage-connect-status came back with ${parts:-0} partitions (expected 5); retrying once"
      kq delete kafkaconnect "$CONNECT" --wait=true >/dev/null 2>&1
      _kafka_in "$pod" "bin/kafka-topics.sh --bootstrap-server $TGT_BOOT --delete --topic mirage-connect-status" >/dev/null 2>&1
      sleep 15
      kubectl apply -f "$LAB_HOME/manifests/connect.yaml" >/dev/null 2>&1
      kq wait kafkaconnect/"$CONNECT" --for=condition=Ready --timeout=420s >/dev/null 2>&1
    fi
  fi
  return 0
}

# Wait until the operator has finished reconciling the connector, so a lab's
# injection patch cannot be lost to DELETED/ADDED churn from restore_baseline.
settle_connector() {
  local timeout="${1:-150}" n=0 step
  # Poll sub-second at first, then back off. The probes are free (conn_ready and
  # conn_tasks_total are local kubectl reads, measured at 0s), so a flat 5s
  # granularity just rounded every wait UP to the next 5s boundary — a connector
  # ready at t=1s was not seen until t=5s. Across 17 labs, each calling this
  # two or three times, that rounding was most of the idle time in a full run.
  while [ "$n" -lt "$timeout" ]; do
    if [ "$(conn_ready)" = "True" ] && [ "$(conn_tasks_total)" -ge 1 ] 2>/dev/null; then
      # Let the status subresource stop moving. 2s is enough: the condition
      # above already requires Ready AND at least one task, so the status has
      # been written once; this only guards a second write landing mid-read.
      sleep 2
      return 0
    fi
    # 0.5s for the first 10s, then 2s, then 5s. Catches the common fast case
    # immediately without hammering the API server through a long outage.
    if   [ "$n" -lt 10 ]; then step=1
    elif [ "$n" -lt 40 ]; then step=2
    else                       step=5
    fi
    sleep "$step"; n=$((n+step))
  done
  return 1
}

# Apply a patch and confirm it actually stuck. Guards against the operator
# replacing the CR mid-flight, which silently discards the injection.
# Usage: patch_connector_verified <merge-json> <jsonpath-after-.spec> <expected>
patch_connector_verified() {
  local patch="$1" path="$2" expect="$3" attempt=0 got
  while [ "$attempt" -lt 4 ]; do
    k patch kafkaconnector "$CONNECTOR" --type merge -p "$patch" >/dev/null 2>&1
    sleep 4
    got="$(conn_get "$path")"
    [ "$got" = "$expect" ] && return 0
    attempt=$((attempt+1))
    sleep 6
  done
  warn "injection did not stick after $attempt attempts (wanted '$expect', got '${got:-empty}')"
  return 1
}

# Read one key from the connector's RUNTIME config via the Connect REST API.
# The CR is desired state; this is what the worker is actually running. They
# diverge whenever the operator cannot reach Connect (e.g. during a rebalance,
# when Connect answers "500 ... a rebalance is expected").
#
# This deliberately execs into the pod rather than going through the ingress
# (see api_curl below). Every lab_check depends on this function, so it must
# keep working when the ingress controller or the port-forward is down. The
# ingress is an operator convenience; it is NOT in the grading path.
runtime_config_value() {
  local key="$1" pod
  pod="$(connect_pod)"
  [ -z "$pod" ] && { echo ""; return; }
  kq exec "$pod" -- curl -s --max-time 15 \
    "localhost:8083/connectors/$CONNECTOR/config" 2>/dev/null \
    | py_json "print(d.get('$key',''))"
}

# ---------- Connect REST API over the ingress ----------
#
# Host-routed like production: the Ingress matches on Host, and we supply that
# host in a header rather than resolving it. See manifests/ingress.yaml.
#
# Nothing in a lab_check may depend on these. The assertion engine reaches
# Connect by exec'ing into the worker pod (runtime_config_value above) so that
# grading survives a dead controller or a dropped port-forward.
API_HOST=mirage.local
API_PORT=8080
API_NS=ingress-nginx
API_SVC=ingress-nginx-controller

api_pid_file() { printf '%s/api-pf.pid' "$STATE_DIR"; }
api_log_file() { printf '%s/api-pf.log' "$STATE_DIR"; }

# The curl line a human should use, printed by several commands.
api_curl_example() { printf "curl -H 'Host: %s' localhost:%s/connectors" "$API_HOST" "$API_PORT"; }

# GET a path through the ingress. $1 = path, with leading slash.
api_curl() {
  curl -s --max-time 15 -H "Host: $API_HOST" "http://localhost:$API_PORT${1:-/}" 2>/dev/null
}

# Is the API reachable end to end (forward alive AND nginx routing to Connect)?
#
# Any HTTP status is proof the forward and the controller are both up; a 404
# from nginx still means the hop worked. Only a connection failure, which curl
# reports as status 000, means the path is down.
api_up() {
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
            -H "Host: $API_HOST" "http://localhost:$API_PORT/connectors" 2>/dev/null)"
  [ -n "$code" ] && [ "$code" != "000" ]
}

# Same question, asked fast, for use inside a poll loop.
#
# api_up's --max-time 5 is right for a one-shot check but wrong for polling:
# every failed probe blocks the full 5s, so a loop that looks like it waits 30s
# actually waits minutes, with no output. That is what made a failed start read
# as a hang. A forward on localhost either accepts immediately or is not there.
api_up_fast() {
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' \
            --connect-timeout 1 --max-time 2 \
            -H "Host: $API_HOST" "http://localhost:$API_PORT/connectors" 2>/dev/null)"
  [ -n "$code" ] && [ "$code" != "000" ]
}

# Did we start a forward that is still a live process?
#
# This is NOT redundant with api_up, and it is not a substitute for it either.
# api_up can fail while the process lives (nginx not ready, Connect Service
# recreated under it), and the process can be gone while a stale PID file
# remains. Both answers are needed to decide between "start one" and "restart
# the one that wedged".
#
# `kill -0` rather than pgrep: pgrep cannot read the process list in some
# sandboxes (it exits 3 with "Cannot get process list"), which makes a pgrep
# guard fall through and report every forward as dead.
api_pf_alive() {
  local f pid; f="$(api_pid_file)"
  [ -f "$f" ] || return 1
  pid="$(tr -d '[:space:]' <"$f")"
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null
}

api_stop() {
  local f pid; f="$(api_pid_file)"
  if [ -f "$f" ]; then
    pid="$(tr -d '[:space:]' <"$f")"
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
    rm -f "$f"
  fi
  return 0
}

# Who is holding API_PORT, whether or not we started it?
#
# The pidfile is not enough. `teardown` does `rm -rf "$STATE_DIR"`, so a
# port-forward started against a previous cluster outlives the only record of
# its PID — it keeps listening on API_PORT, pointed at a namespace that no
# longer exists, and the next api_start dies with
#   "unable to listen on any of the requested ports: [{8080 80}]"
# while api_up fails too, because the forward routes nowhere. That looks like a
# broken ingress and is really an orphan from the cluster before.
api_port_holder() {
  lsof -nP -iTCP:"$API_PORT" -sTCP:LISTEN -t 2>/dev/null | head -1
}

# Kill whatever holds API_PORT, pidfile or not. Used only when the port is
# occupied AND unreachable, so a healthy forward someone else started is never
# taken away.
#
# Poll in 200ms steps, not 1s: a killed port-forward releases its listener
# almost immediately, and a 1s granularity turned a sub-second operation into a
# multi-second silence that read as a hang.
api_stop_orphan() {
  local pid; pid="$(api_port_holder)"
  [ -n "$pid" ] || return 1
  kill "$pid" 2>/dev/null || return 1
  local n=0
  while [ "$n" -lt 25 ]; do
    [ -z "$(api_port_holder)" ] && return 0
    sleep 0.2; n=$((n+1))
  done
  # Still there: escalate once, then report honestly.
  kill -9 "$pid" 2>/dev/null
  local m=0
  while [ "$m" -lt 10 ]; do
    [ -z "$(api_port_holder)" ] && return 0
    sleep 0.2; m=$((m+1))
  done
  return 1
}

# (Re)establish the port-forward to the ingress controller.
# Idempotent: a working path is left alone.
api_start() {
  api_up && return 0
  # A live-but-useless process would make kubectl fail to bind the port, so
  # clear it before trying again.
  api_stop

  # The port can still be held by a forward we have no pidfile for — typically
  # one left over from a cluster that teardown removed along with .state. The
  # port is occupied and api_up already said it routes nowhere, so taking it is
  # safe here in a way it would not be if the path were working.
  if [ -n "$(api_port_holder)" ]; then
    warn "port $API_PORT is held by a stale port-forward; reclaiming it"
    api_stop_orphan || {
      err "could not free port $API_PORT (held by PID $(api_port_holder))"
      return 1; }
  fi

  kubectl -n "$API_NS" port-forward "svc/$API_SVC" "$API_PORT:80" \
    >"$(api_log_file)" 2>&1 &
  printf '%s\n' "$!" >"$(api_pid_file)"

  # The HTTP probe is the only authoritative signal here, and the PID check is
  # advisory. A `kubectl port-forward` backgrounded from a subshell is
  # reparented when that subshell exits, and can then sit unreaped while still
  # serving -- `kill -0` reports it dead and `curl` gets a 200 from it. So a
  # failed api_pf_alive must not end the wait on its own: give the process a
  # couple of consecutive "dead" readings (it really is gone if the port was
  # taken or the Service does not exist) and let api_up decide the outcome.
  #
  # The first probe happens immediately and the loop is sub-second-grained at
  # the start, because a healthy forward answers in well under a second. Say so
  # once it has been slow for a moment: an unexplained multi-second pause here
  # reads as a hang, and gets interrupted.
  local n=0 dead=0 announced=0
  while [ "$n" -lt 60 ]; do
    api_up_fast && { [ "$announced" -eq 1 ] && say "  ${C_GRN}ok${C_0}      REST API reachable"; return 0; }
    if api_pf_alive; then
      dead=0
    else
      dead=$((dead+1))
      [ "$dead" -ge 6 ] && break
    fi
    if [ "$n" -eq 10 ] && [ "$announced" -eq 0 ]; then
      announced=1
      info "waiting for the REST API port-forward to come up"
    fi
    sleep 0.5; n=$((n+1))
  done
  api_up
}

# ---------- kafka port-forward (for the host's own CLI tools) ----------
#
# Lets you run the real Kafka CLI from your own terminal:
#   kafka-topics --bootstrap-server localhost:9092 --describe --topic lab-orders
#
# Why this is NOT just `port-forward svc/..-kafka-bootstrap 9092:9092`:
# a Kafka client bootstraps once, then reconnects to whatever the broker
# ADVERTISES. Strimzi advertises internal cluster DNS:
#   kafka-source-source-pool-0.kafka-source-kafka-brokers.mirage.svc:9092
# which the host cannot resolve, so the bootstrap succeeds and every call after
# it dies with UnknownHostException. Verified: a bootstrap-Service forward on
# 19092 described nothing.
#
# So two things are required, and both matter:
#   1. forward the BROKER POD on the SAME local port it advertises (9092), not
#      an arbitrary one — the client reconnects to port 9092 by name.
#   2. an /etc/hosts entry mapping that advertised name to 127.0.0.1.
#
# Only one cluster can own localhost:9092, because both advertise 9092 and a
# second loopback address (127.0.0.2) does not exist on macOS without
# `sudo ifconfig lo0 alias`. So this forwards ONE cluster at a time, source by
# default, and `mirage kafka target` switches.
KAFKA_PORT=9092

kafka_pf_pid_file()  { printf '%s/kafka-pf.pid' "$STATE_DIR"; }
kafka_pf_log_file()  { printf '%s/kafka-pf.log' "$STATE_DIR"; }
kafka_pf_side_file() { printf '%s/kafka-pf.side' "$STATE_DIR"; }

kafka_pf_side() { cat "$(kafka_pf_side_file)" 2>/dev/null || echo ""; }

# The advertised hostname a client will be redirected to, per side.
kafka_advertised_host() {
  case "${1:-source}" in
    target) printf 'kafka-target-target-pool-0.kafka-target-kafka-brokers.%s.svc' "$NS" ;;
    *)      printf 'kafka-source-source-pool-0.kafka-source-kafka-brokers.%s.svc' "$NS" ;;
  esac
}

kafka_broker_pod_name() {
  case "${1:-source}" in
    target) printf 'kafka-target-target-pool-0' ;;
    *)      printf 'kafka-source-source-pool-0' ;;
  esac
}

# Is the advertised name resolvable to loopback on this host? Without it the
# forward appears to work and then fails on the first real request, which is
# the confusing failure this whole block exists to prevent.
# Matches "127.0.0.1 <host>" with the host anywhere in the alias list. Written
# as two space-delimited field tests rather than one regex: an earlier version
# used `127\.0\.0\.1[[:space:]]+.*([[:space:]])$host` and never matched the
# normal single-space form, because the `.*` demanded a SECOND separator before
# the hostname. The dots in the hostname are also regex metacharacters, so
# compare fields literally instead.
kafka_hosts_entry_ok() {
  local host; host="$(kafka_advertised_host "${1:-source}")"
  awk -v h="$host" '
    /^[[:space:]]*#/ { next }
    $1 == "127.0.0.1" { for (i = 2; i <= NF; i++) if ($i == h) { found = 1; exit } }
    END { exit(found ? 0 : 1) }
  ' /etc/hosts 2>/dev/null
}

kafka_port_holder() {
  lsof -nP -iTCP:"$KAFKA_PORT" -sTCP:LISTEN -t 2>/dev/null | head -1
}

kafka_pf_stop() {
  local f pid; f="$(kafka_pf_pid_file)"
  if [ -f "$f" ]; then
    pid="$(tr -d '[:space:]' <"$f")"
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
    rm -f "$f"
  fi
  rm -f "$(kafka_pf_side_file)" "$(kafka_pf_pod_uid_file)"
  return 0
}

# Reclaim the port from a forward we have no pidfile for — the same orphan
# problem api_stop_orphan solves: teardown removes .state, so a forward against
# a dead cluster keeps the port with no record of its PID.
kafka_pf_stop_orphan() {
  local pid; pid="$(kafka_port_holder)"
  [ -n "$pid" ] || return 1
  kill "$pid" 2>/dev/null || return 1
  local n=0
  while [ "$n" -lt 25 ]; do
    [ -z "$(kafka_port_holder)" ] && return 0
    sleep 0.2; n=$((n+1))
  done
  kill -9 "$pid" 2>/dev/null
  local m=0
  while [ "$m" -lt 10 ]; do
    [ -z "$(kafka_port_holder)" ] && return 0
    sleep 0.2; m=$((m+1))
  done
  return 1
}

# Is the forward actually usable? A TCP accept is not enough — it proves the
# forward binds, not that a client can follow the metadata redirect. Only an
# ApiVersions exchange does, so use the broker's own CLI through the forward.
kafka_pf_up() {
  local side="${1:-$(kafka_pf_side)}"
  [ -n "$side" ] || return 1
  # Reachability, not just occupancy: a forward whose pod was replaced can keep
  # the pidfile while refusing every connection. See kafka_pf_connects.
  kafka_pf_connects || return 1
  kafka_hosts_entry_ok "$side" || return 1
  return 0
}

# (Re)establish the Kafka port-forward. Idempotent for the same side.
# Usage: kafka_pf_start [source|target]
kafka_pf_pod_uid_file() { printf '%s/kafka-pf.uid' "$STATE_DIR"; }

# Does a TCP connection to the forwarded port actually succeed?
#
# "Something holds the port" is NOT enough. `kubectl port-forward` targets ONE
# pod and dies with it, logging
#   failed to find sandbox "<id>" in store: not found
#   error: lost connection to pod
# which is what happened after a lab replaced the source broker: the pidfile
# still named PID 74495 from the previous day and `kafka-topics --list` got
# "Connection to node -1 (localhost/127.0.0.1:9092) could not be established"
# for every retry. Several labs replace broker pods, so this recurs.
kafka_pf_connects() {
  # bash's /dev/tcp is the cheapest reachability probe available here; nc is not
  # guaranteed present. A refused connection returns non-zero immediately.
  (exec 3<>/dev/tcp/127.0.0.1/"$KAFKA_PORT") >/dev/null 2>&1
}

# A cluster's immutable Kafka cluster.id, read from inside the broker pod.
# Topic lists are NOT usable for this: labs create and delete topics, so any
# "does it have X" test is lab-dependent and wrong half the time.
kafka_cluster_id() {        # $1 = source|target
  local pod boot
  if [ "${1:-source}" = "target" ]; then pod="$(tgt_pod)"; boot="$TGT_BOOT"
  else                                   pod="$(src_pod)"; boot="$SRC_BOOT"
  fi
  [ -n "$pod" ] || return 1
  kq exec "$pod" -c kafka -- bin/kafka-cluster.sh cluster-id \
      --bootstrap-server "$boot" 2>/dev/null \
    | sed -n 's/^Cluster ID: *//p' | tr -d '[:space:]'
}

# The cluster.id reachable through the LOCAL forward, as a client sees it.
kafka_pf_cluster_id() {
  kafka-cluster cluster-id --bootstrap-server "127.0.0.1:$KAFKA_PORT" 2>/dev/null \
    | sed -n 's/^Cluster ID: *//p' | tr -d '[:space:]'
}

# Does the forward on KAFKA_PORT actually serve the cluster we think it does?
#
# This exists because "the port accepts a connection" proved insufficient in a
# damaging way: with the source forwarded, `mirage kafka target` adopted that
# forward, recorded side=target, and reported "ready -> target broker" while
# every query returned SOURCE data. Comparing cluster ids is the only check
# that cannot be fooled by whichever process happens to hold the port.
#
# Returns 0 only on a positive match. If the local kafka-cluster CLI is absent
# the comparison cannot be made, so it returns non-zero (force a rebuild rather
# than adopt something unverified).
kafka_pf_serves_side() {    # $1 = source|target
  local want got
  command -v kafka-cluster >/dev/null 2>&1 || return 1
  want="$(kafka_cluster_id "$1")"
  [ -n "$want" ] || return 1
  got="$(kafka_pf_cluster_id)"
  [ -n "$got" ] || return 1
  [ "$want" = "$got" ]
}

kafka_pf_start() {
  local side="${1:-source}" pod host want_uid have_uid
  pod="$(kafka_broker_pod_name "$side")"
  host="$(kafka_advertised_host "$side")"

  # The UID of the pod we are forwarding to, so a replaced broker is detected.
  # Comparing pod NAMES is useless: the broker is a StrimziPodSet member and
  # always comes back as kafka-<cluster>-<pool>-0.
  want_uid="$(kq get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null)"
  have_uid="$(cat "$(kafka_pf_pod_uid_file)" 2>/dev/null)"

  # Reuse an existing forward ONLY when it demonstrably serves this side.
  #
  # "The port accepts a connection" is not sufficient and adopting on that
  # basis was a real bug: with the source forwarded, `mirage kafka target`
  # adopted the SOURCE forward, wrote side=target plus the target's uid, and
  # printed "ready -> target broker" while every query returned source data.
  # Confidently wrong is worse than refusing.
  #
  # So identity is proved against the forward itself, by asking the cluster on
  # the other end which cluster it is (kafka_pf_serves_side). The recorded
  # side/uid are only hints; the live answer decides.
  if kafka_pf_connects && kafka_pf_serves_side "$side"; then
    # Adopt: re-record whatever is missing or stale, so a later pod replacement
    # is still detected by uid.
    printf '%s' "$side" >"$(kafka_pf_side_file)"
    [ -n "$want_uid" ] && printf '%s' "$want_uid" >"$(kafka_pf_pod_uid_file)"
    return 0
  fi

  kafka_pf_stop
  if [ -n "$(kafka_port_holder)" ]; then
    kafka_pf_stop_orphan || return 1
  fi

  [ -n "$want_uid" ] || return 1     # pod does not exist

  kubectl -n "$NS" port-forward "pod/$pod" "$KAFKA_PORT:$KAFKA_PORT" \
    >"$(kafka_pf_log_file)" 2>&1 &
  printf '%s\n' "$!" >"$(kafka_pf_pid_file)"
  printf '%s' "$side" >"$(kafka_pf_side_file)"
  printf '%s' "$want_uid" >"$(kafka_pf_pod_uid_file)"

  # Wait until the port both binds AND accepts. Sub-second steps: a local
  # forward binds fast, and a 1s granularity turns this into a silence that
  # reads as a hang.
  local n=0
  while [ "$n" -lt 40 ]; do
    kafka_pf_connects && return 0
    sleep 0.25; n=$((n+1))
  done
  return 1
}

# Wait for the ingress controller Deployment to be ready. Applying the Ingress
# before the IngressClass and controller exist leaves it unrouted.
api_wait_controller() {
  local timeout="${1:-180}" n=0
  while [ "$n" -lt "$timeout" ]; do
    kubectl -n "$API_NS" get deploy "$API_SVC" \
      -o jsonpath='{.status.readyReplicas}' 2>/dev/null | grep -q '^[1-9]' && return 0
    sleep 3; n=$((n+3))
  done
  return 1
}

# Patch the connector and wait until the RUNTIME reflects it, not just the CR.
# Use this when a lab must leave the worker genuinely broken.
# Usage: patch_connector_runtime <merge-json> <runtime-key> <expected> [timeout]
patch_connector_runtime() {
  local patch="$1" key="$2" expect="$3" timeout="${4:-150}" n=0 got
  k patch kafkaconnector "$CONNECTOR" --type merge -p "$patch" >/dev/null 2>&1
  k annotate kafkaconnector "$CONNECTOR" strimzi.io/restart=true --overwrite >/dev/null 2>&1
  while [ "$n" -lt "$timeout" ]; do
    got="$(runtime_config_value "$key")"
    [ "$got" = "$expect" ] && return 0
    sleep 6; n=$((n+6))
  done
  warn "runtime config did not pick up '$key=$expect' (currently '${got:-empty}')"
  return 1
}

# Remove topics a replication-loop lab created on the target, so the next lab
# starts from a clean target cluster.
#
# IMPORTANT: this must NEVER delete this Connect cluster's own internal topics
# (mirage-connect-{configs,offsets,status}) — deleting those orphans the worker
# and it will spin forever on UnknownTopicOrPartition. Lab 7/12 pollute the
# target with REPLICATED copies, which on this single-cluster lab setup carry
# the same names, so we only remove unambiguous replication artefacts.
clean_replicated_junk() {
  local pod
  pod="$(tgt_pod)"
  [ -z "$pod" ] && return 0
  _kafka_in "$pod" '
    for t in $(bin/kafka-topics.sh --bootstrap-server '"$TGT_BOOT"' --list 2>/dev/null \
                 | grep -E "\.heartbeats$|\.replica$|^mm2-|\.internal$"); do
      bin/kafka-topics.sh --bootstrap-server '"$TGT_BOOT"' --delete --topic "$t" 2>/dev/null
    done' >/dev/null 2>&1
  return 0
}
