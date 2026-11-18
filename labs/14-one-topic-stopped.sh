#!/usr/bin/env bash
# Lab 14 — Connector authorization failure (missing ACL)

LAB_TITLE="One topic stopped replicating"
LAB_DIFFICULTY="medium"
LAB_ALERT="Missing running tasks (<1); 'Not authorized to access topics' in logs"

lab_brief() {
cat <<'EOF'
SYMPTOM
  A credential rotation swapped the connector onto a different service account.
  Replication has stopped dead.

  Read the worker log and establish exactly which stage is refusing the
  connector before you change anything.

YOUR JOB
  Get replication working again over the secured listener (:9094).

WHEN DONE
  mirage stop 14
EOF
}

TGT_SCRAM=kafka-target-kafka-bootstrap:9094

lab_break() {
  require_scram_users kcd-readonly || return 1
  local ca_pw ro_pw
  ca_pw="$(target_ca_password)"
  ro_pw="$(scram_password kcd-readonly)"

  # Credentials are VALID — this user really exists and the password is right.
  # It simply has no Write ACL, so authentication succeeds and the first produce
  # is refused. That distinction is the lesson.
  patch_connector_runtime "$(cat <<EOF
{"spec":{"config":{
  "target.cluster.bootstrap.servers":"$TGT_SCRAM",
  "producer.override.bootstrap.servers":"$TGT_SCRAM",
  "target.cluster.security.protocol":"SASL_SSL",
  "target.cluster.sasl.mechanism":"SCRAM-SHA-512",
  "target.cluster.sasl.jaas.config":"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"kcd-readonly\\" password=\\"$ro_pw\\";",
  "target.cluster.ssl.truststore.location":"/mnt/target-ca/ca.p12",
  "target.cluster.ssl.truststore.password":"$ca_pw",
  "target.cluster.ssl.truststore.type":"PKCS12",
  "producer.override.security.protocol":"SASL_SSL",
  "producer.override.sasl.mechanism":"SCRAM-SHA-512",
  "producer.override.sasl.jaas.config":"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"kcd-readonly\\" password=\\"$ro_pw\\";",
  "producer.override.ssl.truststore.location":"/mnt/target-ca/ca.p12",
  "producer.override.ssl.truststore.password":"$ca_pw",
  "producer.override.ssl.truststore.type":"PKCS12"
}}}
EOF
)" 'target.cluster.bootstrap.servers' "$TGT_SCRAM" 180

  produce 2000 1000 2000
  sleep 20
}

lab_check() {
  local boot jaas running total authorized=1

  boot="$(conn_get '.spec.config.producer\.override\.bootstrap\.servers')"
  assert_eq "still using the secured listener (:9094)" "$boot" "$TGT_SCRAM" \
    "Dropping back to plaintext avoids the lesson rather than fixing the incident." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config}' | tr ',' '\\n' | grep bootstrap"

  # Accept EITHER fix: swapped identity, or ACLs granted to the one in use.
  jaas="$(conn_get '.spec.config.producer\.override\.sasl\.jaas\.config')"
  if printf '%s' "$jaas" | grep -q 'kcd-replicator'; then
    _record_pass "identity swapped to a write-capable account ${C_DIM}(kcd-replicator)${C_0}"
    authorized=0
  elif printf '%s' "$jaas" | grep -q 'kcd-readonly'; then
    # Still on the read-only user, so it must have been granted Write.
    if kq get kafkauser kcd-readonly -o json 2>/dev/null \
         | py_json 'import sys
acls=d.get("spec",{}).get("authorization",{}).get("acls",[])
ops=[o for a in acls if a.get("resource",{}).get("type")=="topic" for o in a.get("operations",[])]
sys.exit(0 if "Write" in ops else 1)' >/dev/null 2>&1; then
      _record_pass "kcd-readonly granted Write on topics"
      authorized=0
    else
      _record_fail "identity is authorized to write" \
        "The connector is still using kcd-readonly, which has no Write ACL on topics. Either point it at kcd-replicator, or add Write (and Create/Describe) to kcd-readonly's ACLs." \
        "kubectl -n mirage get kafkauser kcd-readonly -o jsonpath='{.spec.authorization.acls}'"
    fi
  else
    _record_fail "a known identity is configured" \
      "The JAAS config names neither kcd-replicator nor kcd-readonly." \
      "kubectl -n mirage get kafkauser"
  fi

  wait_connector_tasks 1 150 || true
  running="$(conn_task_count RUNNING)"
  total="$(conn_tasks_total)"

  assert_ge "tasks exist" "${total:-0}" 1 \
    "An authorization failure takes the tasks down." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  assert "every task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "Tasks refused by the broker do not stay running." \
    "running=${running:-0} of total=${total:-0}"

  local hits
  hits="$(worker_log_matches 'TopicAuthorizationException|Not authorized to access topics')"
  assert_eq "no fresh authorization denials in the worker log" "${hits:-unreadable}" "0" \
    "A recurring TopicAuthorizationException means the identity still lacks permission." \
    'kubectl -n mirage logs <connect-pod> --tail=500 | grep -iE "authoriz|denied"'

  info "Confirming replication over the secured listener"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 150)"
  assert_ge "records replicate with a write-capable identity" "${delta:-0}" 1600 \
    "Being allowed to write is not the goal — writing is. Confirm with throughput." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target
  kubectl -n mirage logs <connect-pod> --tail=200 | grep -iE 'authoriz|denied|not authorized'

  A plain tail can still show errors from an earlier lab or an earlier attempt
  at this one, because the worker pod is not restarted between labs. To see
  only what THIS lab produced:  mirage logs --errors

  Then look at the identity in use and what it is permitted to do:

    kubectl -n mirage get kafkauser
    kubectl -n mirage get kafkauser kcd-readonly -o jsonpath='{.spec.authorization}'
    kubectl -n mirage get kafkauser kcd-replicator -o jsonpath='{.spec.authorization}'

  Read the exception class, not just the word "auth":

    POD=mirage-connect-0
    kubectl -n mirage logs $POD --tail=300 | grep -iE 'authoriz|denied|not authorized'

  Expect:
    org.apache.kafka.common.errors.TopicAuthorizationException:
    Not authorized to access topics: [lab-orders]

  That is AUTHORIZATION, not authentication. The login SUCCEEDED — there is no
  SaslAuthenticationException anywhere in the log. The broker knows exactly who
  this client is and is refusing what it asked to do.

    SaslAuthenticationException  -> wrong credentials      (Lab 13)
    TopicAuthorizationException  -> valid identity, no ACL  (this lab)

  Find which identity is in use:

    kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
      -o jsonpath='{.spec.config.producer\.override\.sasl\.jaas\.config}{"\n"}'

  Then compare what the two available users may do:

    kubectl -n mirage get kafkauser kcd-readonly   -o jsonpath='{.spec.authorization.acls}{"\n"}'
    kubectl -n mirage get kafkauser kcd-replicator -o jsonpath='{.spec.authorization.acls}{"\n"}'

  One has Write on topics. One does not. A mirror connector needs Write (and
  Create, if it must make the target topic) plus Read on consumer groups.

  Two valid fixes — think about which belongs in production:
    a) repoint the connector at the account that already has the rights
    b) grant the rights to the account the connector is using
EOF
}

lab_solve() {
cat <<'EOF'
  # Option A — put the connector back on the write-capable identity
  #   (usually right: the read-only account exists for a reason)
  PW=$(kubectl -n mirage get secret kcd-replicator -o jsonpath='{.data.password}' | base64 -d)
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p "{\"spec\":{\"config\":{
      \"target.cluster.sasl.jaas.config\":\"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"kcd-replicator\\\" password=\\\"$PW\\\";\",
      \"producer.override.sasl.jaas.config\":\"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"kcd-replicator\\\" password=\\\"$PW\\\";\"
    }}}"

  # Option B — grant the rights to the identity in use
  #   (right when the new account is the intended one going forward)
  kubectl -n mirage patch kafkauser kcd-readonly --type merge -p '{"spec":{"authorization":{
    "type":"simple",
    "acls":[
      {"resource":{"type":"topic","name":"*","patternType":"literal"},
       "operations":["Read","Write","Create","Describe","DescribeConfigs"],"host":"*"},
      {"resource":{"type":"group","name":"*","patternType":"literal"},
       "operations":["Read","Describe"],"host":"*"},
      {"resource":{"type":"cluster"},
       "operations":["Describe","DescribeConfigs","Create"],"host":"*"}
    ]}}}'


  # IMPORTANT — check the config reached the WORKER before restarting.
  # The operator pushes config asynchronously. Restart too early and the tasks
  # retry the OLD broken value, burn autoRestart's 5 attempts, and then nothing
  # recovers them. Confirm first:
  POD=mirage-connect-0
  kubectl -n mirage exec "$POD" -- curl -s localhost:8083/connectors/lab-orders-from-source-to-target/config \
    | tr ',' '\n' | grep -E 'sasl.jaas.config'

  # Only once the runtime shows the corrected value:
  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite

  # If tasks are still FAILED afterwards, autoRestart had already given up —
  # annotate again to clear them.

  # Verify with throughput, not status
  mirage counts

WHICH FIX IS CORRECT?
  Option A is usually right during an incident: the read-only account is almost
  certainly read-only on purpose, and widening it to stop a page quietly removes
  a control. Repoint the connector, then ask why the rotation moved it.

  Option B is right when the new account is genuinely the intended one — but it
  is a permissions change, so it belongs in review, not in a 03:00 kubectl.

WHAT A MIRROR CONNECTOR ACTUALLY NEEDS ON THE TARGET
  topic:   Write          (produce replicated records)
           Create         (if it must create the target topic)
           Describe       (metadata)
  group:   Read, Describe (MirrorMaker2's own consumers)
  cluster: Describe       (bootstrap)

  Missing Create is a favourite: replication works for existing topics and fails
  the moment a NEW topic matches the pattern — so the incident arrives hours
  after the ACL change, which makes it hard to correlate.

AT MIRAGEDUEL
  ACLs are not managed per-connector by hand; they come with the service
  account. `sync.topic.acls.enabled: "false"` in the KCD mirror template means
  the connector does NOT copy source ACLs to the target — target permissions are
  managed independently, so a source-side grant does not imply a target-side one.
EOF
}
