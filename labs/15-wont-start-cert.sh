#!/usr/bin/env bash
# Lab 15 — TLS trust failure (bad truststore)

LAB_TITLE="Connector will not start after a cert change"
LAB_DIFFICULTY="medium"
LAB_ALERT="Zero throughput; 'SSLHandshakeException' / 'unable to find valid certification path' in logs"

lab_brief() {
cat <<'EOF'
SYMPTOM
  A CA rotation went through. Since then one connector has replicated nothing.

  Look carefully at WHICH level failed. You may see the connector itself FAILED
  while all its tasks still report RUNNING — and, confusingly, data may keep
  moving for a while.

YOUR JOB
  Get the connector replicating again over the secured listener (:9094).
  Keep using TLS — disabling it is not the fix.

WHEN DONE
  mirage stop 15
EOF
}

TGT_SCRAM=kafka-target-kafka-bootstrap:9094

lab_break() {
  require_scram_users kcd-replicator || return 1
  local pw
  pw="$(scram_password kcd-replicator)"

  # Credentials are correct and the ACLs are correct. Only the truststore
  # password is wrong, so the TLS handshake fails before SASL is even attempted.
  # That ordering is the lesson: a trust failure masquerades as an auth failure.
  local patch
  patch="$(py_build_patch "$pw")"
  patch_connector_runtime "$patch" 'target.cluster.bootstrap.servers' "$TGT_SCRAM" 180

  produce 2000 1000 2000
  sleep 20
}

# Build the injection patch with python so JAAS quoting cannot bite us.
py_build_patch() {
  local pw="$1"
  python3 -I -c "
import json,sys
pw=sys.argv[1]
jaas='org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kcd-replicator\" password=\"%s\";' % pw
cfg={
 'target.cluster.bootstrap.servers':'$TGT_SCRAM',
 'producer.override.bootstrap.servers':'$TGT_SCRAM',
 'target.cluster.security.protocol':'SASL_SSL',
 'target.cluster.sasl.mechanism':'SCRAM-SHA-512',
 'target.cluster.sasl.jaas.config':jaas,
 'target.cluster.ssl.truststore.location':'/mnt/target-ca/ca.p12',
 'target.cluster.ssl.truststore.password':'wrong-truststore-password',
 'target.cluster.ssl.truststore.type':'PKCS12',
 'producer.override.security.protocol':'SASL_SSL',
 'producer.override.sasl.mechanism':'SCRAM-SHA-512',
 'producer.override.sasl.jaas.config':jaas,
 'producer.override.ssl.truststore.location':'/mnt/target-ca/ca.p12',
 'producer.override.ssl.truststore.password':'wrong-truststore-password',
 'producer.override.ssl.truststore.type':'PKCS12',
}
print(json.dumps({'spec':{'config':cfg}}))
" "$pw"
}

lab_check() {
  local boot ts_pw_t ts_pw_p real_pw running total

  boot="$(conn_get '.spec.config.target\.cluster\.bootstrap\.servers')"
  assert_eq "still using the secured listener (:9094)" "$boot" "$TGT_SCRAM" \
    "Turning TLS off would 'fix' the symptom by removing the security control. Not the fix." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config}' | tr ',' '\\n' | grep bootstrap"

  local proto
  proto="$(conn_get '.spec.config.producer\.override\.security\.protocol')"
  assert_eq "still using SASL_SSL (TLS not disabled)" "${proto:-unset}" "SASL_SSL" \
    "Downgrading to SASL_PLAINTEXT or PLAINTEXT sidesteps the exercise." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config.producer\\.override\\.security\\.protocol}'"

  # The truststore password must now match the real one from the CA secret.
  real_pw="$(kq get secret kafka-target-cluster-ca-cert -o json 2>/dev/null \
    | py_json 'import base64; print(base64.b64decode(d["data"]["ca.password"]).decode())')"
  ts_pw_t="$(conn_get '.spec.config.target\.cluster\.ssl\.truststore\.password')"
  ts_pw_p="$(conn_get '.spec.config.producer\.override\.ssl\.truststore\.password')"

  assert_eq "target.cluster truststore password corrected" "$ts_pw_t" "$real_pw" \
    "The truststore password must match the one Strimzi generated alongside ca.p12, in the kafka-target-cluster-ca-cert Secret." \
    "kubectl -n mirage get secret kafka-target-cluster-ca-cert -o json | python3 -c \"import json,sys,base64; print(base64.b64decode(json.load(sys.stdin)['data']['ca.password']).decode())\""

  assert_eq "producer truststore password corrected" "$ts_pw_p" "$real_pw" \
    "producer.override.* carries its own truststore settings and wins over target.cluster.*. Both must be right." \
    "Two blocks again — same trap as the credential lab."

  # The connector-level client is what the handshake failure kills. Task threads
  # started before the change keep producing as zombies, so task state is NOT
  # the signal here — exactly as in Lab 2.
  assert_eq "connector state is RUNNING" "$(conn_state_field)" "RUNNING" \
    "A trust failure takes down the connector's own admin client (topic discovery, offset management) while existing task threads carry on. Connector FAILED with tasks RUNNING is the signature." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR | grep -A3 Connector"

  wait_connector_tasks 1 150 || true
  running="$(conn_task_count RUNNING)"
  total="$(conn_tasks_total)"

  assert_ge "tasks exist" "${total:-0}" 1 \
    "No tasks, no replication." "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  assert "every task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "Tasks should be running after the fix. Note they can also read RUNNING while the handshake is failing." \
    "running=${running:-0} of total=${total:-0}"

  local hits
  hits="$(worker_log_matches 'SSLHandshakeException|unable to find valid certification path|Failed to load SSL keystore|keystore password was incorrect')"
  assert_eq "no fresh TLS handshake failures in the worker log" "${hits:-unreadable}" "0" \
    "A recurring SSLHandshakeException or keystore-load failure means the trust material is still wrong. 'unreadable' means the worker log could not be fetched at all." \
    'kubectl -n mirage logs <connect-pod> --tail=500 | grep -iE "ssl|handshake|certif"'

  info "Confirming replication over TLS"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 150)"
  assert_ge "records replicate over TLS" "${delta:-0}" 1600 \
    "Trusting the broker is not the goal — replicating is. Note this can pass even with the fault present, because task threads started before the change keep producing; the connector state and the log are the real signals here." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage logs <connect-pod> --tail=300 | grep -iE 'ssl|handshake|certif|truststore'

  A plain tail can still show errors from an earlier lab or an earlier attempt
  at this one, because the worker pod is not restarted between labs. To see
  only what THIS lab produced:  mirage logs --errors

  Then look at what truststore the connector is pointed at, and what is actually
  mounted in the worker:

    kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
      -o jsonpath='{.spec.config}' | tr ',' '\n' | grep truststore
    kubectl -n mirage exec <connect-pod> -- ls -l /mnt/target-ca/

  Read the log and classify the failure before touching anything:

    POD=mirage-connect-0
    kubectl -n mirage logs $POD --tail=400 | grep -iE 'ssl|handshake|certif|truststore|keystore'

  What each signature means, and who owns it:

    "Failed to load SSL keystore ... password was incorrect"
    "SSLHandshakeException: unable to find valid certification path"
        -> TRUST problem. The client cannot verify the broker. Fix the
           truststore: wrong password, wrong path, wrong/expired CA.

    "Authentication failed ... invalid credentials with SASL mechanism"
        -> CREDENTIALS. (Lab 13)

    "TopicAuthorizationException: Not authorized to access topics"
        -> ACLs. (Lab 14)

  TLS is negotiated BEFORE SASL, so a trust failure means the credentials are
  never even presented. Chasing a password rotation here wastes the outage.

  What Strimzi gives you: the target cluster's CA lives in the Secret
  kafka-target-cluster-ca-cert, mounted into the worker at /mnt/target-ca/.
  It contains a ready-made PKCS12 truststore plus ITS OWN password:

    kubectl -n mirage exec $POD -- ls -l /mnt/target-ca/
    kubectl -n mirage get secret kafka-target-cluster-ca-cert -o json \
      | python3 -c "import json,sys,base64; print(base64.b64decode(json.load(sys.stdin)['data']['ca.password']).decode())"

  Note that password is NOT the SCRAM user's password. Two different secrets for
  two different purposes — mixing them up is the usual cause of this incident.
EOF
}

lab_solve() {
cat <<'EOF'
  # Read the truststore password that belongs with ca.p12
  CA_PW=$(kubectl -n mirage get secret kafka-target-cluster-ca-cert -o json \
    | python3 -c "import json,sys,base64; print(base64.b64decode(json.load(sys.stdin)['data']['ca.password']).decode())")

  # Apply it to BOTH truststore blocks
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p "{\"spec\":{\"config\":{
      \"target.cluster.ssl.truststore.password\":\"$CA_PW\",
      \"producer.override.ssl.truststore.password\":\"$CA_PW\"
    }}}"


  # IMPORTANT — check the config reached the WORKER before restarting.
  # The operator pushes config asynchronously. Restart too early and the tasks
  # retry the OLD broken value, burn autoRestart's 5 attempts, and then nothing
  # recovers them. Confirm first:
  POD=mirage-connect-0
  kubectl -n mirage exec "$POD" -- curl -s localhost:8083/connectors/lab-orders-from-source-to-target/config \
    | tr ',' '\n' | grep -E 'truststore.password'

  # Only once the runtime shows the corrected value:
  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite

  # If tasks are still FAILED afterwards, autoRestart had already given up —
  # annotate again to clear them.

  # Verify with throughput, not status
  mirage counts

THE THREE SECURITY FAILURES, SIDE BY SIDE
  Same symptom (RUNNING, zero throughput), three owners, three fixes:

    trust           SSLHandshakeException / keystore load failure
                    -> truststore path, password, or CA contents
    authentication  SaslAuthenticationException
                    -> username/password
    authorization   TopicAuthorizationException
                    -> ACLs on the service account

  Order of negotiation is TLS, then SASL, then per-operation authorization. So a
  trust failure hides the other two: you cannot tell whether the credentials are
  also wrong until the handshake succeeds. Fix in that order.

WHY THIS IS A COMMON REAL INCIDENT
  - A CA rotation replaces ca.crt AND ca.password. A connector pinned to the old
    password breaks even though the file path is unchanged.
  - A truststore mounted from the wrong cluster's CA: path valid, password valid,
    certificate chain wrong. Signature is "unable to find valid certification
    path", not a password error.
  - An expired CA produces the same handshake failure with no config change at
    all — which makes it look like nothing was touched, because nothing was.

AT MIRAGEDUEL
  KCD mounts its trust material at /mnt/kcd-service-ssl/ and references the
  passwords indirectly through FileConfigProvider:

    ssl.truststore.location: /mnt/kcd-service-ssl/truststore
    ssl.truststore.password: ${file:/mnt/kcd-service-ssl/ssl-passwords.properties:truststore.password}

  Because the password is resolved at runtime, a rotation that updates the
  mounted file needs no connector change — that indirection exists precisely to
  prevent this incident. A connector with a hardcoded password is the one that
  breaks on rotation day.

  Also note ssl.endpoint.identification.algorithm: "" in the real templates:
  hostname verification is deliberately disabled because brokers are reached by
  names that do not match their certificates. That is NOT the same as disabling
  trust — the CA chain is still verified.
EOF
}
