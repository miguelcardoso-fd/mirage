#!/usr/bin/env bash
# Lab 13 — Connector authentication failure (SASL/SCRAM)

LAB_TITLE="Replication stopped after a credential rotation"
LAB_DIFFICULTY="medium"
LAB_ALERT="Missing running tasks (<1); commit error % (<50); 'Authentication failed' in logs"

lab_brief() {
cat <<'EOF'
SYMPTOM
  Replication stopped some time after a security change. Not a single record
  has arrived since.

  The connector reports RUNNING. Its tasks are harder to read: depending on
  when and where you look they show RUNNING or FAILED, because autoRestart
  keeps bringing them back and they keep dying. Throughput is simply zero.

YOUR JOB
  Get the connector replicating again over the secured listener (:9094).
  "Just put it back on the plaintext port" is not the fix your security team
  wants.

WHEN DONE
  mirage stop 13
EOF
}

# The secured listener and the SCRAM users are created by manifests/kafka.yaml
# and manifests/users.yaml at setup time.
TGT_SCRAM=kafka-target-kafka-bootstrap:9094

lab_break() {
  require_scram_users kcd-replicator || return 1
  # The mounted ca.password is a bare value (not key=value), so FileConfigProvider
  # cannot address it by key — read it here and inline it.
  local ca_pw
  ca_pw="$(target_ca_password)"
  # Everything is correct except the password: right user, right mechanism,
  # right truststore. That is deliberate — the failure must be authentication,
  # not a missing truststore or an unreachable broker.
  patch_connector_runtime "$(cat <<EOF
{"spec":{"config":{
  "target.cluster.bootstrap.servers":"$TGT_SCRAM",
  "producer.override.bootstrap.servers":"$TGT_SCRAM",
  "target.cluster.security.protocol":"SASL_SSL",
  "target.cluster.sasl.mechanism":"SCRAM-SHA-512",
  "target.cluster.sasl.jaas.config":"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"kcd-replicator\\" password=\\"definitely-not-the-password\\";",
  "target.cluster.ssl.truststore.location":"/mnt/target-ca/ca.p12",
  "target.cluster.ssl.truststore.password":"$ca_pw",
  "target.cluster.ssl.truststore.type":"PKCS12",
  "producer.override.security.protocol":"SASL_SSL",
  "producer.override.sasl.mechanism":"SCRAM-SHA-512",
  "producer.override.sasl.jaas.config":"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"kcd-replicator\\" password=\\"definitely-not-the-password\\";",
  "producer.override.ssl.truststore.location":"/mnt/target-ca/ca.p12",
  "producer.override.ssl.truststore.password":"$ca_pw",
  "producer.override.ssl.truststore.type":"PKCS12"
}}}
EOF
)" 'target.cluster.bootstrap.servers' "$TGT_SCRAM" 180 || {
    # Without this the lab ran on a healthy baseline: every task RUNNING and no
    # auth error anywhere, with nothing saying the injection never landed.
    err "could not inject the fault (connector runtime never took the new config)"
    return 1
  }

  produce 2000 1000 2000
  sleep 20
}

lab_check() {
  local boot prod jaas running total

  boot="$(conn_get '.spec.config.target\.cluster\.bootstrap\.servers')"
  prod="$(conn_get '.spec.config.producer\.override\.bootstrap\.servers')"

  assert_eq "still using the secured listener (:9094)" "$boot" "$TGT_SCRAM" \
    "Falling back to the plaintext port sidesteps the exercise. The security rollout wants this connector on the authenticated listener." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config.target\\.cluster\\.bootstrap\\.servers}'"

  assert_eq "producer override also on the secured listener" "$prod" "$TGT_SCRAM" \
    "producer.override.* wins over target.cluster.*, so the producer must be configured too." \
    "Both keys matter — this is connector.client.config.override.policy=All."

  # The password must no longer be the bogus literal.
  jaas="$(conn_get '.spec.config.producer\.override\.sasl\.jaas\.config')"
  assert "producer JAAS no longer uses the bogus password" \
    "$(printf '%s' "$jaas" | grep -q 'definitely-not-the-password' && echo 1 || echo 0)" \
    "The credentials are still the placeholder. Read the real password out of the KafkaUser's Secret." \
    "kubectl -n mirage get secret kcd-replicator -o jsonpath='{.data.password}' | base64 -d"

  local jaas_t
  jaas_t="$(conn_get '.spec.config.target\.cluster\.sasl\.jaas\.config')"
  assert "target.cluster JAAS no longer uses the bogus password" \
    "$(printf '%s' "$jaas_t" | grep -q 'definitely-not-the-password' && echo 1 || echo 0)" \
    "MirrorMaker2 authenticates as target.cluster.* for admin work and producer.override.* for the actual writes. Both need valid credentials." \
    "kubectl -n mirage get kafkaconnector $CONNECTOR -o jsonpath='{.spec.config}' | tr ',' '\\n' | grep jaas"

  wait_connector_tasks 1 150 || true
  running="$(conn_task_count RUNNING)"
  total="$(conn_tasks_total)"

  assert_ge "tasks exist" "${total:-0}" 1 \
    "No tasks means no replication regardless of credentials." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # NOTE: tasks report RUNNING even while authentication is failing — the
  # failure is on the admin/producer client, not the task thread. So task state
  # is NOT the signal here; the log and the throughput are.
  assert "every task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "Tasks should be running after the fix. Note they also read RUNNING while auth was failing — which is why this check alone proves nothing." \
    "running=${running:-0} of total=${total:-0}"

  assert_eq "no failed tasks" "$(conn_task_count FAILED)" "0" \
    "Fix the credentials, then restart if autoRestart has given up." \
    "kubectl -n mirage annotate kafkaconnector $CONNECTOR strimzi.io/restart=true --overwrite"

  # No fresh auth errors in the log.
  local hits
  hits="$(worker_log_matches 'Authentication failed|SaslAuthenticationException')"
  assert_eq "no fresh authentication failures in the worker log" "${hits:-unreadable}" "0" \
    "A recurring SaslAuthenticationException means the credentials are still wrong." \
    'kubectl -n mirage logs <connect-pod> --tail=500 | grep -iE "authent|sasl"'

  info "Confirming replication over the secured listener"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 150)"
  assert_ge "records replicate over TLS+SCRAM" "${delta:-0}" 1600 \
    "Authenticating is not the goal — replicating is. Confirm with throughput." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage describe kafkaconnector lab-orders-from-source-to-target
  kubectl -n mirage logs <connect-pod> --tail=200 | grep -iE 'authent|sasl|scram'

  A plain tail can still show errors from an earlier lab or an earlier attempt
  at this one, because the worker pod is not restarted between labs. To see
  only what THIS lab produced:  mirage logs --errors

  Then look at what the connector is actually sending as credentials, and
  compare it to what exists:

    kubectl -n mirage get kafkauser
    kubectl -n mirage get secret kcd-replicator -o jsonpath='{.data}'

  The worker log names the failure precisely:

    POD=mirage-connect-0
    kubectl -n mirage logs $POD --tail=300 | grep -iE 'authent|sasl|scram'

  Expect something like:
    org.apache.kafka.common.errors.SaslAuthenticationException:
    Authentication failed during authentication due to invalid credentials
    with SASL mechanism SCRAM-SHA-512

  That is an AUTHENTICATION failure (who you are), not an AUTHORIZATION failure
  (what you may do). Learn the difference in the logs:

    SaslAuthenticationException / "Authentication failed"  -> bad credentials
    TopicAuthorizationException / "Not authorized to access topics"
                                                           -> valid identity,
                                                              missing ACL

  Strimzi puts each KafkaUser's SCRAM password in a Secret of the same name:

    kubectl -n mirage get secret kcd-replicator -o jsonpath='{.data.password}' | base64 -d; echo

  TWO config blocks carry credentials, and both must be right:
    target.cluster.sasl.jaas.config        (admin client)
    producer.override.sasl.jaas.config     (the actual writes)

  In the JAAS string, mind the quoting:
    org.apache.kafka.common.security.scram.ScramLoginModule required \
      username="kcd-replicator" password="<the real password>";

  Better still, do not paste the password at all — see the solve for the
  config-provider form, which is what production does.
EOF
}

lab_solve() {
cat <<'EOF'
  # Read the real password from the KafkaUser's Secret
  PW=$(kubectl -n mirage get secret kcd-replicator -o jsonpath='{.data.password}' | base64 -d)

  # Apply it to BOTH credential blocks
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p "{\"spec\":{\"config\":{
      \"target.cluster.sasl.jaas.config\":\"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"kcd-replicator\\\" password=\\\"$PW\\\";\",
      \"producer.override.sasl.jaas.config\":\"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"kcd-replicator\\\" password=\\\"$PW\\\";\"
    }}}"


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

AUTHENTICATION vs AUTHORIZATION
  Two different failures with two different fixes. Read the exception class:

    SaslAuthenticationException            -> wrong identity. Fix credentials.
    "Authentication failed ... invalid credentials"

    TopicAuthorizationException            -> right identity, missing ACL.
    "Not authorized to access topics: [x]"    Fix the KafkaUser's ACLs (Lab 14).

  Chasing an ACL change for an authentication failure wastes an outage.

DO NOT PASTE PASSWORDS INTO CONNECTOR CONFIG
  The patch above is the quick lab fix. Production never embeds the secret: it
  mounts it and references it through a config provider, so the password never
  appears in a CR, a rendered manifest, or `kubectl get -o yaml`:

    sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule \
      required username="kcd-replicator" \
      password="${file:/mnt/kcd-service-ssl/ssl-passwords.properties:scram.password}";

  That ${file:...} form is resolved at runtime by FileConfigProvider — the same
  mechanism KCD uses for its truststore and keystore passwords. If you ever see
  that literal string in a config dump, nothing is broken; that is the mechanism
  working.

AT MIRAGEDUEL
  Credentials live in Vault, are mounted into the pod, and are referenced
  indirectly. `authentication.enabled: false` on the KafkaConnect CR means the
  WORKER does not authenticate to its own cluster — but CONNECTORS absolutely do
  authenticate to the clusters they mirror, via these same per-connector
  security settings. A credential rotation that misses one of the two JAAS
  blocks produces exactly this incident.
EOF
}
