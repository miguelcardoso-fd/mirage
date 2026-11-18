#!/usr/bin/env bash
# Lab 10 — SMT misconfiguration: connector FAILED, not just tasks

LAB_TITLE="Connector will not start after a config change"
LAB_DIFFICULTY="medium"
LAB_ALERT="Missing running tasks; connector_status = 3"

lab_brief() {
cat <<'EOF'
SYMPTOM
  A change added a Single Message Transform to the connector. The deploy looked
  like it worked, but the KafkaConnector resource is now unhealthy:

    Ready = Unknown, and no connector/task status at all.

  Meanwhile the REST API insists the connector is RUNNING with healthy tasks,
  and data is still flowing.

  Both of those are true at the same time. Work out how, because it tells you
  which version of the config is actually running.

YOUR JOB
  Get the connector healthy again, with a config the cluster will accept.

WHEN DONE
  mirage stop 10
EOF
}

lab_break() {
  # Mirrors the real failure shape: MirageDuel templates reference
  # com.flutter.kafka.connect.transforms.* but the lab image has no MirageDuel
  # SMTs at all, so this is also a genuine "plugin not in image" lesson.
  k patch kafkaconnector "$CONNECTOR" --type merge -p '{"spec":{"config":{
      "transforms":"InsertLocaleHeader",
      "transforms.InsertLocaleHeader.type":"com.flutter.kafka.connect.transforms.ByteArrayInsertHeader",
      "transforms.InsertLocaleHeader.header":"LOCALE",
      "transforms.InsertLocaleHeader.value.literal":"US-NJ1"
  }}}' >/dev/null
  k annotate kafkaconnector "$CONNECTOR" strimzi.io/restart=true --overwrite >/dev/null 2>&1
  sleep 20
}

lab_check() {
  local transforms ttype running total ready

  transforms="$(conn_get '.spec.config.transforms')"

  # Validate the type of EVERY transform the connector actually declares, not a
  # hardcoded key name. A valid fix may rename the transform, which would leave
  # the original transforms.<old>.type key behind as a harmless orphan.
  if [ -z "$transforms" ]; then
    _record_pass "transform removed (acceptable fix) ${C_DIM}— see notes below${C_0}"
    warn "You removed the transform rather than replacing it."
    warn "Valid here, but in production the header stamping is a requirement:"
    warn "ByteArrayInsertHeader applies LOCALE / product / region / country headers."
  else
    local bad="" name ttype
    # transforms is a comma-separated list of aliases.
    for name in $(printf '%s' "$transforms" | tr ',' ' '); do
      ttype="$(conn_get ".spec.config.transforms\.${name}\.type")"
      if printf '%s' "$ttype" | grep -q '^com\.flutter\.\|^com\.mirageduel\.'; then
        bad="$bad $name=$ttype"
      fi
    done
    assert "every declared transform uses a class present in this image" \
      "$([ -z "$bad" ] && echo 0 || echo 1)" \
      "The lab image contains only upstream Kafka Connect plugins. MirageDuel SMTs (com.flutter.* / com.mirageduel.*) live in a prebuilt ECR image that this lab cannot pull. Use an upstream transform here, or remove it." \
      "offending:${bad:- none} — check: mirage api /connector-plugins"
  fi

  ready="$(conn_ready)"
  assert_eq "connector CR reports Ready" "${ready:-Unknown}" "True" \
    "A connector that fails validation never generates task configs, so it fails at CONNECTOR level with zero tasks — a different shape from a task-level failure." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  assert_eq "connector state is RUNNING" "$(conn_state_field)" "RUNNING" \
    "FAILED at connector level means the plan itself is invalid, not that execution hit a snag." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  wait_connector_tasks 1 90 || true
  total="$(conn_tasks_total)"
  running="$(conn_task_count RUNNING)"

  assert_ge "tasks were generated" "${total:-0}" 1 \
    "Zero tasks is the tell-tale sign of connector-level rather than task-level failure." \
    "kubectl -n mirage describe kafkaconnector $CONNECTOR"

  # Guard against the vacuous 0 == 0: with zero tasks this must not pass.
  assert "every generated task is RUNNING" \
    "$([ "${total:-0}" -ge 1 ] && [ "${running:-0}" = "${total:-0}" ] && echo 0 || echo 1)" \
    "A transform that validates but then throws per-record would fail tasks instead. With zero tasks there is nothing running, so this cannot pass by default." \
    "running=${running:-0} of total=${total:-0} — kubectl -n mirage describe kafkaconnector $CONNECTOR"

  info "Confirming replication works"
  # A leftover connector from another lab would satisfy the delta below.
  assert_no_foreign_connectors

  local delta
  delta="$(replication_delta 2000 120)"
  # Regression guard only. Because Connect REJECTS the invalid config, the
  # previous good connector keeps replicating throughout — so this passing does
  # NOT mean the resource is healthy. The CR-level checks above are what matter.
  assert_ge "replication still working after your change" "${delta:-0}" 1600 \
    "Your repair must not break the pipeline that was still running on the old config. Note this check passes even with the fault in place, which is exactly why it is not the signal to trust here." \
    "target grew by ${delta:-0} of 2000 produced — mirage counts"
}

lab_hint() {
cat <<'EOF'
  Where to look first:
  kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
    -o jsonpath='{.status.conditions[*].message}{"\n"}'

  That message is the whole diagnosis — read it carefully. Then ask the running
  worker which plugin classes it actually has.

  The CR condition message IS the error. Read it first:

    kubectl -n mirage get kafkaconnector lab-orders-from-source-to-target \
      -o jsonpath='{.status.conditions[*].message}{"\n"}'

  It will say something like:

    PUT /connectors/.../config returned 400: Connector configuration is invalid
    ... Invalid value com.flutter.kafka.connect.transforms.ByteArrayInsertHeader
    for configuration transforms.InsertLocaleHeader.type: Class ... could not be found

  Now ask the running worker what plugins it ACTUALLY has:

    POD=mirage-connect-0
    kubectl -n mirage exec $POD -- curl -s localhost:8083/connector-plugins \
      | tr ',' '\n' | grep -i class

  The configured class is not in that list. This mirrors a real constraint:
  MirageDuel's SMTs (ByteArrayInsertHeader, HashPartitionerAssignor,
  CustomIdentityReplicationPolicy) ship inside a prebuilt ECR image. Strimzi's
  `build` feature is DISABLED in production — so "which SMTs do I have?" is
  answered by the image, never by the repo.

  This lab cannot pull that image, so you have two options:

    a) remove the transform block entirely, or
    b) substitute an upstream transform that does exist, e.g.
       org.apache.kafka.connect.transforms.InsertHeader
       (it needs header + value.literal)

  Note what is NOT broken: because Connect rejected the config outright, the
  previous good connector is still replicating. Do not be reassured by
  `mirage counts` looking healthy — the resource is still unhealthy.
EOF
}

lab_solve() {
cat <<'EOF'
  # Option A — remove the transform
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type json -p '[
      {"op":"remove","path":"/spec/config/transforms"},
      {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.type"},
      {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.header"},
      {"op":"remove","path":"/spec/config/transforms.InsertLocaleHeader.value.literal"}
    ]'

  # Option B — substitute an upstream transform that is COMPATIBLE with the
  #   byte-array converters this pipeline uses. DropHeaders only removes
  #   headers, so it never writes a typed value the converter would reject.
  kubectl -n mirage patch kafkaconnector lab-orders-from-source-to-target \
    --type merge -p '{"spec":{"config":{
      "transforms":"StripInternal",
      "transforms.StripInternal.type":"org.apache.kafka.connect.transforms.DropHeaders",
      "transforms.StripInternal.headers":"__internal_debug"
    }}}'

  #   NOT org.apache.kafka.connect.transforms.InsertHeader: it emits a
  #   STRING-schema header, and ByteArrayConverter rejects it at runtime with
  #   "DataException: Invalid schema type for ByteArrayConverter: STRING".
  #   The config validates, the connector goes Ready, and then every task dies.
  #   That is a good illustration of why CR-Ready is not the same as working.

  kubectl -n mirage annotate kafkaconnector lab-orders-from-source-to-target \
    strimzi.io/restart=true --overwrite

CONNECTOR-LEVEL vs TASK-LEVEL FAILURE
  connector FAILED, 0 tasks   -> the plan is invalid: bad class, bad SMT config,
                                 unresolvable brokers at startup
  connector RUNNING, tasks FAILED -> the plan is fine, execution is failing

  Different diagnosis, different fix. Restarting helps the second, never the first.

THE PACKAGE-NAME TRAP
  You will see BOTH com.flutter.kafka.connect.* and com.mirageduel.kafka.connect.*
  in MirageDuel configs and stack traces. Two repos carry identical class trees
  under different packages; the deployed image comes from the flutter-global ECR
  namespace, which is why com.flutter.* shows up in production logs.

  This is VERSION DRIFT, not a typo. Do not "fix" a com.flutter.* reference in a
  working connector. Do verify which package your image actually contains before
  adding a new transform — via /connector-plugins, not by reading the repo.
EOF
}
