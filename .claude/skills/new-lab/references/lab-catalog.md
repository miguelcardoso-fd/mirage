# Lab catalogue — what each lab actually injects

Spoilers by design: this is for the lab *author*, to detect overlap. It was
accurate on 2026-10-30. Before relying on a row, confirm it against the live
source (`sed -n '/^lab_break/,/^}/p' labs/NN-*.sh`), and add a row whenever a
lab is added.

| # | Title (what on-call sees) | Injected fault (mechanism) | Layer | Topics / keywords it covers |
|---|---|---|---|---|
| 1 | Consumer lag climbing on one topic | `tasksMax: 1` on a 12-partition topic under load | connector config | lag, throughput, parallelism, tasksMax |
| 2 | Replication stopped, no deploy went out | source, target **and** `producer.override` bootstrap set to `:9999` (dead port), then restart | connector config | **broker unreachable, wrong bootstrap, connection refused, producer cannot reach broker, consumer cannot reach broker**, connector vs task state |
| 3 | Heap claim exceeds the container | `-Xmx` above the container memory limit (latent) | worker resources | heap, OOM, memory limit, JVM options |
| 4 | Target re-filling from the beginning | connector recreated under a new name → offsets (keyed by name) lost, re-replicates | connector identity / offsets | offsets reset, duplicates, rename, auto.offset.reset |
| 5 | Tasks RUNNING, zero records moving | `topics` pattern matches nothing | connector config | topic regex, no tasks, green but idle |
| 6 | Scale-up requested, pods never appeared | ResourceQuota too small for new worker replicas | Kubernetes | quota, replicas, scaling, pods missing |
| 7 | Topic count and log volume climbing | second connector mirroring target into itself (loop) | connector topology | loop, recursion, alias, topic explosion, cleanup |
| 8 | Replication deficit, nothing unhealthy | **source** broker made unschedulable (outage) | broker / upstream | broker down, disconnect storm, blast radius, escalate, not KCD |
| 9 | Connector idle, every task green | connector `state: paused` | connector state | paused, resume |
| 10 | Connector will not start after a config change | SMT class that does not exist on the classpath | connector config / plugins | SMT, transform, ClassNotFound, 400 on config |
| 11 | Records missing after a naming tidy-up | worker `group.id` and storage topics renamed under load; Strimzi re-creates the connector on the new group with no offsets, so the roll window is skipped | worker config | group.id, config/offset/status topics, lost offsets, silent loss |
| 12 | EXAM | several faults at once | mixed | — |
| 13 | Replication stopped after a credential rotation | wrong SCRAM password on target | security (authn) | SASL, SCRAM, password, credential rotation, SaslAuthenticationException |
| 14 | One topic stopped replicating | valid user without write ACL | security (authz) | ACL, TopicAuthorizationException, KafkaUser permissions |
| 15 | Connector will not start after a cert change | wrong truststore / CA | security (TLS) | TLS, certificate, truststore, SSLHandshakeException |
| 16 | One partition behind, eleven healthy | target `max.message.bytes` below a record's size → the owning task FAILS (not retriable), its 3 partitions stall | broker limits | poison pill, oversized record, RecordTooLarge, per-partition stall |
| 17 | Records rejected before leaving the connector | producer `max.request.size` too small | producer config | producer refuses, large records, RecordTooLarge (client side) |
| 18 | A few partitions frozen, the rest flowing | target `message.timestamp` acceptance window tightened | broker limits | poison pill, timestamp, InvalidTimestampException |
| 19 | Resume merged, connector still paused | Strimzi operator heap-OOM crashloop (`JAVA_OPTS=-Xmx24m`) | operator | operator down, reconcile, generation vs observedGeneration |

## Known-not-reproducible here (from CLAUDE.md)

- Partition-count mismatch data loss — needs `HashPartitionerAssignor` (not in the vanilla image).
- VPC security-group blocks — no VPC in kind. Use a NetworkPolicy instead (kindnet enforces it).

## Lessons a new lab in the same area must respect

- **Lab 2:** breaking only the *target* bootstrap is not observable — existing
  producer threads keep the old connection as zombies and data keeps flowing.
  A target-only network fault needs the tasks rebuilt (restart) or a
  connection-level block (NetworkPolicy) rather than a config change.
- **Labs 16/18:** the mirror uses ByteArrayConverter, so a "poison pill" must be
  something the *target* refuses, not something a converter chokes on.
