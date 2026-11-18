---
name: new-lab
description: Design, build and verify a new mirage incident lab from a plain-language scenario — e.g. "create a lab where the connector cannot reach the producer broker", "add a lab for an expired cert", "new lab: SMT drops headers". Use it whenever the user asks to add, create, build or prototype a lab, drill, scenario or fault in mirage, even if they only describe the symptom. It checks overlap with existing labs, proves the fault reproduces on the kind cluster, writes labs/NN-*.sh plus every registration point, and tests that the grader fails before the fix and passes after.
---

# new-lab — build a mirage lab from a scenario

The user describes a failure. You turn it into a lab that injects it, grades
the fix honestly, and is registered everywhere the harness looks. A lab is
only done when `./run-all-labs.sh <n>` shows `failed-ok / PASS` and a
plausible wrong fix fails.

Read `CLAUDE.md` in this repo first — especially **Anatomy of a lab**,
**`lab_break` must not narrate the fault**, **Writing a check that actually
works** (11 failure modes) and **The harness is not proof**. This skill is the
workflow; CLAUDE.md is the rulebook. `labs/19-operator-down.sh` is the most
recent complete example to copy structure from.

## 1. Understand and scope (no cluster changes yet)

1. Restate the scenario as: **fault** (what is broken), **symptom** (what
   on-call sees), **correct fix**, **tempting wrong fix**. The wrong fix
   matters: the grader must reject it, and it is what makes the lab teach
   something.
2. **Overlap gate — do this before anything else touches the cluster.**
   Compare the scenario against every existing lab by *mechanism*, not by
   title (titles describe symptoms and hide the cause on purpose):
   - Read `references/lab-catalog.md` (what each lab injects, plus keywords).
   - Confirm candidates against the live source: `ls labs/`, then
     `sed -n '/^lab_break/,/^}/p' labs/NN-*.sh` for each plausible match, and
     check for labs newer than the catalogue.

   Then always open your reply with an explicit verdict, one of:
   - **Already covered** — "Lab NN (*title*) already covers this: it injects
     <mechanism>." Name every matching lab number. Stop here and ask whether
     they want a *distinct* variant; offer one concrete angle that would add
     something the existing lab does not teach (different layer, side,
     symptom or wrong-fix trap). Don't build unless they say yes.
   - **Partially covered** — name the lab number(s), say what overlaps and
     what is new, and propose building only the new part.
   - **Not covered** — say so, citing the closest lab number(s) you checked
     so the user can see the comparison happened.

   Example: "cannot reach the producer broker" → partially covered by **lab
   2** (dead bootstrap for source, target and producer) and related to **lab
   8** (source broker outage). A distinct lab would be a *connection-level*
   block of the target only (NetworkPolicy), where config looks right and
   tasks fail on produce.
3. Check feasibility against the environment: kind, Strimzi 0.47, Kafka 4.0,
   vanilla Connect image (no `com.mirageduel` classes). CLAUDE.md lists faults
   that do *not* reproduce here (partition-mismatch loss, VPC security groups).
   Pick an injection that is real in this cluster. Useful levers:
   - NetworkPolicy — kindnet **does** enforce it (verified), so an egress or
     ingress deny is a faithful "network/SG block".
   - Wrong bootstrap host/port, listener/protocol mismatch (9092 plaintext vs
     9094 TLS+SCRAM on the target).
   - KafkaUser/ACL/Secret changes, CR spec, operator Deployment, quotas.
4. If the scenario is ambiguous or not reproducible, stop and tell the user
   the closest reproducible version before building anything.

## 2. Spike the fault by hand

Before writing the lab, prove the mechanism on the live cluster (always via
`./mirage` helpers or `KUBECONFIG=.state/kubeconfig`, never a real cluster):

1. `./mirage status` — note `.state/active`; if a lab is active, tell the user
   their lab will be reset.
2. `./mirage reset`, inject the fault manually, wait, and record exactly what
   is observable: connector/task state, worker log lines, `./mirage counts`.
3. Apply the correct fix; confirm replication recovers.
4. Apply the wrong fix on a fresh injection; confirm it does *not* recover (or
   recovers only superficially — that is the difference your grader encodes).
5. `./mirage reset`. If the fault leaves residue that reset does not clear,
   extend `restore_baseline` in `lib/common.sh` (as `restore_operator` was for
   lab 19) — and note that changing common.sh requires the full suite.

If the spike does not reproduce, report that and stop. A lab that teaches a
fault this environment cannot produce is worse than no lab.

## 3. Write the lab

Next free number: `ls labs/`. File `labs/NN-short-slug.sh`, with the contract
from CLAUDE.md:

- `LAB_TITLE` — the page you'd receive, not the cause ("Tasks failing on
  produce, source healthy", not "Target NetworkPolicy blocks egress").
- `LAB_DIFFICULTY` — easy | medium | hard (see step 6 for placement).
- `LAB_ALERT` — the real Datadog monitor, or say plainly none exists.
- `lab_brief` — symptom, job, `mirage stop NN`. No cause.
- `lab_break` — `info "Injecting the fault"`; verify the fault took hold and
  `return 1` if it did not; `produce` load after; `mark labNN.injected`.
  Explain the mechanism in comments, never on stdout.
- `lab_check` — assertions that fail on the live fault *and* on the wrong
  fix: runtime state not CR (`runtime_config_value`), connector named in task
  counts, logs only via `worker_log_matches`, scope as narrow as the fault,
  `assert_no_foreign_connectors`, and end with a `replication_delta` (2000
  produced, ≥1600 unless there is a documented reason for less).
- `lab_hint` — blank-line-separated blocks, general → specific.
- `lab_solve` — runnable commands, then `AT MIRAGEDUEL` (who owns it, PR vs live
  change, escalation).

## 4. Register it everywhere

Missing one of these is how labs silently drop out of the regression run.

| Where | What |
|---|---|
| `lib/debriefs.sh` | a `NN)` case: mechanism, signal, production, rule |
| `mirage` | number into `LAB_ORDER` |
| `run-all-labs.sh` | number into default `LABS=(...)` and a `fix_NN()` that mirrors `lab_solve` exactly |
| `README.md` | row in the right table, recommended order line |
| `CLAUDE.md` | TSG mapping row; lab count ("N labs") |
| `.claude/skills/new-lab/references/lab-catalog.md` | a row with the mechanism and keywords, so the next overlap check finds it |

Then `bash -n` every touched script.

## 5. Test — three ways, sequentially

Never run two `run-all-labs.sh` at once (lock `.state/run-all-labs.lock`); it
resets the shared cluster. Run long commands in the background and wait for
the notification instead of polling with `pgrep`/`ps` (blocked here).

1. **Harness:** `./run-all-labs.sh NN` → must print `failed-ok  PASS`. On a
   post-fix FAIL read `.state/inject-NN.log` and `.state/grade-NN.log` first.
2. **Cheat test:** `./mirage start NN`, apply the tempting wrong fix,
   `MIRAGE_FORCE_STOP=1 ./mirage stop NN` → must FAIL, and the failing
   assertions should be the ones that explain why it is wrong.
3. **Dirty-cluster test:** run it once without `reset --fresh` after another
   lab, so absolute thresholds are exposed (lab 17 shipped broken this way).

If `lib/common.sh` changed, the full suite is required (~2h). Tell the user
and start it in the background; don't skip it silently.

## 6. Place it by difficulty

Rate against existing labs: how misleading the signal is, how many layers are
involved, whether the obvious action is wrong, how many steps the fix takes.
Insert the number into the order (README groups: easy → medium connector →
security 15→13→14 → medium cluster → hard → 19 → exam 12) in `mirage`,
`run-all-labs.sh` and `README.md` consistently.

## 7. Report

Tell the user: the lab number and title, what it injects (they are the
author, so spoilers are fine here), the harness result line, the cheat-test
result, every file touched, anything not verified, and to `./mirage start NN`
to try it cold. Don't commit unless asked.
