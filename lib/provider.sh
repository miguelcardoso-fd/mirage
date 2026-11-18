#!/usr/bin/env bash
# Cluster-provider abstraction for the KCD lab.
#
# The labs only need a local single-node Kubernetes cluster; nothing in them
# depends on kind specifically. This file is the whole of that dependency, so
# the rest of the harness never names a provider.
#
# Sourced by lib/common.sh, which is sourced by mirage and every labs/*.sh.

LAB_CLUSTER="${LAB_CLUSTER:-mirage}"

# No emoji in setup/teardown output. minikube has a switch for it; kind does
# not, so its output is filtered. The filter strips pictographs and the
# variation/joiner characters that glue them together, and keeps ✓ / ✗ (U+27xx),
# which carry kind's per-step status. Unbuffered so progress still streams.
export MINIKUBE_IN_STYLE=false
_no_emoji() {
  perl -CS -pe 'BEGIN { $| = 1 } s/[\x{1F000}-\x{1FAFF}\x{2600}-\x{26FF}\x{2B50}\x{FE0F}\x{200D}]//g; s/[ \t]+$//'
}

# The chosen provider is persisted on setup. teardown and repair run later, in
# a different shell, and must act on the provider that actually built the
# cluster -- not on whatever detection would pick today. Installing minikube
# after a kind setup must not make `teardown` a silent no-op that leaves the
# kind cluster running.
provider_state_file() { printf '%s/provider' "$STATE_DIR"; }

provider_save() { printf '%s\n' "$1" >"$(provider_state_file)"; }

provider_saved() {
  local f; f="$(provider_state_file)"
  [ -f "$f" ] || return 1
  local p; p="$(tr -d '[:space:]' <"$f")"
  [ -n "$p" ] || return 1
  printf '%s' "$p"
}

# Resolution order: explicit override, then what built the existing cluster,
# then an installed binary (kind preferred), and fail if neither is present.
provider_detect() {
  if [ -n "${MIRAGE_PROVIDER:-}" ]; then printf '%s' "$MIRAGE_PROVIDER"; return 0; fi
  local saved; saved="$(provider_saved)" && { printf '%s' "$saved"; return 0; }
  if command -v kind >/dev/null 2>&1; then printf 'kind'; return 0; fi
  if command -v minikube >/dev/null 2>&1; then printf 'minikube'; return 0; fi
  return 1
}

provider_binary() {
  case "$1" in
    kind)     printf 'kind' ;;
    minikube) printf 'minikube' ;;
    *)        return 1 ;;
  esac
}

provider_cluster_exists() {
  case "$1" in
    kind)
      kind get clusters 2>/dev/null | grep -qx "$LAB_CLUSTER"
      ;;
    minikube)
      # `minikube status` exits non-zero for a stopped or absent profile, which
      # is the distinction we want: a stopped profile cannot serve the labs, so
      # treating it as absent lets provider_create start it.
      minikube status -p "$LAB_CLUSTER" >/dev/null 2>&1
      ;;
    *) return 1 ;;
  esac
}

provider_export_kubeconfig() {
  case "$1" in
    kind)
      kind export kubeconfig --name "$LAB_CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null 2>&1
      ;;
    minikube)
      # minikube writes to $KUBECONFIG when it is set, which lib/common.sh
      # always does -- so this lands in .state/kubeconfig, never ~/.kube/config.
      minikube update-context -p "$LAB_CLUSTER" >/dev/null 2>&1
      ;;
    *) return 1 ;;
  esac
}

provider_create() {
  case "$1" in
    kind)
      # pipefail (set by every caller) keeps kind's exit status through the filter.
      kind create cluster --name "$LAB_CLUSTER" --kubeconfig "$KUBECONFIG" --wait 240s 2>&1 | _no_emoji
      ;;
    minikube)
      # 8 cores / 10GB mirrors the colima sizing the labs need: two Kafka
      # clusters at 1536Mi each, a Connect worker at 1280Mi, Strimzi, and now
      # an ingress controller. Below this the API server starts returning TLS
      # handshake timeouts and the labs look broken.
      minikube start -p "$LAB_CLUSTER" --cpus 8 --memory 10240 --wait all
      ;;
    *) return 1 ;;
  esac
}

provider_delete() {
  case "$1" in
    kind)     kind delete cluster --name "$LAB_CLUSTER" 2>&1 | _no_emoji ;;
    minikube) minikube delete -p "$LAB_CLUSTER" ;;
    *)        return 1 ;;
  esac
}

# Human-readable install hint, used by setup's preflight.
provider_install_hint() {
  case "$1" in
    kind)     printf 'brew install kind' ;;
    minikube) printf 'brew install minikube' ;;
    *)        printf 'brew install kind   # or: brew install minikube' ;;
  esac
}
