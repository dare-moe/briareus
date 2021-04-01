#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

function log_error() {
  echo "ERROR [$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&3
}

function log_info() {
  echo "INFO  [$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&3
}

if ! log_info "Using provided file descriptor for logging" 2>/dev/null ; then
  exec 3>&2
  log_info "Redirected script log to stderr"
fi

java_command=""

if [[ -n "${JAVA_HOME:-}" ]]; then
  if [[ -e "${JAVA_HOME}/bin/java" ]]; then
    canonical_java_home=$(readlink -f "${JAVA_HOME}")
    log_info "Found java in JAVA_HOME. Updating JAVA_HOME to '${canonical_java_home}'"
    export JAVA_HOME="${canonical_java_home}"
    java_command="${JAVA_HOME}/bin/java"
  else
    log_error "JAVA_HOME environment variable is set but java executable not found inside"
  fi
fi

if [[ -z "${java_command}" ]]; then
  java_command="java"
  log_info "Java executable not found in JAVA_HOME. Using plain java command to start jvm"
fi

declare -a java_args
for encoded_arg in "$@"; do
  decoded_arg=$(echo "${encoded_arg}" | base64 -d)
  java_args+=("${decoded_arg}")
done

log_info "Java executable is ${java_command}"
log_info "There are ${#java_args[@]} arguments for java executable"

exec 3>&-

"${java_command}" "${java_args[@]}"
