#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

function log_error() {
  echo "ERROR [$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&3
}

function log_warn() {
  echo "WARN  [$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&3
}

function log_info() {
  echo "INFO  [$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&3
}

if ! log_info "Using provided file descriptor for logging" 2>/dev/null ; then
  exec 3>&2
  log_info "Redirected script log to stderr"
fi

if [[ -n "${BRIAREUS_LOGS_DIR:-}" && -n "${BRIAREUS_LINK_LOGS_DIR:-}" ]]; then
  ln -s "${BRIAREUS_LOGS_DIR}" "${BRIAREUS_LINK_LOGS_DIR}"
fi

declare -a java_args

if [[ "${BRIAREUS_LAUNCHER_CREATE_TEMP:-1}" != "0" ]]; then
  if [[ -e ".tmp" ]]; then
    log_error "${PWD}/.tmp path exists. You can turn off temp directory creation with environment variable BRIAREUS_LAUNCHER_CREATE_TEMP=0"
    exit 1
  else
    mkdir ".tmp"
    TMPDIR=$(readlink -f ./.tmp)
    log_info "Created .tmp directory (${TMPDIR}). You can turn off temp directory creation with environment variable BRIAREUS_LAUNCHER_CREATE_TEMP=0"
    export TMPDIR
    export TEMP="${TMPDIR}"
    export TMP="${TMPDIR}"
    java_args+=("-Djava.io.tmpdir=${TMPDIR}")
  fi
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
  if [[ -e "${JAVA_HOME}/bin/java" ]]; then
    canonical_java_home=$(readlink -f "${JAVA_HOME}")
    log_info "Found java in JAVA_HOME. Updating JAVA_HOME to '${canonical_java_home}'"
    export JAVA_HOME="${canonical_java_home}"
    java_command="${JAVA_HOME}/bin/java"
  else
    log_warn "JAVA_HOME environment variable is set to ${JAVA_HOME} but java executable not found inside"
  fi
fi

if [[ -z "${java_command:-}" ]]; then
  java_command="java"
  log_info "Java executable not found in JAVA_HOME. Using plain java command to start jvm"
fi

for encoded_arg in "$@"; do
  decoded_arg=$(echo "${encoded_arg}" | base64 -d)
  java_args+=("${decoded_arg}")
done

log_info "Java executable is ${java_command}"
log_info "There are ${#java_args[@]} arguments for java executable"
log_info "Will execute 'exec ${java_command} ${java_args[*]}'"

exec 3>&-
exec "${java_command}" "${java_args[@]}"
