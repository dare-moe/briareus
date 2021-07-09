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

if [[ "${BRIAREUS_LAUNCHER_CREATE_TEMP:-1}" != "0" ]]; then
  if [[ -e ".tmp" ]]; then
    log_error "${PWD}/.tmp path exists. You can turn off temp directory creation with environment variable BRIAREUS_LAUNCHER_CREATE_TEMP=0"
    exit 1
  else
    mkdir ".tmp"
    log_info "Created .tmp directory"
  fi
fi

linked_logs=0
if [[ "${BRIAREUS_LAUNCHER_LINK_LOGS:-1}" != "0" ]]; then
  declare -a log_dirs_array=()
  IFS="," read -r -a log_dirs_array <<< "${LOG_DIRS:-}"
  if (( ${#log_dirs_array[@]} == 0 )); then
    log_error "LOG_DIRS environment variable is empty"
    exit 1
  elif [[ -e ".logs" ]]; then
    log_error "${PWD}/.logs path exists. You can turn off log linking with environment variable BRIAREUS_LAUNCHER_LINK_LOGS=0"
    exit 1
  fi
  link_logs_target="${log_dirs_array[ $RANDOM % ${#log_dirs_array[@]} ]}"
  link_logs_target=$(readlink -f "${link_logs_target}")
  if [[ ! -e "${link_logs_target}" ]]; then
    log_error "Target directory ${link_logs_target} for link logs does not exists."
    exit 1
  fi
  log_info "Linking .logs to ${link_logs_target}"
  ln -s "${link_logs_target}" ".logs"
  linked_logs=1
fi

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

if [[ -z "${java_command:-}" ]]; then
  java_command="java"
  log_info "Java executable not found in JAVA_HOME. Using plain java command to start jvm"
fi

declare -a java_args=()
for encoded_arg in "$@"; do
  decoded_arg=$(echo "${encoded_arg}" | base64 -d)
  java_args+=("${decoded_arg}")
done

log_info "Java executable is ${java_command}"
log_info "There are ${#java_args[@]} arguments for java executable"

exit_code=0
(exec 3>&- ; "${java_command}" "${java_args[@]}") || exit_code=$?

log_info "Java process completed with exit code ${exit_code}"

if ((linked_logs)); then
  unlink ".logs" || log_error "Failed unlinking .logs"
fi

exit "${exit_code}"
