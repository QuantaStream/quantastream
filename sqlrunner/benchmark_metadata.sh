#!/usr/bin/env bash

benchmark_suite_name() {
  local suite_file="$1"
  basename "$suite_file" .yaml
}

benchmark_default_dataset() {
  local suite_file="$1"
  case "$suite_file" in
    *tpc-h-benchmark*|*tpch*) printf 'tpch' ;;
    *) printf 'unknown' ;;
  esac
}

benchmark_metadata_join() {
  local first="$1"
  local second="$2"
  if [[ -z "$first" ]]; then
    printf '%s' "$second"
  elif [[ -z "$second" ]]; then
    printf '%s' "$first"
  else
    printf '%s,%s' "$first" "$second"
  fi
}

benchmark_base_metadata() {
  local suite_file="$1"
  local engine="$2"
  local host="${3:-}"
  local port="${4:-}"
  local dataset="${BENCHMARK_DATASET:-}"
  local scale_factor="${BENCHMARK_SCALE_FACTOR:-}"
  local metadata

  if [[ -z "$dataset" ]]; then
    dataset="$(benchmark_default_dataset "$suite_file")"
  fi
  if [[ -z "$scale_factor" ]]; then
    scale_factor="${TPCH_SCALE_FACTOR:-${SCALE_FACTOR:-}}"
  fi

  metadata="suite=$(benchmark_suite_name "$suite_file"),dataset=${dataset},engine=${engine}"
  if [[ -n "$scale_factor" ]]; then
    metadata="${metadata},scale_factor=${scale_factor}"
  fi
  if [[ -n "$host" ]]; then
    metadata="${metadata},host=${host}"
  fi
  if [[ -n "$port" ]]; then
    metadata="${metadata},port=${port}"
  fi
  printf '%s' "$metadata"
}
