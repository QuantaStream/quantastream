#!/usr/bin/env bash
set -euo pipefail

export PATH="$PATH:."
quanta-admin drop lineitem
quanta-admin drop orders
quanta-admin drop partsupp
quanta-admin drop supplier
quanta-admin drop part
quanta-admin drop customer
quanta-admin drop nation
quanta-admin drop region
