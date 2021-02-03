#!/bin/bash

set -eu

apt-get update
apt-get install -y jq

./scripts/check_no_api_breakages.sh $1 $2 $3
