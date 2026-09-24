#!/bin/sh
set -eu

python -m feature_store_demo.init_data
feast -c "${FEAST_REPO_PATH}" apply
exec feast -c "${FEAST_REPO_PATH}" serve -h 0.0.0.0 -p 6566
