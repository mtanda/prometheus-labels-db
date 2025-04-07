#!/bin/bash

set -e

systemctl stop prometheus-labels-db-recorder.service
systemctl stop prometheus-labels-db-query.service
