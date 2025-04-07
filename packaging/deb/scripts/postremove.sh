#!/bin/bash

set -e

systemctl disable prometheus-labels-db-recorder.service
systemctl disable prometheus-labels-db-query.service
systemctl daemon-reload
