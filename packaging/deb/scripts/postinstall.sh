#!/bin/bash

set -e

systemctl enable prometheus-labels-db-recorder.timer
systemctl start prometheus-labels-db-recorder.timer
systemctl enable prometheus-labels-db-query.service
systemctl restart prometheus-labels-db-query.service
