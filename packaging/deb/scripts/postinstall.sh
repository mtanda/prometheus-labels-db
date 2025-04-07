#!/bin/bash

set -e

systemctl enable prometheus-labels-db-recorder.service
systemctl enable prometheus-labels-db-query.service
systemctl restart prometheus-labels-db-recorder.service
systemctl restart prometheus-labels-db-query.service
