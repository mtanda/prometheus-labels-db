#!/bin/bash

set -e

systemctl disable prometheus-labels-db-recorder.timer
systemctl stop prometheus-labels-db-recorder.timer
systemctl disable prometheus-labels-db-query.service
systemctl stop prometheus-labels-db-query.service
