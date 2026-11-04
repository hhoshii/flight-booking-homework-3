#!/bin/sh
set -eu
cat > /data/sentinel.conf <<EOF
port 26379
bind 0.0.0.0
protected-mode no
dir /data
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip ${SENTINEL_HOST}
sentinel monitor flight-cache redis-master 6379 2
sentinel down-after-milliseconds flight-cache 2000
sentinel failover-timeout flight-cache 10000
sentinel parallel-syncs flight-cache 1
EOF
exec redis-server /data/sentinel.conf --sentinel
