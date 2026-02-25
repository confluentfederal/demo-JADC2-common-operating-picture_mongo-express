#!/bin/bash
# Copy keyfile and fix permissions
cp /etc/mongo-keyfile-source /tmp/mongo-keyfile
chmod 400 /tmp/mongo-keyfile
chown mongodb:mongodb /tmp/mongo-keyfile
exec /usr/local/bin/docker-entrypoint.sh mongod --replSet rs0 --bind_ip_all --keyFile /tmp/mongo-keyfile
