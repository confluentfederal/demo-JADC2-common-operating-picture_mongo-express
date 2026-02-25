#!/bin/bash
# =============================================================================
# MongoDB Atlas Sink Connector Setup
# =============================================================================
# This script creates sink connectors to push Kafka topics to MongoDB Atlas
#
# Prerequisites:
# 1. Kafka Connect running with MongoDB connector installed
# 2. MongoDB Atlas cluster accessible (IP whitelist configured)
# 3. Database user with readWrite permissions
# =============================================================================

CONNECT_URL="http://localhost:8083"
ATLAS_CONNECTION_STRING="mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop"
DATABASE="jadc2_cop"

echo "=============================================="
echo "MongoDB Atlas Sink Connector Setup"
echo "=============================================="
echo "Connect URL: $CONNECT_URL"
echo "Database: $DATABASE"
echo ""

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
until curl -s -f "$CONNECT_URL/connectors" > /dev/null 2>&1; do
    echo "  Kafka Connect not ready, waiting 5 seconds..."
    sleep 5
done
echo "✓ Kafka Connect is ready!"
echo ""

# Function to create a sink connector
create_sink() {
    local NAME=$1
    local TOPICS=$2
    local COLLECTION=$3
    
    echo "Creating sink: $NAME -> $COLLECTION"
    
    curl -s -X PUT "$CONNECT_URL/connectors/$NAME/config" \
        -H "Content-Type: application/json" \
        -d "{
            \"connector.class\": \"com.mongodb.kafka.connect.MongoSinkConnector\",
            \"tasks.max\": \"1\",
            \"topics\": \"$TOPICS\",
            \"connection.uri\": \"$ATLAS_CONNECTION_STRING\",
            \"database\": \"$DATABASE\",
            \"collection\": \"$COLLECTION\",
            \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
            \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
            \"value.converter.schemas.enable\": \"false\",
            \"document.id.strategy\": \"com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy\",
            \"document.id.strategy.overwrite.existing\": \"true\",
            \"write.strategy\": \"com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy\",
            \"errors.tolerance\": \"all\",
            \"errors.log.enable\": \"true\",
            \"errors.log.include.messages\": \"true\"
        }" | jq .
    echo ""
}

# =============================================================================
# Create Sink Connectors for each topic
# =============================================================================

echo "Creating sink connectors..."
echo ""

# Aircraft positions
create_sink "mongodb-sink-aircraft" "military-aircraft" "military_aircraft"

# Naval vessels
create_sink "mongodb-sink-naval" "naval-vessels" "naval_vessels"

# Ground force GPS
create_sink "mongodb-sink-gps" "ground-force-gps" "common_operating_picture"

# Cyber threats
create_sink "mongodb-sink-cyber" "cyber-threats" "cyber_threats"

# SIGINT intercepts
create_sink "mongodb-sink-sigint" "sigint-feeds" "sigint_intercepts"

# JSIR/Spectrum interference
create_sink "mongodb-sink-jsir" "spectrum-interference" "jsir_incidents"

# Alerts (from ksqlDB)
create_sink "mongodb-sink-alerts" "all-alerts" "operational_alerts"

# Satellite imagery metadata
create_sink "mongodb-sink-satellite" "satellite-imagery-metadata" "satellite_imagery"

echo "=============================================="
echo "Connector Status"
echo "=============================================="
curl -s "$CONNECT_URL/connectors" | jq .
echo ""

echo "=============================================="
echo "Done! Check connector status with:"
echo "  curl $CONNECT_URL/connectors/<name>/status"
echo "=============================================="
