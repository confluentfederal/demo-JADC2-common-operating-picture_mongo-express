#!/bin/bash
# ============================================================
# MongoDB Atlas Sink Connectors Setup
# ============================================================
# Usage: ./setup-atlas-sinks.sh <atlas-connection-string>
# Example: ./setup-atlas-sinks.sh "mongodb+srv://user:pass@cluster.mongodb.net/"
# ============================================================

CONNECT_URL="http://localhost:8083"

# Check if connection string provided
if [ -z "$1" ]; then
    echo "============================================================"
    echo "MongoDB Atlas Sink Connector Setup"
    echo "============================================================"
    echo ""
    echo "Usage: $0 <atlas-connection-string>"
    echo ""
    echo "Example:"
    echo "  $0 \"mongodb+srv://myuser:mypassword@cluster0.abc123.mongodb.net/\""
    echo ""
    echo "Get your connection string from:"
    echo "  Atlas -> Your Cluster -> Connect -> Connect your application"
    echo "============================================================"
    exit 1
fi

ATLAS_URI="$1"
DATABASE="jadc2_cop"

echo "============================================================"
echo "Setting up MongoDB Atlas Sink Connectors"
echo "============================================================"
echo "Atlas URI: ${ATLAS_URI:0:30}..."
echo "Database: $DATABASE"
echo "Connect URL: $CONNECT_URL"
echo ""

# Check if Kafka Connect is available
echo "Checking Kafka Connect availability..."
if ! curl -s "$CONNECT_URL/connectors" > /dev/null 2>&1; then
    echo "ERROR: Kafka Connect is not available at $CONNECT_URL"
    echo "Make sure Kafka Connect is running: docker compose -f docker/docker-compose.yml up -d kafka-connect"
    exit 1
fi
echo "✓ Kafka Connect is available"
echo ""

# Check if MongoDB connector plugin is installed
echo "Checking for MongoDB connector plugin..."
PLUGINS=$(curl -s "$CONNECT_URL/connector-plugins")
if ! echo "$PLUGINS" | grep -q "MongoSinkConnector"; then
    echo "ERROR: MongoDB connector plugin not found"
    echo "Restart Kafka Connect to install the plugin"
    exit 1
fi
echo "✓ MongoDB connector plugin found"
echo ""

# Function to create a sink connector
create_sink() {
    local NAME=$1
    local TOPIC=$2
    local COLLECTION=$3
    local EXTRA_CONFIG=${4:-""}
    
    echo "Creating sink: $NAME ($TOPIC -> $COLLECTION)"
    
    # Delete existing connector if it exists
    curl -s -X DELETE "$CONNECT_URL/connectors/$NAME" > /dev/null 2>&1
    
    # Base config
    local CONFIG="{
        \"name\": \"$NAME\",
        \"config\": {
            \"connector.class\": \"com.mongodb.kafka.connect.MongoSinkConnector\",
            \"connection.uri\": \"$ATLAS_URI\",
            \"database\": \"$DATABASE\",
            \"collection\": \"$COLLECTION\",
            \"topics\": \"$TOPIC\",
            \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
            \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
            \"value.converter.schemas.enable\": \"false\",
            \"tasks.max\": \"1\",
            \"errors.tolerance\": \"all\",
            \"errors.log.enable\": \"true\",
            \"errors.log.include.messages\": \"true\"
            $EXTRA_CONFIG
        }
    }"
    
    RESULT=$(curl -s -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d "$CONFIG")
    
    if echo "$RESULT" | grep -q "error_code"; then
        echo "  ✗ Failed: $(echo $RESULT | jq -r '.message // .error_code')"
    else
        echo "  ✓ Created successfully"
    fi
}

echo "============================================================"
echo "Creating Sink Connectors"
echo "============================================================"
echo ""

# ============================================================
# OPERATIONAL DATA SINKS
# ============================================================

# Aircraft sink - upsert by aircraft_id
create_sink "sink-military-aircraft" "military-aircraft" "military_aircraft" ',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy",
    "document.id.strategy.overwrite.existing": "true"'

# Naval vessels sink - upsert by vessel_id
create_sink "sink-naval-vessels" "naval-vessels" "naval_vessels" ',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy",
    "document.id.strategy.overwrite.existing": "true"'

# Ground force GPS sink - upsert by unit_id
create_sink "sink-ground-force-gps" "ground-force-gps" "common_operating_picture" ',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy",
    "document.id.strategy.overwrite.existing": "true"'

# Cyber threats sink - insert (keep history)
create_sink "sink-cyber-threats" "cyber-threats" "cyber_threats" ',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy"'

# SIGINT feeds sink - insert (keep history)
create_sink "sink-sigint-feeds" "sigint-feeds" "sigint_intercepts" ',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy"'

# Spectrum interference (JSIR) sink - insert (keep history)
create_sink "sink-spectrum-interference" "spectrum-interference" "jsir_incidents" ',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy"'

# Satellite imagery metadata sink
create_sink "sink-satellite-imagery" "satellite-imagery-metadata" "satellite_imagery" ',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy"'

# ============================================================
# ALERT SINKS (from ksqlDB)
# ============================================================

# All alerts combined stream
create_sink "sink-all-alerts" "all-alerts" "operational_alerts" ',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy"'

echo ""
echo "============================================================"
echo "Verifying Connector Status"
echo "============================================================"
echo ""

sleep 3

# List all connectors and their status
CONNECTORS=$(curl -s "$CONNECT_URL/connectors")
echo "Active connectors:"
echo "$CONNECTORS" | jq -r '.[]' | while read connector; do
    STATUS=$(curl -s "$CONNECT_URL/connectors/$connector/status" | jq -r '.connector.state')
    TASK_STATUS=$(curl -s "$CONNECT_URL/connectors/$connector/status" | jq -r '.tasks[0].state // "NO_TASKS"')
    if [ "$STATUS" == "RUNNING" ] && [ "$TASK_STATUS" == "RUNNING" ]; then
        echo "  ✓ $connector: $STATUS (task: $TASK_STATUS)"
    else
        echo "  ✗ $connector: $STATUS (task: $TASK_STATUS)"
    fi
done

echo ""
echo "============================================================"
echo "Setup Complete!"
echo "============================================================"
echo ""
echo "View connectors in Control Center: http://localhost:9021"
echo "  -> Connect -> jadc2-connect-group"
echo ""
echo "Check connector status:"
echo "  curl http://localhost:8083/connectors/<name>/status | jq"
echo ""
echo "View connector logs:"
echo "  docker logs jadc2-kafka-connect --tail 100"
echo ""
echo "MongoDB Atlas collections created:"
echo "  - military_aircraft (upsert by aircraft_id)"
echo "  - naval_vessels (upsert by vessel_id)"
echo "  - common_operating_picture (upsert by unit_id)"
echo "  - cyber_threats (append)"
echo "  - sigint_intercepts (append)"
echo "  - jsir_incidents (append)"
echo "  - satellite_imagery (append)"
echo "  - operational_alerts (append)"
echo ""
