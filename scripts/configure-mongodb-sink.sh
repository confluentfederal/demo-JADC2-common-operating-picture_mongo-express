#!/bin/bash
# Configure MongoDB Atlas Sink Connectors for JADC2 COP
# Run this after Kafka Connect is healthy

CONNECT_URL="http://localhost:8083"

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
until curl -s -f "$CONNECT_URL/connectors" > /dev/null 2>&1; do
    echo "  Kafka Connect not ready, waiting..."
    sleep 5
done
echo "✓ Kafka Connect is ready"

# Check installed plugins
echo ""
echo "Installed connector plugins:"
curl -s "$CONNECT_URL/connector-plugins" | jq -r '.[].class' | grep -i mongo || echo "MongoDB connector not found!"

echo ""
echo "Creating MongoDB Atlas Sink Connectors..."

# ============================================================
# Connector 1: Military Aircraft
# ============================================================
echo ""
echo "Creating connector: mongodb-sink-aircraft..."
curl -X PUT "$CONNECT_URL/connectors/mongodb-sink-aircraft/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "military_aircraft",
    "topics": "military-aircraft",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "insert.mode": "upsert",
    "max.batch.size": "100",
    "tasks.max": "1"
  }'

# ============================================================
# Connector 2: Naval Vessels
# ============================================================
echo ""
echo "Creating connector: mongodb-sink-naval..."
curl -X PUT "$CONNECT_URL/connectors/mongodb-sink-naval/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "naval_vessels",
    "topics": "naval-vessels",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "insert.mode": "upsert",
    "max.batch.size": "100",
    "tasks.max": "1"
  }'

# ============================================================
# Connector 3: Ground Force GPS
# ============================================================
echo ""
echo "Creating connector: mongodb-sink-gps..."
curl -X PUT "$CONNECT_URL/connectors/mongodb-sink-gps/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "common_operating_picture",
    "topics": "ground-force-gps",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "insert.mode": "upsert",
    "max.batch.size": "100",
    "tasks.max": "1"
  }'

# ============================================================
# Connector 4: Cyber Threats
# ============================================================
echo ""
echo "Creating connector: mongodb-sink-cyber..."
curl -X PUT "$CONNECT_URL/connectors/mongodb-sink-cyber/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "cyber_threats",
    "topics": "cyber-threats",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "insert.mode": "upsert",
    "max.batch.size": "50",
    "tasks.max": "1"
  }'

# ============================================================
# Connector 5: SIGINT Intercepts
# ============================================================
echo ""
echo "Creating connector: mongodb-sink-sigint..."
curl -X PUT "$CONNECT_URL/connectors/mongodb-sink-sigint/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "sigint_intercepts",
    "topics": "sigint-feeds",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "insert.mode": "upsert",
    "max.batch.size": "50",
    "tasks.max": "1"
  }'

# ============================================================
# Connector 6: JSIR Spectrum Interference
# ============================================================
echo ""
echo "Creating connector: mongodb-sink-jsir..."
curl -X PUT "$CONNECT_URL/connectors/mongodb-sink-jsir/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "jsir_incidents",
    "topics": "spectrum-interference",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "insert.mode": "upsert",
    "max.batch.size": "50",
    "tasks.max": "1"
  }'

# ============================================================
# Connector 7: All Alerts (from ksqlDB)
# ============================================================
echo ""
echo "Creating connector: mongodb-sink-alerts..."
curl -X PUT "$CONNECT_URL/connectors/mongodb-sink-alerts/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "operational_alerts",
    "topics": "all-alerts",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "insert.mode": "upsert",
    "max.batch.size": "50",
    "tasks.max": "1"
  }'

# ============================================================
# Check connector status
# ============================================================
echo ""
echo "============================================"
echo "Connector Status:"
echo "============================================"
sleep 3

for connector in mongodb-sink-aircraft mongodb-sink-naval mongodb-sink-gps mongodb-sink-cyber mongodb-sink-sigint mongodb-sink-jsir mongodb-sink-alerts; do
    status=$(curl -s "$CONNECT_URL/connectors/$connector/status" | jq -r '.connector.state')
    echo "  $connector: $status"
done

echo ""
echo "============================================"
echo "MongoDB Atlas Sink Connectors configured!"
echo "============================================"
echo ""
echo "Database: jadc2_cop"
echo "Collections:"
echo "  - military_aircraft"
echo "  - naval_vessels"
echo "  - common_operating_picture"
echo "  - cyber_threats"
echo "  - sigint_intercepts"
echo "  - jsir_incidents"
echo "  - operational_alerts"
echo ""
echo "View connectors: curl http://localhost:8083/connectors"
echo "View connector status: curl http://localhost:8083/connectors/<name>/status"
