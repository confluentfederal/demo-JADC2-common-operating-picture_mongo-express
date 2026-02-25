# =============================================================================
# MongoDB Atlas Sink Connector - CURL Commands
# =============================================================================
# Run these commands AFTER Kafka Connect is running and healthy
# Check status: curl http://localhost:8083/connectors
# =============================================================================

# -----------------------------------------------------------------------------
# 1. AIRCRAFT SINK
# -----------------------------------------------------------------------------
curl -X PUT http://localhost:8083/connectors/mongodb-sink-aircraft/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "military-aircraft",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "military_aircraft",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "write.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }'

# -----------------------------------------------------------------------------
# 2. NAVAL SINK
# -----------------------------------------------------------------------------
curl -X PUT http://localhost:8083/connectors/mongodb-sink-naval/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "naval-vessels",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "naval_vessels",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "write.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }'

# -----------------------------------------------------------------------------
# 3. GROUND FORCE GPS SINK
# -----------------------------------------------------------------------------
curl -X PUT http://localhost:8083/connectors/mongodb-sink-gps/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "ground-force-gps",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "common_operating_picture",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "write.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }'

# -----------------------------------------------------------------------------
# 4. CYBER THREATS SINK
# -----------------------------------------------------------------------------
curl -X PUT http://localhost:8083/connectors/mongodb-sink-cyber/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "cyber-threats",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "cyber_threats",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "write.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }'

# -----------------------------------------------------------------------------
# 5. SIGINT SINK
# -----------------------------------------------------------------------------
curl -X PUT http://localhost:8083/connectors/mongodb-sink-sigint/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "sigint-feeds",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "sigint_intercepts",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "write.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }'

# -----------------------------------------------------------------------------
# 6. JSIR/SPECTRUM INTERFERENCE SINK
# -----------------------------------------------------------------------------
curl -X PUT http://localhost:8083/connectors/mongodb-sink-jsir/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "spectrum-interference",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "jsir_incidents",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "write.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }'

# -----------------------------------------------------------------------------
# 7. ALERTS SINK (from ksqlDB all-alerts topic)
# -----------------------------------------------------------------------------
curl -X PUT http://localhost:8083/connectors/mongodb-sink-alerts/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "all-alerts",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "operational_alerts",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "write.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }'

# -----------------------------------------------------------------------------
# 8. SATELLITE IMAGERY METADATA SINK
# -----------------------------------------------------------------------------
curl -X PUT http://localhost:8083/connectors/mongodb-sink-satellite/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "satellite-imagery-metadata",
    "connection.uri": "mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop",
    "database": "jadc2_cop",
    "collection": "satellite_imagery",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "write.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }'

# =============================================================================
# VERIFICATION COMMANDS
# =============================================================================

# List all connectors
# curl http://localhost:8083/connectors

# Check specific connector status
# curl http://localhost:8083/connectors/mongodb-sink-aircraft/status

# Delete a connector
# curl -X DELETE http://localhost:8083/connectors/mongodb-sink-aircraft

# Check Kafka Connect plugins
# curl http://localhost:8083/connector-plugins
