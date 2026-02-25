#!/bin/bash
# Add all missing MongoDB Atlas sink connectors

CONNECT_URL="http://localhost:8083"
ATLAS_URI="mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop"

create_sink() {
    local NAME=$1
    local TOPIC=$2
    local COLLECTION=$3
    
    echo -n "Creating $NAME ($TOPIC -> $COLLECTION)... "
    
    RESULT=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"$NAME\",
            \"config\": {
                \"connector.class\": \"com.mongodb.kafka.connect.MongoSinkConnector\",
                \"connection.uri\": \"$ATLAS_URI\",
                \"database\": \"jadc2_cop\",
                \"collection\": \"$COLLECTION\",
                \"topics\": \"$TOPIC\",
                \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
                \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
                \"value.converter.schemas.enable\": \"false\",
                \"document.id.strategy\": \"com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy\",
                \"tasks.max\": \"1\",
                \"errors.tolerance\": \"all\",
                \"errors.log.enable\": \"true\",
                \"errors.log.include.messages\": \"true\"
            }
        }")
    
    if [ "$RESULT" = "201" ]; then
        echo "OK"
    elif [ "$RESULT" = "409" ]; then
        echo "ALREADY EXISTS"
    else
        echo "FAILED (HTTP $RESULT)"
    fi
}

echo "=== Creating MongoDB Atlas Sink Connectors ==="
echo ""

# Enriched streams
create_sink "sink-naval-enriched"          "NAVAL_ENRICHED"          "naval_enriched"
create_sink "sink-cyber-enriched"          "CYBER_ENRICHED"          "cyber_enriched"
create_sink "sink-embm-enriched"           "EMBM_ENRICHED"           "embm_enriched"
create_sink "sink-gps-enriched"            "GPS_ENRICHED"            "gps_enriched"
create_sink "sink-aircraft-enriched"       "AIRCRAFT_ENRICHED"       "aircraft_enriched"
create_sink "sink-sigint-enriched"         "SIGINT_ENRICHED"         "sigint_enriched"
create_sink "sink-jsir-enriched"           "JSIR_ENRICHED"           "jsir_enriched"
create_sink "sink-satellite-enriched"      "SATELLITE_ENRICHED"      "satellite_enriched"

# SIGINT breakout streams
create_sink "sink-sigint-air-defense"      "SIGINT_AIR_DEFENSE"      "sigint_air_defense"
create_sink "sink-sigint-hostile-comms"    "SIGINT_HOSTILE_COMMS"    "sigint_hostile_comms"
create_sink "sink-sigint-jamming"          "SIGINT_JAMMING"          "sigint_jamming"
create_sink "sink-sigint-iff-tracks"       "SIGINT_IFF_TRACKS"       "sigint_iff_tracks"

# EMBM breakout streams
create_sink "sink-embm-strike-candidates"  "EMBM_STRIKE_CANDIDATES"  "embm_strike_candidates"
create_sink "sink-embm-active-jammers"     "EMBM_ACTIVE_JAMMERS"     "embm_active_jammers"
create_sink "sink-embm-collection-reqs"    "EMBM_COLLECTION_REQUIREMENTS" "embm_collection_requirements"

# Satellite streams
create_sink "sink-satellite-pass-enriched" "SATELLITE_PASS_ENRICHED" "satellite_pass_enriched"
create_sink "sink-satellite-coverage-alerts" "SATELLITE_COVERAGE_ALERTS" "satellite_coverage_alerts"

# Alert streams
create_sink "sink-alert-hostile-naval"     "ALERT_HOSTILE_NAVAL"     "alert_hostile_naval"
create_sink "sink-alert-cyber-critical"    "ALERT_CYBER_CRITICAL"    "alert_cyber_critical"

echo ""
echo "=== Verification ==="
echo ""
echo "Total connectors:"
curl -s "$CONNECT_URL/connectors" | python3 -c "import sys,json; data=json.load(sys.stdin); print(f'  {len(data)} connectors'); [print(f'  - {c}') for c in sorted(data)]"

echo ""
echo "Checking for errors in 10 seconds..."
sleep 10
echo ""
docker logs jadc2-kafka-connect 2>&1 | grep "ERROR" | grep -v "rest.errors\|Errors\$1\|jackson\|glassfish\|JsonParser\|ParserMinimal\|BeanDeserializer" | tail -10

echo ""
echo "Done! Check Atlas for new collections in jadc2_cop database."
