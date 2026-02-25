#!/bin/bash
# ============================================================
# JADC2 COP - Complete Rebuild Script (Local MongoDB)
# ============================================================
# Recreates ALL ksqlDB streams (37), sink connectors (27),
# and source connectors (8) for the full JADC2 demo.
#
# Includes MongoDB replica set initialization for change streams.
#
# Usage: ./rebuild-jadc2.sh
# ============================================================

set -e

CONNECT_URL="http://localhost:8083"
KSQL_SERVER="http://ksqldb-server:8088"
MONGO_URI="mongodb://admin:jadc2secret@mongodb:27017/?authSource=admin"
DATABASE="jadc2_cop"
EXTERNAL_DB="jadc2_external_sources"

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# ============================================================
# HELPER FUNCTIONS
# ============================================================

run_ksql() {
    local query="$1"
    local description="$2"
    echo -e "${CYAN}[ksqlDB]${NC} $description"
    docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "$query" 2>&1 | grep -v "^$" | grep -v "WARNING" | head -3
}

create_sink() {
    local NAME=$1
    local TOPIC=$2
    local COLLECTION=$3
    local EXTRA_CONFIG=${4:-""}

    echo -e "${CYAN}[Connect]${NC} Creating sink: $NAME ($TOPIC -> $COLLECTION)"
    curl -s -X DELETE "$CONNECT_URL/connectors/$NAME" > /dev/null 2>&1
    sleep 1

    local CONFIG="{
        \"name\": \"$NAME\",
        \"config\": {
            \"connector.class\": \"com.mongodb.kafka.connect.MongoSinkConnector\",
            \"connection.uri\": \"$MONGO_URI\",
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
        echo -e "  ${RED}✗ Failed: $(echo $RESULT | jq -r '.message // .error_code')${NC}"
    else
        echo -e "  ${GREEN}✓ Created${NC}"
    fi
}

create_source() {
    local NAME=$1
    local COLLECTION=$2

    echo -e "${CYAN}[Connect]${NC} Creating source: $NAME ($EXTERNAL_DB.$COLLECTION)"
    curl -s -X DELETE "$CONNECT_URL/connectors/$NAME" > /dev/null 2>&1
    sleep 1

    RESULT=$(curl -s -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"$NAME\",
            \"config\": {
                \"connector.class\": \"com.mongodb.kafka.connect.MongoSourceConnector\",
                \"connection.uri\": \"$MONGO_URI\",
                \"database\": \"$EXTERNAL_DB\",
                \"collection\": \"$COLLECTION\",
                \"topic.prefix\": \"ext\",
                \"publish.full.document.only\": \"true\",
                \"output.format.value\": \"json\",
                \"copy.existing\": \"true\",
                \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
                \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
                \"value.converter.schemas.enable\": \"false\",
                \"tasks.max\": \"1\"
            }
        }")

    if echo "$RESULT" | grep -q "error_code"; then
        echo -e "  ${RED}✗ Failed: $(echo $RESULT | jq -r '.message // .error_code')${NC}"
    else
        echo -e "  ${GREEN}✓ Created${NC}"
    fi
}

# ============================================================
# PREFLIGHT CHECKS
# ============================================================

echo ""
echo -e "${YELLOW}============================================================${NC}"
echo -e "${YELLOW} JADC2 COP - Complete Rebuild (Local MongoDB)${NC}"
echo -e "${YELLOW}============================================================${NC}"
echo ""

# --- MongoDB Replica Set Check ---
echo "Checking MongoDB replica set..."
RS_OK=$(docker exec jadc2-mongodb mongosh -u admin -p jadc2secret --authenticationDatabase admin --quiet --eval 'try { rs.status().ok } catch(e) { 0 }' 2>/dev/null)

if [ "$RS_OK" != "1" ]; then
    echo -e "${YELLOW}Initializing MongoDB replica set...${NC}"
    docker exec jadc2-mongodb mongosh -u admin -p jadc2secret --authenticationDatabase admin --quiet --eval '
        rs.initiate({_id: "rs0", members: [{_id: 0, host: "mongodb:27017"}]})
    ' 2>/dev/null
    echo "Waiting 10 seconds for replica set election..."
    sleep 10
    RS_OK=$(docker exec jadc2-mongodb mongosh -u admin -p jadc2secret --authenticationDatabase admin --quiet --eval 'try { rs.status().ok } catch(e) { 0 }' 2>/dev/null)
    if [ "$RS_OK" == "1" ]; then
        echo -e "${GREEN}✓ Replica set initialized${NC}"
    else
        echo -e "${RED}ERROR: Replica set failed to initialize${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✓ MongoDB replica set is active${NC}"
fi

echo "Checking Kafka Connect..."
if ! curl -s "$CONNECT_URL/connectors" > /dev/null 2>&1; then
    echo -e "${RED}ERROR: Kafka Connect not available at $CONNECT_URL${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Kafka Connect is available${NC}"

echo "Checking ksqlDB..."
if ! docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "SHOW STREAMS;" > /dev/null 2>&1; then
    echo -e "${RED}ERROR: ksqlDB not available${NC}"
    exit 1
fi
echo -e "${GREEN}✓ ksqlDB is available${NC}"
echo ""

# ============================================================
# PART 1: CLEANUP
# ============================================================

echo -e "${YELLOW}=== PART 1: CLEANUP ===${NC}"
run_ksql "TERMINATE ALL;" "Terminating all persistent queries"
sleep 3

run_ksql "DROP STREAM IF EXISTS ALL_ALERTS DELETE TOPIC;" "Dropping ALL_ALERTS"

for obj in ALERT_FUEL_CRITICAL ALERT_AMMO_CRITICAL ALERT_EQUIPMENT ALERT_COMMS \
           ALERT_LOW_READINESS ALERT_THREAT_RADAR ALERT_HOSTILE_AIRCRAFT ALERT_SATCOM_JAMMING \
           ALERT_JSIR_JAMMING UNITS_IN_THREAT_ZONE ALERT_HOSTILE_NAVAL ALERT_CYBER_CRITICAL; do
    run_ksql "DROP STREAM IF EXISTS $obj DELETE TOPIC;" "Dropping $obj"
done

for obj in SIGINT_AIR_DEFENSE SIGINT_HOSTILE_COMMS SIGINT_JAMMING SIGINT_IFF_TRACKS \
           EMBM_STRIKE_CANDIDATES EMBM_ACTIVE_JAMMERS EMBM_COLLECTION_REQUIREMENTS \
           SATELLITE_PASS_ENRICHED SATELLITE_COVERAGE_ALERTS \
           NAVAL_ENRICHED CYBER_ENRICHED EMBM_ENRICHED; do
    run_ksql "DROP STREAM IF EXISTS $obj DELETE TOPIC;" "Dropping $obj"
done

for obj in GPS_ENRICHED SIGINT_ENRICHED SATELLITE_ENRICHED AIRCRAFT_ENRICHED \
           JSIR_ENRICHED AIRCRAFT_WITH_GPS NAVAL_WITH_GPS CYBER_WITH_GPS \
           SIGINT_WITH_CONTEXT UNIFIED_TRACKS UNIFIED_TRACKS_AIR UNIFIED_TRACKS_NAVAL; do
    run_ksql "DROP STREAM IF EXISTS $obj DELETE TOPIC;" "Dropping $obj"
done

for obj in OPERATIONAL_PICTURE_BY_GRID GRID_FORCE_STATUS GRID_THREAT_SUMMARY \
           GRID_ISR_COVERAGE FORCE_COMPOSITION HOSTILE_AIRCRAFT_SUMMARY \
           ACTIVE_THREAT_GRIDS GPS_LATEST; do
    run_ksql "DROP TABLE IF EXISTS $obj DELETE TOPIC;" "Dropping table $obj"
done

for obj in GPS_RAW AIRCRAFT_RAW NAVAL_RAW CYBER_RAW SIGINT_RAW SATELLITE_RAW JSIR_RAW EMBM_RAW; do
    run_ksql "DROP STREAM IF EXISTS $obj;" "Dropping raw $obj"
done

echo "Waiting 5 seconds for cleanup..."
sleep 5

# ============================================================
# PART 2-9: ksqlDB STREAMS (identical to Atlas version)
# ============================================================

echo ""
echo -e "${GREEN}=== PART 2: RAW STREAMS ===${NC}"

run_ksql "CREATE STREAM gps_raw (
    unit_id VARCHAR KEY, unit_name VARCHAR, unit_type VARCHAR, timestamp BIGINT,
    lat DOUBLE, lon DOUBLE, heading_deg DOUBLE, speed_kph DOUBLE, altitude_m DOUBLE,
    fuel_level_pct DOUBLE, ammo_level_pct DOUBLE, personnel_count INT,
    equipment_status VARCHAR, mission_status VARCHAR, fuel_consumption_rate DOUBLE, comms_status VARCHAR
) WITH (KAFKA_TOPIC='ground-force-gps', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "gps_raw"

run_ksql "CREATE STREAM aircraft_raw (
    aircraft_id VARCHAR KEY, tail_number VARCHAR, aircraft_type VARCHAR, role VARCHAR,
    country VARCHAR, weapons VARCHAR, timestamp BIGINT, lat DOUBLE, lon DOUBLE,
    altitude_ft DOUBLE, heading_deg DOUBLE, speed_kts DOUBLE, status VARCHAR,
    fuel_pct DOUBLE, iff_mode VARCHAR
) WITH (KAFKA_TOPIC='military-aircraft', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "aircraft_raw"

run_ksql "CREATE STREAM naval_raw (
    vessel_id VARCHAR KEY, hull_number VARCHAR, vessel_type VARCHAR, vessel_class VARCHAR,
    displacement_tons INT, country VARCHAR, weapons VARCHAR, aircraft_capacity INT,
    timestamp BIGINT, lat DOUBLE, lon DOUBLE, heading_deg DOUBLE, speed_kts DOUBLE,
    status VARCHAR, iff_mode VARCHAR
) WITH (KAFKA_TOPIC='naval-vessels', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "naval_raw"

run_ksql "CREATE STREAM cyber_raw (
    incident_id VARCHAR KEY, timestamp BIGINT, threat_actor VARCHAR, actor_country VARCHAR,
    sophistication VARCHAR, attack_type VARCHAR, severity VARCHAR, target_facility_id VARCHAR,
    target_facility VARCHAR, target_system VARCHAR, target_type VARCHAR, target_region VARCHAR,
    target_lat DOUBLE, target_lon DOUBLE, source_ip VARCHAR, mitre_attack VARCHAR, status VARCHAR
) WITH (KAFKA_TOPIC='cyber-threats', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "cyber_raw"

run_ksql "CREATE STREAM sigint_raw (
    intercept_id VARCHAR KEY, timestamp BIGINT, signal_type VARCHAR, frequency_mhz DOUBLE,
    bandwidth_khz DOUBLE, modulation VARCHAR, bearing_deg DOUBLE, signal_strength_dbm DOUBLE,
    lat DOUBLE, lon DOUBLE, confidence_pct DOUBLE, emitter_classification VARCHAR,
    threat_level VARCHAR, emission_pattern VARCHAR, collector_id VARCHAR
) WITH (KAFKA_TOPIC='sigint-feeds', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "sigint_raw"

run_ksql "CREATE STREAM satellite_raw (
    image_id VARCHAR KEY, source_id VARCHAR, timestamp BIGINT, sensor_type VARCHAR,
    resolution_m DOUBLE, cloud_cover_pct DOUBLE, quality_score DOUBLE,
    center_lat DOUBLE, center_lon DOUBLE, classification VARCHAR
) WITH (KAFKA_TOPIC='satellite-imagery-metadata', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "satellite_raw"

run_ksql "CREATE STREAM jsir_raw (
    incident_id VARCHAR KEY, timestamp BIGINT, event_type VARCHAR, workflow_stage VARCHAR,
    jammer_id VARCHAR, jammer_location VARCHAR, jammer_country VARCHAR,
    jammer_lat DOUBLE, jammer_lon DOUBLE, jammer_terrain VARCHAR, jammer_system VARCHAR,
    jammer_type VARCHAR, jammer_power_kw DOUBLE, jammer_frequency_band VARCHAR,
    target_satellite VARCHAR, target_orbit VARCHAR, target_frequency_ghz DOUBLE,
    tdoa_ms DOUBLE, fdoa_hz DOUBLE, cep_km DOUBLE,
    ellipse_major_km DOUBLE, ellipse_minor_km DOUBLE, ellipse_orientation DOUBLE,
    strike_coords_lat DOUBLE, strike_coords_lon DOUBLE,
    mission_impact VARCHAR, affected_unit_count INT, comms_degradation_pct DOUBLE
) WITH (KAFKA_TOPIC='spectrum-interference', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "jsir_raw"

run_ksql "CREATE STREAM embm_raw (
    track_id VARCHAR KEY, timestamp BIGINT, source_incident VARCHAR, track_type VARCHAR,
    track_category VARCHAR, emitter_function VARCHAR, lat DOUBLE, lon DOUBLE,
    position_accuracy_m DOUBLE, terrain VARCHAR, emitter_system VARCHAR, emitter_country VARCHAR,
    emitter_type VARCHAR, estimated_power_kw DOUBLE, spectral_signature VARCHAR,
    threat_level VARCHAR, confidence DOUBLE, targetable BOOLEAN,
    target_coord_lat DOUBLE, target_coord_lon DOUBLE, nearby_assets VARCHAR,
    display_icon VARCHAR, display_color VARCHAR, display_priority VARCHAR
) WITH (KAFKA_TOPIC='embm-tracks', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "embm_raw"

sleep 5

echo -e "${GREEN}=== PART 3: BASE ENRICHED STREAMS ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS gps_enriched AS
SELECT unit_id, unit_name, unit_type, timestamp, lat, lon, heading_deg, speed_kph, altitude_m,
    fuel_level_pct, ammo_level_pct, personnel_count, equipment_status, mission_status,
    fuel_consumption_rate, comms_status,
    CONCAT(CAST(CAST(lat AS INT) AS VARCHAR), CAST(CAST(lon AS INT) AS VARCHAR)) AS grid_square,
    CAST(fuel_level_pct * 0.3 + ammo_level_pct * 0.3 + 40.0 AS DOUBLE) AS readiness_score,
    CASE WHEN fuel_consumption_rate > 0 THEN CAST(fuel_level_pct / fuel_consumption_rate AS DOUBLE) ELSE CAST(999.0 AS DOUBLE) END AS predicted_fuel_hours,
    CASE WHEN fuel_level_pct >= 50.0 AND ammo_level_pct >= 50.0 THEN 'COMBAT_READY'
         WHEN fuel_level_pct >= 25.0 AND ammo_level_pct >= 25.0 THEN 'DEGRADED' ELSE 'CRITICAL' END AS combat_effectiveness
FROM gps_raw EMIT CHANGES;" "gps_enriched"

run_ksql "CREATE STREAM IF NOT EXISTS aircraft_enriched AS
SELECT aircraft_id, tail_number, aircraft_type, role, country, weapons, timestamp, lat, lon,
    altitude_ft, heading_deg, speed_kts, status, fuel_pct, iff_mode,
    CONCAT(CAST(CAST(lat AS INT) AS VARCHAR), CAST(CAST(lon AS INT) AS VARCHAR)) AS grid_square,
    CASE WHEN iff_mode = 'HOSTILE' THEN 'HOSTILE' WHEN iff_mode = 'UNKNOWN' THEN 'SUSPECT' ELSE 'FRIENDLY' END AS threat_classification
FROM aircraft_raw EMIT CHANGES;" "aircraft_enriched"

run_ksql "CREATE STREAM IF NOT EXISTS sigint_enriched AS
SELECT intercept_id, timestamp, signal_type, frequency_mhz, bandwidth_khz, modulation,
    bearing_deg, signal_strength_dbm, lat, lon, confidence_pct, emitter_classification,
    threat_level, emission_pattern, collector_id,
    CONCAT(CAST(CAST(lat AS INT) AS VARCHAR), CAST(CAST(lon AS INT) AS VARCHAR)) AS grid_square,
    CASE WHEN emitter_classification LIKE '%RADAR%' OR emitter_classification LIKE '%SAM%' OR emitter_classification LIKE '%S-300%' OR emitter_classification LIKE '%S-400%' THEN true ELSE false END AS is_air_defense,
    signal_strength_dbm * confidence_pct / 100.0 AS signal_quality,
    CASE WHEN threat_level = 'CRITICAL' THEN 100 WHEN threat_level = 'HIGH' THEN 75 WHEN threat_level = 'MEDIUM' THEN 50 ELSE 25 END AS threat_score
FROM sigint_raw EMIT CHANGES;" "sigint_enriched"

run_ksql "CREATE STREAM IF NOT EXISTS satellite_enriched AS
SELECT image_id, source_id, timestamp, sensor_type, resolution_m, cloud_cover_pct,
    quality_score, center_lat, center_lon, classification,
    CONCAT(CAST(CAST(center_lat AS INT) AS VARCHAR), CAST(CAST(center_lon AS INT) AS VARCHAR)) AS grid_square,
    CAST(quality_score * 100.0 * (1.0 - cloud_cover_pct / 100.0) * (1.0 / resolution_m * 10.0) AS DOUBLE) AS isr_effectiveness
FROM satellite_raw EMIT CHANGES;" "satellite_enriched"

run_ksql "CREATE STREAM IF NOT EXISTS jsir_enriched AS
SELECT incident_id, timestamp, event_type, workflow_stage, jammer_id, jammer_location,
    jammer_country, jammer_lat, jammer_lon, jammer_terrain, jammer_system, jammer_type,
    jammer_power_kw, jammer_frequency_band, target_satellite, target_orbit, target_frequency_ghz,
    tdoa_ms, fdoa_hz, cep_km, ellipse_major_km, ellipse_minor_km, ellipse_orientation,
    strike_coords_lat, strike_coords_lon, mission_impact, affected_unit_count, comms_degradation_pct,
    CONCAT(CAST(CAST(jammer_lat AS INT) AS VARCHAR), CAST(CAST(jammer_lon AS INT) AS VARCHAR)) AS jammer_grid,
    CASE WHEN mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT') THEN 'CRITICAL'
         WHEN mission_impact IN ('DEGRADED_COMMS', 'REDUCED_BANDWIDTH') THEN 'HIGH'
         WHEN mission_impact = 'MINIMAL_IMPACT' THEN 'MEDIUM' ELSE 'LOW' END AS severity,
    CASE WHEN cep_km <= 1.0 THEN 'PRECISION_STRIKE_VIABLE' WHEN cep_km <= 3.0 THEN 'AREA_STRIKE_VIABLE'
         WHEN cep_km <= 5.0 THEN 'REQUIRES_REFINEMENT' ELSE 'GEOLOCATION_INSUFFICIENT' END AS strike_viability,
    CASE WHEN mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT') AND cep_km <= 3.0 THEN 'KINETIC_STRIKE_RECOMMENDED'
         WHEN mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT') THEN 'ADDITIONAL_GEOLOCATION_REQUIRED'
         WHEN mission_impact IN ('DEGRADED_COMMS', 'REDUCED_BANDWIDTH') THEN 'FREQUENCY_HOP_AND_MONITOR'
         ELSE 'CONTINUE_MONITORING' END AS recommended_response
FROM jsir_raw EMIT CHANGES;" "jsir_enriched"

sleep 5

echo -e "${GREEN}=== PART 4: ADVANCED ENRICHED STREAMS ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS naval_enriched AS
SELECT vessel_id, hull_number, vessel_type, vessel_class, country, weapons, timestamp,
    lat, lon, heading_deg, speed_kts, status, iff_mode,
    CONCAT(CAST(CAST(lat AS INT) AS VARCHAR), CAST(CAST(lon AS INT) AS VARCHAR)) AS grid_square,
    CASE WHEN iff_mode = 'HOSTILE' THEN 'HOSTILE' WHEN iff_mode = 'UNKNOWN' THEN 'SUSPECT' ELSE 'FRIENDLY' END AS threat_classification,
    CASE WHEN vessel_type IN ('SUBMARINE', 'DESTROYER', 'CRUISER', 'CARRIER') THEN 'CAPITAL' ELSE 'SUPPORT' END AS vessel_category
FROM naval_raw EMIT CHANGES;" "naval_enriched"

run_ksql "CREATE STREAM IF NOT EXISTS cyber_enriched AS
SELECT incident_id, timestamp, threat_actor, actor_country, sophistication, attack_type,
    severity, target_facility_id, target_facility, target_system, target_type,
    target_region, target_lat, target_lon, source_ip, mitre_attack, status,
    CONCAT(CAST(CAST(target_lat AS INT) AS VARCHAR), CAST(CAST(target_lon AS INT) AS VARCHAR)) AS grid_square,
    CASE WHEN severity = 'CRITICAL' THEN 100 WHEN severity = 'HIGH' THEN 75 WHEN severity = 'MEDIUM' THEN 50 ELSE 25 END AS threat_score,
    CASE WHEN sophistication IN ('NATION_STATE', 'APT') THEN 'STATE_SPONSORED' ELSE 'NON_STATE' END AS actor_category
FROM cyber_raw EMIT CHANGES;" "cyber_enriched"

run_ksql "CREATE STREAM IF NOT EXISTS embm_enriched AS
SELECT track_id, timestamp, source_incident, track_type, track_category, emitter_function,
    lat, lon, position_accuracy_m, terrain, emitter_system, emitter_country, emitter_type,
    estimated_power_kw, spectral_signature, threat_level, confidence, targetable,
    target_coord_lat, target_coord_lon, nearby_assets, display_icon, display_color, display_priority,
    CONCAT(CAST(CAST(lat AS INT) AS VARCHAR), CAST(CAST(lon AS INT) AS VARCHAR)) AS grid_square,
    CASE WHEN threat_level = 'CRITICAL' THEN 100 WHEN threat_level = 'HIGH' THEN 75 WHEN threat_level = 'MEDIUM' THEN 50 ELSE 25 END AS threat_score
FROM embm_raw EMIT CHANGES;" "embm_enriched"

sleep 3

echo -e "${GREEN}=== PART 5: SIGINT BREAKOUT STREAMS ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS sigint_air_defense AS SELECT * FROM sigint_enriched WHERE is_air_defense = true AND threat_level IN ('CRITICAL', 'HIGH') EMIT CHANGES;" "sigint_air_defense"
run_ksql "CREATE STREAM IF NOT EXISTS sigint_hostile_comms AS SELECT * FROM sigint_enriched WHERE signal_type IN ('VOICE', 'DATA', 'BURST') AND threat_level IN ('CRITICAL', 'HIGH', 'MEDIUM') EMIT CHANGES;" "sigint_hostile_comms"
run_ksql "CREATE STREAM IF NOT EXISTS sigint_jamming AS SELECT * FROM sigint_enriched WHERE emitter_classification LIKE '%JAMM%' OR emission_pattern = 'CONTINUOUS' EMIT CHANGES;" "sigint_jamming"
run_ksql "CREATE STREAM IF NOT EXISTS sigint_iff_tracks AS SELECT * FROM sigint_enriched WHERE emitter_classification LIKE '%IFF%' OR emitter_classification LIKE '%TRANSPONDER%' EMIT CHANGES;" "sigint_iff_tracks"

echo -e "${GREEN}=== PART 6: EMBM BREAKOUT STREAMS ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS embm_strike_candidates AS SELECT * FROM embm_enriched WHERE targetable = true AND threat_level IN ('CRITICAL', 'HIGH') EMIT CHANGES;" "embm_strike_candidates"
run_ksql "CREATE STREAM IF NOT EXISTS embm_active_jammers AS SELECT * FROM embm_enriched WHERE emitter_function LIKE '%JAMM%' OR track_category = 'JAMMER' EMIT CHANGES;" "embm_active_jammers"
run_ksql "CREATE STREAM IF NOT EXISTS embm_collection_requirements AS SELECT * FROM embm_enriched WHERE confidence < 0.6 OR position_accuracy_m > 500 EMIT CHANGES;" "embm_collection_requirements"

echo -e "${GREEN}=== PART 7: SATELLITE STREAMS ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS satellite_pass_enriched AS
SELECT image_id, source_id, timestamp, sensor_type, resolution_m, cloud_cover_pct,
    quality_score, center_lat, center_lon, classification, grid_square, isr_effectiveness,
    CASE WHEN cloud_cover_pct < 20 AND quality_score > 0.7 THEN 'OPTIMAL' WHEN cloud_cover_pct < 50 THEN 'USABLE' ELSE 'DEGRADED' END AS collection_quality
FROM satellite_enriched EMIT CHANGES;" "satellite_pass_enriched"

run_ksql "CREATE STREAM IF NOT EXISTS satellite_coverage_alerts AS SELECT * FROM satellite_enriched WHERE cloud_cover_pct > 80 OR isr_effectiveness < 50.0 EMIT CHANGES;" "satellite_coverage_alerts"

sleep 3

echo -e "${GREEN}=== PART 8: ALERT STREAMS ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS alert_fuel_critical AS SELECT unit_id, unit_name, unit_type, grid_square, fuel_level_pct, predicted_fuel_hours, mission_status, lat, lon, 'FUEL_CRITICAL' AS alert_type, CASE WHEN predicted_fuel_hours < 2.0 THEN 'CRITICAL' WHEN predicted_fuel_hours < 4.0 THEN 'HIGH' ELSE 'WARNING' END AS severity, CONCAT('FUEL EMERGENCY: ', unit_name, ' at ', grid_square, ' has only ', CAST(ROUND(predicted_fuel_hours * 10.0) / 10.0 AS VARCHAR), ' hours remaining. Level: ', CAST(ROUND(fuel_level_pct) AS VARCHAR), '%') AS alert_message, timestamp FROM gps_enriched WHERE predicted_fuel_hours < 6.0 EMIT CHANGES;" "alert_fuel_critical"

run_ksql "CREATE STREAM IF NOT EXISTS alert_ammo_critical AS SELECT unit_id, unit_name, unit_type, grid_square, ammo_level_pct, mission_status, lat, lon, 'AMMO_CRITICAL' AS alert_type, CASE WHEN ammo_level_pct < 15.0 THEN 'CRITICAL' WHEN ammo_level_pct < 25.0 THEN 'HIGH' ELSE 'WARNING' END AS severity, CONCAT('AMMO CRITICAL: ', unit_name, ' at ', grid_square, ' ammunition at ', CAST(ROUND(ammo_level_pct) AS VARCHAR), '%. Immediate resupply required.') AS alert_message, timestamp FROM gps_enriched WHERE ammo_level_pct < 30.0 AND mission_status = 'ENGAGED' EMIT CHANGES;" "alert_ammo_critical"

run_ksql "CREATE STREAM IF NOT EXISTS alert_equipment AS SELECT unit_id, unit_name, unit_type, grid_square, equipment_status, mission_status, lat, lon, 'EQUIPMENT_FAILURE' AS alert_type, CASE WHEN equipment_status = 'NON_OPERATIONAL' THEN 'CRITICAL' WHEN equipment_status = 'MAINTENANCE_REQUIRED' THEN 'HIGH' ELSE 'WARNING' END AS severity, CONCAT('EQUIPMENT: ', unit_name, ' at ', grid_square, ' - ', equipment_status, ' (Readiness: ', CAST(CAST(readiness_score AS INT) AS VARCHAR), '%)') AS alert_message, timestamp FROM gps_enriched WHERE equipment_status IN ('NON_OPERATIONAL', 'MAINTENANCE_REQUIRED') EMIT CHANGES;" "alert_equipment"

run_ksql "CREATE STREAM IF NOT EXISTS alert_comms AS SELECT unit_id, unit_name, unit_type, grid_square, comms_status, mission_status, lat, lon, 'COMMS_ALERT' AS alert_type, CASE WHEN comms_status = 'OFFLINE' THEN 'CRITICAL' ELSE 'WARNING' END AS severity, CONCAT('COMMS ', comms_status, ': ', unit_name, ' at ', grid_square, ' - Last pos: ', CAST(lat AS VARCHAR), ', ', CAST(lon AS VARCHAR)) AS alert_message, timestamp FROM gps_enriched WHERE comms_status IN ('DEGRADED', 'OFFLINE') EMIT CHANGES;" "alert_comms"

run_ksql "CREATE STREAM IF NOT EXISTS alert_low_readiness AS SELECT unit_id, unit_name, unit_type, grid_square, readiness_score, combat_effectiveness, fuel_level_pct, ammo_level_pct, equipment_status, lat, lon, 'LOW_READINESS' AS alert_type, CASE WHEN readiness_score < 30.0 THEN 'CRITICAL' WHEN readiness_score < 40.0 THEN 'HIGH' ELSE 'WARNING' END AS severity, CONCAT('LOW READINESS: ', unit_name, ' - ', CAST(CAST(readiness_score AS INT) AS VARCHAR), '% (', combat_effectiveness, ')') AS alert_message, timestamp FROM gps_enriched WHERE readiness_score < 50.0 EMIT CHANGES;" "alert_low_readiness"

run_ksql "CREATE STREAM IF NOT EXISTS alert_threat_radar AS SELECT intercept_id, emitter_classification, threat_level, grid_square, lat, lon, signal_strength_dbm, signal_quality, frequency_mhz, collector_id, 'THREAT_RADAR' AS alert_type, threat_level AS severity, CONCAT('THREAT: ', emitter_classification, ' in ', grid_square, ' - ', CAST(ROUND(signal_strength_dbm) AS VARCHAR), ' dBm') AS alert_message, timestamp FROM sigint_enriched WHERE is_air_defense = true AND signal_strength_dbm > -70.0 AND threat_level IN ('CRITICAL', 'HIGH') EMIT CHANGES;" "alert_threat_radar"

run_ksql "CREATE STREAM IF NOT EXISTS alert_hostile_aircraft AS SELECT aircraft_id, tail_number, aircraft_type, country, lat, lon, altitude_ft, heading_deg, speed_kts, status, weapons, iff_mode, 'HOSTILE_AIRCRAFT' AS alert_type, CASE WHEN status IN ('INTERCEPT', 'STRIKE', 'ATTACK') THEN 'CRITICAL' ELSE 'HIGH' END AS severity, CONCAT('HOSTILE AIRCRAFT: ', aircraft_type, ' (', country, ') ', tail_number, ' - Status: ', status, ' at ', CAST(CAST(altitude_ft AS INT) AS VARCHAR), 'ft, ', CAST(CAST(speed_kts AS INT) AS VARCHAR), 'kts') AS alert_message, timestamp FROM aircraft_enriched WHERE iff_mode = 'HOSTILE' AND status IN ('INTERCEPT', 'STRIKE', 'PATROL', 'ATTACK') EMIT CHANGES;" "alert_hostile_aircraft"

run_ksql "CREATE STREAM IF NOT EXISTS alert_satcom_jamming AS SELECT incident_id, jammer_location, jammer_country, jammer_system, jammer_lat, jammer_lon, jammer_grid, target_satellite, mission_impact, affected_unit_count, cep_km, strike_coords_lat, strike_coords_lon, severity, strike_viability, recommended_response, 'SATCOM_JAMMING' AS alert_type, CONCAT('SATCOM JAMMING: ', jammer_system, ' (', jammer_country, ') targeting ', target_satellite, '. Impact: ', mission_impact, '. Affected: ', CAST(affected_unit_count AS VARCHAR), '. CEP: ', CAST(cep_km AS VARCHAR), 'km') AS alert_message, timestamp FROM jsir_enriched WHERE mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT', 'DEGRADED_COMMS', 'REDUCED_BANDWIDTH') EMIT CHANGES;" "alert_satcom_jamming"

run_ksql "CREATE STREAM IF NOT EXISTS alert_hostile_naval AS SELECT vessel_id, hull_number, vessel_type, vessel_class, country, lat, lon, heading_deg, speed_kts, status, weapons, iff_mode, grid_square, 'HOSTILE_NAVAL' AS alert_type, CASE WHEN vessel_category = 'CAPITAL' THEN 'CRITICAL' ELSE 'HIGH' END AS severity, CONCAT('HOSTILE NAVAL: ', vessel_type, ' (', country, ') ', hull_number, ' - ', status, ' at ', CAST(CAST(speed_kts AS INT) AS VARCHAR), 'kts') AS alert_message, timestamp FROM naval_enriched WHERE iff_mode = 'HOSTILE' EMIT CHANGES;" "alert_hostile_naval"

run_ksql "CREATE STREAM IF NOT EXISTS alert_cyber_critical AS SELECT incident_id, threat_actor, actor_country, attack_type, severity, target_facility, target_system, target_lat AS lat, target_lon AS lon, grid_square, source_ip, mitre_attack, status, 'CYBER_CRITICAL' AS alert_type, CONCAT('CYBER: ', attack_type, ' by ', threat_actor, ' (', actor_country, ') targeting ', target_facility, ' - ', severity) AS alert_message, timestamp FROM cyber_enriched WHERE severity IN ('CRITICAL', 'HIGH') EMIT CHANGES;" "alert_cyber_critical"

sleep 5

echo -e "${GREEN}=== PART 9: UNIFIED ALERTS ===${NC}"

run_ksql "CREATE STREAM all_alerts (alert_id VARCHAR KEY, alert_type VARCHAR, severity VARCHAR, alert_message VARCHAR, entity_id VARCHAR, grid_square VARCHAR, lat DOUBLE, lon DOUBLE, timestamp BIGINT) WITH (KAFKA_TOPIC = 'all-alerts', VALUE_FORMAT = 'JSON', PARTITIONS = 3);" "all_alerts stream"

run_ksql "INSERT INTO all_alerts SELECT CONCAT('FUEL-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id, alert_type, severity, alert_message, unit_id AS entity_id, grid_square, lat, lon, timestamp FROM alert_fuel_critical PARTITION BY CONCAT('FUEL-', unit_id, '-', CAST(timestamp AS VARCHAR)) EMIT CHANGES;" "INSERT fuel alerts"
run_ksql "INSERT INTO all_alerts SELECT CONCAT('AMMO-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id, alert_type, severity, alert_message, unit_id AS entity_id, grid_square, lat, lon, timestamp FROM alert_ammo_critical PARTITION BY CONCAT('AMMO-', unit_id, '-', CAST(timestamp AS VARCHAR)) EMIT CHANGES;" "INSERT ammo alerts"
run_ksql "INSERT INTO all_alerts SELECT CONCAT('EQUIP-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id, alert_type, severity, alert_message, unit_id AS entity_id, grid_square, lat, lon, timestamp FROM alert_equipment PARTITION BY CONCAT('EQUIP-', unit_id, '-', CAST(timestamp AS VARCHAR)) EMIT CHANGES;" "INSERT equipment alerts"
run_ksql "INSERT INTO all_alerts SELECT CONCAT('COMMS-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id, alert_type, severity, alert_message, unit_id AS entity_id, grid_square, lat, lon, timestamp FROM alert_comms PARTITION BY CONCAT('COMMS-', unit_id, '-', CAST(timestamp AS VARCHAR)) EMIT CHANGES;" "INSERT comms alerts"
run_ksql "INSERT INTO all_alerts SELECT CONCAT('READY-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id, alert_type, severity, alert_message, unit_id AS entity_id, grid_square, lat, lon, timestamp FROM alert_low_readiness PARTITION BY CONCAT('READY-', unit_id, '-', CAST(timestamp AS VARCHAR)) EMIT CHANGES;" "INSERT readiness alerts"
run_ksql "INSERT INTO all_alerts SELECT CONCAT('THREAT-', intercept_id) AS alert_id, alert_type, severity, alert_message, intercept_id AS entity_id, grid_square, lat, lon, timestamp FROM alert_threat_radar PARTITION BY CONCAT('THREAT-', intercept_id) EMIT CHANGES;" "INSERT threat radar alerts"
run_ksql "INSERT INTO all_alerts SELECT CONCAT('HOSTILE-', aircraft_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id, alert_type, severity, alert_message, aircraft_id AS entity_id, CONCAT(CAST(CAST(lat AS INT) AS VARCHAR), CAST(CAST(lon AS INT) AS VARCHAR)) AS grid_square, lat, lon, timestamp FROM alert_hostile_aircraft PARTITION BY CONCAT('HOSTILE-', aircraft_id, '-', CAST(timestamp AS VARCHAR)) EMIT CHANGES;" "INSERT hostile aircraft alerts"
run_ksql "INSERT INTO all_alerts SELECT CONCAT('JSIR-', incident_id) AS alert_id, alert_type, severity, alert_message, incident_id AS entity_id, jammer_grid AS grid_square, jammer_lat AS lat, jammer_lon AS lon, timestamp FROM alert_satcom_jamming PARTITION BY CONCAT('JSIR-', incident_id) EMIT CHANGES;" "INSERT JSIR alerts"

sleep 5

# ============================================================
# PART 10: MONGODB SINK CONNECTORS (27)
# ============================================================

echo -e "${GREEN}=== PART 10: SINK CONNECTORS (27) ===${NC}"

UPSERT_CONFIG=',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy",
    "document.id.strategy.overwrite.existing": "true"'

create_sink "sink-military-aircraft" "military-aircraft" "military_aircraft" "$UPSERT_CONFIG"
create_sink "sink-naval-vessels" "naval-vessels" "naval_vessels" "$UPSERT_CONFIG"
create_sink "sink-ground-force-gps" "ground-force-gps" "common_operating_picture" "$UPSERT_CONFIG"

APPEND_CONFIG=',
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy"'

create_sink "sink-cyber-threats" "cyber-threats" "cyber_threats" "$APPEND_CONFIG"
create_sink "sink-sigint-feeds" "sigint-feeds" "sigint_intercepts" "$APPEND_CONFIG"
create_sink "sink-spectrum-interference" "spectrum-interference" "jsir_incidents" "$APPEND_CONFIG"
create_sink "sink-satellite-imagery" "satellite-imagery-metadata" "satellite_imagery" "$APPEND_CONFIG"
create_sink "sink-all-alerts" "all-alerts" "operational_alerts" "$APPEND_CONFIG"
create_sink "sink-gps-enriched" "GPS_ENRICHED" "gps_enriched" "$APPEND_CONFIG"
create_sink "sink-aircraft-enriched" "AIRCRAFT_ENRICHED" "aircraft_enriched" "$APPEND_CONFIG"
create_sink "sink-naval-enriched" "NAVAL_ENRICHED" "naval_enriched" "$APPEND_CONFIG"
create_sink "sink-cyber-enriched" "CYBER_ENRICHED" "cyber_enriched" "$APPEND_CONFIG"
create_sink "sink-embm-enriched" "EMBM_ENRICHED" "embm_enriched" "$APPEND_CONFIG"
create_sink "sink-sigint-enriched" "SIGINT_ENRICHED" "sigint_enriched" "$APPEND_CONFIG"
create_sink "sink-jsir-enriched" "JSIR_ENRICHED" "jsir_enriched" "$APPEND_CONFIG"
create_sink "sink-satellite-enriched" "SATELLITE_ENRICHED" "satellite_enriched" "$APPEND_CONFIG"
create_sink "sink-sigint-air-defense" "SIGINT_AIR_DEFENSE" "sigint_air_defense" "$APPEND_CONFIG"
create_sink "sink-sigint-hostile-comms" "SIGINT_HOSTILE_COMMS" "sigint_hostile_comms" "$APPEND_CONFIG"
create_sink "sink-sigint-jamming" "SIGINT_JAMMING" "sigint_jamming" "$APPEND_CONFIG"
create_sink "sink-sigint-iff-tracks" "SIGINT_IFF_TRACKS" "sigint_iff_tracks" "$APPEND_CONFIG"
create_sink "sink-embm-strike-candidates" "EMBM_STRIKE_CANDIDATES" "embm_strike_candidates" "$APPEND_CONFIG"
create_sink "sink-embm-active-jammers" "EMBM_ACTIVE_JAMMERS" "embm_active_jammers" "$APPEND_CONFIG"
create_sink "sink-embm-collection-reqs" "EMBM_COLLECTION_REQUIREMENTS" "embm_collection_requirements" "$APPEND_CONFIG"
create_sink "sink-satellite-pass-enriched" "SATELLITE_PASS_ENRICHED" "satellite_pass_enriched" "$APPEND_CONFIG"
create_sink "sink-satellite-coverage-alerts" "SATELLITE_COVERAGE_ALERTS" "satellite_coverage_alerts" "$APPEND_CONFIG"
create_sink "sink-alert-hostile-naval" "ALERT_HOSTILE_NAVAL" "alert_hostile_naval" "$APPEND_CONFIG"
create_sink "sink-alert-cyber-critical" "ALERT_CYBER_CRITICAL" "alert_cyber_critical" "$APPEND_CONFIG"

sleep 5

# ============================================================
# PART 11: MONGODB SOURCE CONNECTORS (8)
# ============================================================

echo -e "${GREEN}=== PART 11: SOURCE CONNECTORS (8) ===${NC}"

create_source "source-dia-country-assessments" "dia_country_assessments"
create_source "source-dia-threat-actors"       "dia_threat_actors"
create_source "source-dia-threat-reports"       "dia_threat_reports"
create_source "source-dia-iocs"                 "dia_iocs"
create_source "source-centcom-readiness"        "centcom_unit_readiness"
create_source "source-centcom-equipment"        "centcom_equipment_status"
create_source "source-dla-supply-levels"        "dla_supply_levels"
create_source "source-dla-shipments"            "dla_shipments"

echo ""
echo "Waiting 10 seconds for all connectors..."
sleep 10

# ============================================================
# PART 12: VERIFICATION
# ============================================================

echo ""
echo -e "${GREEN}=== VERIFICATION ===${NC}"

echo ""
echo "ksqlDB Streams:"
STREAM_COUNT=$(docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "SHOW STREAMS;" 2>/dev/null | grep -c "|")
echo "  Total: $STREAM_COUNT streams"

echo ""
echo "ksqlDB Queries:"
QUERY_COUNT=$(docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "SHOW QUERIES;" 2>/dev/null | grep "RUNNING" | wc -l)
echo "  Total: $QUERY_COUNT running queries"

echo ""
echo "Connector Status:"
CONNECTORS=$(curl -s "$CONNECT_URL/connectors")
TOTAL=$(echo "$CONNECTORS" | jq -r '.[]' | wc -l)
echo "  Total: $TOTAL connectors"
echo "$CONNECTORS" | jq -r '.[]' | while read connector; do
    STATUS=$(curl -s "$CONNECT_URL/connectors/$connector/status" | jq -r '.connector.state')
    TASK_STATUS=$(curl -s "$CONNECT_URL/connectors/$connector/status" | jq -r '.tasks[0].state // "NO_TASKS"')
    if [ "$STATUS" == "RUNNING" ] && [ "$TASK_STATUS" == "RUNNING" ]; then
        echo -e "  ${GREEN}✓${NC} $connector"
    else
        echo -e "  ${RED}✗${NC} $connector: $STATUS (task: $TASK_STATUS)"
    fi
done

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN} Rebuild Complete!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "  Streams:    $STREAM_COUNT"
echo "  Queries:    $QUERY_COUNT"
echo "  Connectors: $TOTAL (27 sinks + 8 sources)"
echo ""
echo "Services:"
echo "  COP Dashboard:  http://localhost:3000"
echo "  Control Center:  http://localhost:9021"
echo "  Mongo Express:   http://localhost:8082  (admin/jadc2)"
echo "  ksqlDB:          http://localhost:8088"
echo "  Kafka Connect:   http://localhost:8083"
echo ""
