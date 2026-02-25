#!/bin/bash
# ============================================================
# JADC2 COP - ksqlDB CLEAN Setup (drops and recreates all)
# ============================================================

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

KSQL_SERVER="http://ksqldb-server:8088"

run_ksql() {
    local query="$1"
    local description="$2"
    echo -e "${CYAN}[ksqlDB]${NC} $description"
    docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "$query" 2>&1 | grep -v "^$" | grep -v "WARNING" | head -3
}

echo ""
echo -e "${YELLOW}=== DROPPING ALL EXISTING OBJECTS ===${NC}"

# Drop all derived streams first (in order of dependencies)
for obj in UNIFIED_TRACKS_NAVAL UNIFIED_TRACKS_AIR UNIFIED_TRACKS UNITS_IN_THREAT_ZONE \
           AIRCRAFT_WITH_GPS NAVAL_WITH_GPS CYBER_WITH_GPS SIGINT_WITH_CONTEXT \
           ALERT_FUEL_CRITICAL ALERT_AMMO_CRITICAL ALERT_EQUIPMENT ALERT_COMMS \
           ALERT_LOW_READINESS ALERT_THREAT_RADAR ALERT_HOSTILE_AIRCRAFT ALERT_SATCOM_JAMMING \
           GPS_ENRICHED SIGINT_ENRICHED SATELLITE_ENRICHED AIRCRAFT_ENRICHED; do
    run_ksql "DROP STREAM IF EXISTS $obj DELETE TOPIC;" "Dropping stream $obj"
done

# Drop tables
for obj in OPERATIONAL_PICTURE_BY_GRID GRID_FORCE_STATUS GRID_THREAT_SUMMARY GRID_ISR_COVERAGE \
           FORCE_COMPOSITION HOSTILE_AIRCRAFT_SUMMARY ACTIVE_THREAT_GRIDS GPS_LATEST; do
    run_ksql "DROP TABLE IF EXISTS $obj DELETE TOPIC;" "Dropping table $obj"
done

# Drop raw streams (don't delete underlying topics)
for obj in GPS_RAW SIGINT_RAW AIRCRAFT_RAW SATELLITE_RAW NAVAL_RAW CYBER_RAW JSIR_RAW EMBM_RAW; do
    run_ksql "DROP STREAM IF EXISTS $obj;" "Dropping raw stream $obj"
done

echo "Waiting 5 seconds for cleanup..."
sleep 5

echo ""
echo -e "${GREEN}=== CREATING RAW STREAMS ===${NC}"

run_ksql "CREATE STREAM gps_raw (
    unit_id VARCHAR KEY,
    unit_name VARCHAR,
    unit_type VARCHAR,
    timestamp BIGINT,
    lat DOUBLE,
    lon DOUBLE,
    heading_deg DOUBLE,
    speed_kph DOUBLE,
    altitude_m DOUBLE,
    fuel_level_pct DOUBLE,
    ammo_level_pct DOUBLE,
    personnel_count INT,
    equipment_status VARCHAR,
    mission_status VARCHAR,
    fuel_consumption_rate DOUBLE,
    comms_status VARCHAR
) WITH (KAFKA_TOPIC='ground-force-gps', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "gps_raw"

run_ksql "CREATE STREAM aircraft_raw (
    aircraft_id VARCHAR KEY,
    tail_number VARCHAR,
    aircraft_type VARCHAR,
    role VARCHAR,
    country VARCHAR,
    weapons VARCHAR,
    timestamp BIGINT,
    lat DOUBLE,
    lon DOUBLE,
    altitude_ft DOUBLE,
    heading_deg DOUBLE,
    speed_kts DOUBLE,
    status VARCHAR,
    fuel_pct DOUBLE,
    iff_mode VARCHAR
) WITH (KAFKA_TOPIC='military-aircraft', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "aircraft_raw"

run_ksql "CREATE STREAM naval_raw (
    vessel_id VARCHAR KEY,
    hull_number VARCHAR,
    vessel_type VARCHAR,
    vessel_class VARCHAR,
    displacement_tons INT,
    country VARCHAR,
    weapons VARCHAR,
    aircraft_capacity INT,
    timestamp BIGINT,
    lat DOUBLE,
    lon DOUBLE,
    heading_deg DOUBLE,
    speed_kts DOUBLE,
    status VARCHAR,
    iff_mode VARCHAR
) WITH (KAFKA_TOPIC='naval-vessels', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "naval_raw"

run_ksql "CREATE STREAM cyber_raw (
    incident_id VARCHAR KEY,
    timestamp BIGINT,
    threat_actor VARCHAR,
    actor_country VARCHAR,
    sophistication VARCHAR,
    attack_type VARCHAR,
    severity VARCHAR,
    target_facility_id VARCHAR,
    target_facility VARCHAR,
    target_system VARCHAR,
    target_type VARCHAR,
    target_region VARCHAR,
    target_lat DOUBLE,
    target_lon DOUBLE,
    source_ip VARCHAR,
    mitre_attack VARCHAR,
    status VARCHAR
) WITH (KAFKA_TOPIC='cyber-threats', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "cyber_raw"

run_ksql "CREATE STREAM sigint_raw (
    intercept_id VARCHAR KEY,
    timestamp BIGINT,
    signal_type VARCHAR,
    frequency_mhz DOUBLE,
    bandwidth_khz DOUBLE,
    modulation VARCHAR,
    bearing_deg DOUBLE,
    signal_strength_dbm DOUBLE,
    lat DOUBLE,
    lon DOUBLE,
    confidence_pct DOUBLE,
    emitter_classification VARCHAR,
    threat_level VARCHAR,
    emission_pattern VARCHAR,
    collector_id VARCHAR
) WITH (KAFKA_TOPIC='sigint-feeds', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "sigint_raw"

run_ksql "CREATE STREAM jsir_raw (
    incident_id VARCHAR KEY,
    timestamp BIGINT,
    event_type VARCHAR,
    workflow_stage VARCHAR,
    jammer_id VARCHAR,
    jammer_location VARCHAR,
    jammer_country VARCHAR,
    jammer_lat DOUBLE,
    jammer_lon DOUBLE,
    jammer_terrain VARCHAR,
    jammer_system VARCHAR,
    jammer_type VARCHAR,
    jammer_power_kw DOUBLE,
    target_satellite VARCHAR,
    target_sat_type VARCHAR,
    target_freq_ghz DOUBLE,
    interference_type VARCHAR,
    noise_floor_increase_db DOUBLE,
    geolocation_method VARCHAR,
    tdoa_milliseconds DOUBLE,
    fdoa_hertz DOUBLE,
    cep_km DOUBLE,
    uncertainty_ellipse_major_km DOUBLE,
    uncertainty_ellipse_minor_km DOUBLE,
    geolocation_confidence_pct DOUBLE,
    mission_impact VARCHAR,
    mission_impact_severity VARCHAR,
    jsir_ticket_status VARCHAR,
    recommended_action VARCHAR,
    strike_coords_lat DOUBLE,
    strike_coords_lon DOUBLE
) WITH (KAFKA_TOPIC='spectrum-interference', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "jsir_raw (SATCOM jamming events)"

run_ksql "CREATE STREAM embm_raw (
    track_id VARCHAR KEY,
    timestamp BIGINT,
    source_incident VARCHAR,
    track_type VARCHAR,
    track_category VARCHAR,
    emitter_function VARCHAR,
    lat DOUBLE,
    lon DOUBLE,
    position_accuracy_m DOUBLE,
    terrain VARCHAR,
    emitter_system VARCHAR,
    emitter_country VARCHAR,
    emitter_type VARCHAR,
    estimated_power_kw DOUBLE,
    spectral_signature VARCHAR,
    threat_level VARCHAR,
    confidence DOUBLE,
    targetable BOOLEAN,
    target_coord_lat DOUBLE,
    target_coord_lon DOUBLE,
    nearby_assets VARCHAR,
    display_icon VARCHAR,
    display_color VARCHAR,
    display_priority VARCHAR
) WITH (KAFKA_TOPIC='embm-tracks', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');" "embm_raw (EMBM-J tracks)"

echo ""
echo -e "${GREEN}=== CREATING GPS TABLE ===${NC}"

run_ksql "CREATE TABLE gps_latest AS
SELECT
    unit_id,
    LATEST_BY_OFFSET(unit_name) AS unit_name,
    LATEST_BY_OFFSET(unit_type) AS unit_type,
    LATEST_BY_OFFSET(lat) AS lat,
    LATEST_BY_OFFSET(lon) AS lon,
    LATEST_BY_OFFSET(heading_deg) AS heading_deg,
    LATEST_BY_OFFSET(speed_kph) AS speed_kph,
    LATEST_BY_OFFSET(altitude_m) AS altitude_m,
    LATEST_BY_OFFSET(fuel_level_pct) AS fuel_level_pct,
    LATEST_BY_OFFSET(mission_status) AS mission_status,
    LATEST_BY_OFFSET(comms_status) AS comms_status
FROM gps_raw
GROUP BY unit_id
EMIT CHANGES;" "gps_latest table"

echo "Waiting 8 seconds for GPS table..."
sleep 8

echo ""
echo -e "${GREEN}=== CREATING GPS-JOINED STREAMS ===${NC}"

run_ksql "CREATE STREAM aircraft_with_gps AS
SELECT
    a.aircraft_id AS aircraft_id,
    a.tail_number AS tail_number,
    a.aircraft_type AS aircraft_type,
    a.role AS role,
    a.country AS country,
    a.timestamp AS timestamp,
    a.lat AS aircraft_lat,
    a.lon AS aircraft_lon,
    a.altitude_ft AS altitude_ft,
    a.heading_deg AS heading_deg,
    a.speed_kts AS speed_kts,
    a.status AS status,
    a.fuel_pct AS fuel_pct,
    a.iff_mode AS iff_mode,
    g.unit_id AS gps_unit_id,
    g.lat AS gps_lat,
    g.lon AS gps_lon,
    g.mission_status AS gps_status,
    COALESCE(g.lat, a.lat) AS final_lat,
    COALESCE(g.lon, a.lon) AS final_lon
FROM aircraft_raw a
LEFT JOIN gps_latest g ON CONCAT('AC-', a.aircraft_id) = g.unit_id
EMIT CHANGES;" "aircraft_with_gps"

run_ksql "CREATE STREAM naval_with_gps AS
SELECT
    n.vessel_id AS vessel_id,
    n.hull_number AS hull_number,
    n.vessel_type AS vessel_type,
    n.vessel_class AS vessel_class,
    n.country AS country,
    n.timestamp AS timestamp,
    n.lat AS vessel_lat,
    n.lon AS vessel_lon,
    n.heading_deg AS heading_deg,
    n.speed_kts AS speed_kts,
    n.status AS status,
    n.iff_mode AS iff_mode,
    g.unit_id AS gps_unit_id,
    g.lat AS gps_lat,
    g.lon AS gps_lon,
    g.mission_status AS gps_status,
    COALESCE(g.lat, n.lat) AS final_lat,
    COALESCE(g.lon, n.lon) AS final_lon
FROM naval_raw n
LEFT JOIN gps_latest g ON CONCAT('SHIP-', n.hull_number) = g.unit_id
EMIT CHANGES;" "naval_with_gps"

run_ksql "CREATE STREAM cyber_with_gps AS
SELECT
    c.incident_id AS incident_id,
    c.timestamp AS timestamp,
    c.threat_actor AS threat_actor,
    c.actor_country AS actor_country,
    c.attack_type AS attack_type,
    c.severity AS severity,
    c.target_facility_id AS target_facility_id,
    c.target_facility AS target_facility,
    c.target_system AS target_system,
    c.target_region AS target_region,
    c.target_lat AS target_lat,
    c.target_lon AS target_lon,
    c.mitre_attack AS mitre_attack,
    c.status AS status,
    g.unit_id AS gps_unit_id,
    g.lat AS facility_gps_lat,
    g.lon AS facility_gps_lon,
    COALESCE(g.lat, c.target_lat) AS final_lat,
    COALESCE(g.lon, c.target_lon) AS final_lon
FROM cyber_raw c
LEFT JOIN gps_latest g ON c.target_facility_id = g.unit_id
EMIT CHANGES;" "cyber_with_gps"

echo ""
echo -e "${GREEN}=== CREATING ENRICHED STREAMS ===${NC}"

run_ksql "CREATE STREAM gps_enriched AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    timestamp,
    lat,
    lon,
    heading_deg,
    speed_kph,
    fuel_level_pct,
    ammo_level_pct,
    personnel_count,
    equipment_status,
    mission_status,
    comms_status,
    CONCAT('GRID-', CAST(CAST(lat/5 AS INT) AS VARCHAR), '-', CAST(CAST(lon/5 AS INT) AS VARCHAR)) AS grid_square,
    CAST(fuel_level_pct AS DOUBLE) * 0.4 + CAST(ammo_level_pct AS DOUBLE) * 0.4 + 
        CASE WHEN equipment_status = 'FULLY_OPERATIONAL' THEN CAST(20 AS DOUBLE) ELSE CAST(5 AS DOUBLE) END AS readiness_score,
    CASE
        WHEN CAST(fuel_level_pct AS DOUBLE) < CAST(15 AS DOUBLE) OR CAST(ammo_level_pct AS DOUBLE) < CAST(15 AS DOUBLE) THEN 'CRITICAL'
        WHEN CAST(fuel_level_pct AS DOUBLE) < CAST(30 AS DOUBLE) OR CAST(ammo_level_pct AS DOUBLE) < CAST(30 AS DOUBLE) THEN 'DEGRADED'
        ELSE 'COMBAT_READY'
    END AS combat_effectiveness
FROM gps_raw
EMIT CHANGES;" "gps_enriched"

run_ksql "CREATE STREAM sigint_enriched AS
SELECT
    intercept_id,
    timestamp,
    signal_type,
    frequency_mhz,
    signal_strength_dbm,
    lat,
    lon,
    emitter_classification,
    threat_level,
    emission_pattern,
    collector_id,
    confidence_pct,
    CONCAT('GRID-', CAST(CAST(lat/5 AS INT) AS VARCHAR), '-', CAST(CAST(lon/5 AS INT) AS VARCHAR)) AS grid_square,
    CASE WHEN threat_level = 'CRITICAL' THEN 100 WHEN threat_level = 'HIGH' THEN 75 ELSE 25 END AS threat_score,
    CASE WHEN emitter_classification LIKE '%RADAR%' THEN true ELSE false END AS is_air_defense
FROM sigint_raw
EMIT CHANGES;" "sigint_enriched"

run_ksql "CREATE STREAM aircraft_enriched AS
SELECT
    aircraft_id,
    tail_number,
    aircraft_type,
    role,
    country,
    timestamp,
    lat,
    lon,
    altitude_ft,
    heading_deg,
    speed_kts,
    status,
    fuel_pct,
    iff_mode,
    CASE WHEN role IN ('STEALTH', 'AIR_SUPERIORITY', 'STRIKE', 'BOMBER') THEN 'HIGH' ELSE 'MEDIUM' END AS computed_threat
FROM aircraft_raw
EMIT CHANGES;" "aircraft_enriched"

echo ""
echo -e "${GREEN}=== CREATING ALERT STREAMS ===${NC}"

run_ksql "CREATE STREAM alert_hostile_aircraft AS
SELECT
    aircraft_id,
    tail_number,
    aircraft_type,
    country,
    role,
    lat,
    lon,
    altitude_ft,
    speed_kts,
    status,
    heading_deg,
    timestamp,
    'HOSTILE_AIRCRAFT' AS alert_type,
    CASE WHEN status IN ('INTERCEPT', 'STRIKE') THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
    CONCAT('HOSTILE: ', country, ' ', aircraft_type, ' ', status) AS alert_message
FROM aircraft_enriched
WHERE iff_mode = 'HOSTILE'
EMIT CHANGES;" "alert_hostile_aircraft"

run_ksql "CREATE STREAM alert_satcom_jamming AS
SELECT
    incident_id,
    jammer_location,
    jammer_country,
    jammer_system,
    jammer_lat,
    jammer_lon,
    target_satellite,
    interference_type,
    mission_impact,
    mission_impact_severity AS severity,
    cep_km,
    geolocation_confidence_pct,
    recommended_action,
    strike_coords_lat,
    strike_coords_lon,
    timestamp,
    'SATCOM_JAMMING' AS alert_type,
    CONCAT('JSIR ALERT: ', jammer_country, ' ', jammer_system, ' jamming ', target_satellite, ' - ', mission_impact) AS alert_message
FROM jsir_raw
WHERE mission_impact_severity IN ('HIGH', 'CRITICAL')
EMIT CHANGES;" "alert_satcom_jamming (JSIR critical alerts)"

echo ""
echo -e "${GREEN}=== CREATING UNIFIED TRACKS ===${NC}"

run_ksql "CREATE STREAM unified_tracks_air AS
SELECT
    gps_unit_id,
    aircraft_id AS track_id,
    CONCAT(aircraft_type, ' (', tail_number, ')') AS track_name,
    role AS track_type,
    'AIR' AS domain,
    country,
    iff_mode AS iff_status,
    final_lat AS lat,
    final_lon AS lon,
    heading_deg,
    speed_kts * 1.852 AS speed_kph,
    altitude_ft * 0.3048 AS altitude_m,
    status,
    timestamp
FROM aircraft_with_gps
EMIT CHANGES;" "unified_tracks_air"

run_ksql "CREATE STREAM unified_tracks_naval AS
SELECT
    gps_unit_id,
    vessel_id AS track_id,
    CONCAT(vessel_type, ' (', hull_number, ')') AS track_name,
    vessel_class AS track_type,
    'NAVAL' AS domain,
    country,
    iff_mode AS iff_status,
    final_lat AS lat,
    final_lon AS lon,
    heading_deg,
    speed_kts * 1.852 AS speed_kph,
    CAST(0 AS DOUBLE) AS altitude_m,
    status,
    timestamp
FROM naval_with_gps
EMIT CHANGES;" "unified_tracks_naval"

echo ""
echo -e "${GREEN}============================================"
echo " ksqlDB Setup Complete!"
echo "============================================${NC}"
echo ""
echo "Streams created:"
echo "  RAW INPUT:"
echo "    • gps_raw, aircraft_raw, naval_raw, cyber_raw, sigint_raw"
echo "    • jsir_raw (SATCOM jamming), embm_raw (EMBM-J tracks)"
echo ""
echo "  ENRICHED:"
echo "    • gps_enriched, sigint_enriched, aircraft_enriched"
echo ""
echo "  GPS-JOINED:"
echo "    • aircraft_with_gps, naval_with_gps, cyber_with_gps"
echo ""
echo "  UNIFIED TRACKS:"
echo "    • unified_tracks_air, unified_tracks_naval"
echo ""
echo "  ALERTS:"
echo "    • alert_hostile_aircraft"
echo "    • alert_satcom_jamming (JSIR critical events)"
echo ""
echo "Tables created:"
echo "  • gps_latest"
echo ""
echo "Test commands:"
echo "  docker exec -it jadc2-ksqldb-cli ksql http://ksqldb-server:8088"
echo ""
echo "  -- JSIR Jamming Events:"
echo "  SELECT * FROM jsir_raw EMIT CHANGES LIMIT 5;"
echo ""
echo "  -- EMBM-J Hostile Emitter Tracks:"
echo "  SELECT * FROM embm_raw EMIT CHANGES LIMIT 5;"
echo ""
echo "  -- SATCOM Jamming Alerts:"
echo "  SELECT * FROM alert_satcom_jamming EMIT CHANGES LIMIT 5;"
echo ""
