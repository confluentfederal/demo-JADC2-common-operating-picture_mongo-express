#!/bin/bash
# ============================================================
# JADC2 COP - ksqlDB Stream Processing Setup
# Run this AFTER the main deployment is up and running
# ============================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${CYAN}[ksqlDB]${NC} $1"; }
success() { echo -e "${GREEN}[✓]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; }

KSQL_SERVER="http://ksqldb-server:8088"

run_ksql() {
    local query="$1"
    local description="$2"
    log "Creating: $description"
    docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "$query" 2>&1 | grep -v "^$" | head -5
}

echo ""
echo -e "${CYAN}============================================"
echo " JADC2 ksqlDB Multi-Domain Stream Processing"
echo "============================================${NC}"
echo ""

# Check ksqlDB is running
log "Checking ksqlDB server..."
for i in {1..30}; do
    if docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "SHOW STREAMS;" &>/dev/null; then
        success "ksqlDB is ready"
        break
    fi
    echo "  Waiting for ksqlDB... ($i/30)"
    sleep 2
done

# ============================================================
# STEP 1: RAW INPUT STREAMS
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 1: Creating Raw Input Streams ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS gps_raw (
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
) WITH (
    KAFKA_TOPIC = 'ground-force-gps',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);" "gps_raw stream"

run_ksql "CREATE STREAM IF NOT EXISTS sigint_raw (
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
    elevation_deg DOUBLE,
    confidence_pct DOUBLE,
    emitter_classification VARCHAR,
    threat_level VARCHAR,
    emission_pattern VARCHAR,
    collector_id VARCHAR
) WITH (
    KAFKA_TOPIC = 'sigint-feeds',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);" "sigint_raw stream"

run_ksql "CREATE STREAM IF NOT EXISTS aircraft_raw (
    aircraft_id VARCHAR KEY,
    tail_number VARCHAR,
    aircraft_type VARCHAR,
    role VARCHAR,
    country VARCHAR,
    timestamp BIGINT,
    lat DOUBLE,
    lon DOUBLE,
    altitude_m DOUBLE,
    altitude_ft DOUBLE,
    heading_deg DOUBLE,
    speed_kph DOUBLE,
    speed_kts DOUBLE,
    status VARCHAR,
    fuel_pct DOUBLE,
    threat_level VARCHAR,
    iff_mode VARCHAR
) WITH (
    KAFKA_TOPIC = 'military-aircraft',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);" "aircraft_raw stream (Russian & Chinese military aircraft)"

run_ksql "CREATE STREAM IF NOT EXISTS satellite_raw (
    image_id VARCHAR KEY,
    source_id VARCHAR,
    timestamp BIGINT,
    acquisition_time VARCHAR,
    sensor_type VARCHAR,
    resolution_m DOUBLE,
    cloud_cover_pct DOUBLE,
    sun_elevation_deg DOUBLE,
    off_nadir_angle_deg DOUBLE,
    center_lat DOUBLE,
    center_lon DOUBLE,
    classification VARCHAR,
    quality_score DOUBLE,
    processing_status VARCHAR
) WITH (
    KAFKA_TOPIC = 'satellite-imagery-metadata',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);" "satellite_raw stream"

# ============================================================
# STEP 2: ENRICHED STREAMS
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 2: Creating Enriched Streams ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS gps_enriched AS
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
    fuel_consumption_rate,
    comms_status,
    CONCAT('33T',
        CASE WHEN lon < 35.75 THEN 'W' WHEN lon < 36.25 THEN 'X' ELSE 'Y' END,
        CASE WHEN lat < 44.25 THEN 'M' ELSE 'N' END
    ) AS grid_square,
    CASE 
        WHEN fuel_consumption_rate > 0 
        THEN ROUND((fuel_level_pct / 100.0) * 1000.0 / fuel_consumption_rate, 1)
        ELSE 999.0
    END AS fuel_hours_remaining,
    ROUND(
        (fuel_level_pct * 0.30) +
        (ammo_level_pct * 0.30) +
        (CASE 
            WHEN equipment_status = 'FULLY_OPERATIONAL' THEN 100.0
            WHEN equipment_status = 'DEGRADED' THEN 60.0
            WHEN equipment_status = 'MAINTENANCE_REQUIRED' THEN 30.0
            ELSE 10.0
        END * 0.25) +
        (CASE 
            WHEN comms_status = 'NOMINAL' THEN 100.0
            WHEN comms_status = 'DEGRADED' THEN 50.0
            ELSE 0.0
        END * 0.15)
    , 1) AS readiness_score,
    CASE
        WHEN equipment_status = 'NON_OPERATIONAL' THEN 'NOT_COMBAT_READY'
        WHEN fuel_level_pct < 15 OR ammo_level_pct < 15 THEN 'CRITICAL'
        WHEN fuel_level_pct < 30 OR ammo_level_pct < 30 THEN 'DEGRADED'
        WHEN comms_status = 'OFFLINE' THEN 'DEGRADED'
        ELSE 'COMBAT_READY'
    END AS combat_effectiveness
FROM gps_raw
EMIT CHANGES;" "gps_enriched stream (with grid, readiness, combat effectiveness)"

run_ksql "CREATE STREAM IF NOT EXISTS sigint_enriched AS
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
    CONCAT('33T',
        CASE WHEN lon < 35.75 THEN 'W' WHEN lon < 36.25 THEN 'X' ELSE 'Y' END,
        CASE WHEN lat < 44.25 THEN 'M' ELSE 'N' END
    ) AS grid_square,
    CASE 
        WHEN threat_level = 'CRITICAL' THEN 100
        WHEN threat_level = 'HIGH' THEN 75
        WHEN threat_level = 'MEDIUM' THEN 50
        ELSE 25
    END AS threat_score,
    CASE
        WHEN signal_strength_dbm > -50 THEN 'STRONG'
        WHEN signal_strength_dbm > -70 THEN 'MODERATE'
        ELSE 'WEAK'
    END AS signal_quality,
    CASE
        WHEN emitter_classification LIKE '%SA-%' THEN true
        WHEN emitter_classification LIKE '%RADAR%' THEN true
        ELSE false
    END AS is_air_defense
FROM sigint_raw
EMIT CHANGES;" "sigint_enriched stream (with grid, threat score, air defense flag)"

run_ksql "CREATE STREAM IF NOT EXISTS satellite_enriched AS
SELECT
    image_id,
    source_id,
    timestamp,
    sensor_type,
    resolution_m,
    cloud_cover_pct,
    quality_score,
    center_lat,
    center_lon,
    classification,
    CONCAT('33T',
        CASE WHEN center_lon < 35.75 THEN 'W' WHEN center_lon < 36.25 THEN 'X' ELSE 'Y' END,
        CASE WHEN center_lat < 44.25 THEN 'M' ELSE 'N' END
    ) AS grid_square,
    ROUND(
        quality_score * 100 * 
        (1.0 - cloud_cover_pct / 100.0) * 
        (CASE 
            WHEN resolution_m <= 1.0 THEN 1.0
            WHEN resolution_m <= 5.0 THEN 0.7
            ELSE 0.4
        END)
    , 1) AS isr_effectiveness,
    CASE
        WHEN cloud_cover_pct > 70 THEN 'POOR'
        WHEN cloud_cover_pct > 40 THEN 'MODERATE'
        ELSE 'GOOD'
    END AS coverage_quality
FROM satellite_raw
EMIT CHANGES;" "satellite_enriched stream (with grid, ISR effectiveness)"

run_ksql "CREATE STREAM IF NOT EXISTS aircraft_enriched AS
SELECT
    aircraft_id,
    tail_number,
    aircraft_type,
    role,
    country,
    timestamp,
    lat,
    lon,
    altitude_m,
    altitude_ft,
    heading_deg,
    speed_kph,
    speed_kts,
    status,
    fuel_pct,
    threat_level,
    iff_mode,
    CONCAT(country, '-', aircraft_type, '-', tail_number) AS track_id,
    CASE 
        WHEN role IN ('STEALTH', 'AIR_SUPERIORITY', 'STRIKE', 'BOMBER', 'STRATEGIC_BOMBER') THEN 'HIGH'
        WHEN role IN ('MULTIROLE', 'INTERCEPTOR', 'ATTACK_HELO') THEN 'MEDIUM'
        ELSE 'LOW'
    END AS computed_threat,
    CASE
        WHEN status = 'INTERCEPT' THEN 'AGGRESSIVE'
        WHEN status = 'STRIKE' THEN 'AGGRESSIVE'
        WHEN status IN ('PATROL', 'RECON', 'ESCORT') THEN 'MONITORING'
        ELSE 'NEUTRAL'
    END AS posture
FROM aircraft_raw
EMIT CHANGES;" "aircraft_enriched stream (with threat assessment)"

# ============================================================
# STEP 3: ALERT STREAMS
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 3: Creating Alert Streams ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS alert_fuel_critical AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    grid_square,
    fuel_level_pct,
    fuel_hours_remaining,
    mission_status,
    lat,
    lon,
    timestamp,
    'FUEL_CRITICAL' AS alert_type,
    CASE
        WHEN fuel_level_pct < 10 OR fuel_hours_remaining < 2 THEN 'CRITICAL'
        WHEN fuel_level_pct < 15 OR fuel_hours_remaining < 3 THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('FUEL EMERGENCY: ', unit_name, ' at ', grid_square, ' - ', 
           CAST(ROUND(fuel_level_pct) AS VARCHAR), '% (', 
           CAST(fuel_hours_remaining AS VARCHAR), 'h remaining)') AS alert_message
FROM gps_enriched
WHERE fuel_level_pct < 20 OR fuel_hours_remaining < 4
EMIT CHANGES;" "alert_fuel_critical stream"

run_ksql "CREATE STREAM IF NOT EXISTS alert_ammo_critical AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    grid_square,
    ammo_level_pct,
    mission_status,
    lat,
    lon,
    timestamp,
    'AMMO_CRITICAL' AS alert_type,
    CASE
        WHEN ammo_level_pct < 10 THEN 'CRITICAL'
        WHEN ammo_level_pct < 20 THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('AMMO CRITICAL: ', unit_name, ' at ', grid_square, ' - ',
           CAST(ROUND(ammo_level_pct) AS VARCHAR), '% while ', mission_status) AS alert_message
FROM gps_enriched
WHERE ammo_level_pct < 25 AND mission_status IN ('ENGAGED', 'DEFENSIVE')
EMIT CHANGES;" "alert_ammo_critical stream"

run_ksql "CREATE STREAM IF NOT EXISTS alert_equipment AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    grid_square,
    equipment_status,
    mission_status,
    readiness_score,
    lat,
    lon,
    timestamp,
    'EQUIPMENT_FAILURE' AS alert_type,
    CASE
        WHEN equipment_status = 'NON_OPERATIONAL' THEN 'CRITICAL'
        ELSE 'HIGH'
    END AS severity,
    CONCAT('EQUIPMENT: ', unit_name, ' at ', grid_square, ' - ', equipment_status,
           ' (Readiness: ', CAST(readiness_score AS VARCHAR), '%)') AS alert_message
FROM gps_enriched
WHERE equipment_status IN ('NON_OPERATIONAL', 'MAINTENANCE_REQUIRED')
EMIT CHANGES;" "alert_equipment stream"

run_ksql "CREATE STREAM IF NOT EXISTS alert_comms AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    grid_square,
    comms_status,
    mission_status,
    lat,
    lon,
    timestamp,
    'COMMS_ALERT' AS alert_type,
    CASE
        WHEN comms_status = 'OFFLINE' THEN 'CRITICAL'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('COMMS ', comms_status, ': ', unit_name, ' at ', grid_square,
           ' - Last pos: ', CAST(lat AS VARCHAR), ', ', CAST(lon AS VARCHAR)) AS alert_message
FROM gps_enriched
WHERE comms_status IN ('DEGRADED', 'OFFLINE')
EMIT CHANGES;" "alert_comms stream"

run_ksql "CREATE STREAM IF NOT EXISTS alert_low_readiness AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    grid_square,
    readiness_score,
    combat_effectiveness,
    fuel_level_pct,
    ammo_level_pct,
    equipment_status,
    lat,
    lon,
    timestamp,
    'LOW_READINESS' AS alert_type,
    CASE
        WHEN readiness_score < 30 THEN 'CRITICAL'
        WHEN readiness_score < 40 THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('LOW READINESS: ', unit_name, ' - ', CAST(readiness_score AS VARCHAR), 
           '% (', combat_effectiveness, ')') AS alert_message
FROM gps_enriched
WHERE readiness_score < 50
EMIT CHANGES;" "alert_low_readiness stream"

run_ksql "CREATE STREAM IF NOT EXISTS alert_threat_radar AS
SELECT
    intercept_id,
    emitter_classification,
    threat_level,
    grid_square,
    lat,
    lon,
    signal_strength_dbm,
    signal_quality,
    frequency_mhz,
    collector_id,
    timestamp,
    'THREAT_RADAR' AS alert_type,
    threat_level AS severity,
    CONCAT('THREAT: ', emitter_classification, ' in ', grid_square, ' - ',
           signal_quality, ' signal (', CAST(ROUND(signal_strength_dbm) AS VARCHAR), ' dBm)') AS alert_message
FROM sigint_enriched
WHERE is_air_defense = true
  AND signal_strength_dbm > -70
  AND threat_level IN ('CRITICAL', 'HIGH')
EMIT CHANGES;" "alert_threat_radar stream"

run_ksql "CREATE STREAM IF NOT EXISTS alert_hostile_aircraft AS
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
    CASE
        WHEN status IN ('INTERCEPT', 'STRIKE') THEN 'CRITICAL'
        WHEN role IN ('STEALTH', 'BOMBER', 'STRATEGIC_BOMBER') THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity,
    CONCAT('HOSTILE: ', country, ' ', aircraft_type, ' (', tail_number, ') ',
           status, ' at FL', CAST(ROUND(altitude_ft/100) AS VARCHAR),
           ' HDG ', CAST(ROUND(heading_deg) AS VARCHAR), '° ',
           CAST(ROUND(speed_kts) AS VARCHAR), 'kts') AS alert_message
FROM aircraft_enriched
WHERE iff_mode = 'HOSTILE'
  AND status IN ('INTERCEPT', 'STRIKE', 'PATROL', 'RECON')
EMIT CHANGES;" "alert_hostile_aircraft stream"

# ============================================================
# STEP 4: AGGREGATED TABLES
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 4: Creating Aggregated Tables ===${NC}"

run_ksql "CREATE TABLE IF NOT EXISTS grid_force_status AS
SELECT
    grid_square,
    COUNT(*) AS unit_count,
    ROUND(AVG(readiness_score), 1) AS avg_readiness,
    ROUND(MIN(readiness_score), 1) AS min_readiness,
    ROUND(AVG(fuel_level_pct), 1) AS avg_fuel,
    ROUND(MIN(fuel_level_pct), 1) AS min_fuel,
    ROUND(AVG(ammo_level_pct), 1) AS avg_ammo,
    ROUND(MIN(ammo_level_pct), 1) AS min_ammo,
    SUM(CASE WHEN combat_effectiveness = 'COMBAT_READY' THEN 1 ELSE 0 END) AS combat_ready_count,
    SUM(CASE WHEN combat_effectiveness = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN mission_status = 'ENGAGED' THEN 1 ELSE 0 END) AS engaged_count,
    SUM(personnel_count) AS total_personnel
FROM gps_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY grid_square
EMIT CHANGES;" "grid_force_status table (force readiness by grid)"

run_ksql "CREATE TABLE IF NOT EXISTS grid_threat_summary AS
SELECT
    grid_square,
    COUNT(*) AS intercept_count,
    COUNT_DISTINCT(emitter_classification) AS unique_emitters,
    MAX(threat_score) AS max_threat_score,
    ROUND(AVG(threat_score), 1) AS avg_threat_score,
    ROUND(AVG(signal_strength_dbm), 1) AS avg_signal_strength,
    SUM(CASE WHEN threat_level = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_threats,
    SUM(CASE WHEN threat_level = 'HIGH' THEN 1 ELSE 0 END) AS high_threats,
    SUM(CASE WHEN is_air_defense = true THEN 1 ELSE 0 END) AS air_defense_count
FROM sigint_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY grid_square
EMIT CHANGES;" "grid_threat_summary table (threats by grid)"

run_ksql "CREATE TABLE IF NOT EXISTS grid_isr_coverage AS
SELECT
    grid_square,
    COUNT(*) AS image_count,
    COUNT_DISTINCT(source_id) AS satellites_covering,
    COUNT_DISTINCT(sensor_type) AS sensor_types,
    ROUND(AVG(isr_effectiveness), 1) AS avg_effectiveness,
    ROUND(MAX(isr_effectiveness), 1) AS best_effectiveness,
    ROUND(AVG(cloud_cover_pct), 1) AS avg_cloud_cover,
    SUM(CASE WHEN coverage_quality = 'GOOD' THEN 1 ELSE 0 END) AS good_coverage_count
FROM satellite_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY grid_square
EMIT CHANGES;" "grid_isr_coverage table (ISR coverage by grid)"

run_ksql "CREATE TABLE IF NOT EXISTS force_composition AS
SELECT
    unit_type,
    COUNT(*) AS unit_count,
    ROUND(AVG(readiness_score), 1) AS avg_readiness,
    SUM(personnel_count) AS total_personnel,
    ROUND(AVG(fuel_level_pct), 1) AS avg_fuel,
    ROUND(AVG(ammo_level_pct), 1) AS avg_ammo,
    SUM(CASE WHEN mission_status = 'ENGAGED' THEN 1 ELSE 0 END) AS engaged_count,
    SUM(CASE WHEN combat_effectiveness = 'COMBAT_READY' THEN 1 ELSE 0 END) AS combat_ready
FROM gps_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY unit_type
EMIT CHANGES;" "force_composition table (by unit type)"

run_ksql "CREATE TABLE IF NOT EXISTS hostile_aircraft_summary AS
SELECT
    country,
    COUNT(*) AS aircraft_count,
    COUNT_DISTINCT(aircraft_type) AS aircraft_types,
    SUM(CASE WHEN status = 'INTERCEPT' THEN 1 ELSE 0 END) AS intercepting,
    SUM(CASE WHEN status = 'STRIKE' THEN 1 ELSE 0 END) AS on_strike,
    SUM(CASE WHEN status = 'PATROL' THEN 1 ELSE 0 END) AS patrolling,
    SUM(CASE WHEN role IN ('STEALTH', 'BOMBER', 'STRATEGIC_BOMBER') THEN 1 ELSE 0 END) AS high_value_targets,
    ROUND(AVG(altitude_ft), 0) AS avg_altitude_ft,
    ROUND(AVG(speed_kts), 0) AS avg_speed_kts
FROM aircraft_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY country
EMIT CHANGES;" "hostile_aircraft_summary table (by country)"

# ============================================================
# STEP 5: GPS TABLE FOR LOOKUPS
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 5: Creating GPS Table for Lookups ===${NC}"

run_ksql "CREATE TABLE IF NOT EXISTS gps_latest AS
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
    LATEST_BY_OFFSET(ammo_level_pct) AS ammo_level_pct,
    LATEST_BY_OFFSET(personnel_count) AS personnel_count,
    LATEST_BY_OFFSET(equipment_status) AS equipment_status,
    LATEST_BY_OFFSET(mission_status) AS mission_status,
    LATEST_BY_OFFSET(comms_status) AS comms_status,
    LATEST_BY_OFFSET(timestamp) AS last_update
FROM gps_raw
GROUP BY unit_id
EMIT CHANGES;" "gps_latest table (latest position for each unit)"

log "Waiting 5 seconds for GPS table to populate..."
sleep 5

# ============================================================
# STEP 6: AIRCRAFT WITH GPS JOIN
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 6: Aircraft Stream with GPS Data ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS aircraft_with_gps AS
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
    CONCAT('AC-', a.aircraft_id) AS gps_key,
    g.lat AS gps_lat,
    g.lon AS gps_lon,
    g.altitude_m AS gps_altitude_m,
    g.heading_deg AS gps_heading,
    g.speed_kph AS gps_speed_kph,
    g.fuel_level_pct AS gps_fuel_pct,
    g.mission_status AS gps_mission_status,
    g.last_update AS gps_last_update,
    COALESCE(g.lat, a.lat) AS final_lat,
    COALESCE(g.lon, a.lon) AS final_lon,
    CASE 
        WHEN g.lat IS NOT NULL THEN 'GPS_VERIFIED'
        ELSE 'RADAR_ONLY'
    END AS position_source
FROM aircraft_enriched a
LEFT JOIN gps_latest g ON CONCAT('AC-', a.aircraft_id) = g.unit_id
EMIT CHANGES;" "aircraft_with_gps stream (aircraft joined with GPS)"

# ============================================================
# STEP 7: NAVAL WITH GPS JOIN
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 7: Naval Stream with GPS Data ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS naval_raw (
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
) WITH (
    KAFKA_TOPIC = 'naval-vessels',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);" "naval_raw stream"

run_ksql "CREATE STREAM IF NOT EXISTS naval_with_gps AS
SELECT
    n.vessel_id AS vessel_id,
    n.hull_number AS hull_number,
    n.vessel_type AS vessel_type,
    n.vessel_class AS vessel_class,
    n.displacement_tons AS displacement_tons,
    n.country AS country,
    n.weapons AS weapons,
    n.aircraft_capacity AS aircraft_capacity,
    n.timestamp AS timestamp,
    n.lat AS vessel_lat,
    n.lon AS vessel_lon,
    n.heading_deg AS heading_deg,
    n.speed_kts AS speed_kts,
    n.status AS status,
    n.iff_mode AS iff_mode,
    CONCAT('SHIP-', n.hull_number) AS gps_key,
    g.lat AS gps_lat,
    g.lon AS gps_lon,
    g.heading_deg AS gps_heading,
    g.speed_kph AS gps_speed_kph,
    g.mission_status AS gps_mission_status,
    g.last_update AS gps_last_update,
    COALESCE(g.lat, n.lat) AS final_lat,
    COALESCE(g.lon, n.lon) AS final_lon,
    CASE 
        WHEN g.lat IS NOT NULL THEN 'GPS_VERIFIED'
        ELSE 'AIS_ONLY'
    END AS position_source
FROM naval_raw n
LEFT JOIN gps_latest g ON CONCAT('SHIP-', n.hull_number) = g.unit_id
EMIT CHANGES;" "naval_with_gps stream (naval joined with GPS)"

# ============================================================
# STEP 8: CYBER THREATS WITH FACILITY GPS
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 8: Cyber Threats with Facility GPS ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS cyber_raw (
    incident_id VARCHAR KEY,
    timestamp BIGINT,
    threat_actor VARCHAR,
    actor_country VARCHAR,
    sophistication VARCHAR,
    attack_type VARCHAR,
    severity VARCHAR,
    description VARCHAR,
    target_facility_id VARCHAR,
    target_facility VARCHAR,
    target_system VARCHAR,
    target_type VARCHAR,
    target_region VARCHAR,
    target_country VARCHAR,
    target_lat DOUBLE,
    target_lon DOUBLE,
    source_ip VARCHAR,
    destination_ip VARCHAR,
    protocol VARCHAR,
    port INT,
    mitre_attack VARCHAR,
    status VARCHAR
) WITH (
    KAFKA_TOPIC = 'cyber-threats',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);" "cyber_raw stream"

run_ksql "CREATE STREAM IF NOT EXISTS cyber_with_gps AS
SELECT
    c.incident_id AS incident_id,
    c.timestamp AS timestamp,
    c.threat_actor AS threat_actor,
    c.actor_country AS actor_country,
    c.sophistication AS sophistication,
    c.attack_type AS attack_type,
    c.severity AS severity,
    c.description AS description,
    c.target_facility_id AS target_facility_id,
    c.target_facility AS target_facility,
    c.target_system AS target_system,
    c.target_type AS target_type,
    c.target_region AS target_region,
    c.target_lat AS target_lat,
    c.target_lon AS target_lon,
    c.source_ip AS source_ip,
    c.mitre_attack AS mitre_attack,
    c.status AS status,
    g.lat AS facility_gps_lat,
    g.lon AS facility_gps_lon,
    g.unit_name AS facility_name_from_gps,
    g.comms_status AS facility_comms_status,
    g.last_update AS facility_last_update,
    COALESCE(g.lat, c.target_lat) AS final_lat,
    COALESCE(g.lon, c.target_lon) AS final_lon,
    CASE 
        WHEN g.lat IS NOT NULL THEN 'GPS_VERIFIED'
        ELSE 'STATIC_COORDS'
    END AS position_source,
    CASE
        WHEN c.severity = 'CRITICAL' AND c.status = 'INVESTIGATING' THEN 'ACTIVE_BREACH'
        WHEN c.severity = 'CRITICAL' THEN 'CRITICAL_ALERT'
        WHEN c.severity = 'HIGH' THEN 'HIGH_PRIORITY'
        ELSE 'MONITORING'
    END AS threat_status
FROM cyber_raw c
LEFT JOIN gps_latest g ON c.target_facility_id = g.unit_id
EMIT CHANGES;" "cyber_with_gps stream (cyber joined with facility GPS)"

# ============================================================
# STEP 9: SIGINT WITH GPS CORRELATION
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 9: SIGINT with Nearest Unit GPS ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS sigint_with_context AS
SELECT
    s.intercept_id AS intercept_id,
    s.timestamp AS timestamp,
    s.signal_type AS signal_type,
    s.frequency_mhz AS frequency_mhz,
    s.signal_strength_dbm AS signal_strength_dbm,
    s.lat AS signal_lat,
    s.lon AS signal_lon,
    s.emitter_classification AS emitter_classification,
    s.threat_level AS threat_level,
    s.emission_pattern AS emission_pattern,
    s.collector_id AS collector_id,
    s.grid_square AS grid_square,
    s.threat_score AS threat_score,
    s.signal_quality AS signal_quality,
    s.is_air_defense AS is_air_defense,
    CASE
        WHEN s.emitter_classification LIKE 'RU-%' THEN 'RUSSIA'
        WHEN s.emitter_classification LIKE 'CN-%' THEN 'CHINA'
        WHEN s.emitter_classification LIKE 'IR-%' THEN 'IRAN'
        ELSE 'UNKNOWN'
    END AS emitter_country,
    CASE
        WHEN s.emission_pattern = 'FIRE_CONTROL' THEN 'WEAPONS_LOCKED'
        WHEN s.emission_pattern = 'ACQUISITION' THEN 'TRACKING'
        WHEN s.emission_pattern = 'TRACK' THEN 'MONITORING'
        ELSE 'SEARCH'
    END AS threat_posture
FROM sigint_enriched s
EMIT CHANGES;" "sigint_with_context stream (enriched SIGINT with threat analysis)"

# ============================================================
# STEP 10: UNIFIED TRACK STREAM (ALL DOMAINS)
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 10: Creating Unified Track Stream ===${NC}"

run_ksql "CREATE STREAM IF NOT EXISTS unified_tracks AS
SELECT
    unit_id AS track_id,
    unit_name AS track_name,
    unit_type AS track_type,
    'GROUND' AS domain,
    'US' AS country,
    'FRIENDLY' AS iff_status,
    lat,
    lon,
    heading_deg,
    speed_kph,
    altitude_m,
    mission_status AS status,
    timestamp,
    CONCAT('33T',
        CASE WHEN lon < 35.75 THEN 'W' WHEN lon < 36.25 THEN 'X' ELSE 'Y' END,
        CASE WHEN lat < 44.25 THEN 'M' ELSE 'N' END
    ) AS grid_square
FROM gps_raw
WHERE unit_id NOT LIKE 'AC-%' 
  AND unit_id NOT LIKE 'SHIP-%'
  AND unit_id NOT LIKE 'FAC-%'
EMIT CHANGES;" "unified_tracks stream - ground units"

run_ksql "CREATE STREAM IF NOT EXISTS unified_tracks_air AS
SELECT
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
    timestamp,
    CONCAT('33T',
        CASE WHEN final_lon < 35.75 THEN 'W' WHEN final_lon < 36.25 THEN 'X' ELSE 'Y' END,
        CASE WHEN final_lat < 44.25 THEN 'M' ELSE 'N' END
    ) AS grid_square
FROM aircraft_with_gps
EMIT CHANGES;" "unified_tracks_air stream - aircraft"

run_ksql "CREATE STREAM IF NOT EXISTS unified_tracks_naval AS
SELECT
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
    0.0 AS altitude_m,
    status,
    timestamp,
    CONCAT('33T',
        CASE WHEN final_lon < 35.75 THEN 'W' WHEN final_lon < 36.25 THEN 'X' ELSE 'Y' END,
        CASE WHEN final_lat < 44.25 THEN 'M' ELSE 'N' END
    ) AS grid_square
FROM naval_with_gps
EMIT CHANGES;" "unified_tracks_naval stream - naval"

# ============================================================
# STEP 11: MULTI-DOMAIN CORRELATION (JOINS)
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 11: Creating Multi-Domain Correlations ===${NC}"

run_ksql "CREATE TABLE IF NOT EXISTS active_threat_grids AS
SELECT
    grid_square,
    COUNT(*) AS threat_count,
    MAX(threat_score) AS max_threat,
    LATEST_BY_OFFSET(emitter_classification) AS primary_threat,
    LATEST_BY_OFFSET(threat_level) AS highest_threat_level
FROM sigint_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
WHERE threat_level IN ('CRITICAL', 'HIGH')
  AND signal_strength_dbm > -70
GROUP BY grid_square
EMIT CHANGES;" "active_threat_grids table (grids with active threats)"

log "Waiting 10 seconds for tables to populate before creating joins..."
sleep 10

run_ksql "CREATE STREAM IF NOT EXISTS units_in_threat_zone AS
SELECT
    g.unit_id AS unit_id,
    g.unit_name AS unit_name,
    g.unit_type AS unit_type,
    g.grid_square AS grid_square,
    g.lat AS lat,
    g.lon AS lon,
    g.mission_status AS mission_status,
    g.readiness_score AS readiness_score,
    g.combat_effectiveness AS combat_effectiveness,
    t.threat_count AS threat_count,
    t.max_threat AS max_threat,
    t.primary_threat AS primary_threat,
    g.timestamp AS timestamp,
    'UNIT_UNDER_THREAT' AS alert_type,
    CASE 
        WHEN t.max_threat >= 100 THEN 'CRITICAL'
        WHEN t.max_threat >= 75 THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('THREAT PROXIMITY: ', g.unit_name, ' in ', g.grid_square,
           ' with ', CAST(t.threat_count AS VARCHAR), ' active threat(s). ',
           'Primary: ', t.primary_threat) AS alert_message
FROM gps_enriched g
INNER JOIN active_threat_grids t
ON g.grid_square = t.grid_square
EMIT CHANGES;" "units_in_threat_zone stream (GPS + SIGINT JOIN)"

# ============================================================
# STEP 6: COMBINED OPERATIONAL PICTURE
# ============================================================
echo ""
echo -e "${YELLOW}=== STEP 6: Creating Combined Operational Picture ===${NC}"

run_ksql "CREATE TABLE IF NOT EXISTS operational_picture_by_grid AS
SELECT
    f.grid_square AS grid_square,
    f.unit_count AS unit_count,
    f.avg_readiness AS avg_readiness,
    f.combat_ready_count AS combat_ready_count,
    f.critical_count AS critical_count,
    f.engaged_count AS engaged_count,
    f.total_personnel AS total_personnel,
    f.avg_fuel AS avg_fuel,
    f.avg_ammo AS avg_ammo,
    COALESCE(t.intercept_count, 0) AS threat_intercepts,
    COALESCE(t.critical_threats, 0) AS critical_threats,
    COALESCE(t.air_defense_count, 0) AS air_defense_threats,
    COALESCE(t.max_threat_score, 0) AS max_threat_score,
    COALESCE(i.image_count, 0) AS isr_images,
    COALESCE(i.avg_effectiveness, 0.0) AS isr_effectiveness,
    CASE
        WHEN COALESCE(t.critical_threats, 0) > 0 AND f.engaged_count > 0 THEN 'ACTIVE_COMBAT'
        WHEN COALESCE(t.critical_threats, 0) > 0 THEN 'HIGH_THREAT'
        WHEN f.critical_count > 0 THEN 'FORCE_CRITICAL'
        WHEN f.avg_readiness < 50 THEN 'DEGRADED'
        ELSE 'STABLE'
    END AS grid_status
FROM grid_force_status f
LEFT JOIN grid_threat_summary t ON f.grid_square = t.grid_square
LEFT JOIN grid_isr_coverage i ON f.grid_square = i.grid_square
EMIT CHANGES;" "operational_picture_by_grid table (FULL MULTI-DOMAIN JOIN)"

# ============================================================
# VERIFICATION
# ============================================================
echo ""
echo -e "${YELLOW}=== Verification ===${NC}"

log "Listing all streams..."
docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "SHOW STREAMS;" 2>&1 | grep -E "Stream Name|------|GPS|SIGINT|SATELLITE|ALERT|UNITS"

echo ""
log "Listing all tables..."
docker exec jadc2-ksqldb-cli ksql $KSQL_SERVER -e "SHOW TABLES;" 2>&1 | grep -E "Table Name|------|GRID|FORCE|ACTIVE|OPERATIONAL"

echo ""
echo -e "${GREEN}============================================"
echo " ksqlDB Setup Complete!"
echo "============================================${NC}"
echo ""
echo "Created Streams:"
echo "  RAW INPUT:"
echo "    • gps_raw, sigint_raw, satellite_raw, aircraft_raw, naval_raw, cyber_raw"
echo ""
echo "  ENRICHED:"
echo "    • gps_enriched, sigint_enriched, satellite_enriched, aircraft_enriched"
echo ""
echo "  GPS-JOINED (All domains with GPS coordinates):"
echo "    • aircraft_with_gps (aircraft + GPS positions)"
echo "    • naval_with_gps (naval vessels + GPS positions)"
echo "    • cyber_with_gps (cyber threats + facility GPS)"
echo "    • sigint_with_context (SIGINT + threat analysis)"
echo ""
echo "  UNIFIED TRACKS:"
echo "    • unified_tracks (ground units)"
echo "    • unified_tracks_air (aircraft)"
echo "    • unified_tracks_naval (naval vessels)"
echo ""
echo "  ALERTS:"
echo "    • alert_fuel_critical, alert_ammo_critical, alert_equipment"
echo "    • alert_comms, alert_low_readiness, alert_threat_radar"
echo "    • alert_hostile_aircraft, units_in_threat_zone"
echo ""
echo "Created Tables:"
echo "  • gps_latest (latest position for each GPS-tracked entity)"
echo "  • grid_force_status, grid_threat_summary, grid_isr_coverage"
echo "  • force_composition, hostile_aircraft_summary"
echo "  • active_threat_grids, operational_picture_by_grid"
echo ""
echo "Test commands:"
echo "  docker exec -it jadc2-ksqldb-cli ksql http://ksqldb-server:8088"
echo ""
echo "  -- View GPS-joined aircraft:"
echo "  SELECT * FROM aircraft_with_gps EMIT CHANGES LIMIT 5;"
echo ""
echo "  -- View GPS-joined naval:"
echo "  SELECT * FROM naval_with_gps EMIT CHANGES LIMIT 5;"
echo ""
echo "  -- View cyber with facility GPS:"
echo "  SELECT * FROM cyber_with_gps EMIT CHANGES LIMIT 5;"
echo ""
echo "  -- View unified tracks (all domains):"
echo "  SELECT * FROM unified_tracks_air EMIT CHANGES LIMIT 5;"
echo ""
