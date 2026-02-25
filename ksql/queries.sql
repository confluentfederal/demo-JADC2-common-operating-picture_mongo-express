-- ============================================================
-- JADC2 Common Operating Picture - ksqlDB Stream Processing
-- ============================================================
-- Multi-domain correlation of:
--   - Ground Force GPS tracking
--   - SIGINT intercepts  
--   - Satellite imagery metadata
--
-- Run: docker exec -it jadc2-ksqldb-cli ksql http://ksqldb-server:8088
-- Then: RUN SCRIPT '/ksql/queries.sql';
-- Or paste sections manually
-- ============================================================

SET 'auto.offset.reset' = 'earliest';

-- ============================================================
-- PART 1: RAW INPUT STREAMS
-- ============================================================

-- Ground Force GPS Stream
CREATE STREAM IF NOT EXISTS gps_raw (
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
);

-- SIGINT Intercepts Stream
CREATE STREAM IF NOT EXISTS sigint_raw (
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
);

-- Satellite Imagery Stream
CREATE STREAM IF NOT EXISTS satellite_raw (
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
);

-- ============================================================
-- PART 2: ENRICHED STREAMS WITH COMPUTED FIELDS
-- ============================================================

-- Enriched GPS with grid square, readiness score, fuel prediction
CREATE STREAM IF NOT EXISTS gps_enriched AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    timestamp,
    lat,
    lon,
    heading_deg,
    speed_kph,
    altitude_m,
    fuel_level_pct,
    ammo_level_pct,
    personnel_count,
    equipment_status,
    mission_status,
    fuel_consumption_rate,
    comms_status,
    -- Grid square calculation (MGRS approximation for Black Sea region)
    CONCAT('33T',
        CASE 
            WHEN lon < 35.75 THEN 'W'
            WHEN lon < 36.25 THEN 'X'
            ELSE 'Y'
        END,
        CASE 
            WHEN lat < 44.25 THEN 'M'
            ELSE 'N'
        END
    ) AS grid_square,
    -- Predicted hours until fuel exhaustion
    CASE 
        WHEN fuel_consumption_rate > 0 
        THEN ROUND((fuel_level_pct / 100.0) * 1000.0 / fuel_consumption_rate, 1)
        ELSE 999.0
    END AS fuel_hours_remaining,
    -- Mission readiness score (0-100)
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
    -- Combat effectiveness rating
    CASE
        WHEN equipment_status = 'NON_OPERATIONAL' THEN 'NOT_COMBAT_READY'
        WHEN fuel_level_pct < 15 OR ammo_level_pct < 15 THEN 'CRITICAL'
        WHEN fuel_level_pct < 30 OR ammo_level_pct < 30 THEN 'DEGRADED'
        WHEN comms_status = 'OFFLINE' THEN 'DEGRADED'
        ELSE 'COMBAT_READY'
    END AS combat_effectiveness
FROM gps_raw
EMIT CHANGES;

-- Enriched SIGINT with grid square and threat scoring
CREATE STREAM IF NOT EXISTS sigint_enriched AS
SELECT
    intercept_id,
    timestamp,
    signal_type,
    frequency_mhz,
    bandwidth_khz,
    modulation,
    bearing_deg,
    signal_strength_dbm,
    lat,
    lon,
    elevation_deg,
    confidence_pct,
    emitter_classification,
    threat_level,
    emission_pattern,
    collector_id,
    -- Grid square
    CONCAT('33T',
        CASE 
            WHEN lon < 35.75 THEN 'W'
            WHEN lon < 36.25 THEN 'X'
            ELSE 'Y'
        END,
        CASE 
            WHEN lat < 44.25 THEN 'M'
            ELSE 'N'
        END
    ) AS grid_square,
    -- Numeric threat score for aggregation
    CASE 
        WHEN threat_level = 'CRITICAL' THEN 100
        WHEN threat_level = 'HIGH' THEN 75
        WHEN threat_level = 'MEDIUM' THEN 50
        ELSE 25
    END AS threat_score,
    -- Signal quality assessment
    CASE
        WHEN signal_strength_dbm > -50 THEN 'STRONG'
        WHEN signal_strength_dbm > -70 THEN 'MODERATE'
        ELSE 'WEAK'
    END AS signal_quality,
    -- Is this an air defense radar?
    CASE
        WHEN emitter_classification LIKE '%SA-%' THEN true
        WHEN emitter_classification LIKE '%RADAR%' THEN true
        ELSE false
    END AS is_air_defense
FROM sigint_raw
EMIT CHANGES;

-- Enriched Satellite imagery with grid square and effectiveness
CREATE STREAM IF NOT EXISTS satellite_enriched AS
SELECT
    image_id,
    source_id,
    timestamp,
    acquisition_time,
    sensor_type,
    resolution_m,
    cloud_cover_pct,
    sun_elevation_deg,
    off_nadir_angle_deg,
    center_lat,
    center_lon,
    classification,
    quality_score,
    processing_status,
    -- Grid square for imagery center
    CONCAT('33T',
        CASE 
            WHEN center_lon < 35.75 THEN 'W'
            WHEN center_lon < 36.25 THEN 'X'
            ELSE 'Y'
        END,
        CASE 
            WHEN center_lat < 44.25 THEN 'M'
            ELSE 'N'
        END
    ) AS grid_square,
    -- ISR effectiveness score (higher is better)
    ROUND(
        quality_score * 100 * 
        (1.0 - cloud_cover_pct / 100.0) * 
        (CASE 
            WHEN resolution_m <= 1.0 THEN 1.0
            WHEN resolution_m <= 5.0 THEN 0.7
            ELSE 0.4
        END)
    , 1) AS isr_effectiveness,
    -- Coverage quality rating
    CASE
        WHEN cloud_cover_pct > 70 THEN 'POOR'
        WHEN cloud_cover_pct > 40 THEN 'MODERATE'
        WHEN quality_score < 0.6 THEN 'MODERATE'
        ELSE 'GOOD'
    END AS coverage_quality
FROM satellite_raw
EMIT CHANGES;

-- ============================================================
-- PART 3: ALERT STREAMS
-- ============================================================

-- CRITICAL: Fuel Emergency (< 20% or < 4 hours)
CREATE STREAM IF NOT EXISTS alert_fuel_critical AS
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
    'FUEL_CRITICAL' AS alert_type,
    CASE
        WHEN fuel_level_pct < 10 OR fuel_hours_remaining < 2 THEN 'CRITICAL'
        WHEN fuel_level_pct < 15 OR fuel_hours_remaining < 3 THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('FUEL EMERGENCY: ', unit_name, ' (', unit_type, ') at ', grid_square,
           ' has only ', CAST(ROUND(fuel_level_pct, 1) AS VARCHAR), '% fuel (',
           CAST(ROUND(fuel_hours_remaining, 1) AS VARCHAR), ' hours remaining). ',
           'Mission status: ', mission_status) AS alert_message,
    timestamp
FROM gps_enriched
WHERE fuel_level_pct < 20 OR fuel_hours_remaining < 4
EMIT CHANGES;

-- Ammo Critical Alert
CREATE STREAM IF NOT EXISTS alert_ammo_critical AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    grid_square,
    ammo_level_pct,
    mission_status,
    lat,
    lon,
    'AMMO_CRITICAL' AS alert_type,
    CASE
        WHEN ammo_level_pct < 10 THEN 'CRITICAL'
        WHEN ammo_level_pct < 20 THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('AMMO CRITICAL: ', unit_name, ' (', unit_type, ') at ', grid_square,
           ' has only ', CAST(ROUND(ammo_level_pct, 1) AS VARCHAR), '% ammunition. ',
           'Mission status: ', mission_status) AS alert_message,
    timestamp
FROM gps_enriched
WHERE ammo_level_pct < 25
EMIT CHANGES;

-- Equipment Failure Alert
CREATE STREAM IF NOT EXISTS alert_equipment AS
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
    'EQUIPMENT_FAILURE' AS alert_type,
    CASE
        WHEN equipment_status = 'NON_OPERATIONAL' THEN 'CRITICAL'
        WHEN equipment_status = 'MAINTENANCE_REQUIRED' THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('EQUIPMENT ALERT: ', unit_name, ' (', unit_type, ') at ', grid_square,
           ' - Status: ', equipment_status, '. Readiness: ',
           CAST(ROUND(readiness_score, 1) AS VARCHAR), '%') AS alert_message,
    timestamp
FROM gps_enriched
WHERE equipment_status IN ('NON_OPERATIONAL', 'MAINTENANCE_REQUIRED', 'DEGRADED')
EMIT CHANGES;

-- Communications Alert
CREATE STREAM IF NOT EXISTS alert_comms AS
SELECT
    unit_id,
    unit_name,
    unit_type,
    grid_square,
    comms_status,
    mission_status,
    lat,
    lon,
    'COMMS_DEGRADED' AS alert_type,
    CASE
        WHEN comms_status = 'OFFLINE' THEN 'CRITICAL'
        WHEN comms_status = 'DEGRADED' THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('COMMS ALERT: ', unit_name, ' (', unit_type, ') at ', grid_square,
           ' - Communications ', comms_status, '. Mission: ', mission_status) AS alert_message,
    timestamp
FROM gps_enriched
WHERE comms_status IN ('OFFLINE', 'DEGRADED', 'JAMMING_DETECTED')
EMIT CHANGES;

-- Low Readiness Alert
CREATE STREAM IF NOT EXISTS alert_low_readiness AS
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
    'LOW_READINESS' AS alert_type,
    CASE
        WHEN readiness_score < 30 THEN 'CRITICAL'
        WHEN readiness_score < 50 THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('READINESS ALERT: ', unit_name, ' at ', grid_square,
           ' - Score: ', CAST(ROUND(readiness_score, 1) AS VARCHAR), '% (',
           combat_effectiveness, '). Fuel: ', CAST(ROUND(fuel_level_pct, 0) AS VARCHAR),
           '%, Ammo: ', CAST(ROUND(ammo_level_pct, 0) AS VARCHAR), '%') AS alert_message,
    timestamp
FROM gps_enriched
WHERE readiness_score < 60
EMIT CHANGES;

-- SIGINT Threat Alert (High/Critical threats with strong signal)
CREATE STREAM IF NOT EXISTS alert_threat_radar AS
SELECT
    intercept_id,
    signal_type,
    frequency_mhz,
    emitter_classification,
    threat_level,
    threat_score,
    signal_strength_dbm,
    signal_quality,
    grid_square,
    lat,
    lon,
    'THREAT_RADAR' AS alert_type,
    CASE
        WHEN threat_level = 'CRITICAL' THEN 'CRITICAL'
        WHEN threat_level = 'HIGH' AND signal_quality = 'STRONG' THEN 'CRITICAL'
        WHEN threat_level = 'HIGH' THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('THREAT DETECTED: ', emitter_classification, ' (', signal_type, ') at ', grid_square,
           '. Threat level: ', threat_level, '. Signal: ', signal_quality,
           ' (', CAST(ROUND(signal_strength_dbm, 1) AS VARCHAR), ' dBm). ',
           'Freq: ', CAST(ROUND(frequency_mhz, 2) AS VARCHAR), ' MHz') AS alert_message,
    timestamp
FROM sigint_enriched
WHERE threat_level IN ('HIGH', 'CRITICAL')
  AND signal_strength_dbm > -80
EMIT CHANGES;

-- ============================================================
-- PART 4: AGGREGATION TABLES
-- ============================================================

-- Force Status by Grid Square (5-minute windows)
CREATE TABLE IF NOT EXISTS grid_force_status AS
SELECT
    grid_square,
    COUNT(*) AS unit_count,
    ROUND(AVG(readiness_score), 1) AS avg_readiness,
    ROUND(MIN(readiness_score), 1) AS min_readiness,
    SUM(CASE WHEN combat_effectiveness = 'COMBAT_READY' THEN 1 ELSE 0 END) AS combat_ready_count,
    SUM(CASE WHEN combat_effectiveness = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN mission_status = 'ENGAGED' THEN 1 ELSE 0 END) AS engaged_count,
    SUM(personnel_count) AS total_personnel,
    ROUND(AVG(fuel_level_pct), 1) AS avg_fuel,
    ROUND(AVG(ammo_level_pct), 1) AS avg_ammo
FROM gps_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY grid_square
EMIT CHANGES;

-- Threat Summary by Grid Square
CREATE TABLE IF NOT EXISTS grid_threat_summary AS
SELECT
    grid_square,
    COUNT(*) AS intercept_count,
    MAX(threat_score) AS max_threat_score,
    ROUND(AVG(signal_strength_dbm), 1) AS avg_signal_strength,
    SUM(CASE WHEN threat_level = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_threats,
    SUM(CASE WHEN threat_level = 'HIGH' THEN 1 ELSE 0 END) AS high_threats,
    SUM(CASE WHEN is_air_defense = true THEN 1 ELSE 0 END) AS air_defense_count
FROM sigint_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY grid_square
EMIT CHANGES;

-- ISR Coverage by Grid Square
CREATE TABLE IF NOT EXISTS grid_isr_coverage AS
SELECT
    grid_square,
    COUNT(*) AS image_count,
    COUNT_DISTINCT(source_id) AS satellites_covering,
    COUNT_DISTINCT(sensor_type) AS sensor_types,
    ROUND(AVG(isr_effectiveness), 1) AS avg_effectiveness,
    ROUND(MAX(isr_effectiveness), 1) AS best_effectiveness,
    ROUND(AVG(cloud_cover_pct), 1) AS avg_cloud_cover,
    ROUND(AVG(resolution_m), 2) AS avg_resolution,
    SUM(CASE WHEN coverage_quality = 'GOOD' THEN 1 ELSE 0 END) AS good_coverage_count
FROM satellite_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY grid_square
EMIT CHANGES;

-- Unit Type Summary (force composition)
CREATE TABLE IF NOT EXISTS force_composition AS
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
EMIT CHANGES;

-- ============================================================
-- PART 5: MULTI-DOMAIN CORRELATION STREAMS
-- ============================================================

-- Units in Threatened Grid Squares (JOIN)
-- First, create a table of recent high threats
CREATE TABLE IF NOT EXISTS active_threat_grids AS
SELECT
    grid_square,
    COUNT(*) AS threat_count,
    MAX(threat_score) AS max_threat,
    LATEST_BY_OFFSET(emitter_classification) AS primary_threat
FROM sigint_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
WHERE threat_level IN ('CRITICAL', 'HIGH')
  AND signal_strength_dbm > -70
GROUP BY grid_square
EMIT CHANGES;

-- Stream of units entering/operating in threatened areas
CREATE STREAM IF NOT EXISTS units_in_threat_zone AS
SELECT
    g.unit_id,
    g.unit_name,
    g.unit_type,
    g.grid_square,
    g.lat,
    g.lon,
    g.mission_status,
    g.readiness_score,
    g.combat_effectiveness,
    t.threat_count,
    t.max_threat,
    t.primary_threat,
    'UNIT_UNDER_THREAT' AS alert_type,
    CASE 
        WHEN t.max_threat >= 100 THEN 'CRITICAL'
        WHEN t.max_threat >= 75 THEN 'HIGH'
        ELSE 'WARNING'
    END AS severity,
    CONCAT('THREAT PROXIMITY: ', g.unit_name, ' operating in ', g.grid_square,
           ' with ', CAST(t.threat_count AS VARCHAR), ' active threat(s). ',
           'Primary threat: ', t.primary_threat, '. ',
           'Unit readiness: ', CAST(g.readiness_score AS VARCHAR), '% (',
           g.combat_effectiveness, ')') AS alert_message,
    g.timestamp
FROM gps_enriched g
INNER JOIN active_threat_grids t
ON g.grid_square = t.grid_square
EMIT CHANGES;

-- ISR Coverage Gaps (grids with units but poor/no ISR)
CREATE TABLE IF NOT EXISTS isr_coverage_gaps AS
SELECT
    f.grid_square,
    f.unit_count,
    f.engaged_count,
    f.avg_readiness,
    COALESCE(i.image_count, 0) AS recent_images,
    COALESCE(i.avg_effectiveness, 0.0) AS isr_effectiveness,
    CASE
        WHEN COALESCE(i.image_count, 0) = 0 THEN 'NO_COVERAGE'
        WHEN COALESCE(i.avg_effectiveness, 0) < 30 THEN 'POOR_COVERAGE'
        WHEN COALESCE(i.avg_effectiveness, 0) < 60 THEN 'LIMITED_COVERAGE'
        ELSE 'ADEQUATE_COVERAGE'
    END AS coverage_status
FROM grid_force_status f
LEFT JOIN grid_isr_coverage i ON f.grid_square = i.grid_square
EMIT CHANGES;

-- ============================================================
-- PART 6: COMBINED OPERATIONAL PICTURE
-- ============================================================

-- Master Grid Status (combines all domains)
CREATE TABLE IF NOT EXISTS operational_picture_by_grid AS
SELECT
    f.grid_square,
    -- Force status
    f.unit_count,
    f.avg_readiness,
    f.combat_ready_count,
    f.critical_count,
    f.engaged_count,
    f.total_personnel,
    f.avg_fuel,
    f.avg_ammo,
    -- Threat status
    COALESCE(t.intercept_count, 0) AS threat_intercepts,
    COALESCE(t.critical_threats, 0) AS critical_threats,
    COALESCE(t.high_threats, 0) AS high_threats,
    COALESCE(t.air_defense_count, 0) AS air_defense_threats,
    COALESCE(t.max_threat_score, 0) AS max_threat_score,
    -- ISR status
    COALESCE(i.image_count, 0) AS isr_images,
    COALESCE(i.avg_effectiveness, 0.0) AS isr_effectiveness,
    COALESCE(i.satellites_covering, 0) AS satellite_count,
    -- Computed risk score (0-100, higher = more risk)
    CAST(
        -- Threat component (40%)
        (COALESCE(t.max_threat_score, 0) * 0.40) +
        -- Readiness inverse component (30%)
        ((100.0 - f.avg_readiness) * 0.30) +
        -- ISR gap component (30%)
        ((100.0 - COALESCE(i.avg_effectiveness, 0)) * 0.30)
    AS INT) AS grid_risk_score,
    -- Overall grid status
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
EMIT CHANGES;

-- ============================================================
-- PART 7: COMBINED ALERT STREAM (for MongoDB sink)
-- ============================================================

-- Union all alerts into single stream
CREATE STREAM IF NOT EXISTS all_alerts (
    alert_id VARCHAR KEY,
    alert_type VARCHAR,
    severity VARCHAR,
    alert_message VARCHAR,
    entity_id VARCHAR,
    grid_square VARCHAR,
    lat DOUBLE,
    lon DOUBLE,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC = 'all-alerts',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
);

INSERT INTO all_alerts
SELECT
    CONCAT('FUEL-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id,
    alert_type,
    severity,
    alert_message,
    unit_id AS entity_id,
    grid_square,
    lat,
    lon,
    timestamp
FROM alert_fuel_critical
EMIT CHANGES;

INSERT INTO all_alerts
SELECT
    CONCAT('AMMO-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id,
    alert_type,
    severity,
    alert_message,
    unit_id AS entity_id,
    grid_square,
    lat,
    lon,
    timestamp
FROM alert_ammo_critical
EMIT CHANGES;

INSERT INTO all_alerts
SELECT
    CONCAT('EQUIP-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id,
    alert_type,
    severity,
    alert_message,
    unit_id AS entity_id,
    grid_square,
    lat,
    lon,
    timestamp
FROM alert_equipment
EMIT CHANGES;

INSERT INTO all_alerts
SELECT
    CONCAT('COMMS-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id,
    alert_type,
    severity,
    alert_message,
    unit_id AS entity_id,
    grid_square,
    lat,
    lon,
    timestamp
FROM alert_comms
EMIT CHANGES;

INSERT INTO all_alerts
SELECT
    CONCAT('READY-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id,
    alert_type,
    severity,
    alert_message,
    unit_id AS entity_id,
    grid_square,
    lat,
    lon,
    timestamp
FROM alert_low_readiness
EMIT CHANGES;

INSERT INTO all_alerts
SELECT
    CONCAT('THREAT-', intercept_id) AS alert_id,
    alert_type,
    severity,
    alert_message,
    intercept_id AS entity_id,
    grid_square,
    lat,
    lon,
    timestamp
FROM alert_threat_radar
EMIT CHANGES;

INSERT INTO all_alerts
SELECT
    CONCAT('PROXIM-', unit_id, '-', CAST(timestamp AS VARCHAR)) AS alert_id,
    alert_type,
    severity,
    alert_message,
    unit_id AS entity_id,
    grid_square,
    lat,
    lon,
    timestamp
FROM units_in_threat_zone
EMIT CHANGES;

-- ============================================================
-- PART 8: JSIR (Joint Spectrum Interference Resolution)
-- SATCOM jamming incidents and geolocation
-- ============================================================

-- JSIR Raw Stream from spectrum-interference topic
CREATE STREAM IF NOT EXISTS jsir_raw (
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
    jammer_frequency_band VARCHAR,
    target_satellite VARCHAR,
    target_orbit VARCHAR,
    target_frequency_ghz DOUBLE,
    tdoa_ms DOUBLE,
    fdoa_hz DOUBLE,
    cep_km DOUBLE,
    ellipse_major_km DOUBLE,
    ellipse_minor_km DOUBLE,
    ellipse_orientation DOUBLE,
    strike_coords_lat DOUBLE,
    strike_coords_lon DOUBLE,
    mission_impact VARCHAR,
    affected_unit_count INT,
    comms_degradation_pct DOUBLE
) WITH (
    KAFKA_TOPIC = 'spectrum-interference',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);

-- Enriched JSIR with threat scoring and strike recommendations
CREATE STREAM IF NOT EXISTS jsir_enriched AS
SELECT
    incident_id,
    timestamp,
    event_type,
    workflow_stage,
    jammer_id,
    jammer_location,
    jammer_country,
    jammer_lat,
    jammer_lon,
    jammer_terrain,
    jammer_system,
    jammer_type,
    jammer_power_kw,
    jammer_frequency_band,
    target_satellite,
    target_orbit,
    target_frequency_ghz,
    tdoa_ms,
    fdoa_hz,
    cep_km,
    ellipse_major_km,
    ellipse_minor_km,
    ellipse_orientation,
    strike_coords_lat,
    strike_coords_lon,
    mission_impact,
    affected_unit_count,
    comms_degradation_pct,
    -- Grid square for jammer location
    CONCAT(
        CAST(CAST(jammer_lat AS INT) AS VARCHAR),
        CAST(CAST(jammer_lon AS INT) AS VARCHAR)
    ) AS jammer_grid,
    -- Threat severity based on mission impact
    CASE
        WHEN mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT') THEN 'CRITICAL'
        WHEN mission_impact IN ('DEGRADED_COMMS', 'REDUCED_BANDWIDTH') THEN 'HIGH'
        WHEN mission_impact = 'MINIMAL_IMPACT' THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    -- Strike viability based on geolocation accuracy
    CASE
        WHEN cep_km <= 1.0 THEN 'PRECISION_STRIKE_VIABLE'
        WHEN cep_km <= 3.0 THEN 'AREA_STRIKE_VIABLE'
        WHEN cep_km <= 5.0 THEN 'REQUIRES_REFINEMENT'
        ELSE 'GEOLOCATION_INSUFFICIENT'
    END AS strike_viability,
    -- Recommended response
    CASE
        WHEN mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT') AND cep_km <= 3.0 
            THEN 'KINETIC_STRIKE_RECOMMENDED'
        WHEN mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT') 
            THEN 'ADDITIONAL_GEOLOCATION_REQUIRED'
        WHEN mission_impact IN ('DEGRADED_COMMS', 'REDUCED_BANDWIDTH') 
            THEN 'FREQUENCY_HOP_AND_MONITOR'
        ELSE 'CONTINUE_MONITORING'
    END AS recommended_response
FROM jsir_raw
EMIT CHANGES;

-- JSIR Alert Stream - SATCOM Jamming Incidents
CREATE STREAM IF NOT EXISTS alert_jsir_jamming AS
SELECT
    incident_id,
    timestamp,
    jammer_location,
    jammer_country,
    jammer_system,
    jammer_lat,
    jammer_lon,
    jammer_grid,
    target_satellite,
    mission_impact,
    affected_unit_count,
    cep_km,
    strike_coords_lat,
    strike_coords_lon,
    severity,
    strike_viability,
    recommended_response,
    'SATCOM_JAMMING' AS alert_type,
    CONCAT('SATCOM JAMMING: ', jammer_system, ' (', jammer_country, ') targeting ', 
           target_satellite, '. Impact: ', mission_impact, 
           '. Affected units: ', CAST(affected_unit_count AS VARCHAR),
           '. CEP: ', CAST(cep_km AS VARCHAR), 'km. ',
           'Response: ', recommended_response) AS alert_message
FROM jsir_enriched
WHERE mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT', 'DEGRADED_COMMS', 'REDUCED_BANDWIDTH')
EMIT CHANGES;

-- Insert JSIR alerts into combined stream
INSERT INTO all_alerts
SELECT
    CONCAT('JSIR-', incident_id) AS alert_id,
    alert_type,
    severity,
    alert_message,
    incident_id AS entity_id,
    jammer_grid AS grid_square,
    jammer_lat AS lat,
    jammer_lon AS lon,
    timestamp
FROM alert_jsir_jamming
EMIT CHANGES;

-- Active Jammers by Country (15-minute window)
CREATE TABLE IF NOT EXISTS jsir_jammers_by_country AS
SELECT
    jammer_country,
    COUNT(*) AS incident_count,
    COUNT_DISTINCT(jammer_system) AS unique_systems,
    COUNT_DISTINCT(target_satellite) AS satellites_targeted,
    SUM(affected_unit_count) AS total_affected_units,
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_incidents,
    ROUND(AVG(cep_km), 2) AS avg_geolocation_accuracy
FROM jsir_enriched
WINDOW TUMBLING (SIZE 15 MINUTES)
GROUP BY jammer_country
EMIT CHANGES;

-- SATCOM Systems Under Attack
CREATE TABLE IF NOT EXISTS jsir_satcom_status AS
SELECT
    target_satellite,
    COUNT(*) AS jamming_incidents,
    COUNT_DISTINCT(jammer_country) AS attacking_countries,
    SUM(affected_unit_count) AS total_affected_units,
    ROUND(AVG(comms_degradation_pct), 1) AS avg_degradation,
    MAX(CASE WHEN mission_impact = 'COMMS_DENIED' THEN 1 ELSE 0 END) AS has_denial,
    LATEST_BY_OFFSET(recommended_response) AS current_recommendation
FROM jsir_enriched
WINDOW TUMBLING (SIZE 15 MINUTES)
GROUP BY target_satellite
EMIT CHANGES;

-- Strike-Ready Jammer Locations (high confidence geolocation)
CREATE TABLE IF NOT EXISTS jsir_strike_targets AS
SELECT
    jammer_id,
    LATEST_BY_OFFSET(jammer_location) AS location,
    LATEST_BY_OFFSET(jammer_country) AS country,
    LATEST_BY_OFFSET(jammer_system) AS system,
    LATEST_BY_OFFSET(strike_coords_lat) AS strike_lat,
    LATEST_BY_OFFSET(strike_coords_lon) AS strike_lon,
    LATEST_BY_OFFSET(cep_km) AS cep_km,
    LATEST_BY_OFFSET(strike_viability) AS viability,
    COUNT(*) AS incident_count,
    MAX(CASE WHEN mission_impact IN ('COMMS_DENIED', 'MISSION_ABORT') THEN 1 ELSE 0 END) AS caused_mission_impact
FROM jsir_enriched
WHERE cep_km <= 5.0
WINDOW TUMBLING (SIZE 30 MINUTES)
GROUP BY jammer_id
EMIT CHANGES;
