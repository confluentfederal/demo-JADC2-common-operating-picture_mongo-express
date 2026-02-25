#!/bin/bash
# JADC2 COP Full Deployment with ksqlDB Stream Processing

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo -e "${CYAN}========================================"
echo "  JADC2 Common Operating Picture"
echo "  Confluent Platform 7.7.1 + KRaft"
echo "  Multi-Domain Stream Processing"
echo -e "========================================${NC}"
echo ""

# Check Docker
if ! docker info &>/dev/null; then
    error "Docker is not running"
    exit 1
fi
success "Docker is running"

# Determine compose command
if docker compose version &>/dev/null 2>&1; then
    COMPOSE="docker compose"
else
    COMPOSE="docker-compose"
fi

cd "$SCRIPT_DIR/docker"

# Clean if requested
if [ "$1" == "--clean" ]; then
    log "Cleaning existing deployment..."
    $COMPOSE down -v --remove-orphans 2>/dev/null || true
    docker volume prune -f 2>/dev/null || true
    success "Cleanup complete"
fi

# Start infrastructure
log "Starting Kafka (KRaft mode - no Zookeeper)..."
$COMPOSE up -d kafka
sleep 5

log "Waiting for Kafka to be healthy..."
for i in {1..60}; do
    if docker exec jadc2-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        success "Kafka is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        error "Kafka failed to start"
        docker logs jadc2-kafka | tail -20
        exit 1
    fi
    sleep 2
done

log "Creating Kafka topics..."
docker exec jadc2-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic ground-force-gps --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec jadc2-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic sigint-feeds --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec jadc2-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic satellite-imagery-metadata --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec jadc2-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic military-aircraft --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec jadc2-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic naval-vessels --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec jadc2-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic cyber-threats --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec jadc2-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic all-alerts --partitions 3 --replication-factor 1 2>/dev/null || true
success "Topics created"

log "Starting Schema Registry..."
$COMPOSE up -d schema-registry
sleep 10

log "Starting MongoDB..."
$COMPOSE up -d mongodb
sleep 10

log "Starting ksqlDB Server..."
$COMPOSE up -d ksqldb-server ksqldb-cli
log "Waiting for ksqlDB to be ready (this takes ~60 seconds)..."
for i in {1..60}; do
    if curl -s http://localhost:8088/healthcheck | grep -q "RUNNING" 2>/dev/null; then
        success "ksqlDB is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        warn "ksqlDB may not be fully ready yet"
    fi
    sleep 2
done

log "Starting Control Center..."
$COMPOSE up -d control-center

log "Starting Mongo Express..."
$COMPOSE up -d mongo-express

log "Building and starting Data Generator..."
$COMPOSE up -d --build data-generator

log "Building and starting Mission Readiness Service..."
$COMPOSE up -d --build mission-readiness-service

log "Building and starting RAG Query Service..."
$COMPOSE up -d --build rag-query-service

log "Building and starting Dashboard..."
$COMPOSE up -d --build cop-dashboard

# Wait for data to start flowing
log "Waiting for data generation to begin..."
sleep 15

# Load ksqlDB queries
log "Loading ksqlDB stream processing queries..."
echo ""
echo -e "${CYAN}--- ksqlDB Streams and Tables ---${NC}"

# Load queries in sections to handle errors gracefully
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 <<'KSQL_SCRIPT'
SET 'auto.offset.reset' = 'earliest';

-- Raw Streams
CREATE STREAM IF NOT EXISTS gps_raw (
    unit_id VARCHAR KEY, unit_name VARCHAR, unit_type VARCHAR, timestamp BIGINT,
    lat DOUBLE, lon DOUBLE, heading_deg DOUBLE, speed_kph DOUBLE, altitude_m DOUBLE,
    fuel_level_pct DOUBLE, ammo_level_pct DOUBLE, personnel_count INT,
    equipment_status VARCHAR, mission_status VARCHAR, fuel_consumption_rate DOUBLE, comms_status VARCHAR
) WITH (KAFKA_TOPIC='ground-force-gps', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');

CREATE STREAM IF NOT EXISTS sigint_raw (
    intercept_id VARCHAR KEY, timestamp BIGINT, signal_type VARCHAR, frequency_mhz DOUBLE,
    bandwidth_khz DOUBLE, modulation VARCHAR, bearing_deg DOUBLE, signal_strength_dbm DOUBLE,
    lat DOUBLE, lon DOUBLE, elevation_deg DOUBLE, confidence_pct DOUBLE,
    emitter_classification VARCHAR, threat_level VARCHAR, emission_pattern VARCHAR, collector_id VARCHAR
) WITH (KAFKA_TOPIC='sigint-feeds', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');

CREATE STREAM IF NOT EXISTS satellite_raw (
    image_id VARCHAR KEY, source_id VARCHAR, timestamp BIGINT, acquisition_time VARCHAR,
    sensor_type VARCHAR, resolution_m DOUBLE, cloud_cover_pct DOUBLE, sun_elevation_deg DOUBLE,
    off_nadir_angle_deg DOUBLE, center_lat DOUBLE, center_lon DOUBLE,
    classification VARCHAR, quality_score DOUBLE, processing_status VARCHAR
) WITH (KAFKA_TOPIC='satellite-imagery-metadata', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');

KSQL_SCRIPT

success "Raw streams created"

# Create enriched streams
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 <<'KSQL_SCRIPT'
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM IF NOT EXISTS gps_enriched AS
SELECT unit_id, unit_name, unit_type, timestamp, lat, lon, heading_deg, speed_kph,
    fuel_level_pct, ammo_level_pct, personnel_count, equipment_status, mission_status,
    fuel_consumption_rate, comms_status,
    CONCAT('33T', CASE WHEN lon < 35.75 THEN 'W' WHEN lon < 36.25 THEN 'X' ELSE 'Y' END,
           CASE WHEN lat < 44.25 THEN 'M' ELSE 'N' END) AS grid_square,
    CASE WHEN fuel_consumption_rate > 0 THEN ROUND((fuel_level_pct/100.0)*1000.0/fuel_consumption_rate,1) ELSE 999.0 END AS fuel_hours_remaining,
    ROUND((fuel_level_pct*0.30)+(ammo_level_pct*0.30)+
        (CASE WHEN equipment_status='FULLY_OPERATIONAL' THEN 100.0 WHEN equipment_status='DEGRADED' THEN 60.0 
              WHEN equipment_status='MAINTENANCE_REQUIRED' THEN 30.0 ELSE 10.0 END * 0.25)+
        (CASE WHEN comms_status='NOMINAL' THEN 100.0 WHEN comms_status='DEGRADED' THEN 50.0 ELSE 0.0 END * 0.15), 1) AS readiness_score,
    CASE WHEN equipment_status='NON_OPERATIONAL' THEN 'NOT_COMBAT_READY'
         WHEN fuel_level_pct<15 OR ammo_level_pct<15 THEN 'CRITICAL'
         WHEN fuel_level_pct<30 OR ammo_level_pct<30 THEN 'DEGRADED'
         WHEN comms_status='OFFLINE' THEN 'DEGRADED' ELSE 'COMBAT_READY' END AS combat_effectiveness
FROM gps_raw EMIT CHANGES;

CREATE STREAM IF NOT EXISTS sigint_enriched AS
SELECT intercept_id, timestamp, signal_type, frequency_mhz, signal_strength_dbm, lat, lon,
    emitter_classification, threat_level, emission_pattern, collector_id, confidence_pct,
    CONCAT('33T', CASE WHEN lon < 35.75 THEN 'W' WHEN lon < 36.25 THEN 'X' ELSE 'Y' END,
           CASE WHEN lat < 44.25 THEN 'M' ELSE 'N' END) AS grid_square,
    CASE WHEN threat_level='CRITICAL' THEN 100 WHEN threat_level='HIGH' THEN 75 
         WHEN threat_level='MEDIUM' THEN 50 ELSE 25 END AS threat_score,
    CASE WHEN signal_strength_dbm > -50 THEN 'STRONG' WHEN signal_strength_dbm > -70 THEN 'MODERATE' ELSE 'WEAK' END AS signal_quality,
    CASE WHEN emitter_classification LIKE '%SA-%' THEN true WHEN emitter_classification LIKE '%RADAR%' THEN true ELSE false END AS is_air_defense
FROM sigint_raw EMIT CHANGES;

CREATE STREAM IF NOT EXISTS satellite_enriched AS
SELECT image_id, source_id, timestamp, sensor_type, resolution_m, cloud_cover_pct, quality_score,
    center_lat, center_lon, classification,
    CONCAT('33T', CASE WHEN center_lon < 35.75 THEN 'W' WHEN center_lon < 36.25 THEN 'X' ELSE 'Y' END,
           CASE WHEN center_lat < 44.25 THEN 'M' ELSE 'N' END) AS grid_square,
    ROUND(quality_score*100*(1.0-cloud_cover_pct/100.0)*(CASE WHEN resolution_m<=1.0 THEN 1.0 WHEN resolution_m<=5.0 THEN 0.7 ELSE 0.4 END),1) AS isr_effectiveness,
    CASE WHEN cloud_cover_pct>70 THEN 'POOR' WHEN cloud_cover_pct>40 THEN 'MODERATE' ELSE 'GOOD' END AS coverage_quality
FROM satellite_raw EMIT CHANGES;

KSQL_SCRIPT

success "Enriched streams created"

# Create alert streams
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 <<'KSQL_SCRIPT'
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM IF NOT EXISTS alert_fuel_critical AS
SELECT unit_id, unit_name, unit_type, grid_square, fuel_level_pct, fuel_hours_remaining, mission_status,
    'FUEL_CRITICAL' AS alert_type,
    CASE WHEN fuel_level_pct<10 OR fuel_hours_remaining<2 THEN 'CRITICAL' WHEN fuel_level_pct<15 THEN 'HIGH' ELSE 'WARNING' END AS severity,
    CONCAT('FUEL: ',unit_name,' at ',grid_square,' - ',CAST(ROUND(fuel_level_pct) AS VARCHAR),'% (',CAST(fuel_hours_remaining AS VARCHAR),'h)') AS alert_message,
    timestamp, lat, lon
FROM gps_enriched WHERE fuel_level_pct < 20 OR fuel_hours_remaining < 4 EMIT CHANGES;

CREATE STREAM IF NOT EXISTS alert_ammo_critical AS
SELECT unit_id, unit_name, unit_type, grid_square, ammo_level_pct, mission_status,
    'AMMO_CRITICAL' AS alert_type,
    CASE WHEN ammo_level_pct<10 THEN 'CRITICAL' WHEN ammo_level_pct<20 THEN 'HIGH' ELSE 'WARNING' END AS severity,
    CONCAT('AMMO: ',unit_name,' at ',grid_square,' - ',CAST(ROUND(ammo_level_pct) AS VARCHAR),'% while ',mission_status) AS alert_message,
    timestamp, lat, lon
FROM gps_enriched WHERE ammo_level_pct < 25 AND mission_status IN ('ENGAGED','DEFENSIVE') EMIT CHANGES;

CREATE STREAM IF NOT EXISTS alert_threat_radar AS
SELECT intercept_id, emitter_classification, threat_level, grid_square, lat, lon, signal_strength_dbm, signal_quality,
    'THREAT_RADAR' AS alert_type, threat_level AS severity,
    CONCAT('THREAT: ',emitter_classification,' in ',grid_square,' (',signal_quality,')') AS alert_message, timestamp
FROM sigint_enriched WHERE is_air_defense=true AND signal_strength_dbm>-70 AND threat_level IN ('CRITICAL','HIGH') EMIT CHANGES;

CREATE STREAM IF NOT EXISTS alert_comms AS
SELECT unit_id, unit_name, unit_type, grid_square, comms_status, mission_status,
    'COMMS_ALERT' AS alert_type,
    CASE WHEN comms_status='OFFLINE' THEN 'CRITICAL' ELSE 'WARNING' END AS severity,
    CONCAT('COMMS ',comms_status,': ',unit_name,' at ',grid_square) AS alert_message,
    timestamp, lat, lon
FROM gps_enriched WHERE comms_status IN ('DEGRADED','OFFLINE') EMIT CHANGES;

KSQL_SCRIPT

success "Alert streams created"

# Create aggregated tables
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 <<'KSQL_SCRIPT'
SET 'auto.offset.reset' = 'earliest';

CREATE TABLE IF NOT EXISTS grid_force_status AS
SELECT grid_square, COUNT(*) AS unit_count, ROUND(AVG(readiness_score),1) AS avg_readiness,
    ROUND(MIN(fuel_level_pct),1) AS min_fuel, ROUND(MIN(ammo_level_pct),1) AS min_ammo,
    SUM(CASE WHEN combat_effectiveness='COMBAT_READY' THEN 1 ELSE 0 END) AS combat_ready_count,
    SUM(CASE WHEN mission_status='ENGAGED' THEN 1 ELSE 0 END) AS engaged_count,
    SUM(personnel_count) AS total_personnel
FROM gps_enriched WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY grid_square EMIT CHANGES;

CREATE TABLE IF NOT EXISTS grid_threat_summary AS
SELECT grid_square, COUNT(*) AS intercept_count, MAX(threat_score) AS max_threat_score,
    SUM(CASE WHEN threat_level='CRITICAL' THEN 1 ELSE 0 END) AS critical_threats,
    SUM(CASE WHEN is_air_defense=true THEN 1 ELSE 0 END) AS air_defense_count
FROM sigint_enriched WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY grid_square EMIT CHANGES;

CREATE TABLE IF NOT EXISTS grid_isr_coverage AS
SELECT grid_square, COUNT(*) AS image_count, COUNT_DISTINCT(source_id) AS satellites_covering,
    ROUND(AVG(isr_effectiveness),1) AS avg_effectiveness
FROM satellite_enriched WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY grid_square EMIT CHANGES;

CREATE TABLE IF NOT EXISTS force_composition AS
SELECT unit_type, COUNT(*) AS unit_count, ROUND(AVG(readiness_score),1) AS avg_readiness,
    SUM(personnel_count) AS total_personnel, ROUND(AVG(fuel_level_pct),1) AS avg_fuel
FROM gps_enriched WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY unit_type EMIT CHANGES;

KSQL_SCRIPT

success "Aggregated tables created"

# Create correlation table
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 <<'KSQL_SCRIPT'
SET 'auto.offset.reset' = 'earliest';

CREATE TABLE IF NOT EXISTS active_threat_grids AS
SELECT grid_square, COUNT(*) AS threat_count, MAX(threat_score) AS max_threat,
    LATEST_BY_OFFSET(emitter_classification) AS primary_threat
FROM sigint_enriched WINDOW TUMBLING (SIZE 5 MINUTES)
WHERE threat_level IN ('CRITICAL','HIGH') AND signal_strength_dbm > -70
GROUP BY grid_square EMIT CHANGES;

KSQL_SCRIPT

success "Threat correlation table created"

# Try to create the join stream (may fail if tables not populated yet)
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 <<'KSQL_SCRIPT' 2>/dev/null || true
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM IF NOT EXISTS units_in_threat_zone AS
SELECT g.unit_id, g.unit_name, g.unit_type, g.grid_square, g.lat, g.lon,
    g.mission_status, g.readiness_score, g.combat_effectiveness,
    t.threat_count, t.max_threat, t.primary_threat,
    'UNIT_UNDER_THREAT' AS alert_type,
    CASE WHEN t.max_threat>=100 THEN 'CRITICAL' WHEN t.max_threat>=75 THEN 'HIGH' ELSE 'WARNING' END AS severity,
    CONCAT('PROXIMITY: ',g.unit_name,' in ',g.grid_square,' with ',CAST(t.threat_count AS VARCHAR),' threats') AS alert_message,
    g.timestamp
FROM gps_enriched g INNER JOIN active_threat_grids t ON g.grid_square = t.grid_square
EMIT CHANGES;

KSQL_SCRIPT

success "Stream-table join created (units in threat zones)"

echo ""
echo -e "${CYAN}========================================"
echo "  Deployment Complete!"
echo -e "========================================${NC}"
echo ""
echo -e "${GREEN}Access Points:${NC}"
echo "  • Dashboard:        http://localhost:3000"
echo "  • Control Center:   http://localhost:9021"
echo "  • Mongo Express:    http://localhost:8082  (admin/jadc2)"
echo "  • ksqlDB:           http://localhost:8088"
echo "  • Readiness API:    http://localhost:5001/api/readiness"
echo "  • Query API:        http://localhost:5002/api/query"
echo ""
echo -e "${GREEN}ksqlDB Streams Created:${NC}"
echo "  • gps_raw, sigint_raw, satellite_raw (raw input)"
echo "  • gps_enriched, sigint_enriched, satellite_enriched"
echo "  • alert_fuel_critical, alert_ammo_critical, alert_threat_radar, alert_comms"
echo "  • units_in_threat_zone (GPS + SIGINT correlation)"
echo ""
echo -e "${GREEN}ksqlDB Tables Created:${NC}"
echo "  • grid_force_status (force readiness by grid)"
echo "  • grid_threat_summary (threats by grid)"
echo "  • grid_isr_coverage (satellite coverage by grid)"
echo "  • force_composition (by unit type)"
echo "  • active_threat_grids (for correlations)"
echo ""
echo -e "${YELLOW}Useful Commands:${NC}"
echo "  View generator logs:  docker logs -f jadc2-data-generator"
echo "  ksqlDB CLI:           docker exec -it jadc2-ksqldb-cli ksql http://ksqldb-server:8088"
echo "  Check streams:        SHOW STREAMS;"
echo "  Check tables:         SHOW TABLES;"
echo "  Query a stream:       SELECT * FROM gps_enriched EMIT CHANGES LIMIT 5;"
echo "  Query a table:        SELECT * FROM grid_force_status;"
echo "  Stop all:             cd docker && $COMPOSE down"
echo ""
