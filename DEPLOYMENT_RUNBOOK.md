# JADC2 COP Deployment Runbook

Step-by-step deployment guide for the JADC2 Common Operating Picture demonstration system. This version runs entirely on local Docker — no cloud accounts required.

## Pre-Demo Setup (Day Before)

### 1. Verify Prerequisites

```bash
# Docker Desktop running with 8GB+ RAM
docker info | grep "Total Memory"

# Docker Compose v2
docker compose version
```

### 2. Start the Stack

```bash
cd cflt_mongo_demo
docker compose -f docker/docker-compose.yml up -d --build
```

### 3. Wait for Health Checks

All services need to be healthy before running the rebuild script. This takes ~90 seconds.

```bash
# Watch startup progress (macOS: use without -n flag if watch not installed)
watch -n 5 'docker ps --format "table {{.Names}}\t{{.Status}}" | grep jadc2'

# Or wait for Kafka Connect (last to be ready)
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
  echo "Waiting for Kafka Connect..."
  sleep 10
done
echo "Ready!"
```

### 4. Verify Data Generators

Two generators start automatically with the stack:

| Generator | Container | What It Produces |
|-----------|-----------|-----------------|
| **data-generator** | `jadc2-data-generator` | GPS, aircraft, naval, cyber, SIGINT, satellite, JSIR, EMBM → Kafka topics |
| **external-systems-generator** | `jadc2-external-systems` | DIA threat intel, CENTCOM readiness, DLA logistics → MongoDB + MQTT + Kafka |

```bash
docker logs jadc2-data-generator --tail 5
docker logs jadc2-external-systems --tail 5
```

### 5. Run Complete Rebuild

```bash
chmod +x rebuild-jadc2.sh
./rebuild-jadc2.sh
```

The rebuild script handles everything: MongoDB replica set initialization, 37 ksqlDB streams, 27 sink connectors, 8 source connectors, and verification. Expected output:

- 37 ksqlDB streams
- ~28 running queries
- 35 connectors (27 sinks + 8 sources) all RUNNING

### 6. Verify Data Flow

```bash
# Check data in MongoDB
docker exec jadc2-mongodb mongosh -u admin -p jadc2secret --authenticationDatabase admin --eval '
  db = db.getSiblingDB("jadc2_cop");
  db.getCollectionNames().forEach(c => print("  " + c + ": " + db[c].countDocuments({})));
'

# Verify live alerts
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e \
  "SELECT alert_type, severity, alert_message FROM ALL_ALERTS EMIT CHANGES LIMIT 3;"
```

### 7. Open Browser Tabs

Open these tabs before the demo:

1. **COP Dashboard**: http://localhost:3000
2. **Confluent Control Center**: http://localhost:9021
3. **Mongo Express**: http://localhost:8082 (admin/jadc2)

## Demo Day Quick Start

If the stack is already running from the night before:

```bash
# Verify everything is still healthy
docker ps --format "{{.Names}}: {{.Status}}" | grep jadc2 | sort

# Quick connector check
curl -s http://localhost:8083/connectors | python3 -c "import sys,json; print(f'{len(json.load(sys.stdin))} connectors registered')"
```

If containers were stopped:

```bash
docker compose -f docker/docker-compose.yml start
sleep 30  # ksqlDB streams auto-resume
```

## Demo Flow (23 minutes)

### Opening (2 min)
- Show architecture diagram from README
- "8 real-time data sources → Kafka → ksqlDB enrichment → MongoDB → COP Dashboard"
- "7 external systems feeding intelligence data back in — bidirectional flow"

### Confluent Platform (5 min)
1. **Control Center** → Topics: Show 8 raw topics with live throughput
2. **Control Center** → ksqlDB: Show running queries, explain enrichment
3. **Control Center** → Connect: Show 35 connectors, highlight source vs. sink

### ksqlDB Deep Dive (5 min)
```bash
# Show live enriched data with threat classification
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e \
  "SELECT aircraft_id, aircraft_type, iff_mode, threat_classification
   FROM AIRCRAFT_ENRICHED EMIT CHANGES LIMIT 5;"

# Show SIGINT air defense detection
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e \
  "SELECT intercept_id, emitter_classification, threat_level, grid_square
   FROM SIGINT_AIR_DEFENSE EMIT CHANGES LIMIT 3;"

# Show unified alerts
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e \
  "SELECT alert_type, severity, alert_message
   FROM ALL_ALERTS EMIT CHANGES LIMIT 5;"
```

### MongoDB (5 min)
1. **Mongo Express** → `jadc2_cop` → Show enriched documents updating in real-time
2. **Mongo Express** → `jadc2_external_sources` → Show DIA/CENTCOM/DLA data
3. Explain: Change streams enable source connectors to bring external data into Kafka

### COP Dashboard (3 min)
- http://localhost:3000
- Show unit positions updating in real-time
- Click objects to see enriched data popups
- Show alert feed
- Toggle layers (aircraft, naval, SIGINT, cyber)

### External Systems Integration (3 min)
- "DIA, CENTCOM, DLA write to MongoDB → Source connectors bring into Kafka"
- Show in Control Center → Connect → source connectors
- "Bidirectional data flow — operational data goes out, intelligence data comes in"

## Recovery Procedures

### Connector Failed

```bash
# Identify failed connectors
curl -s http://localhost:8083/connectors | jq -r '.[]' | while read c; do
  STATUS=$(curl -s "http://localhost:8083/connectors/$c/status" | jq -r '.connector.state')
  TASK=$(curl -s "http://localhost:8083/connectors/$c/status" | jq -r '.tasks[0].state // "NO_TASKS"')
  if [ "$STATUS" != "RUNNING" ] || [ "$TASK" != "RUNNING" ]; then
    echo "FAILED: $c ($STATUS / $TASK)"
  fi
done

# Restart all connectors
curl -s http://localhost:8083/connectors | jq -r '.[]' | while read c; do
  curl -s -X POST "http://localhost:8083/connectors/$c/restart?includeTasks=true"
done
```

### Source Connectors Failing (Replica Set)

```bash
# Check replica set
docker exec jadc2-mongodb mongosh -u admin -p jadc2secret --authenticationDatabase admin --eval 'rs.status().ok'

# Re-initiate if needed
docker exec jadc2-mongodb mongosh -u admin -p jadc2secret --authenticationDatabase admin --eval \
  'rs.initiate({_id: "rs0", members: [{_id: 0, host: "mongodb:27017"}]})'
```

### ksqlDB Streams Lost

Re-run the rebuild script — it drops everything first and recreates cleanly:

```bash
./rebuild-jadc2.sh
```

### Full Reset

```bash
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d --build
# Wait ~90 seconds
./rebuild-jadc2.sh
```

## Teardown

```bash
# Stop everything
docker compose -f docker/docker-compose.yml down

# Full cleanup including volumes
docker compose -f docker/docker-compose.yml down -v

# Remove built images
docker images | grep jadc2 | awk '{print $3}' | xargs docker rmi -f 2>/dev/null
```

## Inventory Checklist

Before demo, verify these counts:

| Component | Expected | Check Command |
|-----------|----------|---------------|
| Docker containers | ~14 | `docker ps \| grep jadc2 \| wc -l` |
| ksqlDB streams | ~37 | `docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e "SHOW STREAMS;" 2>/dev/null \| grep -c "\|"` |
| Running queries | ~28 | `docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e "SHOW QUERIES;" 2>/dev/null \| grep -c RUNNING` |
| Connectors | 35 | `curl -s http://localhost:8083/connectors \| python3 -c "import sys,json; print(len(json.load(sys.stdin)))"` |
| Kafka topics | 30+ | `docker exec jadc2-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \| grep -v "^_" \| wc -l` |
