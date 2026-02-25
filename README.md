# JADC2 Common Operating Picture — Confluent + MongoDB

A fully containerized demonstration of a Joint All-Domain Command and Control (JADC2) Common Operating Picture built on Confluent Platform and MongoDB. The system simulates real-time military operations across air, sea, land, cyber, and space domains with sensor-to-decision data flow in seconds rather than hours.

**Architecture:**
- 8 Kafka topics with 37 ksqlDB streams for real-time enrichment and alerting
- 35 MongoDB connectors (sink, source, and MQTT)
- 7 external system integrations: DIA, MDA, CENTCOM, NGA, Coalition C2, DLA, FAA
- Browser-based Leaflet.js COP dashboard with live aircraft, naval, SIGINT, cyber, and satellite overlays

**Features:**
- Complete streaming data pipeline from generation through enrichment to visualization
- Automated ksqlDB stream processing with threat correlation and operational alerts
- AI-ready data persistence with full audit trails
- Ships as a single `docker compose up` with automated rebuild scripting

## Architecture

```
┌─────────────────────────────────┐     ┌──────────────────────────────┐
│     DATA GENERATORS             │     │   EXTERNAL SYSTEMS           │
│                                 │     │                              │
│  ┌──────────┐ ┌──────────┐     │     │  DIA   CENTCOM   DLA        │
│  │ Ground   │ │ Aircraft │     │     │  ┌──┐  ┌──┐    ┌──┐        │
│  │ Force GPS│ │ Tracker  │     │     │  │  │  │  │    │  │        │
│  └────┬─────┘ └────┬─────┘     │     │  └──┘  └──┘    └──┘        │
│  ┌────┴─────┐ ┌────┴─────┐     │     │    │      │       │         │
│  │ Naval    │ │ Cyber    │     │     │    └──────┼───────┘         │
│  │ Vessels  │ │ Threats  │     │     │           │                  │
│  └────┬─────┘ └────┬─────┘     │     │     MongoDB (local)         │
│  ┌────┴─────┐ ┌────┴─────┐     │     │   (jadc2_external_sources)  │
│  │ SIGINT   │ │ Satellite│     │     └───────────┬──────────────────┘
│  │ Feeds    │ │ Imagery  │     │                  │
│  └────┬─────┘ └────┬─────┘     │        Source Connectors (8)
│  ┌────┴─────┐ ┌────┴─────┐     │                  │
│  │ JSIR/    │ │ EMBM     │     │                  ▼
│  │ Spectrum │ │ Tracks   │     │     ┌────────────────────────────┐
│  └────┬─────┘ └────┬─────┘     │     │      CONFLUENT PLATFORM    │
│       └──────┬─────┘           │     │                            │
└──────────────┼─────────────────┘     │  Kafka → ksqlDB (37       │
               │                       │           streams)         │
               ▼                       │  Schema Registry           │
        Kafka Topics (8)               │  Kafka Connect             │
               │                       │  Control Center            │
               └──────────────────────►│                            │
                                       └────────────┬───────────────┘
                                                    │
                                          Sink Connectors (27)
                                                    │
                                                    ▼
                                       ┌────────────────────────────┐
                                       │     MONGODB (local)        │
                                       │     (jadc2_cop)            │
                                       │                            │
                                       │  Operational data          │
                                       │  Enriched streams          │
                                       │  SIGINT/EMBM breakouts     │
                                       │  Alerts                    │
                                       │  Knowledge base            │
                                       │  Vector embeddings         │
                                       └────────────┬───────────────┘
                                                    │
                                       ┌────────────┼────────────┐
                                       │            │            │
                                       ▼            ▼            ▼
                                   Mission      RAG Query    COP Dashboard
                                   Readiness    Service      (Leaflet.js)
                                   Service      :5002        :3000
                                   :5001
```

## Data Pipeline

| Layer | Count | Description |
|-------|-------|-------------|
| Kafka Topics | 8 | Raw operational data streams |
| ksqlDB Streams | 37 | Real-time enrichment, correlation, alerting |
| Sink Connectors | 27 | Kafka → MongoDB (operational + enriched) |
| Source Connectors | 8 | MongoDB → Kafka (DIA, CENTCOM, DLA external systems) |
| MongoDB Collections | 30+ | Operational, enriched, alerts, knowledge base, vectors |

### ksqlDB Stream Breakdown

| Category | Streams | Purpose |
|----------|---------|---------|-
| Raw | 8 | GPS, Aircraft, Naval, Cyber, SIGINT, Satellite, JSIR, EMBM |
| Enriched | 8 | Grid squares, threat scores, readiness, strike viability |
| SIGINT Breakouts | 4 | Air defense, hostile comms, jamming, IFF tracks |
| EMBM Breakouts | 3 | Strike candidates, active jammers, collection requirements |
| Satellite | 2 | Pass enrichment, coverage alerts |
| Alerts | 10 | Fuel, ammo, equipment, comms, readiness, threats, hostile, cyber |
| Unified | 1 | ALL_ALERTS (aggregated from all alert streams) |
| System | 1 | KSQL_PROCESSING_LOG |

## Prerequisites

- Docker Desktop with 8GB+ RAM allocated
- Docker Compose v2
- No cloud accounts required — everything runs locally

## Project Structure

```
cflt_mongo_demo/
├── docker/
│   └── docker-compose.yml          # Full Confluent Platform + MongoDB stack
├── generators/
│   ├── Dockerfile
│   └── generator.py                # Multi-threaded Kafka data generator
├── external-systems/
│   ├── Dockerfile
│   └── external-systems-generator.py  # DIA/CENTCOM/DLA + MQTT + Kafka data
├── microservices/
│   ├── mission-readiness/
│   │   ├── Dockerfile
│   │   └── app.py                  # Change streams, predictive readiness
│   └── rag-query/
│       ├── Dockerfile
│       └── app.py                  # Semantic search, NL query interface
├── visualization/
│   ├── Dockerfile
│   ├── server.js
│   └── index.html                  # Leaflet.js COP map dashboard
├── scripts/
│   ├── mongo-init.js               # MongoDB initialization
│   ├── mongo-keyfile               # Replica set auth key
│   ├── mongo-entrypoint.sh         # MongoDB startup wrapper
│   └── seed_knowledge_base.py      # Knowledge base seeder
├── rebuild-jadc2.sh                # ★ Complete rebuild script (37 streams, 35 connectors)
├── cleanup-docker.sh               # Docker cleanup utility
├── DEPLOYMENT_RUNBOOK.md           # Step-by-step deployment guide
├── DEMO_SCRIPT.md                  # Live demo script
└── README.md
```

## Quick Start

### 1. Start the Stack

```bash
cd cflt_mongo_demo
docker compose -f docker/docker-compose.yml up -d --build
```

Wait approximately 90 seconds for all services to become healthy.

### 2. Verify Services

```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep jadc2
```

All containers should show `healthy` or `Up`.

### 3. Run the Rebuild Script

```bash
chmod +x rebuild-jadc2.sh
./rebuild-jadc2.sh
```

The rebuild script automatically initializes the MongoDB replica set (required for change streams and source connectors), creates all 37 ksqlDB streams, 27 sink connectors, and 8 source connectors. Takes approximately 3-4 minutes.

### 4. Verify the Pipeline

```bash
# Stream count (expect ~37)
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e "SHOW STREAMS;" 2>/dev/null | grep -c "|"

# Connector count (expect 35)
curl -s http://localhost:8083/connectors | python3 -c "import sys,json; print(len(json.load(sys.stdin)), 'connectors')"

# Watch live alerts
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e "SELECT * FROM ALL_ALERTS EMIT CHANGES LIMIT 5;"
```

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| COP Dashboard | http://localhost:3000 | — |
| Control Center | http://localhost:9021 | — |
| Mongo Express | http://localhost:8082 | admin / jadc2 |
| Schema Registry | http://localhost:8081 | — |
| ksqlDB REST | http://localhost:8088 | — |
| Kafka Connect REST | http://localhost:8083 | — |
| Mission Readiness API | http://localhost:5001/api/readiness | — |
| RAG Query API | http://localhost:5002/api/query | — |

## Key Demonstration Points

### Real-Time Stream Processing (ksqlDB)

- **Enrichment**: Raw GPS data → grid squares, readiness scores, fuel predictions
- **Correlation**: SIGINT emitter classification → air defense detection, hostile comms filtering
- **Alerting**: Multi-condition alerts (fuel < 6hrs AND mission engaged → CRITICAL)
- **JSIR Workflow**: Satellite jamming → geolocation → strike viability assessment

### Connector Architecture

- **Upsert sinks** (aircraft, naval, ground force): Latest position overwrites previous — always current state
- **Append sinks** (cyber, SIGINT, alerts): Full event history preserved for analysis
- **Source connectors**: External agency data (DIA threat intel, CENTCOM readiness, DLA logistics) flows into Kafka for enrichment

### MongoDB Capabilities

- **Change Streams**: Mission readiness service reacts to data changes in real-time (requires replica set)
- **Geospatial Queries**: COP dashboard queries units by location
- **Flexible Schema**: Each domain (air, naval, cyber, SIGINT) has its own document structure

## Troubleshooting

### Connectors in FAILED state

```bash
# Check specific connector error
curl -s "http://localhost:8083/connectors/<name>/status" | python3 -m json.tool

# Restart a failed connector
curl -s -X POST "http://localhost:8083/connectors/<name>/restart?includeTasks=true"
```

### Source connectors failing

Source connectors require MongoDB to run as a replica set. The rebuild script handles this automatically. If they still fail:

```bash
# Check replica set status
docker exec jadc2-mongodb mongosh -u admin -p jadc2secret --authenticationDatabase admin --eval 'rs.status().ok'

# If returns 0 or error, re-initiate
docker exec jadc2-mongodb mongosh -u admin -p jadc2secret --authenticationDatabase admin --eval \
  'rs.initiate({_id: "rs0", members: [{_id: 0, host: "mongodb:27017"}]})'
```

### ksqlDB streams not producing data

```bash
# Verify raw topics have data
docker exec jadc2-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ground-force-gps --from-beginning --max-messages 1

# Check for ksqlDB errors
docker exec jadc2-ksqldb-cli ksql http://ksqldb-server:8088 -e "SELECT * FROM KSQL_PROCESSING_LOG EMIT CHANGES LIMIT 5;"
```

### Full rebuild from scratch

```bash
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d --build
# Wait ~90 seconds
./rebuild-jadc2.sh
```

## Stopping the Demo

```bash
# Stop containers (preserves data)
docker compose -f docker/docker-compose.yml stop

# Stop and remove containers + networks (preserves volumes)
docker compose -f docker/docker-compose.yml down

# Full teardown including all data
docker compose -f docker/docker-compose.yml down -v
```
