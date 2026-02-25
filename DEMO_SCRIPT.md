# JADC2 COP — Demo Script

> **Duration:** 12–15 minutes
> **Audience:** Army, Cyber, Intelligence personnel
> **Flow:** Connectors → Topics → ksqlDB → COP Dashboard → AI

## Setup (Before Demo)

1. All containers running: `docker compose ps` (should show 12 services healthy)
2. Open two browser tabs:
   - **Tab 1:** Confluent Control Center — http://localhost:9021
   - **Tab 2:** COP Dashboard — http://localhost:3000
3. Optionally open Atlas in a third tab for the AI section

---

## Act 1: Where the Data Comes From (3 min)

**Open:** Control Center → Connect

> "Seven external systems feed this pipeline right now — the same integration patterns used in production CENTCOM environments."

**Show:** Scroll the connector list (45 connectors visible)

> "45 connectors, three different types. The Army runs databases, the Navy runs sensor feeds, coalition partners run their own Kafka clusters. This architecture handles all of them."

**Click:** `source-dia-iocs`

> "When a DIA analyst publishes a new Indicator of Compromise — a malware hash, a C2 IP — MongoDB Change Streams push it into Kafka instantly. No polling, no batch. Available to every downstream system the moment the analyst hits save."

**Click:** `source-mda-vessel-tracks`

> "ONI maritime data. AIS transponder feeds over MQTT — that's how AIS actually moves in the real world. 60 vessels tracked across the Strait of Hormuz, Red Sea, and Arabian Sea. Different protocol, same pipeline."

**Click:** Any sink connector briefly

> "And 31 sink connectors on the output side persist every enriched stream into MongoDB Atlas in real time."

---

## Act 2: What the Data Looks Like (2 min)

**Open:** Control Center → Topics → `military-aircraft`

> "Every topic is a domain. Aircraft telemetry: tail number, type, coordinates, altitude, heading, IFF mode, weapons loadout."

**Click:** Messages tab, show a payload

> "Real JSON updating every few seconds. F-35s, Su-57s, J-20s — US, Russian, Chinese aircraft across the CENTCOM AOR."

**Click:** Topics → `ext-mda-vessel-tracks`

> "Maritime data from MQTT. Same AIS format that NAVCENT uses."

**Click:** Topics → `coalition-gbr-air-tracks`

> "UK Royal Air Force — Typhoons, F-35Bs, Voyager tankers. Five nations feeding this pipeline."

---

## Act 3: Turning Data into Intelligence (3 min)

**Open:** Control Center → ksqlDB

> "ksqlDB processes every event as it arrives. Not a batch job. Not manual correlation. Continuous, real-time enrichment."

**Show:** Streams list

> "Raw streams like CYBER_THREATS become enriched streams with MITRE ATT&CK kill chain phase, threat actor attribution, and risk scoring. That enrichment happens in milliseconds."

**Run:** `SELECT * FROM CYBER_ENRICHED EMIT CHANGES LIMIT 3;`

> "Watch — raw cyber events come in, ksqlDB enriches them automatically. The intelligence gets better as we add threat actors to the lookup."

**Mention:** OPERATIONAL_ALERTS stream

> "ksqlDB generates alerts the instant thresholds are crossed. Hostile aircraft near US bases, critical cyber threats — these fire in real time."

---

## Act 4: The Common Operating Picture (3 min)

**Switch to:** COP Dashboard (Tab 2)

> "Everything we just walked through feeds this map. Multi-domain awareness, updating every three seconds."

**Point to:** Map elements

> "Aircraft with country colors. Naval vessels shaped by class. SIGINT intercepts. Cyber threats. All live, all enriched."

**Demo:** Toggle layers on/off

> "Intel officer needs cyber and SIGINT only? Two clicks. Naval commander wants maritime? One click."

**Demo:** Click a cyber threat → show enriched details

> "Full picture: threat actor, target, MITRE classification, kill chain phase, risk score. All enriched by ksqlDB before it hit the screen."

**Demo:** Click BLOCK IP

> "I take action. That becomes a Kafka event, persists to Atlas, feeds analytics. The audit trail is automatic."

**Demo:** Show ACTIONS tab

> "Every action, every operator, every timestamp. The accountability chain that IG and CYBERCOM require — built in, not bolted on."

---

## Act 5: Enabling AI (2 min)

**Show:** Atlas collections (browser tab or describe)

> "Everything lands in MongoDB Atlas. 38 collections, millions of documents. This is your AI foundation — structured, enriched, current data."

> "Atlas vector search lets you find threats by meaning — 'show me patterns similar to SolarWinds' — not just keywords. The streaming pipeline keeps embeddings current."

**Close:**

> "Seven external systems, three connector types, one unified picture. Sensor to decision in seconds. Every event enriched, persisted, and auditable. And the same data feeds AI models. That's the architecture."

---

## Anticipated Questions

| Question | Answer |
|----------|--------|
| Classified data? | Architecture is classification-agnostic. Topics segmented by level. Cross-domain guards at topic boundaries. |
| Source goes down? | Kafka retains all data. Connectors auto-resume on reconnect. No data loss. |
| Scale to real AOR? | Confluent scales horizontally (more partitions/brokers). Atlas scales independently. |
| Add a new source? | One JSON connector config. Minutes, not months. |
| Latency? | Sub-second Kafka processing + 3s dashboard refresh. Operator sees threats within seconds. |
| AI/ML? | Every enriched event persists to Atlas with full context. Vector search today, predictive analysis tomorrow. |
