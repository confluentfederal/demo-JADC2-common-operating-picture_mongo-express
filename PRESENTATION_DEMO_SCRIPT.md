# JADC2 COP — Presentation Demo Script

> Quick-reference script for live presentation with slides.
> Each section is one slide transition. Bold text = what to say. Regular text = what to do.

---

## Slide 1: Title

**"JADC2 Common Operating Picture. Real-time streaming data architecture. Seven external systems, one unified picture."**

---

## Slide 2: The Problem

**"Intelligence sits in silos. DIA has threat actors. ONI has maritime tracks. CENTCOM has readiness. NGA has imagery. By the time someone manually correlates across systems, the picture is stale."**

---

## Slide 3: The Architecture (diagram)

**"We stream everything into one pipeline. Enrich it automatically. Persist it for AI. Display it in real time."**

Point to diagram elements:
- Left: 7 external systems with 3 connector types
- Center: Kafka + ksqlDB enrichment
- Right: Atlas + COP Dashboard

---

## LIVE DEMO START

### Switch to Control Center

**"Let me show you what's running. 45 connectors. Three types."**

Click `source-dia-iocs`:
**"DIA publishes an IOC, Change Streams push it to Kafka instantly."**

Click `source-mda-vessel-tracks`:
**"Maritime AIS over MQTT. Same protocol the Navy uses."**

*(60 seconds)*

### Switch to Topics

Click `military-aircraft` → Messages:
**"Raw sensor data. Aircraft type, coordinates, weapons loadout. Updating every few seconds."**

*(30 seconds)*

### Switch to ksqlDB

**"ksqlDB enriches every event in milliseconds. MITRE ATT&CK classification, threat actor attribution, risk scoring. Declarative SQL, not fragile scripts."**

*(30 seconds)*

### Switch to COP Dashboard

**"This is where the commander lives."**

Toggle layers, point to icons:
**"Aircraft, naval, SIGINT, cyber — all live, all enriched."**

Click a cyber threat → show details:
**"Threat actor, kill chain phase, risk score — all added by ksqlDB."**

Click BLOCK IP:
**"Action becomes a Kafka event. Persists to Atlas. Automatic audit trail."**

Show ACTIONS tab:
**"Every decision recorded. Who, what, when, on which threat."**

*(3 minutes)*

### LIVE DEMO END

---

## Slide 4: AI-Ready

**"Everything lands in MongoDB Atlas. 38 collections. Vector search on threat intelligence. The streaming pipeline keeps data current — models train on minutes-old data, not weeks-old data."**

---

## Slide 5: By the Numbers

| | |
|---|---|
| External Systems | 7 |
| Connector Types | 3 (MongoSink, MongoSource, MqttSource) |
| Total Connectors | 45 |
| Kafka Topics | 60+ |
| Coalition Nations | 5 |
| Atlas Collections | 38+ |
| Enrichment Latency | Milliseconds |
| COP Refresh | 3 seconds |

---

## Slide 6: Key Takeaways

**Three points:**

1. **"Sensor to decision in seconds, not hours."**
2. **"Every action auditable — built into the architecture."**
3. **"Same data that feeds the COP feeds AI models."**

---

## Slide 7: Q&A

Common questions and one-line answers:

- **Classified data?** → Topics segmented by classification. Same architecture at any level.
- **Scale?** → Kafka scales horizontally. Atlas scales independently. Theater-wide ready.
- **New source?** → One connector config. Minutes, not months.
- **Latency?** → Sub-second processing. 3-second display refresh.
- **AI/ML?** → Enriched data in Atlas with vector search. Foundation already in place.
