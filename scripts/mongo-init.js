// ============================================================
// JADC2 COP - MongoDB Initialization with Vector Search & RAG
// Supports semantic search across all military data domains
// ============================================================

db = db.getSiblingDB('jadc2_cop');

print("Initializing JADC2 COP database with Vector Search & RAG support...");

// ============================================================
// CORE COLLECTIONS
// ============================================================

// Common Operating Picture - All entities with GPS
db.createCollection('common_operating_picture');
db.common_operating_picture.createIndex({ "entity_id": 1 }, { unique: true });
db.common_operating_picture.createIndex({ "entity_type": 1 });
db.common_operating_picture.createIndex({ "location": "2dsphere" });
db.common_operating_picture.createIndex({ "timestamp": -1 });
db.common_operating_picture.createIndex({ "country": 1, "entity_type": 1 });

// Operational Alerts
db.createCollection('operational_alerts');
db.operational_alerts.createIndex({ "alert_id": 1 }, { unique: true });
db.operational_alerts.createIndex({ "severity": 1, "timestamp": -1 });
db.operational_alerts.createIndex({ "alert_type": 1 });
db.operational_alerts.createIndex({ "acknowledged": 1 });
db.operational_alerts.createIndex({ "facility_id": 1 });

// Cyber Threats
db.createCollection('cyber_threats');
db.cyber_threats.createIndex({ "incident_id": 1 }, { unique: true });
db.cyber_threats.createIndex({ "severity": 1, "timestamp": -1 });
db.cyber_threats.createIndex({ "target_facility_id": 1 });
db.cyber_threats.createIndex({ "actor_country": 1 });
db.cyber_threats.createIndex({ "attack_type": 1 });

// ============================================================
// RAG DOCUMENT COLLECTIONS - For semantic search
// ============================================================

// Aircraft Documents - Enriched for RAG
db.createCollection('rag_aircraft');
db.rag_aircraft.createIndex({ "aircraft_id": 1 }, { unique: true });
db.rag_aircraft.createIndex({ "country": 1 });
db.rag_aircraft.createIndex({ "role": 1 });
db.rag_aircraft.createIndex({ "threat_level": 1 });
db.rag_aircraft.createIndex({ "location": "2dsphere" });

// Naval Documents - Enriched for RAG
db.createCollection('rag_naval');
db.rag_naval.createIndex({ "vessel_id": 1 }, { unique: true });
db.rag_naval.createIndex({ "country": 1 });
db.rag_naval.createIndex({ "vessel_class": 1 });
db.rag_naval.createIndex({ "location": "2dsphere" });

// Ground Force Documents - Enriched for RAG
db.createCollection('rag_ground_forces');
db.rag_ground_forces.createIndex({ "unit_id": 1 }, { unique: true });
db.rag_ground_forces.createIndex({ "unit_type": 1 });
db.rag_ground_forces.createIndex({ "base_facility_id": 1 });
db.rag_ground_forces.createIndex({ "location": "2dsphere" });

// Facility Documents - Enriched for RAG
db.createCollection('rag_facilities');
db.rag_facilities.createIndex({ "facility_id": 1 }, { unique: true });
db.rag_facilities.createIndex({ "facility_type": 1 });
db.rag_facilities.createIndex({ "region": 1 });
db.rag_facilities.createIndex({ "location": "2dsphere" });

// Cyber Incident Documents - Enriched for RAG
db.createCollection('rag_cyber_incidents');
db.rag_cyber_incidents.createIndex({ "incident_id": 1 }, { unique: true });
db.rag_cyber_incidents.createIndex({ "threat_actor": 1 });
db.rag_cyber_incidents.createIndex({ "attack_type": 1 });
db.rag_cyber_incidents.createIndex({ "target_facility_id": 1 });

// SIGINT Documents - Enriched for RAG
db.createCollection('rag_sigint');
db.rag_sigint.createIndex({ "intercept_id": 1 }, { unique: true });
db.rag_sigint.createIndex({ "signal_type": 1 });
db.rag_sigint.createIndex({ "threat_level": 1 });
db.rag_sigint.createIndex({ "emitter_country": 1 });

// Satellite Imagery Documents - Enriched for RAG
db.createCollection('rag_satellite');
db.rag_satellite.createIndex({ "image_id": 1 }, { unique: true });
db.rag_satellite.createIndex({ "sensor_type": 1 });
db.rag_satellite.createIndex({ "classification": 1 });
db.rag_satellite.createIndex({ "location": "2dsphere" });

// ============================================================
// VECTOR EMBEDDINGS COLLECTION
// Stores pre-computed embeddings for all documents
// ============================================================

db.createCollection('vector_embeddings');
db.vector_embeddings.createIndex({ "source_collection": 1, "source_id": 1 }, { unique: true });
db.vector_embeddings.createIndex({ "entity_type": 1 });
db.vector_embeddings.createIndex({ "timestamp": -1 });

// ============================================================
// RAG METADATA & KNOWLEDGE BASE
// ============================================================

// Military Equipment Knowledge Base
db.createCollection('kb_equipment');
db.kb_equipment.createIndex({ "equipment_id": 1 }, { unique: true });
db.kb_equipment.createIndex({ "equipment_type": 1 });
db.kb_equipment.createIndex({ "country_of_origin": 1 });

// Threat Actor Knowledge Base
db.createCollection('kb_threat_actors');
db.kb_threat_actors.createIndex({ "actor_id": 1 }, { unique: true });
db.kb_threat_actors.createIndex({ "country": 1 });
db.kb_threat_actors.createIndex({ "sophistication": 1 });

// Geopolitical Context
db.createCollection('kb_geopolitical');
db.kb_geopolitical.createIndex({ "entity_id": 1 }, { unique: true });
db.kb_geopolitical.createIndex({ "entity_type": 1 });
db.kb_geopolitical.createIndex({ "region": 1 });

// ============================================================
// QUERY HISTORY & ANALYTICS
// ============================================================

db.createCollection('rag_query_history');
db.rag_query_history.createIndex({ "timestamp": -1 });
db.rag_query_history.createIndex({ "query_type": 1 });
db.rag_query_history.createIndex({ "user_id": 1 });

// ============================================================
// SEED KNOWLEDGE BASE DATA
// ============================================================

// Equipment Knowledge Base
db.kb_equipment.insertMany([
    // US Aircraft
    {equipment_id: "F22A", equipment_type: "FIGHTER", country_of_origin: "US", name: "F-22A Raptor", 
     description: "Fifth-generation stealth tactical fighter aircraft with supercruise capability",
     capabilities: ["Air superiority", "Stealth", "Supercruise", "Advanced avionics"],
     weapons: ["AIM-120 AMRAAM", "AIM-9 Sidewinder", "GBU-32 JDAM", "M61A2 Vulcan"],
     max_speed_kts: 1500, combat_radius_nm: 460, ceiling_ft: 65000,
     rag_text: "The F-22A Raptor is America's premier air superiority fighter. It combines stealth technology, supercruise capability, and advanced avionics to dominate any aerial battlefield. Armed with AIM-120 AMRAAMs and AIM-9 Sidewinders, it can engage multiple targets beyond visual range while remaining undetected."},
    
    {equipment_id: "F35A", equipment_type: "FIGHTER", country_of_origin: "US", name: "F-35A Lightning II",
     description: "Fifth-generation multirole stealth fighter with advanced sensor fusion",
     capabilities: ["Stealth", "Sensor fusion", "Network-centric warfare", "Ground attack"],
     weapons: ["AIM-120 AMRAAM", "AIM-9X", "GBU-31 JDAM", "GAU-22/A cannon"],
     max_speed_kts: 1200, combat_radius_nm: 670, ceiling_ft: 50000,
     rag_text: "The F-35A Lightning II is a fifth-generation multirole stealth fighter featuring unprecedented sensor fusion and network connectivity. It serves as a force multiplier, sharing targeting data across the battlespace while conducting precision strikes."},
    
    {equipment_id: "MQ9", equipment_type: "UAV", country_of_origin: "US", name: "MQ-9 Reaper",
     description: "Remotely piloted aircraft for persistent ISR and precision strike",
     capabilities: ["Long endurance ISR", "Precision strike", "Signals intelligence"],
     weapons: ["AGM-114 Hellfire", "GBU-12 Paveway II", "GBU-38 JDAM"],
     max_speed_kts: 260, endurance_hours: 27, ceiling_ft: 50000,
     rag_text: "The MQ-9 Reaper is a hunter-killer UAV providing persistent surveillance and precision strike capability. With 27+ hours of endurance, it can loiter over target areas conducting ISR before engaging with Hellfire missiles or laser-guided bombs."},
    
    // Russian Aircraft
    {equipment_id: "SU35S", equipment_type: "FIGHTER", country_of_origin: "RU", name: "Su-35S Flanker-E",
     description: "Russian 4++ generation air superiority fighter with thrust vectoring",
     capabilities: ["Supermaneuverability", "Long-range engagement", "Multi-target tracking"],
     weapons: ["R-77-1", "R-27", "R-73", "Kh-31", "KAB-500"],
     max_speed_kts: 1500, combat_radius_nm: 970, ceiling_ft: 59000,
     rag_text: "The Su-35S Flanker-E is Russia's most capable non-stealth fighter. Equipped with thrust-vectoring engines and the Irbis-E radar, it can track 30 targets and engage 8 simultaneously. A formidable threat in both BVR and WVR engagements."},
    
    {equipment_id: "SU57", equipment_type: "FIGHTER", country_of_origin: "RU", name: "Su-57 Felon",
     description: "Russian fifth-generation stealth multirole fighter",
     capabilities: ["Stealth", "Supermaneuverability", "Advanced avionics", "Internal weapons bays"],
     weapons: ["R-77M", "R-37M", "Kh-59MK2", "Kh-38M"],
     max_speed_kts: 1600, combat_radius_nm: 930, ceiling_ft: 65000,
     rag_text: "The Su-57 Felon is Russia's answer to the F-22 and F-35. Featuring reduced radar cross-section, internal weapons bays, and advanced avionics, it represents a significant threat. Limited production numbers reduce its operational impact."},
    
    // Chinese Aircraft
    {equipment_id: "J20", equipment_type: "FIGHTER", country_of_origin: "CN", name: "J-20 Mighty Dragon",
     description: "Chinese fifth-generation stealth fighter optimized for long-range interception",
     capabilities: ["Stealth", "Long-range strike", "BVR combat"],
     weapons: ["PL-15", "PL-10", "PL-21"],
     max_speed_kts: 1300, combat_radius_nm: 680, ceiling_ft: 65000,
     rag_text: "The J-20 Mighty Dragon is China's first fifth-generation stealth fighter. Optimized for long-range interception and strike missions, it poses a significant threat to force projection assets like tankers and AWACS."},
    
    // Iranian Aircraft
    {equipment_id: "F14A_IR", equipment_type: "FIGHTER", country_of_origin: "IR", name: "F-14A Tomcat (Iranian)",
     description: "Legacy US fighter operated by Iranian Air Force since 1976",
     capabilities: ["Long-range interception", "Fleet defense", "Phoenix missile capable"],
     weapons: ["AIM-54 Phoenix", "AIM-9 Sidewinder", "AIM-7 Sparrow"],
     max_speed_kts: 1500, combat_radius_nm: 500, ceiling_ft: 53000,
     rag_text: "Iran operates the last flying F-14 Tomcats globally. Despite their age, these aircraft remain capable with domestically-maintained AIM-54 Phoenix missiles. They serve as Iran's primary long-range interceptors."},
    
    // Naval Vessels
    {equipment_id: "CVN_NIMITZ", equipment_type: "CARRIER", country_of_origin: "US", name: "Nimitz-class CVN",
     description: "Nuclear-powered supercarrier forming the backbone of US naval power projection",
     capabilities: ["Power projection", "Air wing of 90+ aircraft", "4.5 acres of flight deck"],
     displacement_tons: 100000, crew: 5700, aircraft_capacity: 90,
     rag_text: "The Nimitz-class aircraft carrier is the most powerful warship ever built. Each carrier deploys with an air wing of 70-90 aircraft and serves as a mobile airbase capable of projecting American power anywhere in the world."},
    
    {equipment_id: "DDG_BURKE", equipment_type: "DESTROYER", country_of_origin: "US", name: "Arleigh Burke-class DDG",
     description: "Aegis-equipped guided missile destroyer with advanced air defense",
     capabilities: ["Aegis Combat System", "BMD capable", "Tomahawk strike", "ASW"],
     weapons: ["Mk-41 VLS (96 cells)", "Harpoon", "Mk-45 5-inch gun", "Phalanx CIWS"],
     displacement_tons: 9700, crew: 330,
     rag_text: "The Arleigh Burke-class destroyer is the US Navy's premier surface combatant. Equipped with the Aegis Combat System and 96-cell VLS, it provides fleet air defense, ballistic missile defense, and land attack capabilities."},
]);

// Threat Actor Knowledge Base
db.kb_threat_actors.insertMany([
    {actor_id: "APT28", name: "APT28 (Fancy Bear)", country: "RU", sophistication: "ADVANCED",
     aliases: ["Fancy Bear", "Sofacy", "Sednit", "Pawn Storm"],
     attribution: "Russian GRU (Unit 26165)",
     targets: ["Government", "Military", "Defense contractors", "Media"],
     ttps: ["T1566 Phishing", "T1190 Exploit Public Apps", "T1078 Valid Accounts", "T1071 Application Layer Protocol"],
     rag_text: "APT28, also known as Fancy Bear, is a Russian state-sponsored threat actor attributed to GRU Unit 26165. They specialize in espionage against government and military targets using sophisticated spearphishing and zero-day exploits. Notable operations include attacks on NATO, the DNC, and WADA."},
    
    {actor_id: "APT29", name: "APT29 (Cozy Bear)", country: "RU", sophistication: "ADVANCED",
     aliases: ["Cozy Bear", "The Dukes", "Nobelium"],
     attribution: "Russian SVR (Foreign Intelligence Service)",
     targets: ["Government", "Think tanks", "Healthcare", "Energy"],
     ttps: ["T1195 Supply Chain", "T1027 Obfuscation", "T1071 C2", "T1484 Domain Policy Modification"],
     rag_text: "APT29, or Cozy Bear, is attributed to Russia's SVR intelligence service. Known for patient, stealthy operations, they conducted the SolarWinds supply chain attack affecting thousands of organizations including US government agencies."},
    
    {actor_id: "SANDWORM", name: "Sandworm Team", country: "RU", sophistication: "ADVANCED",
     aliases: ["Sandworm", "Voodoo Bear", "IRIDIUM"],
     attribution: "Russian GRU (Unit 74455)",
     targets: ["Critical infrastructure", "Energy", "Government", "Elections"],
     ttps: ["T1059 Command Line", "T1486 Data Encryption", "T1561 Disk Wipe", "T1495 Firmware Corruption"],
     rag_text: "Sandworm is Russia's most destructive cyber unit, attributed to GRU Unit 74455. They are responsible for NotPetya, attacks on Ukraine's power grid, and the 2018 Winter Olympics cyberattack. They specialize in destructive operations against critical infrastructure."},
    
    {actor_id: "APT40", name: "APT40 (Leviathan)", country: "CN", sophistication: "ADVANCED",
     aliases: ["Leviathan", "TEMP.Periscope", "Bronze Mohawk"],
     attribution: "Chinese MSS (Hainan State Security)",
     targets: ["Maritime", "Defense", "Aviation", "Research institutions"],
     ttps: ["T1566 Phishing", "T1203 Exploitation", "T1048 Exfiltration Over Alternative Protocol"],
     rag_text: "APT40 is a Chinese state-sponsored group focused on maritime and defense targets. Operating from Hainan province, they support China's naval modernization by stealing technology from defense contractors and research institutions."},
    
    {actor_id: "APT41", name: "APT41 (Double Dragon)", country: "CN", sophistication: "ADVANCED",
     aliases: ["Double Dragon", "Wicked Panda", "Barium"],
     attribution: "Chinese MSS with criminal ties",
     targets: ["Healthcare", "Telecom", "Gaming", "Government"],
     ttps: ["T1190 Exploit", "T1055 Process Injection", "T1021 Remote Services"],
     rag_text: "APT41 uniquely blends state-sponsored espionage with financially-motivated cybercrime. They have compromised telecom providers for surveillance while simultaneously targeting video game companies for profit."},
    
    {actor_id: "CHARMING_KITTEN", name: "Charming Kitten", country: "IR", sophistication: "ADVANCED",
     aliases: ["APT35", "Phosphorus", "Ajax Security Team"],
     attribution: "Iranian IRGC",
     targets: ["Dissidents", "Journalists", "Academia", "Government"],
     ttps: ["T1566 Phishing", "T1528 Steal Tokens", "T1114 Email Collection"],
     rag_text: "Charming Kitten is an Iranian threat actor tied to the IRGC. They focus on surveillance of dissidents and journalists but have expanded to target Western government officials and defense contractors using sophisticated social engineering."},
    
    {actor_id: "MUDDYWATER", name: "MuddyWater", country: "IR", sophistication: "MODERATE",
     aliases: ["Seedworm", "TEMP.Zagros", "Static Kitten"],
     attribution: "Iranian MOIS",
     targets: ["Government", "Telecom", "Oil & Gas", "Middle East entities"],
     ttps: ["T1566 Phishing", "T1059 PowerShell", "T1105 Ingress Tool Transfer"],
     rag_text: "MuddyWater is an Iranian espionage group targeting Middle Eastern governments and organizations. They use PowerShell-based backdoors and living-off-the-land techniques for persistent access."},
    
    {actor_id: "LAZARUS", name: "Lazarus Group", country: "KP", sophistication: "ADVANCED",
     aliases: ["Hidden Cobra", "Zinc", "APT38"],
     attribution: "North Korean RGB",
     targets: ["Financial", "Cryptocurrency", "Defense", "Media"],
     ttps: ["T1566 Phishing", "T1059 Scripting", "T1486 Ransomware"],
     rag_text: "Lazarus Group is North Korea's primary cyber operations unit. Responsible for the Sony Pictures hack, WannaCry ransomware, and billions in cryptocurrency theft. They fund North Korea's weapons programs through cybercrime."},
]);

// Geopolitical Context
db.kb_geopolitical.insertMany([
    {entity_id: "IRAN", entity_type: "NATION_STATE", region: "MIDDLE_EAST",
     name: "Islamic Republic of Iran",
     allies: ["Russia", "China", "Syria", "Hezbollah", "Hamas"],
     adversaries: ["United States", "Israel", "Saudi Arabia", "UAE"],
     military_strength: "Regional power with asymmetric capabilities",
     nuclear_status: "Enrichment program, threshold state",
     rag_text: "Iran is a regional power pursuing nuclear capability while maintaining extensive proxy networks across the Middle East. Key adversary to US and allied forces in the region, with advanced missile and drone programs."},
    
    {entity_id: "RUSSIA", entity_type: "NATION_STATE", region: "EURASIA",
     name: "Russian Federation",
     allies: ["China", "Iran", "Syria", "Belarus"],
     adversaries: ["NATO", "United States", "Ukraine"],
     military_strength: "Major conventional and nuclear power",
     rag_text: "Russia maintains significant military presence in Syria and the Eastern Mediterranean. Advanced cyber capabilities and extensive intelligence operations target NATO and US interests globally."},
    
    {entity_id: "CHINA", entity_type: "NATION_STATE", region: "ASIA_PACIFIC",
     name: "People's Republic of China",
     allies: ["Russia", "North Korea", "Pakistan"],
     adversaries: ["United States", "Taiwan", "Japan", "India"],
     military_strength: "Near-peer competitor, rapidly modernizing",
     rag_text: "China is rapidly expanding its military presence beyond the First Island Chain. PLAN operations in the Indian Ocean and Arabian Sea support their Belt and Road Initiative and challenge US naval dominance."},
    
    {entity_id: "STRAIT_OF_HORMUZ", entity_type: "CHOKE_POINT", region: "PERSIAN_GULF",
     name: "Strait of Hormuz",
     significance: "20% of global oil transits daily",
     threats: ["Iranian mining", "IRGC fast boats", "Anti-ship missiles"],
     rag_text: "The Strait of Hormuz is the world's most important oil chokepoint. Iran threatens to close it during conflicts, with IRGC naval forces and coastal anti-ship missiles posing significant risks to tanker traffic."},
    
    {entity_id: "RED_SEA", entity_type: "WATERWAY", region: "MIDDLE_EAST",
     name: "Red Sea",
     significance: "Suez Canal access, 12% of global trade",
     threats: ["Houthi attacks", "Piracy", "Iranian proxy operations"],
     rag_text: "The Red Sea has become increasingly contested with Houthi attacks on commercial shipping. US naval forces maintain presence to protect the Bab el-Mandeb strait and Suez Canal access."},
]);

// ============================================================
// VECTOR SEARCH INDEX DEFINITIONS (Atlas Search compatible)
// Note: These require Atlas or equivalent vector search capability
// ============================================================

print("Creating vector search index definitions...");

// Store index definitions for reference (actual creation requires Atlas)
db.vector_index_definitions.insertMany([
    {
        index_name: "aircraft_vector_index",
        collection: "rag_aircraft",
        definition: {
            mappings: {
                dynamic: true,
                fields: {
                    embedding: {
                        type: "knnVector",
                        dimensions: 1536,
                        similarity: "cosine"
                    },
                    rag_text: { type: "string" },
                    country: { type: "string" },
                    role: { type: "string" },
                    aircraft_type: { type: "string" }
                }
            }
        }
    },
    {
        index_name: "naval_vector_index",
        collection: "rag_naval",
        definition: {
            mappings: {
                dynamic: true,
                fields: {
                    embedding: {
                        type: "knnVector",
                        dimensions: 1536,
                        similarity: "cosine"
                    },
                    rag_text: { type: "string" },
                    country: { type: "string" },
                    vessel_class: { type: "string" }
                }
            }
        }
    },
    {
        index_name: "cyber_vector_index",
        collection: "rag_cyber_incidents",
        definition: {
            mappings: {
                dynamic: true,
                fields: {
                    embedding: {
                        type: "knnVector",
                        dimensions: 1536,
                        similarity: "cosine"
                    },
                    rag_text: { type: "string" },
                    threat_actor: { type: "string" },
                    attack_type: { type: "string" },
                    target_facility: { type: "string" }
                }
            }
        }
    },
    {
        index_name: "knowledge_base_vector_index",
        collection: "kb_equipment",
        definition: {
            mappings: {
                dynamic: true,
                fields: {
                    embedding: {
                        type: "knnVector",
                        dimensions: 1536,
                        similarity: "cosine"
                    },
                    rag_text: { type: "string" },
                    equipment_type: { type: "string" },
                    country_of_origin: { type: "string" }
                }
            }
        }
    }
]);

print("✓ JADC2 COP database initialized with Vector Search & RAG support");
print("✓ Knowledge base seeded with equipment, threat actors, and geopolitical data");
print("✓ Vector index definitions stored (requires Atlas for actual creation)");
