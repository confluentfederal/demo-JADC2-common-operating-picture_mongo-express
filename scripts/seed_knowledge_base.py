#!/usr/bin/env python3
"""
Seed Knowledge Base and RAG Data into MongoDB Atlas
====================================================
Creates initial data for:
- kb_equipment (military equipment specs)
- kb_threat_actors (APT groups)
- kb_facilities (US military installations)
- kb_geopolitical (country/region data)
- rag_documents (source documents)
- rag_chunks (chunked text for retrieval)

Usage:
    python3 seed_knowledge_base.py <atlas-connection-string>
"""

import sys
import os
from datetime import datetime
import hashlib

try:
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pymongo not installed")
    sys.exit(1)

DATABASE = "jadc2_cop"

# ============================================================
# KNOWLEDGE BASE: EQUIPMENT
# ============================================================
KB_EQUIPMENT = [
    {
        "equipment_id": "F-35A",
        "name": "F-35A Lightning II",
        "category": "FIGHTER",
        "subcategory": "5TH_GEN_MULTIROLE",
        "manufacturer": "Lockheed Martin",
        "country": "US",
        "specifications": {
            "max_speed_mach": 1.6,
            "combat_radius_nm": 669,
            "service_ceiling_ft": 50000,
            "max_takeoff_weight_lb": 70000,
            "weapons_payload_lb": 18000,
            "crew": 1
        },
        "sensors": ["AN/APG-81 AESA Radar", "AN/AAQ-37 DAS", "AN/ASQ-239 EW Suite", "EOTS"],
        "weapons": ["AIM-120 AMRAAM", "AIM-9X Sidewinder", "GBU-31 JDAM", "GBU-39 SDB", "25mm GAU-22"],
        "description": "5th generation stealth multirole fighter with advanced sensor fusion and network-centric warfare capabilities. Primary strike fighter for USAF.",
        "operational_status": "ACTIVE",
        "first_flight": "2006-12-15"
    },
    {
        "equipment_id": "F-22A",
        "name": "F-22A Raptor",
        "category": "FIGHTER",
        "subcategory": "5TH_GEN_AIR_SUPERIORITY",
        "manufacturer": "Lockheed Martin",
        "country": "US",
        "specifications": {
            "max_speed_mach": 2.25,
            "combat_radius_nm": 460,
            "service_ceiling_ft": 65000,
            "max_takeoff_weight_lb": 83500,
            "weapons_payload_lb": 8000,
            "crew": 1
        },
        "sensors": ["AN/APG-77 AESA Radar", "AN/AAR-56 MWS", "AN/ALR-94 EW"],
        "weapons": ["AIM-120 AMRAAM", "AIM-9X Sidewinder", "GBU-32 JDAM", "20mm M61A2"],
        "description": "Premier air superiority fighter with supercruise capability and all-aspect stealth. Designed to dominate contested airspace.",
        "operational_status": "ACTIVE",
        "first_flight": "1997-09-07"
    },
    {
        "equipment_id": "Su-57",
        "name": "Su-57 Felon",
        "category": "FIGHTER",
        "subcategory": "5TH_GEN_MULTIROLE",
        "manufacturer": "Sukhoi",
        "country": "RU",
        "specifications": {
            "max_speed_mach": 2.0,
            "combat_radius_nm": 800,
            "service_ceiling_ft": 65000,
            "max_takeoff_weight_lb": 77000,
            "weapons_payload_lb": 22000,
            "crew": 1
        },
        "sensors": ["N036 Byelka AESA Radar", "101KS Atoll EO", "L402 Himalayas EW"],
        "weapons": ["R-77M", "R-74M2", "Kh-59MK2", "Kh-38M", "30mm GSh-30-1"],
        "description": "Russian 5th generation stealth fighter. Limited production. Features advanced avionics and internal weapons bays.",
        "operational_status": "LIMITED_OPERATIONAL",
        "first_flight": "2010-01-29"
    },
    {
        "equipment_id": "S-400",
        "name": "S-400 Triumf",
        "category": "SAM",
        "subcategory": "LONG_RANGE_SAM",
        "manufacturer": "Almaz-Antey",
        "country": "RU",
        "specifications": {
            "max_range_km": 400,
            "max_altitude_km": 30,
            "max_target_speed_mach": 14.5,
            "simultaneous_targets": 80,
            "missiles_per_battery": 112
        },
        "radar_systems": ["91N6E Big Bird", "92N6E Grave Stone", "96L6E"],
        "missiles": ["40N6 (400km)", "48N6 (250km)", "9M96E2 (120km)", "9M96E (40km)"],
        "description": "Russia's most advanced operational air defense system. Capable of engaging aircraft, cruise missiles, and ballistic missiles.",
        "operational_status": "ACTIVE",
        "first_deployment": "2007"
    },
    {
        "equipment_id": "CVN-78",
        "name": "Gerald R. Ford-class Aircraft Carrier",
        "category": "NAVAL",
        "subcategory": "AIRCRAFT_CARRIER",
        "manufacturer": "Huntington Ingalls",
        "country": "US",
        "specifications": {
            "displacement_tons": 100000,
            "length_ft": 1106,
            "beam_ft": 256,
            "max_speed_kts": 30,
            "aircraft_capacity": 75,
            "crew": 4539
        },
        "systems": ["EMALS", "AAG", "DBR", "SSDS Mk 2"],
        "weapons": ["RIM-162 ESSM", "RIM-116 RAM", "Phalanx CIWS"],
        "description": "Most advanced aircraft carrier ever built. Features electromagnetic launch system and advanced arresting gear.",
        "operational_status": "ACTIVE",
        "commissioned": "2017-07-22"
    },
    {
        "equipment_id": "MQ-9",
        "name": "MQ-9 Reaper",
        "category": "UAV",
        "subcategory": "MALE_UAS",
        "manufacturer": "General Atomics",
        "country": "US",
        "specifications": {
            "max_speed_kts": 240,
            "endurance_hours": 27,
            "service_ceiling_ft": 50000,
            "max_payload_lb": 3750,
            "wingspan_ft": 66,
            "crew": 0
        },
        "sensors": ["MTS-B EO/IR", "Lynx SAR", "AN/APY-8 Radar"],
        "weapons": ["AGM-114 Hellfire", "GBU-12 Paveway II", "GBU-38 JDAM", "AIM-9X"],
        "description": "Primary armed UAV for USAF. Persistent ISR and precision strike capability.",
        "operational_status": "ACTIVE",
        "first_flight": "2001-02-02"
    },
    {
        "equipment_id": "THAAD",
        "name": "Terminal High Altitude Area Defense",
        "category": "BMD",
        "subcategory": "TERMINAL_PHASE_BMD",
        "manufacturer": "Lockheed Martin",
        "country": "US",
        "specifications": {
            "max_range_km": 200,
            "max_altitude_km": 150,
            "interceptor_speed_mach": 8,
            "launchers_per_battery": 6,
            "missiles_per_launcher": 8
        },
        "radar_systems": ["AN/TPY-2 X-Band Radar"],
        "description": "Ballistic missile defense system designed to intercept short and medium-range ballistic missiles in terminal phase.",
        "operational_status": "ACTIVE",
        "first_deployment": "2008"
    },
    {
        "equipment_id": "J-20",
        "name": "J-20 Mighty Dragon",
        "category": "FIGHTER",
        "subcategory": "5TH_GEN_AIR_SUPERIORITY",
        "manufacturer": "Chengdu Aerospace",
        "country": "CN",
        "specifications": {
            "max_speed_mach": 2.0,
            "combat_radius_nm": 650,
            "service_ceiling_ft": 65000,
            "max_takeoff_weight_lb": 80000,
            "crew": 1
        },
        "sensors": ["Type 1475 AESA Radar", "EOTS-86", "EORD-31"],
        "weapons": ["PL-15", "PL-10", "PL-21"],
        "description": "China's first 5th generation stealth fighter. Long-range interceptor designed to counter US ISR and tanker assets.",
        "operational_status": "ACTIVE",
        "first_flight": "2011-01-11"
    }
]

# ============================================================
# KNOWLEDGE BASE: THREAT ACTORS
# ============================================================
KB_THREAT_ACTORS = [
    {
        "actor_id": "APT28",
        "names": ["APT28", "Fancy Bear", "Sofacy", "Pawn Storm", "Sednit", "STRONTIUM"],
        "attribution": "Russia - GRU Unit 26165 (85th GTsSS)",
        "country": "RU",
        "sophistication": "ADVANCED",
        "motivation": "ESPIONAGE",
        "active_since": "2004",
        "typical_targets": ["Government", "Military", "Defense Contractors", "Media", "Political Organizations"],
        "geographic_focus": ["NATO Countries", "Ukraine", "Georgia", "Western Europe", "United States"],
        "ttps": {
            "initial_access": ["T1566 Phishing", "T1190 Exploit Public-Facing Application"],
            "execution": ["T1059 Command and Scripting Interpreter", "T1204 User Execution"],
            "persistence": ["T1547 Boot or Logon Autostart", "T1053 Scheduled Task"],
            "c2": ["T1071 Application Layer Protocol", "T1573 Encrypted Channel"]
        },
        "malware_families": ["X-Agent", "Zebrocy", "Drovorub", "Cannon", "Koadic"],
        "notable_campaigns": ["DNC Hack 2016", "Bundestag Attack 2015", "WADA Leak 2016"],
        "description": "Russian state-sponsored threat actor associated with GRU military intelligence. Highly capable cyber espionage group targeting government and military organizations worldwide."
    },
    {
        "actor_id": "APT29",
        "names": ["APT29", "Cozy Bear", "The Dukes", "NOBELIUM", "Midnight Blizzard"],
        "attribution": "Russia - SVR (Foreign Intelligence Service)",
        "country": "RU",
        "sophistication": "ADVANCED",
        "motivation": "ESPIONAGE",
        "active_since": "2008",
        "typical_targets": ["Government", "Think Tanks", "Healthcare", "Technology", "Energy"],
        "geographic_focus": ["United States", "Western Europe", "NATO Countries"],
        "ttps": {
            "initial_access": ["T1195 Supply Chain Compromise", "T1566 Phishing"],
            "execution": ["T1059 PowerShell", "T1047 WMI"],
            "persistence": ["T1098 Account Manipulation", "T1078 Valid Accounts"],
            "defense_evasion": ["T1027 Obfuscated Files", "T1070 Indicator Removal"]
        },
        "malware_families": ["SUNBURST", "TEARDROP", "WellMess", "WellMail", "EnvyScout"],
        "notable_campaigns": ["SolarWinds Supply Chain Attack 2020", "DNC Hack 2016", "COVID-19 Research Targeting 2020"],
        "description": "Russian state-sponsored threat actor associated with SVR foreign intelligence. Known for sophisticated supply chain attacks and long-term persistent access operations."
    },
    {
        "actor_id": "APT41",
        "names": ["APT41", "Double Dragon", "Winnti", "BARIUM", "Wicked Panda"],
        "attribution": "China - MSS Contractor (Chengdu 404)",
        "country": "CN",
        "sophistication": "ADVANCED",
        "motivation": "ESPIONAGE_AND_FINANCIAL",
        "active_since": "2012",
        "typical_targets": ["Healthcare", "Telecommunications", "Technology", "Video Games", "Higher Education"],
        "geographic_focus": ["Global", "United States", "Europe", "Asia"],
        "ttps": {
            "initial_access": ["T1190 Exploit Public-Facing Application", "T1195 Supply Chain"],
            "execution": ["T1059 PowerShell", "T1569 System Services"],
            "persistence": ["T1543 Create or Modify System Process", "T1547 Boot Autostart"],
            "collection": ["T1005 Data from Local System", "T1119 Automated Collection"]
        },
        "malware_families": ["ShadowPad", "PlugX", "Winnti", "POISONPLUG", "Speculoos"],
        "notable_campaigns": ["CCleaner Supply Chain 2017", "ASUS Live Update 2019"],
        "description": "Chinese state-sponsored threat actor that conducts both espionage and financially motivated operations. Unique dual-mission capability."
    },
    {
        "actor_id": "LAZARUS",
        "names": ["Lazarus Group", "Hidden Cobra", "Zinc", "APT38", "Guardians of Peace"],
        "attribution": "North Korea - RGB (Reconnaissance General Bureau)",
        "country": "KP",
        "sophistication": "ADVANCED",
        "motivation": "FINANCIAL_AND_ESPIONAGE",
        "active_since": "2009",
        "typical_targets": ["Financial Institutions", "Cryptocurrency", "Defense", "Entertainment", "Critical Infrastructure"],
        "geographic_focus": ["Global", "South Korea", "United States", "SWIFT Network"],
        "ttps": {
            "initial_access": ["T1566 Spearphishing", "T1189 Drive-by Compromise"],
            "execution": ["T1059 Command Line", "T1203 Exploitation for Client Execution"],
            "impact": ["T1486 Data Encrypted for Impact", "T1489 Service Stop"],
            "collection": ["T1560 Archive Collected Data"]
        },
        "malware_families": ["WannaCry", "HOPLIGHT", "ELECTRICFISH", "AppleJeus", "DTrack"],
        "notable_campaigns": ["Sony Pictures Hack 2014", "Bangladesh Bank Heist 2016", "WannaCry 2017"],
        "description": "North Korean state-sponsored threat actor focused on financial theft to fund regime and espionage against adversaries. Responsible for major ransomware attacks."
    },
    {
        "actor_id": "APT40",
        "names": ["APT40", "Leviathan", "TEMP.Periscope", "TEMP.Jumper", "Bronze Mohawk"],
        "attribution": "China - MSS Hainan State Security Department",
        "country": "CN",
        "sophistication": "ADVANCED",
        "motivation": "ESPIONAGE",
        "active_since": "2013",
        "typical_targets": ["Maritime", "Defense", "Aviation", "Chemicals", "Government", "Technology"],
        "geographic_focus": ["South China Sea Region", "United States", "Europe", "Southeast Asia"],
        "ttps": {
            "initial_access": ["T1566 Phishing", "T1133 External Remote Services"],
            "execution": ["T1059 PowerShell", "T1047 WMI"],
            "persistence": ["T1505 Server Software Component", "T1133 External Remote Services"],
            "exfiltration": ["T1048 Exfiltration Over Alternative Protocol"]
        },
        "malware_families": ["AIRBREAK", "FRESHAIR", "PHOTO", "BADFLICK", "China Chopper"],
        "notable_campaigns": ["Naval University Targeting", "South China Sea Research Theft"],
        "description": "Chinese state-sponsored threat actor focused on maritime and naval targets. Supports PRC strategic interests in South China Sea and Belt and Road Initiative."
    },
    {
        "actor_id": "SANDWORM",
        "names": ["Sandworm", "Voodoo Bear", "IRIDIUM", "ELECTRUM", "Telebots"],
        "attribution": "Russia - GRU Unit 74455 (GTsST)",
        "country": "RU",
        "sophistication": "ADVANCED",
        "motivation": "SABOTAGE_AND_ESPIONAGE",
        "active_since": "2009",
        "typical_targets": ["Critical Infrastructure", "Energy", "Government", "Media", "Transportation"],
        "geographic_focus": ["Ukraine", "NATO Countries", "United States", "Europe"],
        "ttps": {
            "initial_access": ["T1566 Phishing", "T1195 Supply Chain Compromise"],
            "execution": ["T1059 PowerShell", "T1569 System Services"],
            "impact": ["T1485 Data Destruction", "T1495 Firmware Corruption", "T1529 System Shutdown"],
            "c2": ["T1071 Web Protocols"]
        },
        "malware_families": ["NotPetya", "Industroyer", "Olympic Destroyer", "VPNFilter", "CyclopsBlink"],
        "notable_campaigns": ["Ukraine Power Grid Attacks 2015-2016", "NotPetya 2017", "Winter Olympics 2018"],
        "description": "Russian state-sponsored threat actor specializing in destructive attacks against critical infrastructure. Responsible for most destructive cyberattacks in history."
    },
    {
        "actor_id": "MUDDYWATER",
        "names": ["MuddyWater", "MERCURY", "Static Kitten", "Seedworm", "TEMP.Zagros"],
        "attribution": "Iran - MOIS (Ministry of Intelligence and Security)",
        "country": "IR",
        "sophistication": "MODERATE",
        "motivation": "ESPIONAGE",
        "active_since": "2017",
        "typical_targets": ["Government", "Telecommunications", "Oil and Gas", "Defense"],
        "geographic_focus": ["Middle East", "Central Asia", "Pakistan", "Turkey"],
        "ttps": {
            "initial_access": ["T1566 Spearphishing Attachment"],
            "execution": ["T1059 PowerShell", "T1059 VBScript"],
            "persistence": ["T1053 Scheduled Task", "T1547 Registry Run Keys"],
            "defense_evasion": ["T1027 Obfuscated Files", "T1140 Deobfuscate/Decode"]
        },
        "malware_families": ["POWERSTATS", "Small Sieve", "Mori", "PowGoop"],
        "notable_campaigns": ["Telecommunications Targeting in Middle East", "Government Espionage Campaigns"],
        "description": "Iranian state-sponsored threat actor focused on regional espionage. Uses spearphishing and PowerShell-based tools for persistent access."
    },
    {
        "actor_id": "OILRIG",
        "names": ["OilRig", "APT34", "Helix Kitten", "CHRYSENE", "Crambus"],
        "attribution": "Iran - MOIS (Ministry of Intelligence and Security)",
        "country": "IR",
        "sophistication": "ADVANCED",
        "motivation": "ESPIONAGE",
        "active_since": "2014",
        "typical_targets": ["Financial", "Government", "Energy", "Chemical", "Telecommunications"],
        "geographic_focus": ["Middle East", "United States", "Europe"],
        "ttps": {
            "initial_access": ["T1566 Spearphishing", "T1133 External Remote Services"],
            "execution": ["T1059 PowerShell", "T1059 VBScript"],
            "credential_access": ["T1003 OS Credential Dumping", "T1110 Brute Force"],
            "exfiltration": ["T1048 Exfiltration Over C2", "T1567 Exfiltration to Cloud"]
        },
        "malware_families": ["QUADAGENT", "BONDUPDATER", "POWRUNER", "ALMA Communicator", "Glimpse"],
        "notable_campaigns": ["Middle East Government Targeting", "Gulf Region Financial Sector"],
        "description": "Iranian state-sponsored threat actor with sophisticated capabilities. Focuses on strategic intelligence collection in Middle East and against Western interests."
    }
]

# ============================================================
# KNOWLEDGE BASE: FACILITIES
# ============================================================
KB_FACILITIES = [
    {
        "facility_id": "AUAB",
        "name": "Al Udeid Air Base",
        "type": "AIR_BASE",
        "country": "QA",
        "region": "CENTCOM",
        "coordinates": {"lat": 25.1173, "lon": 51.3150},
        "host_nation": "Qatar",
        "primary_mission": "Combined Air Operations Center (CAOC)",
        "units": ["379th AEW", "AFCENT Forward HQ", "CAOC"],
        "aircraft_types": ["KC-135", "B-52H", "F-15E", "MQ-9", "E-8C JSTARS"],
        "capacity": {"personnel": 11000, "aircraft": 120},
        "description": "Largest US military facility in Middle East. Hosts CAOC for all CENTCOM air operations."
    },
    {
        "facility_id": "PSAB",
        "name": "Prince Sultan Air Base",
        "type": "AIR_BASE",
        "country": "SA",
        "region": "CENTCOM",
        "coordinates": {"lat": 24.0625, "lon": 47.5802},
        "host_nation": "Saudi Arabia",
        "primary_mission": "Fighter Operations / Air Defense",
        "units": ["378th AEW", "F-15 Squadrons"],
        "aircraft_types": ["F-15C", "F-15E", "F-22A", "Patriot"],
        "capacity": {"personnel": 2500, "aircraft": 80},
        "description": "Key CENTCOM fighter base in Saudi Arabia. Reopened 2019 for regional defense."
    },
    {
        "facility_id": "NAVCENT",
        "name": "NSA Bahrain",
        "type": "NAVAL_BASE",
        "country": "BH",
        "region": "CENTCOM",
        "coordinates": {"lat": 26.2361, "lon": 50.6508},
        "host_nation": "Bahrain",
        "primary_mission": "5th Fleet HQ / Naval Operations",
        "units": ["NAVCENT", "5th Fleet", "CTF 50-59"],
        "vessel_types": ["Destroyers", "Cruisers", "LCS", "MCM"],
        "capacity": {"personnel": 7000},
        "description": "Headquarters of US Naval Forces Central Command and US 5th Fleet."
    },
    {
        "facility_id": "CLDJ",
        "name": "Camp Lemonnier",
        "type": "NAVAL_BASE",
        "country": "DJ",
        "region": "AFRICOM",
        "coordinates": {"lat": 11.5472, "lon": 43.1553},
        "host_nation": "Djibouti",
        "primary_mission": "AFRICOM Forward Base / CT Operations",
        "units": ["CJTF-HOA", "NSWU-10", "VUP-19"],
        "aircraft_types": ["MQ-9", "P-3C", "F-15E (deployed)"],
        "capacity": {"personnel": 4500},
        "description": "Only permanent US military base in Africa. Supports counterterrorism operations in East Africa and Yemen."
    },
    {
        "facility_id": "RAMSTEIN",
        "name": "Ramstein Air Base",
        "type": "AIR_BASE",
        "country": "DE",
        "region": "EUCOM",
        "coordinates": {"lat": 49.4369, "lon": 7.6003},
        "host_nation": "Germany",
        "primary_mission": "USAFE HQ / Airlift Hub",
        "units": ["USAFE HQ", "86th AW", "603rd AOC"],
        "aircraft_types": ["C-130J", "C-17", "C-5M"],
        "capacity": {"personnel": 9200, "aircraft": 60},
        "description": "Headquarters of US Air Forces Europe. Primary airlift hub for European and Middle East operations."
    },
    {
        "facility_id": "INCIRLIK",
        "name": "Incirlik Air Base",
        "type": "AIR_BASE",
        "country": "TR",
        "region": "EUCOM",
        "coordinates": {"lat": 37.0011, "lon": 35.4259},
        "host_nation": "Turkey",
        "primary_mission": "Strike Operations / NATO Southern Flank",
        "units": ["39th ABW", "728th AMS"],
        "aircraft_types": ["F-16 (deployed)", "KC-135", "A-10 (deployed)"],
        "capacity": {"personnel": 5000, "aircraft": 100},
        "nuclear_capable": True,
        "description": "Key NATO base in Turkey. Hosts B61 nuclear weapons. Critical for Middle East and Black Sea operations."
    },
    {
        "facility_id": "ROTA",
        "name": "Naval Station Rota",
        "type": "NAVAL_BASE",
        "country": "ES",
        "region": "EUCOM",
        "coordinates": {"lat": 36.6236, "lon": -6.3519},
        "host_nation": "Spain",
        "primary_mission": "Aegis Ashore Support / Atlantic Operations",
        "units": ["COMNAVACT Spain", "Aegis BMD Destroyers"],
        "vessel_types": ["DDG (Aegis)", "T-AKE"],
        "capacity": {"personnel": 4000},
        "description": "Forward base for Aegis BMD destroyers supporting European missile defense."
    },
    {
        "facility_id": "SOUDA",
        "name": "NSA Souda Bay",
        "type": "NAVAL_BASE",
        "country": "GR",
        "region": "EUCOM",
        "coordinates": {"lat": 35.4867, "lon": 24.0875},
        "host_nation": "Greece",
        "primary_mission": "Mediterranean Operations / Logistics",
        "units": ["NAVSUPSACT Souda Bay"],
        "vessel_types": ["Submarines", "Surface Combatants (visiting)"],
        "capacity": {"personnel": 1000},
        "description": "Deep water port supporting submarine and surface operations in Eastern Mediterranean."
    }
]

# ============================================================
# KNOWLEDGE BASE: GEOPOLITICAL
# ============================================================
KB_GEOPOLITICAL = [
    {
        "country_code": "RU",
        "name": "Russian Federation",
        "classification": "NEAR_PEER_ADVERSARY",
        "military_alliance": "CSTO",
        "nuclear_status": "NWS",
        "military_strength_rank": 2,
        "defense_budget_usd_billions": 86,
        "active_personnel": 1150000,
        "key_capabilities": ["Nuclear Triad", "A2/AD Systems", "Cyber/EW", "Hypersonic Missiles"],
        "threat_assessment": "Primary near-peer threat. Advanced cyber and EW capabilities. Nuclear-capable with large conventional forces.",
        "regional_interests": ["Ukraine", "Baltic States", "Arctic", "Syria", "Central Asia"],
        "current_operations": ["Ukraine Invasion", "Syria Intervention", "Arctic Buildup"]
    },
    {
        "country_code": "CN",
        "name": "People's Republic of China",
        "classification": "NEAR_PEER_ADVERSARY",
        "military_alliance": "SCO",
        "nuclear_status": "NWS",
        "military_strength_rank": 3,
        "defense_budget_usd_billions": 224,
        "active_personnel": 2035000,
        "key_capabilities": ["A2/AD", "Carrier Fleet", "Space/Cyber", "DF Missile Family"],
        "threat_assessment": "Pacing threat. Rapidly modernizing military with focus on Western Pacific dominance.",
        "regional_interests": ["Taiwan", "South China Sea", "East China Sea", "Belt and Road"],
        "current_operations": ["Taiwan Gray Zone", "South China Sea Militarization", "ADIZ Incursions"]
    },
    {
        "country_code": "IR",
        "name": "Islamic Republic of Iran",
        "classification": "REGIONAL_ADVERSARY",
        "military_alliance": "None (Proxy Network)",
        "nuclear_status": "THRESHOLD",
        "military_strength_rank": 14,
        "defense_budget_usd_billions": 25,
        "active_personnel": 610000,
        "key_capabilities": ["Ballistic Missiles", "Naval Mines", "Proxy Forces", "UAVs/UCAVs"],
        "threat_assessment": "Regional threat with asymmetric capabilities. Extensive proxy network threatens US interests.",
        "regional_interests": ["Iraq", "Syria", "Lebanon", "Yemen", "Strait of Hormuz"],
        "proxy_forces": ["Hezbollah", "Hamas", "PMF/Hashd", "Houthis", "Various Iraqi Militias"],
        "current_operations": ["Yemen Support", "Iraq Influence", "Nuclear Program"]
    },
    {
        "country_code": "KP",
        "name": "Democratic People's Republic of Korea",
        "classification": "ROGUE_STATE",
        "military_alliance": "China (Mutual Defense)",
        "nuclear_status": "NWS_UNDECLARED",
        "military_strength_rank": 5,
        "defense_budget_usd_billions": 4,
        "active_personnel": 1280000,
        "key_capabilities": ["Nuclear Weapons", "ICBMs", "Artillery", "Cyber Operations"],
        "threat_assessment": "Nuclear-armed rogue state. Unpredictable leadership with demonstrated ICBM capability.",
        "regional_interests": ["Korean Peninsula", "Japan", "US Homeland (ICBM)"],
        "current_operations": ["Nuclear/Missile Testing", "Cyber Financial Theft", "Sanctions Evasion"]
    },
    {
        "country_code": "VE",
        "name": "Bolivarian Republic of Venezuela",
        "classification": "HOSTILE_STATE",
        "military_alliance": "ALBA",
        "nuclear_status": "NNWS",
        "military_strength_rank": 43,
        "defense_budget_usd_billions": 2,
        "active_personnel": 123000,
        "key_capabilities": ["Russian Equipment", "Cuban Intel Support", "Colectivos"],
        "threat_assessment": "Hostile government with Russian and Chinese military ties. Potential staging for adversary operations in Western Hemisphere.",
        "regional_interests": ["Caribbean", "Guyana Border", "Colombia Border"],
        "foreign_military_presence": ["Russia (advisors)", "Cuba (intelligence)"],
        "current_operations": ["Guyana Border Dispute", "Opposition Suppression"]
    }
]

# ============================================================
# RAG DOCUMENTS (Sample)
# ============================================================
RAG_DOCUMENTS = [
    {
        "document_id": "DOC-001",
        "title": "JADC2 Concept Overview",
        "source": "DoD Strategic Documents",
        "classification": "UNCLASSIFIED",
        "date_published": "2023-01-15",
        "content": """Joint All-Domain Command and Control (JADC2) is the Department of Defense's concept to connect sensors from all military services into a unified network. JADC2 enables faster decision-making by providing commanders with real-time situational awareness across air, land, sea, space, and cyberspace domains.

Key objectives of JADC2 include:
1. Sensor-to-shooter connectivity across all domains
2. Artificial intelligence for data fusion and decision support
3. Resilient and redundant communication pathways
4. Interoperability with allies and partners

The JADC2 architecture relies on cloud computing, advanced networking, and AI/ML to process the massive amounts of data generated by modern sensors and platforms.""",
        "keywords": ["JADC2", "C2", "all-domain", "sensor fusion", "AI", "decision support"]
    },
    {
        "document_id": "DOC-002",
        "title": "Understanding Russian A2/AD Systems",
        "source": "Intelligence Assessment",
        "classification": "UNCLASSIFIED",
        "date_published": "2023-06-20",
        "content": """Russia has developed sophisticated Anti-Access/Area Denial (A2/AD) capabilities designed to prevent US and NATO forces from operating freely in contested regions.

Key A2/AD systems include:
- S-400 Triumf: Long-range SAM with 400km engagement range
- S-300V4: Theater ballistic missile defense
- Bastion-P: Coastal defense cruise missiles (300km range)
- Kalibr: Land-attack and anti-ship cruise missiles
- Iskander-M: Short-range ballistic missiles (500km)

These systems create overlapping defensive bubbles that threaten aircraft, ships, and ground forces. Russia has deployed A2/AD systems in Kaliningrad, Crimea, Syria, and the Kola Peninsula.

Countering A2/AD requires standoff weapons, electronic warfare, stealth platforms, and distributed operations.""",
        "keywords": ["A2/AD", "S-400", "Russia", "air defense", "cruise missiles", "Kalibr"]
    },
    {
        "document_id": "DOC-003",
        "title": "Cyber Threat Landscape 2024",
        "source": "CISA/NSA Joint Advisory",
        "classification": "UNCLASSIFIED",
        "date_published": "2024-02-01",
        "content": """The cyber threat landscape continues to evolve with nation-state actors becoming increasingly sophisticated.

Russian Threat Actors:
- APT28 (Fancy Bear): GRU-linked, targets government and defense
- APT29 (Cozy Bear): SVR-linked, supply chain attacks
- Sandworm: GRU-linked, destructive attacks on critical infrastructure

Chinese Threat Actors:
- APT41: MSS-linked, dual espionage and financial motivation
- APT40: MSS-linked, maritime and defense targeting

Iranian Threat Actors:
- MuddyWater: MOIS-linked, regional espionage
- OilRig/APT34: Financial and energy sector targeting

North Korean Threat Actors:
- Lazarus Group: Financial theft and ransomware

Recommended mitigations include zero-trust architecture, endpoint detection and response, network segmentation, and regular security assessments.""",
        "keywords": ["cyber", "APT", "threat actors", "Russia", "China", "Iran", "DPRK"]
    }
]


def create_chunks(documents):
    """Create text chunks from documents for RAG."""
    chunks = []
    for doc in documents:
        content = doc["content"]
        # Simple chunking by paragraphs
        paragraphs = [p.strip() for p in content.split("\n\n") if p.strip()]
        
        for i, para in enumerate(paragraphs):
            chunk = {
                "chunk_id": f"{doc['document_id']}-{i:03d}",
                "document_id": doc["document_id"],
                "document_title": doc["title"],
                "chunk_index": i,
                "content": para,
                "word_count": len(para.split()),
                "char_count": len(para),
                # Placeholder for embedding - would be populated by embedding model
                "embedding": None,
                "embedding_model": "text-embedding-ada-002"
            }
            chunks.append(chunk)
    return chunks


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    atlas_uri = sys.argv[1]
    
    print("=" * 70)
    print("Seeding Knowledge Base into MongoDB Atlas")
    print("=" * 70)
    print(f"Database: {DATABASE}")
    print(f"Atlas URI: {atlas_uri[:50]}...")
    print("")
    
    # Connect to Atlas
    print("Connecting to Atlas...")
    try:
        client = MongoClient(atlas_uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        print("✓ Connected to Atlas")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        sys.exit(1)
    
    db = client[DATABASE]
    
    # Seed collections
    print("\n" + "=" * 70)
    print("Seeding Collections")
    print("=" * 70)
    
    collections_data = [
        ("kb_equipment", KB_EQUIPMENT),
        ("kb_threat_actors", KB_THREAT_ACTORS),
        ("kb_facilities", KB_FACILITIES),
        ("kb_geopolitical", KB_GEOPOLITICAL),
        ("rag_documents", RAG_DOCUMENTS),
        ("rag_chunks", create_chunks(RAG_DOCUMENTS)),
    ]
    
    for collection_name, data in collections_data:
        try:
            col = db[collection_name]
            col.drop()  # Clear existing
            if data:
                result = col.insert_many(data)
                print(f"  ✓ {collection_name}: {len(result.inserted_ids)} documents")
            else:
                print(f"  - {collection_name}: (no data)")
        except Exception as e:
            print(f"  ✗ {collection_name}: {e}")
    
    # Create indexes
    print("\n" + "=" * 70)
    print("Creating Indexes")
    print("=" * 70)
    
    try:
        db.kb_equipment.create_index("equipment_id", unique=True)
        db.kb_equipment.create_index("category")
        print("  ✓ kb_equipment indexes")
        
        db.kb_threat_actors.create_index("actor_id", unique=True)
        db.kb_threat_actors.create_index("country")
        print("  ✓ kb_threat_actors indexes")
        
        db.kb_facilities.create_index("facility_id", unique=True)
        db.kb_facilities.create_index("region")
        print("  ✓ kb_facilities indexes")
        
        db.kb_geopolitical.create_index("country_code", unique=True)
        print("  ✓ kb_geopolitical indexes")
        
        db.rag_documents.create_index("document_id", unique=True)
        print("  ✓ rag_documents indexes")
        
        db.rag_chunks.create_index("chunk_id", unique=True)
        db.rag_chunks.create_index("document_id")
        print("  ✓ rag_chunks indexes")
        
    except Exception as e:
        print(f"  ✗ Index creation error: {e}")
    
    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    for col_name in db.list_collection_names():
        count = db[col_name].count_documents({})
        print(f"  {col_name}: {count} documents")
    
    print("\n✓ Knowledge base seeding complete!")
    print("\nNote: Vector embeddings are placeholders.")
    print("To enable semantic search, run embedding generation separately.")
    
    client.close()


if __name__ == "__main__":
    main()
