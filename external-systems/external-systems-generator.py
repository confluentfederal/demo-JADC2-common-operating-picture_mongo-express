#!/usr/bin/env python3
"""
JADC2 External Systems Data Generator
Simulates 7 external operational systems feeding into the JADC2 Kafka pipeline

System 1: DIA Threat Intelligence (MongoSourceConnector)
System 2: Maritime Domain Awareness (MqttSourceConnector)
System 3: CENTCOM Force Readiness (MongoSourceConnector)
System 4: NGA Geospatial Intelligence (MqttSourceConnector)
System 5: Coalition Partner C2 (MirrorSourceConnector / Kafka Producer)
System 6: Defense Logistics Agency (MongoSourceConnector)
System 7: FAA Civil Aviation (MqttSourceConnector)
"""

import json
import random
import time
import threading
import uuid
import hashlib
import math
from datetime import datetime, timedelta
import os
import logging

from pymongo import MongoClient
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('external-systems')

ATLAS_URI = os.environ.get('ATLAS_URI', 'mongodb://admin:jadc2secret@mongodb:27017/?authSource=admin')
EXTERNAL_DB = 'jadc2_external_sources'
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'mosquitto')
MQTT_PORT = int(os.environ.get('MQTT_PORT', '1883'))

# ============================================================
# 1. DIA THREAT INTELLIGENCE DATA
# ============================================================
THREAT_ACTORS = [
    {'actor_id': 'TA-APT28', 'name': 'APT28 / Fancy Bear', 'country': 'RU', 'affiliation': 'GRU Unit 26165', 'active_since': '2004',
     'primary_targets': ['Government', 'Military', 'Defense Contractors', 'Media'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['Olympic Destroyer', 'SolarWinds Adjacent', 'Bundestag Hack']},
    {'actor_id': 'TA-APT29', 'name': 'APT29 / Cozy Bear', 'country': 'RU', 'affiliation': 'SVR Foreign Intelligence Service', 'active_since': '2008',
     'primary_targets': ['Government', 'Think Tanks', 'Healthcare', 'Energy'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['SolarWinds SUNBURST', 'WellMess Campaign', 'COVID Vaccine Espionage']},
    {'actor_id': 'TA-SANDWORM', 'name': 'Sandworm Team', 'country': 'RU', 'affiliation': 'GRU Unit 74455', 'active_since': '2009',
     'primary_targets': ['Critical Infrastructure', 'Energy Grid', 'ICS/SCADA', 'Elections'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['NotPetya', 'Ukraine Power Grid 2015/2016', 'Cyclops Blink']},
    {'actor_id': 'TA-APT41', 'name': 'APT41 / Double Dragon', 'country': 'CN', 'affiliation': 'MSS / Chengdu 404 Network', 'active_since': '2012',
     'primary_targets': ['Healthcare', 'Telecom', 'Technology', 'Gaming'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['ShadowPad', 'Winnti Backdoor', 'Supply Chain Attacks']},
    {'actor_id': 'TA-APT40', 'name': 'APT40 / Leviathan', 'country': 'CN', 'affiliation': 'MSS Hainan State Security', 'active_since': '2013',
     'primary_targets': ['Maritime', 'Defense', 'Aviation', 'Chemical'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['South China Sea Espionage', 'Naval Technology Theft', 'University Targeting']},
    {'actor_id': 'TA-LAZARUS', 'name': 'Lazarus Group', 'country': 'KP', 'affiliation': 'RGB Bureau 121', 'active_since': '2007',
     'primary_targets': ['Financial', 'Cryptocurrency', 'Defense', 'Entertainment'], 'sophistication': 'TIER_2_MODERATE',
     'known_operations': ['Sony Hack', 'WannaCry', 'Bangladesh Bank Heist', 'Axie Infinity']},
    {'actor_id': 'TA-MUDDYWATER', 'name': 'MuddyWater', 'country': 'IR', 'affiliation': 'MOIS Intelligence Ministry', 'active_since': '2017',
     'primary_targets': ['Government', 'Telecom', 'Oil & Gas', 'Middle East Targets'], 'sophistication': 'TIER_2_MODERATE',
     'known_operations': ['Earth Vetala', 'Telecommunication Targeting', 'Exchange Exploitation']},
    {'actor_id': 'TA-CHARMING', 'name': 'Charming Kitten / APT35', 'country': 'IR', 'affiliation': 'IRGC Intelligence', 'active_since': '2014',
     'primary_targets': ['Academic', 'Journalists', 'Dissidents', 'Government'], 'sophistication': 'TIER_2_MODERATE',
     'known_operations': ['HBO Hack', 'Academic Credential Theft', 'Phosphorus Campaign']},
]

MITRE_TECHNIQUES = [
    {'id': 'T1566.001', 'name': 'Spearphishing Attachment', 'tactic': 'Initial Access', 'severity': 'HIGH'},
    {'id': 'T1190', 'name': 'Exploit Public-Facing Application', 'tactic': 'Initial Access', 'severity': 'CRITICAL'},
    {'id': 'T1059.001', 'name': 'PowerShell Execution', 'tactic': 'Execution', 'severity': 'HIGH'},
    {'id': 'T1053.005', 'name': 'Scheduled Task', 'tactic': 'Persistence', 'severity': 'MEDIUM'},
    {'id': 'T1078', 'name': 'Valid Accounts', 'tactic': 'Defense Evasion', 'severity': 'CRITICAL'},
    {'id': 'T1048.002', 'name': 'Exfiltration Over Asymmetric Encrypted Channel', 'tactic': 'Exfiltration', 'severity': 'CRITICAL'},
    {'id': 'T1071.001', 'name': 'Web Protocols C2', 'tactic': 'Command and Control', 'severity': 'HIGH'},
    {'id': 'T1027', 'name': 'Obfuscated Files or Information', 'tactic': 'Defense Evasion', 'severity': 'MEDIUM'},
    {'id': 'T1003.001', 'name': 'LSASS Memory Credential Dump', 'tactic': 'Credential Access', 'severity': 'CRITICAL'},
    {'id': 'T1021.001', 'name': 'Remote Desktop Protocol', 'tactic': 'Lateral Movement', 'severity': 'HIGH'},
    {'id': 'T1486', 'name': 'Data Encrypted for Impact', 'tactic': 'Impact', 'severity': 'CRITICAL'},
    {'id': 'T1505.003', 'name': 'Web Shell', 'tactic': 'Persistence', 'severity': 'HIGH'},
]

IOC_TYPES = ['ipv4', 'domain', 'hash_sha256', 'hash_md5', 'url', 'email', 'file_path', 'registry_key']
MALWARE_FAMILIES = ['SUNBURST', 'Cobalt Strike', 'Mimikatz', 'ShadowPad', 'PlugX', 'Emotet', 'TrickBot', 'Ryuk',
                    'BlackCat/ALPHV', 'Cyclops Blink', 'WellMess', 'NotPetya', 'TEARDROP', 'GoldMax', 'Winnti',
                    'DUSTMAN', 'Shamoon', 'MuddyC3', 'POWERSTATS', 'BladedWrench']
CENTCOM_REGIONS = ['CENTCOM AOR', 'Persian Gulf', 'Red Sea', 'Eastern Mediterranean', 'Horn of Africa', 'Central Asia']
COUNTRY_THREAT_LEVELS = {
    'RU': {'level': 'CRITICAL', 'posture': 'AGGRESSIVE', 'priority': 1},
    'CN': {'level': 'HIGH', 'posture': 'PERSISTENT', 'priority': 2},
    'IR': {'level': 'HIGH', 'posture': 'REGIONAL_AGGRESSIVE', 'priority': 3},
    'KP': {'level': 'ELEVATED', 'posture': 'OPPORTUNISTIC', 'priority': 4},
}

# ============================================================
# 2. MARITIME DOMAIN AWARENESS DATA
# ============================================================
COMMERCIAL_VESSEL_TYPES = [
    'VLCC', 'Suezmax Tanker', 'Aframax Tanker', 'Product Tanker', 'Chemical Tanker', 'LNG Carrier', 'LPG Carrier',
    'Container Ship', 'Bulk Carrier', 'General Cargo', 'Ro-Ro', 'Car Carrier',
    'Fishing Vessel', 'Research Vessel', 'Passenger Ferry', 'Cruise Ship', 'Yacht', 'Tug', 'Supply Vessel', 'Cable Layer', 'Dredger']
VESSEL_FLAGS = [
    {'country': 'PA', 'name': 'Panama'}, {'country': 'LR', 'name': 'Liberia'}, {'country': 'MH', 'name': 'Marshall Islands'},
    {'country': 'HK', 'name': 'Hong Kong'}, {'country': 'SG', 'name': 'Singapore'}, {'country': 'MT', 'name': 'Malta'},
    {'country': 'CN', 'name': 'China'}, {'country': 'JP', 'name': 'Japan'}, {'country': 'IR', 'name': 'Iran'},
    {'country': 'RU', 'name': 'Russia'}, {'country': 'AE', 'name': 'UAE'}, {'country': 'SA', 'name': 'Saudi Arabia'},
    {'country': 'GB', 'name': 'United Kingdom'}, {'country': 'US', 'name': 'United States'}, {'country': 'IN', 'name': 'India'},
    {'country': 'GR', 'name': 'Greece'}, {'country': 'NO', 'name': 'Norway'}, {'country': 'TR', 'name': 'Turkey'},
    {'country': 'KR', 'name': 'South Korea'}, {'country': 'BS', 'name': 'Bahamas'},
]
SHIPPING_LANES = [
    {'name': 'Strait of Hormuz', 'lat_range': (26.2, 26.8), 'lon_range': (56.0, 56.8)},
    {'name': 'Bab el-Mandeb', 'lat_range': (12.4, 13.0), 'lon_range': (43.0, 43.6)},
    {'name': 'Suez Canal Approach', 'lat_range': (29.8, 31.0), 'lon_range': (32.0, 33.0)},
    {'name': 'Persian Gulf Central', 'lat_range': (25.0, 28.0), 'lon_range': (50.0, 54.0)},
    {'name': 'Gulf of Oman', 'lat_range': (23.5, 25.5), 'lon_range': (57.0, 60.0)},
    {'name': 'Red Sea North', 'lat_range': (22.0, 28.0), 'lon_range': (34.0, 38.0)},
    {'name': 'Red Sea South', 'lat_range': (13.0, 20.0), 'lon_range': (38.0, 43.0)},
    {'name': 'Arabian Sea', 'lat_range': (15.0, 22.0), 'lon_range': (55.0, 65.0)},
    {'name': 'Eastern Mediterranean', 'lat_range': (31.0, 36.0), 'lon_range': (28.0, 36.0)},
    {'name': 'Gulf of Aden', 'lat_range': (11.5, 14.0), 'lon_range': (43.0, 51.0)},
]
PORTS = [
    {'name': 'Jebel Ali', 'country': 'AE', 'lat': 25.02, 'lon': 55.06}, {'name': 'Fujairah', 'country': 'AE', 'lat': 25.12, 'lon': 56.35},
    {'name': 'Ras Tanura', 'country': 'SA', 'lat': 26.63, 'lon': 50.16}, {'name': 'Bandar Abbas', 'country': 'IR', 'lat': 27.18, 'lon': 56.28},
    {'name': 'Jeddah', 'country': 'SA', 'lat': 21.49, 'lon': 39.17}, {'name': 'Djibouti', 'country': 'DJ', 'lat': 11.59, 'lon': 43.14},
    {'name': 'Port Said', 'country': 'EG', 'lat': 31.26, 'lon': 32.31}, {'name': 'Karachi', 'country': 'PK', 'lat': 24.85, 'lon': 66.99},
    {'name': 'Muscat', 'country': 'OM', 'lat': 23.62, 'lon': 58.57}, {'name': 'Aden', 'country': 'YE', 'lat': 12.80, 'lon': 45.03},
]
VOI_REASONS = [
    'Sanctions evasion - flagged vessel', 'AIS transponder disabled in sensitive area',
    'Unusual loitering near military installation', 'Ship-to-ship transfer suspected',
    'Vessel associated with known smuggling network', 'Deviating from declared route',
    'Previously flagged for arms transport', 'Iranian-linked ownership chain',
    'North Korean sanctions circumvention', 'Operating in restricted military zone',
]

# ============================================================
# 3. CENTCOM FORCE READINESS DATA
# ============================================================
CENTCOM_UNITS = [
    {'unit_id': 'UNIT-5ID', 'name': '5th Infantry Division', 'type': 'INFANTRY', 'base': 'Camp Arifjan', 'lat': 28.933, 'lon': 48.100, 'auth': 18000},
    {'unit_id': 'UNIT-1AD', 'name': '1st Armored Division', 'type': 'ARMOR', 'base': 'Camp Buehring', 'lat': 29.28, 'lon': 47.68, 'auth': 16000},
    {'unit_id': 'UNIT-82ABN', 'name': '82nd Airborne Division', 'type': 'AIRBORNE', 'base': 'Prince Sultan AB', 'lat': 24.062, 'lon': 47.580, 'auth': 14000},
    {'unit_id': 'UNIT-101ABN', 'name': '101st Airborne (Air Assault)', 'type': 'AIR_ASSAULT', 'base': 'Al Udeid AB', 'lat': 25.117, 'lon': 51.315, 'auth': 15000},
    {'unit_id': 'UNIT-3MEF', 'name': '3rd Marine Expeditionary Force', 'type': 'MARINES', 'base': 'NSA Bahrain', 'lat': 26.237, 'lon': 50.652, 'auth': 12000},
    {'unit_id': 'UNIT-CSG5', 'name': 'Carrier Strike Group 5', 'type': 'NAVAL', 'base': 'NSA Bahrain', 'lat': 26.237, 'lon': 50.652, 'auth': 7500},
    {'unit_id': 'UNIT-380AEW', 'name': '380th Air Expeditionary Wing', 'type': 'AIR_FORCE', 'base': 'Al Dhafra AB', 'lat': 24.248, 'lon': 54.547, 'auth': 5000},
    {'unit_id': 'UNIT-332AEW', 'name': '332nd Air Expeditionary Wing', 'type': 'AIR_FORCE', 'base': 'Al Udeid AB', 'lat': 25.117, 'lon': 51.315, 'auth': 4500},
    {'unit_id': 'UNIT-NAVCENT', 'name': 'US Naval Forces Central Command', 'type': 'NAVAL_HQ', 'base': 'NSA Bahrain', 'lat': 26.237, 'lon': 50.652, 'auth': 3000},
    {'unit_id': 'UNIT-SOCCENT', 'name': 'Special Operations Command Central', 'type': 'SOF', 'base': 'MacDill Fwd', 'lat': 25.30, 'lon': 51.50, 'auth': 2000},
]
EQUIPMENT_TYPES = [
    {'type': 'M1A2 Abrams', 'category': 'ARMOR'}, {'type': 'M2A3 Bradley', 'category': 'IFV'},
    {'type': 'HMMWV', 'category': 'VEHICLE'}, {'type': 'M777 Howitzer', 'category': 'ARTILLERY'},
    {'type': 'AH-64E Apache', 'category': 'ROTARY_WING'}, {'type': 'UH-60M Black Hawk', 'category': 'ROTARY_WING'},
    {'type': 'F-15E Strike Eagle', 'category': 'FIXED_WING'}, {'type': 'F-35A Lightning II', 'category': 'FIXED_WING'},
    {'type': 'MQ-9 Reaper', 'category': 'UAS'}, {'type': 'Patriot PAC-3', 'category': 'AIR_DEFENSE'},
    {'type': 'THAAD', 'category': 'MISSILE_DEFENSE'}, {'type': 'CH-47F Chinook', 'category': 'ROTARY_WING'},
]

# ============================================================
# 4. NGA GEOSPATIAL INTELLIGENCE DATA
# ============================================================
NGA_AREAS = [
    {'name': 'Strait of Hormuz Chokepoint', 'type': 'MARITIME_CHOKEPOINT', 'lat': 26.5, 'lon': 56.4, 'radius_km': 50},
    {'name': 'Bandar Abbas Naval Complex', 'type': 'MILITARY_INSTALLATION', 'lat': 27.15, 'lon': 56.25, 'radius_km': 15},
    {'name': 'Bushehr Nuclear Facility', 'type': 'NUCLEAR_SITE', 'lat': 28.83, 'lon': 50.88, 'radius_km': 10},
    {'name': 'Natanz Enrichment Plant', 'type': 'NUCLEAR_SITE', 'lat': 33.72, 'lon': 51.73, 'radius_km': 8},
    {'name': 'Tartus Naval Base (RU)', 'type': 'FOREIGN_MILITARY', 'lat': 34.89, 'lon': 35.87, 'radius_km': 10},
    {'name': 'Hmeimim Air Base (RU)', 'type': 'FOREIGN_MILITARY', 'lat': 35.41, 'lon': 35.95, 'radius_km': 12},
    {'name': 'Bab el-Mandeb Chokepoint', 'type': 'MARITIME_CHOKEPOINT', 'lat': 12.6, 'lon': 43.3, 'radius_km': 40},
    {'name': 'Suez Canal Zone', 'type': 'CRITICAL_INFRASTRUCTURE', 'lat': 30.45, 'lon': 32.35, 'radius_km': 30},
    {'name': 'Camp Lemonnier Perimeter', 'type': 'US_INSTALLATION', 'lat': 11.55, 'lon': 43.15, 'radius_km': 5},
    {'name': 'Chabahar Port Complex', 'type': 'DUAL_USE_PORT', 'lat': 25.30, 'lon': 60.62, 'radius_km': 10},
    {'name': 'Socotra Island', 'type': 'STRATEGIC_TERRAIN', 'lat': 12.50, 'lon': 54.00, 'radius_km': 25},
    {'name': 'Qeshm Island Military Zone', 'type': 'MILITARY_INSTALLATION', 'lat': 26.85, 'lon': 55.90, 'radius_km': 15},
]
NGA_CHANGE_TYPES = [
    'New construction detected', 'Vehicle movement observed', 'Increased activity at site',
    'Vessel concentration anomaly', 'SAM battery repositioned', 'Troop staging area detected',
    'Underground facility entrance activity', 'Runway extension in progress', 'Port loading activity surge',
    'Camouflage netting deployed', 'Radar installation identified', 'Road/infrastructure development',
]

# ============================================================
# 5. COALITION PARTNER C2 DATA
# ============================================================
COALITION_NATIONS = [
    {'code': 'GBR', 'name': 'United Kingdom', 'alliance': 'FIVE_EYES'},
    {'code': 'AUS', 'name': 'Australia', 'alliance': 'FIVE_EYES'},
    {'code': 'FRA', 'name': 'France', 'alliance': 'NATO'},
    {'code': 'DEU', 'name': 'Germany', 'alliance': 'NATO'},
    {'code': 'JPN', 'name': 'Japan', 'alliance': 'BILATERAL'},
]
COALITION_AIRCRAFT = {
    'GBR': ['Typhoon FGR4', 'F-35B Lightning', 'Voyager KC3', 'RC-135W Rivet Joint'],
    'AUS': ['F/A-18F Super Hornet', 'EA-18G Growler', 'P-8A Poseidon', 'E-7A Wedgetail'],
    'FRA': ['Rafale C', 'Rafale M', 'E-3F Sentry', 'Atlantique 2'],
    'DEU': ['Eurofighter Typhoon', 'A400M Atlas', 'Tornado ECR'],
    'JPN': ['F-35A Lightning II', 'P-1 Maritime Patrol', 'E-767 AWACS'],
}
COALITION_NAVAL = {
    'GBR': ['HMS Queen Elizabeth (CVN)', 'HMS Daring (DDG)', 'HMS Astute (SSN)', 'RFA Tidespring (AO)'],
    'AUS': ['HMAS Hobart (DDG)', 'HMAS Collins (SSK)', 'HMAS Canberra (LHD)'],
    'FRA': ['FS Charles de Gaulle (CVN)', 'FS Aquitaine (FFG)', 'FS Suffren (SSN)'],
    'DEU': ['FGS Sachsen (FFG)', 'FGS U-36 (SSK)'],
    'JPN': ['JS Izumo (DDH)', 'JS Atago (DDG)', 'JS Soryu (SSK)'],
}

# ============================================================
# 6. DLA DEFENSE LOGISTICS DATA
# ============================================================
DLA_SUPPLY_CLASSES = [
    {'cls': 'I', 'name': 'Subsistence', 'unit': 'short_tons', 'critical_days': 14},
    {'cls': 'III', 'name': 'POL (Fuel)', 'unit': 'gallons', 'critical_days': 7},
    {'cls': 'V', 'name': 'Ammunition', 'unit': 'short_tons', 'critical_days': 10},
    {'cls': 'VIII', 'name': 'Medical Material', 'unit': 'pallets', 'critical_days': 7},
    {'cls': 'IX', 'name': 'Repair Parts', 'unit': 'pallets', 'critical_days': 14},
]
DLA_DEPOTS = [
    {'depot_id': 'DEP-AJ', 'name': 'Camp Arifjan LSA', 'lat': 28.933, 'lon': 48.100, 'cap': 1.0},
    {'depot_id': 'DEP-BA', 'name': 'NSA Bahrain Logistics', 'lat': 26.237, 'lon': 50.652, 'cap': 0.6},
    {'depot_id': 'DEP-DJ', 'name': 'Camp Lemonnier Supply', 'lat': 11.55, 'lon': 43.15, 'cap': 0.4},
    {'depot_id': 'DEP-PS', 'name': 'Prince Sultan AB Depot', 'lat': 24.062, 'lon': 47.580, 'cap': 0.7},
    {'depot_id': 'DEP-AD', 'name': 'Al Dhafra AB Supply', 'lat': 24.248, 'lon': 54.547, 'cap': 0.5},
    {'depot_id': 'DEP-AU', 'name': 'Al Udeid AB Logistics', 'lat': 25.117, 'lon': 51.315, 'cap': 0.8},
]

# ============================================================
# 7. FAA CIVIL AVIATION DATA
# ============================================================
CIVIL_AIRCRAFT_TYPES = ['B737-800', 'B737 MAX 8', 'B777-300ER', 'B787-9', 'B747-8F', 'A320neo', 'A321neo', 'A330-300', 'A350-900', 'A380-800']
CIVIL_AIRLINES = [
    {'code': 'UAE', 'name': 'Emirates'}, {'code': 'ETH', 'name': 'Ethiopian Airlines'}, {'code': 'QTR', 'name': 'Qatar Airways'},
    {'code': 'BAW', 'name': 'British Airways'}, {'code': 'AFR', 'name': 'Air France'}, {'code': 'DLH', 'name': 'Lufthansa'},
    {'code': 'THY', 'name': 'Turkish Airlines'}, {'code': 'SVA', 'name': 'Saudia'}, {'code': 'AIC', 'name': 'Air India'},
    {'code': 'IRA', 'name': 'Iran Air'}, {'code': 'FDB', 'name': 'flydubai'}, {'code': 'MEA', 'name': 'Middle East Airlines'},
]
CIVIL_AIRPORTS = [
    {'icao': 'OMDB', 'name': 'Dubai Intl', 'lat': 25.25, 'lon': 55.36}, {'icao': 'OTHH', 'name': 'Hamad Intl', 'lat': 25.27, 'lon': 51.61},
    {'icao': 'OEJN', 'name': 'Jeddah Intl', 'lat': 21.68, 'lon': 39.16}, {'icao': 'OBBI', 'name': 'Bahrain Intl', 'lat': 26.27, 'lon': 50.63},
    {'icao': 'OOMS', 'name': 'Muscat Intl', 'lat': 23.59, 'lon': 58.28}, {'icao': 'OIIE', 'name': 'Tehran IKA', 'lat': 35.42, 'lon': 51.15},
    {'icao': 'LTFM', 'name': 'Istanbul Intl', 'lat': 41.27, 'lon': 28.74}, {'icao': 'OPKC', 'name': 'Karachi Intl', 'lat': 24.91, 'lon': 67.16},
    {'icao': 'HECA', 'name': 'Cairo Intl', 'lat': 30.12, 'lon': 31.41}, {'icao': 'HDAM', 'name': 'Djibouti Ambouli', 'lat': 11.55, 'lon': 43.16},
]


# ============================================================
# GENERATOR CLASS
# ============================================================
class ExternalSystemsGenerator:
    def __init__(self):
        self.db = None
        self.mqtt_client = None
        self.kafka_producer = None
        self.running = False
        self.mda_vessels = []
        self.faa_aircraft = []

    def connect(self):
        # MongoDB for DIA, CENTCOM, DLA
        try:
            client = MongoClient(ATLAS_URI)
            self.db = client[EXTERNAL_DB]
            client.admin.command('ping')
            logger.info(f"✓ Connected to MongoDB: {EXTERNAL_DB}")
            self.db.dia_threat_actors.create_index('actor_id', unique=True)
            self.db.dia_iocs.create_index('created_at')
            self.db.dia_threat_reports.create_index('report_id', unique=True)
            self.db.dia_country_assessments.create_index('country_code', unique=True)
            self.db.centcom_unit_readiness.create_index('unit_id', unique=True)
            self.db.centcom_equipment_status.create_index('equipment_id')
            self.db.dla_supply_levels.create_index([('depot_id', 1), ('supply_class', 1)])
            self.db.dla_shipments.create_index('shipment_id', unique=True)
            logger.info("✓ MongoDB indexes created")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False

        # MQTT for MDA, NGA, FAA
        try:
            self.mqtt_client = mqtt.Client(client_id="external-systems-feed", protocol=mqtt.MQTTv311)
            self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            self.mqtt_client.loop_start()
            logger.info(f"✓ MQTT connected: {MQTT_BROKER}:{MQTT_PORT}")
        except Exception as e:
            logger.error(f"MQTT connection failed: {e}")
            return False

        # Kafka producer for Coalition Partner C2 (simulates mirrored allied Kafka cluster)
        try:
            from confluent_kafka import Producer
            self.kafka_producer = Producer({
                'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
                'client.id': 'coalition-c2-feed'
            })
            logger.info("✓ Kafka producer connected (Coalition C2)")
        except Exception as e:
            logger.error(f"Kafka producer failed: {e}")
            return False

        return True

    # ============================================================
    # 1. DIA THREAT INTELLIGENCE
    # ============================================================
    def _init_dia(self):
        for actor in THREAT_ACTORS:
            self.db.dia_threat_actors.update_one(
                {'actor_id': actor['actor_id']},
                {'$set': {**actor, 'status': 'ACTIVE', 'last_activity': datetime.utcnow().isoformat(),
                          'updated_by': 'DIA_ANALYST', 'updated_at': datetime.utcnow().isoformat()}},
                upsert=True)
        for country, a in COUNTRY_THREAT_LEVELS.items():
            names = {'RU': 'Russia', 'CN': 'China', 'IR': 'Iran', 'KP': 'North Korea'}
            self.db.dia_country_assessments.update_one(
                {'country_code': country},
                {'$set': {'country_code': country, 'country_name': names.get(country), 'threat_level': a['level'],
                          'cyber_posture': a['posture'], 'priority': a['priority'], 'active_campaigns': random.randint(2, 8),
                          'assessment_date': datetime.utcnow().isoformat(), 'source': 'DIA_THREAT_INTEL_DB'}},
                upsert=True)
        logger.info(f"  Seeded {len(THREAT_ACTORS)} threat actors, {len(COUNTRY_THREAT_LEVELS)} country assessments")

    def _dia_ioc_loop(self):
        while self.running:
            time.sleep(random.uniform(15, 45))
            try:
                actor = random.choice(THREAT_ACTORS)
                tech = random.choice(MITRE_TECHNIQUES)
                ioc_type = random.choice(IOC_TYPES)
                if ioc_type == 'ipv4':
                    val = f"{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"
                elif ioc_type == 'domain':
                    val = f"{random.choice(['secure','update','cdn','api','vpn'])}-{random.randint(1,99)}{random.choice(['.xyz','.top','.club','.cc'])}"
                elif ioc_type.startswith('hash'):
                    val = hashlib.sha256(str(random.random()).encode()).hexdigest()[:64 if 'sha' in ioc_type else 32]
                else:
                    val = f"{''.join(random.choices('abcdef0123456789', k=12))}"
                ioc = {'ioc_id': f"IOC-{uuid.uuid4().hex[:10].upper()}", 'ioc_type': ioc_type, 'ioc_value': val,
                       'threat_actor': actor['name'], 'actor_id': actor['actor_id'], 'actor_country': actor['country'],
                       'malware_family': random.choice(MALWARE_FAMILIES), 'mitre_technique': tech['id'], 'mitre_name': tech['name'],
                       'severity': tech['severity'], 'confidence': random.choice(['HIGH', 'HIGH', 'MODERATE']),
                       'created_at': datetime.utcnow().isoformat(), 'source': 'DIA_THREAT_INTEL_DB'}
                self.db.dia_iocs.insert_one(ioc)
                logger.info(f"[DIA] IOC: {ioc_type} → {actor['name']}")
            except Exception as e:
                logger.error(f"DIA IOC error: {e}")

    def _dia_report_loop(self):
        while self.running:
            time.sleep(random.uniform(60, 180))
            try:
                actor = random.choice(THREAT_ACTORS)
                report = {
                    'report_id': f"DIA-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000,9999)}",
                    'title': f"{actor['name']} activity targeting {random.choice(CENTCOM_REGIONS)}",
                    'threat_actor': actor['name'], 'actor_country': actor['country'],
                    'severity': random.choice(['CRITICAL', 'HIGH', 'HIGH', 'MEDIUM']),
                    'techniques': [{'id': t['id'], 'name': t['name']} for t in random.sample(MITRE_TECHNIQUES, k=random.randint(2, 4))],
                    'published_at': datetime.utcnow().isoformat(), 'source': 'DIA_THREAT_INTEL_DB'}
                self.db.dia_threat_reports.insert_one(report)
                logger.info(f"[DIA] Report: {report['title'][:50]}...")
            except Exception as e:
                logger.error(f"DIA report error: {e}")

    def _dia_actor_update_loop(self):
        while self.running:
            time.sleep(random.uniform(120, 300))
            try:
                actor = random.choice(THREAT_ACTORS)
                self.db.dia_threat_actors.update_one(
                    {'actor_id': actor['actor_id']},
                    {'$set': {'last_activity': datetime.utcnow().isoformat(), 'threat_score': random.randint(60, 99),
                              'updated_at': datetime.utcnow().isoformat(), 'updated_by': f'DIA-ANALYST-{random.randint(100,999)}'}})
                logger.info(f"[DIA] Updated: {actor['name']}")
            except Exception as e:
                logger.error(f"DIA actor error: {e}")

    # ============================================================
    # 2. MARITIME DOMAIN AWARENESS (MQTT)
    # ============================================================
    def _init_mda(self):
        self.mda_vessels = []
        for i in range(60):
            lane = random.choice(SHIPPING_LANES)
            vtype = random.choice(COMMERCIAL_VESSEL_TYPES)
            flag = random.choice(VESSEL_FLAGS)
            speed = random.uniform(5, 18) if vtype not in ['Fishing Vessel', 'Tug'] else random.uniform(2, 8)
            hdg = random.uniform(0, 360)
            prefix = random.choice(['OCEAN', 'SEA', 'STAR', 'GOLDEN', 'BLUE', 'PACIFIC', 'GULF', 'EVER', 'MAERSK'])
            suffix = random.choice(['FORTUNE', 'SPIRIT', 'GRACE', 'PRIDE', 'GLORY', 'HARMONY', 'PIONEER', 'NAVIGATOR', 'TRADER', 'VOYAGER'])
            v = {'mmsi': f"{random.randint(200,799)}{random.randint(100000,999999)}", 'vessel_name': f"{prefix} {suffix}",
                 'vessel_type': vtype, 'flag_country': flag['country'], 'flag_name': flag['name'],
                 'lat': round(random.uniform(*lane['lat_range']), 6), 'lon': round(random.uniform(*lane['lon_range']), 6),
                 'speed_knots': round(speed, 1), 'heading_deg': round(hdg, 1), 'destination': random.choice(PORTS)['name'],
                 'last_update': datetime.utcnow().isoformat(), 'source': 'ONI_MDA_AIS_FEED'}
            self.mda_vessels.append(v)
            self.mqtt_client.publish(f"mda/ais/vessel/{v['mmsi']}", json.dumps(v), qos=0)
        logger.info(f"  Seeded {len(self.mda_vessels)} vessels via MQTT")

    def _mda_vessel_loop(self):
        while self.running:
            time.sleep(random.uniform(3, 8))
            try:
                v = random.choice(self.mda_vessels)
                speed_nm_s = v['speed_knots'] / 3600.0
                dist = speed_nm_s * random.uniform(10, 60)
                hr = math.radians(v['heading_deg'])
                v['lat'] = round(max(10.0, min(38.0, v['lat'] + (dist / 60.0) * math.cos(hr))), 6)
                v['lon'] = round(max(25.0, min(70.0, v['lon'] + (dist / 60.0) * math.sin(hr) / math.cos(math.radians(v['lat'])))), 6)
                v['heading_deg'] = round((v['heading_deg'] + random.uniform(-3, 3)) % 360, 1)
                v['speed_knots'] = round(max(0.5, v['speed_knots'] + random.uniform(-0.5, 0.5)), 1)
                v['last_update'] = datetime.utcnow().isoformat()
                self.mqtt_client.publish(f"mda/ais/vessel/{v['mmsi']}", json.dumps(v), qos=0)
            except Exception as e:
                logger.error(f"MDA vessel error: {e}")

    def _mda_port_loop(self):
        while self.running:
            time.sleep(random.uniform(20, 60))
            try:
                port = random.choice(PORTS)
                v = random.choice(self.mda_vessels)
                activity = {'event_id': f"PORT-{uuid.uuid4().hex[:8].upper()}", 'port_name': port['name'],
                           'port_lat': port['lat'], 'port_lon': port['lon'], 'vessel_name': v['vessel_name'],
                           'vessel_type': v['vessel_type'], 'vessel_flag': v['flag_country'],
                           'event_type': random.choice(['ARRIVAL', 'DEPARTURE', 'ANCHORED', 'PILOT_BOARDED']),
                           'timestamp': datetime.utcnow().isoformat(), 'source': 'ONI_MDA_AIS_FEED'}
                self.mqtt_client.publish(f"mda/port/{port['name'].lower().replace(' ', '_')}", json.dumps(activity), qos=1)
                logger.info(f"[MDA] Port: {activity['event_type']} - {v['vessel_name']} at {port['name']}")
            except Exception as e:
                logger.error(f"MDA port error: {e}")

    def _mda_voi_loop(self):
        while self.running:
            time.sleep(random.uniform(30, 90))
            try:
                v = random.choice(self.mda_vessels)
                if v['flag_country'] not in ['IR', 'RU', 'CN', 'KP'] and random.random() > 0.3:
                    continue
                alert = {'alert_id': f"VOI-{uuid.uuid4().hex[:8].upper()}", 'vessel_name': v['vessel_name'],
                         'vessel_flag': v['flag_country'], 'lat': v['lat'], 'lon': v['lon'],
                         'reason': random.choice(VOI_REASONS),
                         'severity': 'CRITICAL' if v['flag_country'] in ['IR', 'KP'] else random.choice(['HIGH', 'MEDIUM']),
                         'timestamp': datetime.utcnow().isoformat(), 'source': 'ONI_MDA_AIS_FEED'}
                self.mqtt_client.publish("mda/voi/alerts", json.dumps(alert), qos=1)
                logger.info(f"[MDA] VOI: {alert['severity']} - {v['vessel_name']} ({v['flag_country']})")
            except Exception as e:
                logger.error(f"MDA VOI error: {e}")

    # ============================================================
    # 3. CENTCOM FORCE READINESS (MongoDB)
    # ============================================================
    def _init_centcom(self):
        for unit in CENTCOM_UNITS:
            personnel_pct = random.uniform(0.85, 0.98)
            self.db.centcom_unit_readiness.update_one(
                {'unit_id': unit['unit_id']},
                {'$set': {**unit, 'readiness': random.choice(['C1', 'C1', 'C2', 'C2', 'C3']),
                          'personnel_assigned': int(unit['auth'] * personnel_pct),
                          'personnel_available': int(unit['auth'] * personnel_pct * random.uniform(0.90, 0.98)),
                          'training_status': random.choice(['GREEN', 'GREEN', 'AMBER', 'AMBER', 'RED']),
                          'equipment_or_rate': round(random.uniform(0.80, 0.97), 2),
                          'last_assessment': datetime.utcnow().isoformat(), 'source': 'CENTCOM_FORCE_READINESS'}},
                upsert=True)
        # Seed equipment
        eq_id = 0
        for unit in CENTCOM_UNITS:
            for equip in random.sample(EQUIPMENT_TYPES, k=random.randint(3, 6)):
                eq_id += 1
                total = random.randint(10, 80)
                fmc = int(total * random.uniform(0.70, 0.95))
                self.db.centcom_equipment_status.update_one(
                    {'equipment_id': f"EQ-{eq_id:04d}"},
                    {'$set': {'equipment_id': f"EQ-{eq_id:04d}", 'unit_id': unit['unit_id'], 'unit_name': unit['name'],
                              'equipment_type': equip['type'], 'category': equip['category'],
                              'total': total, 'fmc': fmc, 'nmc': total - fmc,
                              'or_rate': round(fmc / total, 2),
                              'last_update': datetime.utcnow().isoformat(), 'source': 'CENTCOM_FORCE_READINESS'}},
                    upsert=True)
        logger.info(f"  Seeded {len(CENTCOM_UNITS)} units, {eq_id} equipment records")

    def _centcom_readiness_loop(self):
        while self.running:
            time.sleep(random.uniform(30, 90))
            try:
                unit = random.choice(CENTCOM_UNITS)
                personnel_pct = random.uniform(0.85, 0.98)
                self.db.centcom_unit_readiness.update_one(
                    {'unit_id': unit['unit_id']},
                    {'$set': {'readiness': random.choice(['C1', 'C1', 'C2', 'C2', 'C3']),
                              'personnel_assigned': int(unit['auth'] * personnel_pct),
                              'personnel_available': int(unit['auth'] * personnel_pct * random.uniform(0.90, 0.98)),
                              'training_status': random.choice(['GREEN', 'GREEN', 'AMBER', 'RED']),
                              'equipment_or_rate': round(random.uniform(0.78, 0.97), 2),
                              'last_assessment': datetime.utcnow().isoformat()}})
                logger.info(f"[CENTCOM] Readiness update: {unit['name']}")
            except Exception as e:
                logger.error(f"CENTCOM readiness error: {e}")

    def _centcom_equipment_loop(self):
        while self.running:
            time.sleep(random.uniform(20, 60))
            try:
                # Get random equipment record
                count = self.db.centcom_equipment_status.count_documents({})
                if count > 0:
                    rec = self.db.centcom_equipment_status.find_one(skip=random.randint(0, max(0, count - 1)))
                    if rec:
                        total = rec['total']
                        fmc = max(1, min(total, rec['fmc'] + random.randint(-2, 2)))
                        self.db.centcom_equipment_status.update_one(
                            {'equipment_id': rec['equipment_id']},
                            {'$set': {'fmc': fmc, 'nmc': total - fmc, 'or_rate': round(fmc / total, 2),
                                      'last_update': datetime.utcnow().isoformat()}})
            except Exception as e:
                logger.error(f"CENTCOM equipment error: {e}")

    # ============================================================
    # 4. NGA GEOSPATIAL INTELLIGENCE (MQTT)
    # ============================================================
    def _nga_change_detection_loop(self):
        while self.running:
            time.sleep(random.uniform(20, 60))
            try:
                area = random.choice(NGA_AREAS)
                change = random.choice(NGA_CHANGE_TYPES)
                report = {
                    'report_id': f"NGA-CD-{uuid.uuid4().hex[:8].upper()}",
                    'area_name': area['name'], 'area_type': area['type'],
                    'lat': round(area['lat'] + random.uniform(-0.05, 0.05), 6),
                    'lon': round(area['lon'] + random.uniform(-0.05, 0.05), 6),
                    'change_type': change,
                    'confidence': random.choice(['HIGH', 'HIGH', 'MODERATE']),
                    'imagery_source': random.choice(['ELECTRO-OPTICAL', 'SAR', 'INFRARED', 'MULTISPECTRAL']),
                    'resolution_m': round(random.uniform(0.3, 5.0), 1),
                    'classification': random.choice(['SECRET', 'SECRET//NOFORN', 'TOP SECRET//SI']),
                    'timestamp': datetime.utcnow().isoformat(),
                    'analyst': f'NGA-ANALYST-{random.randint(100,999)}',
                    'source': 'NGA_GEOINT_SYSTEM',
                }
                self.mqtt_client.publish(f"nga/geoint/change_detection", json.dumps(report), qos=1)
                logger.info(f"[NGA] Change: {change[:30]} at {area['name'][:25]}")
            except Exception as e:
                logger.error(f"NGA change detection error: {e}")

    def _nga_restriction_loop(self):
        while self.running:
            time.sleep(random.uniform(60, 180))
            try:
                area = random.choice(NGA_AREAS)
                restriction = {
                    'restriction_id': f"NGA-R-{uuid.uuid4().hex[:8].upper()}",
                    'area_name': area['name'],
                    'restriction_type': random.choice(['NO_FLY_ZONE', 'RESTRICTED_AREA', 'DANGER_AREA', 'WARNING_AREA']),
                    'lat': area['lat'], 'lon': area['lon'], 'radius_km': area['radius_km'],
                    'floor_ft': random.choice([0, 0, 5000, 10000]),
                    'ceiling_ft': random.choice([25000, 40000, 60000, 99999]),
                    'effective_from': datetime.utcnow().isoformat(),
                    'effective_until': (datetime.utcnow() + timedelta(hours=random.randint(6, 72))).isoformat(),
                    'authority': random.choice(['CENTCOM', 'CAOC', 'Host Nation']),
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': 'NGA_GEOINT_SYSTEM',
                }
                self.mqtt_client.publish("nga/geoint/restrictions", json.dumps(restriction), qos=1)
                logger.info(f"[NGA] Restriction: {restriction['restriction_type']} at {area['name'][:25]}")
            except Exception as e:
                logger.error(f"NGA restriction error: {e}")

    # ============================================================
    # 5. COALITION PARTNER C2 (Kafka Producer → mirrored topics)
    # ============================================================
    def _coalition_air_loop(self):
        while self.running:
            time.sleep(random.uniform(5, 15))
            try:
                nation = random.choice(COALITION_NATIONS)
                ac_type = random.choice(COALITION_AIRCRAFT.get(nation['code'], ['Unknown']))
                track = {
                    'track_id': f"{nation['code']}-AIR-{uuid.uuid4().hex[:6].upper()}",
                    'nation': nation['code'], 'nation_name': nation['name'], 'alliance': nation['alliance'],
                    'aircraft_type': ac_type, 'callsign': f"{nation['code'][:2]}{random.randint(100,999)}",
                    'lat': round(random.uniform(12.0, 38.0), 6), 'lon': round(random.uniform(28.0, 65.0), 6),
                    'altitude_ft': random.choice([15000, 20000, 25000, 30000, 35000, 40000]),
                    'heading_deg': round(random.uniform(0, 360), 1),
                    'speed_knots': random.randint(250, 550),
                    'mission_type': random.choice(['CAP', 'ISR', 'TANKER', 'TRANSPORT', 'STRIKE', 'SEAD', 'AEW']),
                    'iff_mode': random.choice(['MODE_4', 'MODE_5']),
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': f'COALITION_{nation["code"]}_C2',
                }
                self.kafka_producer.produce(f'coalition-{nation["code"].lower()}-air-tracks',
                                           key=track['track_id'], value=json.dumps(track).encode())
                self.kafka_producer.poll(0)
            except Exception as e:
                logger.error(f"Coalition air error: {e}")

    def _coalition_naval_loop(self):
        while self.running:
            time.sleep(random.uniform(10, 30))
            try:
                nation = random.choice(COALITION_NATIONS)
                vessel = random.choice(COALITION_NAVAL.get(nation['code'], ['Unknown']))
                track = {
                    'track_id': f"{nation['code']}-NAV-{uuid.uuid4().hex[:6].upper()}",
                    'nation': nation['code'], 'nation_name': nation['name'],
                    'vessel_name': vessel, 'hull_type': vessel.split('(')[-1].rstrip(')') if '(' in vessel else 'FFG',
                    'lat': round(random.uniform(12.0, 35.0), 6), 'lon': round(random.uniform(30.0, 65.0), 6),
                    'heading_deg': round(random.uniform(0, 360), 1),
                    'speed_knots': round(random.uniform(8, 25), 1),
                    'mission': random.choice(['PATROL', 'ESCORT', 'ASW', 'STRIKE_GROUP', 'MINE_WARFARE', 'AMPHIBIOUS']),
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': f'COALITION_{nation["code"]}_C2',
                }
                self.kafka_producer.produce(f'coalition-{nation["code"].lower()}-naval-tracks',
                                           key=track['track_id'], value=json.dumps(track).encode())
                self.kafka_producer.poll(0)
            except Exception as e:
                logger.error(f"Coalition naval error: {e}")

    def _coalition_intel_loop(self):
        while self.running:
            time.sleep(random.uniform(45, 120))
            try:
                nation = random.choice(COALITION_NATIONS)
                report = {
                    'report_id': f"{nation['code']}-INTEL-{uuid.uuid4().hex[:8].upper()}",
                    'nation': nation['code'], 'nation_name': nation['name'],
                    'report_type': random.choice(['INTSUM', 'SIGINT_REPORT', 'HUMINT_REPORT', 'IMINT_REPORT', 'SPOT_REPORT']),
                    'priority': random.choice(['FLASH', 'IMMEDIATE', 'PRIORITY', 'ROUTINE']),
                    'region': random.choice(CENTCOM_REGIONS),
                    'subject': random.choice([
                        'Hostile naval movement detected', 'New SAM site identified', 'Militia activity increase',
                        'Suspected weapons transfer', 'Communications intercept summary', 'Force buildup assessment',
                        'Airfield activity change', 'Maritime interdiction result']),
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': f'COALITION_{nation["code"]}_C2',
                }
                self.kafka_producer.produce(f'coalition-{nation["code"].lower()}-intel',
                                           key=report['report_id'], value=json.dumps(report).encode())
                self.kafka_producer.poll(0)
                logger.info(f"[COALITION] {nation['code']} intel: {report['subject'][:40]}...")
            except Exception as e:
                logger.error(f"Coalition intel error: {e}")

    # ============================================================
    # 6. DLA DEFENSE LOGISTICS (MongoDB)
    # ============================================================
    def _init_dla(self):
        for depot in DLA_DEPOTS:
            for supply in DLA_SUPPLY_CLASSES:
                max_qty = int({'I': 500, 'III': 2000000, 'V': 300, 'VIII': 200, 'IX': 150}.get(supply['cls'], 100) * depot['cap'])
                on_hand = int(max_qty * random.uniform(0.40, 0.95))
                self.db.dla_supply_levels.update_one(
                    {'depot_id': depot['depot_id'], 'supply_class': supply['cls']},
                    {'$set': {'depot_id': depot['depot_id'], 'depot_name': depot['name'],
                              'lat': depot['lat'], 'lon': depot['lon'],
                              'supply_class': supply['cls'], 'supply_name': supply['name'], 'unit': supply['unit'],
                              'on_hand': on_hand, 'max_capacity': max_qty,
                              'fill_rate': round(on_hand / max_qty, 2),
                              'days_of_supply': round(on_hand / max(1, max_qty / 30), 1),
                              'status': 'GREEN' if on_hand / max_qty > 0.6 else ('AMBER' if on_hand / max_qty > 0.3 else 'RED'),
                              'last_update': datetime.utcnow().isoformat(), 'source': 'DLA_LOGISTICS_SYSTEM'}},
                    upsert=True)
        logger.info(f"  Seeded {len(DLA_DEPOTS)} depots x {len(DLA_SUPPLY_CLASSES)} supply classes")

    def _dla_supply_loop(self):
        while self.running:
            time.sleep(random.uniform(15, 45))
            try:
                depot = random.choice(DLA_DEPOTS)
                supply = random.choice(DLA_SUPPLY_CLASSES)
                rec = self.db.dla_supply_levels.find_one({'depot_id': depot['depot_id'], 'supply_class': supply['cls']})
                if rec:
                    # Consume or resupply
                    change = random.randint(-50, 30)
                    new_qty = max(0, min(rec['max_capacity'], rec['on_hand'] + change))
                    fill = round(new_qty / rec['max_capacity'], 2)
                    self.db.dla_supply_levels.update_one(
                        {'depot_id': depot['depot_id'], 'supply_class': supply['cls']},
                        {'$set': {'on_hand': new_qty, 'fill_rate': fill,
                                  'days_of_supply': round(new_qty / max(1, rec['max_capacity'] / 30), 1),
                                  'status': 'GREEN' if fill > 0.6 else ('AMBER' if fill > 0.3 else 'RED'),
                                  'last_update': datetime.utcnow().isoformat()}})
            except Exception as e:
                logger.error(f"DLA supply error: {e}")

    def _dla_shipment_loop(self):
        while self.running:
            time.sleep(random.uniform(30, 90))
            try:
                depot = random.choice(DLA_DEPOTS)
                supply = random.choice(DLA_SUPPLY_CLASSES)
                shipment = {
                    'shipment_id': f"DLA-{uuid.uuid4().hex[:8].upper()}",
                    'depot_id': depot['depot_id'], 'depot_name': depot['name'],
                    'supply_class': supply['cls'], 'supply_name': supply['name'],
                    'quantity': random.randint(10, 500),
                    'unit': supply['unit'],
                    'origin': random.choice(['CONUS', 'Rota Spain', 'Diego Garcia', 'Sigonella Italy', 'Guam']),
                    'status': random.choice(['IN_TRANSIT', 'IN_TRANSIT', 'DELIVERED', 'CUSTOMS_HOLD', 'LOADING']),
                    'eta': (datetime.utcnow() + timedelta(days=random.randint(1, 14))).isoformat(),
                    'priority': random.choice(['ROUTINE', 'PRIORITY', 'IMMEDIATE']),
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': 'DLA_LOGISTICS_SYSTEM',
                }
                self.db.dla_shipments.insert_one(shipment)
                logger.info(f"[DLA] Shipment: {supply['name']} → {depot['name']}")
            except Exception as e:
                logger.error(f"DLA shipment error: {e}")

    # ============================================================
    # 7. FAA CIVIL AVIATION (MQTT)
    # ============================================================
    def _init_faa(self):
        self.faa_aircraft = []
        for i in range(40):
            airline = random.choice(CIVIL_AIRLINES)
            ac_type = random.choice(CIVIL_AIRCRAFT_TYPES)
            dep = random.choice(CIVIL_AIRPORTS)
            arr = random.choice([a for a in CIVIL_AIRPORTS if a['icao'] != dep['icao']])
            # Position between departure and arrival
            t = random.uniform(0.1, 0.9)
            lat = dep['lat'] + t * (arr['lat'] - dep['lat']) + random.uniform(-1, 1)
            lon = dep['lon'] + t * (arr['lon'] - dep['lon']) + random.uniform(-1, 1)
            ac = {
                'hex_code': f"{random.randint(0xA00000, 0xAFFFFF):06X}",
                'flight': f"{airline['code']}{random.randint(100, 999)}",
                'airline': airline['name'], 'airline_code': airline['code'],
                'aircraft_type': ac_type,
                'lat': round(lat, 6), 'lon': round(lon, 6),
                'altitude_ft': random.choice([31000, 33000, 35000, 37000, 39000, 41000]),
                'heading_deg': round(random.uniform(0, 360), 1),
                'speed_knots': random.randint(400, 520),
                'squawk': f"{random.randint(1000, 7777):04d}",
                'departure': dep['icao'], 'departure_name': dep['name'],
                'arrival': arr['icao'], 'arrival_name': arr['name'],
                'source': 'FAA_ADSB_FEED',
            }
            self.faa_aircraft.append(ac)
            self.mqtt_client.publish(f"faa/adsb/{ac['hex_code']}", json.dumps(ac), qos=0)
        logger.info(f"  Seeded {len(self.faa_aircraft)} civil aircraft via MQTT")

    def _faa_adsb_loop(self):
        while self.running:
            time.sleep(random.uniform(2, 6))
            try:
                ac = random.choice(self.faa_aircraft)
                speed_nm_s = ac['speed_knots'] / 3600.0
                dist = speed_nm_s * random.uniform(10, 30)
                hr = math.radians(ac['heading_deg'])
                ac['lat'] = round(max(10.0, min(42.0, ac['lat'] + (dist / 60.0) * math.cos(hr))), 6)
                ac['lon'] = round(max(25.0, min(70.0, ac['lon'] + (dist / 60.0) * math.sin(hr) / math.cos(math.radians(ac['lat'])))), 6)
                ac['heading_deg'] = round((ac['heading_deg'] + random.uniform(-2, 2)) % 360, 1)
                ac['altitude_ft'] = ac['altitude_ft'] + random.choice([-1000, 0, 0, 0, 1000])
                ac['altitude_ft'] = max(5000, min(45000, ac['altitude_ft']))
                ac['last_update'] = datetime.utcnow().isoformat()
                self.mqtt_client.publish(f"faa/adsb/{ac['hex_code']}", json.dumps(ac), qos=0)
            except Exception as e:
                logger.error(f"FAA ADS-B error: {e}")

    # ============================================================
    # STARTUP
    # ============================================================
    def start(self):
        if not self.connect():
            return False

        logger.info("\nInitializing external systems...")
        logger.info("\n--- 1. DIA Threat Intelligence ---")
        self._init_dia()
        logger.info("\n--- 2. Maritime Domain Awareness ---")
        self._init_mda()
        logger.info("\n--- 3. CENTCOM Force Readiness ---")
        self._init_centcom()
        logger.info("\n--- 4. NGA Geospatial Intelligence ---")
        logger.info("  (MQTT feed - no seed data)")
        logger.info("\n--- 5. Coalition Partner C2 ---")
        logger.info("  (Kafka producer - no seed data)")
        logger.info("\n--- 6. Defense Logistics Agency ---")
        self._init_dla()
        logger.info("\n--- 7. FAA Civil Aviation ---")
        self._init_faa()

        self.running = True

        threads = [
            ('DIA IOC Feed', self._dia_ioc_loop),
            ('DIA Threat Reports', self._dia_report_loop),
            ('DIA Actor Updates', self._dia_actor_update_loop),
            ('MDA Vessel Tracks', self._mda_vessel_loop),
            ('MDA Port Activity', self._mda_port_loop),
            ('MDA VOI Alerts', self._mda_voi_loop),
            ('CENTCOM Readiness', self._centcom_readiness_loop),
            ('CENTCOM Equipment', self._centcom_equipment_loop),
            ('NGA Change Detection', self._nga_change_detection_loop),
            ('NGA Restrictions', self._nga_restriction_loop),
            ('Coalition Air Tracks', self._coalition_air_loop),
            ('Coalition Naval Tracks', self._coalition_naval_loop),
            ('Coalition Intel', self._coalition_intel_loop),
            ('DLA Supply Levels', self._dla_supply_loop),
            ('DLA Shipments', self._dla_shipment_loop),
            ('FAA ADS-B Feed', self._faa_adsb_loop),
        ]

        for name, fn in threads:
            t = threading.Thread(target=fn, daemon=True, name=name)
            t.start()
            logger.info(f"  Started: {name}")

        return True

    def stop(self):
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        if self.kafka_producer:
            self.kafka_producer.flush()


def main():
    print("\n" + "=" * 60)
    print("JADC2 External Systems Data Generator")
    print("=" * 60)
    print("\n  1. DIA Threat Intelligence    (MongoDB → MongoSource)")
    print("  2. Maritime Domain Awareness  (MQTT → MqttSource)")
    print("  3. CENTCOM Force Readiness    (MongoDB → MongoSource)")
    print("  4. NGA Geospatial Intel       (MQTT → MqttSource)")
    print("  5. Coalition Partner C2       (Kafka → MirrorSource)")
    print("  6. Defense Logistics Agency   (MongoDB → MongoSource)")
    print("  7. FAA Civil Aviation         (MQTT → MqttSource)")
    print("\n" + "=" * 60 + "\n")

    gen = ExternalSystemsGenerator()
    if not gen.start():
        return 1

    logger.info(f"\n✓ All 7 external systems running")
    logger.info(f"  MongoDB → {EXTERNAL_DB}")
    logger.info(f"  MQTT    → {MQTT_BROKER}:{MQTT_PORT}")
    logger.info(f"  Kafka   → Coalition C2 topics\n")

    try:
        while True:
            time.sleep(30)
            try:
                dia = gen.db.dia_iocs.count_documents({})
                rpts = gen.db.dia_threat_reports.count_documents({})
                shp = gen.db.dla_shipments.count_documents({})
                logger.info(f"[STATS] DIA: {dia} IOCs, {rpts} reports | DLA: {shp} shipments | "
                           f"MDA: {len(gen.mda_vessels)} vessels | FAA: {len(gen.faa_aircraft)} aircraft")
            except:
                pass
    except KeyboardInterrupt:
        gen.stop()
        logger.info("Shutting down...")
    return 0


if __name__ == '__main__':
    exit(main())
