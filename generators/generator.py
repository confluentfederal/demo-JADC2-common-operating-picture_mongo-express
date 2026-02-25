#!/usr/bin/env python3
"""
JADC2 COP - Multi-Domain Data Generator
All entities emit to GPS stream for unified tracking
Cyber events tied to US Military facilities
"""

import json
import random
import time
import threading
import uuid
import math
from datetime import datetime
import os

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Topics
AIRCRAFT_TOPIC = 'military-aircraft'
NAVAL_TOPIC = 'naval-vessels'
GPS_TOPIC = 'ground-force-gps'
CYBER_TOPIC = 'cyber-threats'
SIGINT_TOPIC = 'sigint-feeds'
SATELLITE_TOPIC = 'satellite-imagery-metadata'
SPECTRUM_TOPIC = 'spectrum-interference'
EMBM_TOPIC = 'embm-tracks'

# US SATCOM assets and frequencies
US_SATCOM_SYSTEMS = [
    {'name': 'MUOS-1', 'type': 'UHF_SATCOM', 'freq_ghz': 0.36, 'orbit': 'GEO', 'lon_slot': 75.0, 'users': ['CSG', 'MEU', 'SOCOM']},
    {'name': 'MUOS-4', 'type': 'UHF_SATCOM', 'freq_ghz': 0.38, 'orbit': 'GEO', 'lon_slot': 100.0, 'users': ['PACFLT', 'MEF']},
    {'name': 'WGS-6', 'type': 'SHF_SATCOM', 'freq_ghz': 7.9, 'orbit': 'GEO', 'lon_slot': 65.0, 'users': ['CSG', 'AWACS', 'THAAD']},
    {'name': 'WGS-8', 'type': 'SHF_SATCOM', 'freq_ghz': 8.4, 'orbit': 'GEO', 'lon_slot': 105.0, 'users': ['INDOPACOM', 'CENTCOM']},
    {'name': 'AEHF-4', 'type': 'EHF_SATCOM', 'freq_ghz': 44.0, 'orbit': 'GEO', 'lon_slot': 85.0, 'users': ['STRATCOM', 'NC3']},
    {'name': 'AEHF-5', 'type': 'EHF_SATCOM', 'freq_ghz': 44.5, 'orbit': 'GEO', 'lon_slot': 145.0, 'users': ['PACFLT', 'SPACECOM']},
]

# Enemy jammer types
HOSTILE_JAMMERS = [
    {'type': 'TRUCK_JAMMER', 'country': 'RU', 'system': 'Tirada-2S', 'power_kw': 5, 'range_km': 500, 'bands': ['SHF', 'EHF']},
    {'type': 'SHIP_JAMMER', 'country': 'RU', 'system': 'TK-25', 'power_kw': 10, 'range_km': 800, 'bands': ['UHF', 'SHF']},
    {'type': 'FIXED_SITE', 'country': 'CN', 'system': 'BM/KG-8601', 'power_kw': 50, 'range_km': 1500, 'bands': ['UHF', 'SHF', 'EHF']},
    {'type': 'MOBILE_JAMMER', 'country': 'CN', 'system': 'JN-1101', 'power_kw': 8, 'range_km': 600, 'bands': ['SHF']},
    {'type': 'TRUCK_JAMMER', 'country': 'IR', 'system': 'Mohajer-J', 'power_kw': 2, 'range_km': 200, 'bands': ['UHF', 'SHF']},
    {'type': 'NAVAL_EW', 'country': 'KP', 'system': 'Musudan-EW', 'power_kw': 3, 'range_km': 300, 'bands': ['UHF']},
]

# Hostile jammer locations (potential sites)
JAMMER_SITES = [
    {'id': 'JAM-001', 'name': 'Hainan Island EW Site', 'country': 'CN', 'lat': 18.35, 'lon': 109.68, 'terrain': 'COASTAL'},
    {'id': 'JAM-002', 'name': 'Paracel Islands', 'country': 'CN', 'lat': 16.83, 'lon': 112.33, 'terrain': 'ISLAND'},
    {'id': 'JAM-003', 'name': 'Tartus Naval EW', 'country': 'RU', 'lat': 34.89, 'lon': 35.87, 'terrain': 'COASTAL'},
    {'id': 'JAM-004', 'name': 'Latakia Mobile Site', 'country': 'RU', 'lat': 35.52, 'lon': 35.78, 'terrain': 'RIDGE'},
    {'id': 'JAM-005', 'name': 'Bandar Abbas EW', 'country': 'IR', 'lat': 27.18, 'lon': 56.28, 'terrain': 'COASTAL'},
    {'id': 'JAM-006', 'name': 'Qeshm Island', 'country': 'IR', 'lat': 26.95, 'lon': 56.27, 'terrain': 'ISLAND'},
    {'id': 'JAM-007', 'name': 'Wonsan EW Battery', 'country': 'KP', 'lat': 39.15, 'lon': 127.44, 'terrain': 'COASTAL'},
]

# Affected US units for jamming scenarios
AFFECTED_UNITS = [
    {'callsign': 'CSG-5', 'type': 'CARRIER_STRIKE_GROUP', 'region': 'INDOPACOM', 'satcom_deps': ['MUOS-4', 'WGS-8']},
    {'callsign': 'MEU-31', 'type': 'MARINE_EXPEDITIONARY_UNIT', 'region': 'INDOPACOM', 'satcom_deps': ['MUOS-4']},
    {'callsign': 'CSG-2', 'type': 'CARRIER_STRIKE_GROUP', 'region': 'CENTCOM', 'satcom_deps': ['WGS-6', 'AEHF-4']},
    {'callsign': 'THAAD-1', 'type': 'MISSILE_DEFENSE', 'region': 'CENTCOM', 'satcom_deps': ['WGS-6', 'AEHF-4']},
    {'callsign': 'SOF-TF', 'type': 'SPECIAL_OPS_TASK_FORCE', 'region': 'AFRICOM', 'satcom_deps': ['MUOS-1', 'AEHF-4']},
]

# US Military Facilities - All cyber targets
US_FACILITIES = [
    {'id': 'FAC-001', 'name': 'Al Udeid AB', 'country': 'Qatar', 'lat': 25.117, 'lon': 51.315, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['CENTCOM HQ Network', 'NIPR Gateway', 'Air Ops Center']},
    {'id': 'FAC-002', 'name': 'Al Dhafra AB', 'country': 'UAE', 'lat': 24.248, 'lon': 54.547, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['Air Defense Network', 'ISR Control', 'Logistics Hub']},
    {'id': 'FAC-003', 'name': 'Camp Arifjan', 'country': 'Kuwait', 'lat': 28.933, 'lon': 48.100, 'type': 'ARMY_BASE', 'region': 'CENTCOM', 'networks': ['SIPR Enclave', 'Army Battle Command', 'Supply Chain DB']},
    {'id': 'FAC-004', 'name': 'NSA Bahrain', 'country': 'Bahrain', 'lat': 26.237, 'lon': 50.652, 'type': 'NAVAL_BASE', 'region': 'CENTCOM', 'networks': ['Fleet Networks', 'Naval Intel Center', 'Maritime Domain Awareness']},
    {'id': 'FAC-005', 'name': 'Prince Sultan AB', 'country': 'Saudi Arabia', 'lat': 24.062, 'lon': 47.580, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['Combined Air Ops', 'Patriot Battery Net', 'CAOC Link']},
    {'id': 'FAC-006', 'name': 'Al Asad AB', 'country': 'Iraq', 'lat': 33.786, 'lon': 42.441, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['Tactical Data Link', 'UAV Control Station', 'Force Protection']},
    {'id': 'FAC-007', 'name': 'Incirlik AB', 'country': 'Turkey', 'lat': 37.002, 'lon': 35.426, 'type': 'AIR_BASE', 'region': 'EUCOM', 'networks': ['NATO Secure Net', 'Nuclear Storage Sys', 'Satellite Uplink']},
    {'id': 'FAC-008', 'name': 'Izmir AS', 'country': 'Turkey', 'lat': 38.422, 'lon': 27.134, 'type': 'SUPPORT_BASE', 'region': 'EUCOM', 'networks': ['NATO Command Net', 'Personnel Database']},
    {'id': 'FAC-009', 'name': 'NAS Sigonella', 'country': 'Italy', 'lat': 37.407, 'lon': 14.922, 'type': 'NAVAL_AIR', 'region': 'EUCOM', 'networks': ['P-8 Mission Systems', 'Med Surveillance', 'Logistics Database']},
    {'id': 'FAC-010', 'name': 'NSA Souda Bay', 'country': 'Greece', 'lat': 35.481, 'lon': 24.126, 'type': 'NAVAL_BASE', 'region': 'EUCOM', 'networks': ['Sub Support Net', 'Ammo Storage Sys', 'Port Control']},
    {'id': 'FAC-011', 'name': 'RAF Akrotiri', 'country': 'Cyprus', 'lat': 34.590, 'lon': 32.988, 'type': 'AIR_BASE', 'region': 'EUCOM', 'networks': ['UK-US Intel Share', 'Strike Planning', 'SIGINT Collection']},
    {'id': 'FAC-012', 'name': 'Camp Lemonnier', 'country': 'Djibouti', 'lat': 11.547, 'lon': 43.145, 'type': 'EXPEDITIONARY', 'region': 'AFRICOM', 'networks': ['AFRICOM Fusion Center', 'SOF Ops Network', 'Counter-Terror DB']},
    {'id': 'FAC-013', 'name': 'Chabelley Airfield', 'country': 'Djibouti', 'lat': 11.517, 'lon': 42.847, 'type': 'UAV_BASE', 'region': 'AFRICOM', 'networks': ['Drone Control System', 'ISR Downlink', 'Strike Coordination']},
    {'id': 'FAC-014', 'name': 'Cairo West AB', 'country': 'Egypt', 'lat': 30.116, 'lon': 30.915, 'type': 'JOINT_USE', 'region': 'CENTCOM', 'networks': ['Joint Exercise Net', 'Egyptian Liaison']},
]

# Aircraft definitions
US_AIRCRAFT = [
    {'type': 'F-22A Raptor', 'role': 'AIR_SUPERIORITY', 'max_speed': 1500, 'weapons': 'AIM-120D x6, AIM-9X x2'},
    {'type': 'F-35A Lightning II', 'role': 'STEALTH', 'max_speed': 1200, 'weapons': 'AIM-120D x4, GBU-31 x2'},
    {'type': 'F-15E Strike Eagle', 'role': 'STRIKE', 'max_speed': 1600, 'weapons': 'AIM-120C x4, GBU-39 x8'},
    {'type': 'F-16C Fighting Falcon', 'role': 'MULTIROLE', 'max_speed': 1300, 'weapons': 'AIM-120 x4, AIM-9X x2'},
    {'type': 'MQ-9 Reaper', 'role': 'ISR_UAV', 'max_speed': 300, 'weapons': 'AGM-114 Hellfire x4, GBU-12 x2'},
    {'type': 'E-3 Sentry AWACS', 'role': 'AWACS', 'max_speed': 500, 'weapons': 'None - Radar/C2'},
    {'type': 'KC-135 Stratotanker', 'role': 'TANKER', 'max_speed': 500, 'weapons': 'None - Refueling'},
]

RUSSIAN_AIRCRAFT = [
    {'type': 'Su-35S Flanker-E', 'role': 'AIR_SUPERIORITY', 'max_speed': 1500, 'weapons': 'R-77-1 x6, R-73M x4'},
    {'type': 'Su-34 Fullback', 'role': 'STRIKE', 'max_speed': 1200, 'weapons': 'Kh-59MK x4, KAB-1500 x3'},
    {'type': 'Su-57 Felon', 'role': 'STEALTH', 'max_speed': 1600, 'weapons': 'R-77M x4, Kh-59MK2 x4'},
    {'type': 'MiG-31BM Foxhound', 'role': 'INTERCEPTOR', 'max_speed': 1900, 'weapons': 'R-37M x4, R-77 x2'},
    {'type': 'Tu-22M3 Backfire', 'role': 'BOMBER', 'max_speed': 1400, 'weapons': 'Kh-22 x3'},
]

CHINESE_AIRCRAFT = [
    {'type': 'J-20 Mighty Dragon', 'role': 'STEALTH', 'max_speed': 1300, 'weapons': 'PL-15 x4, PL-10 x2'},
    {'type': 'J-16', 'role': 'MULTIROLE', 'max_speed': 1500, 'weapons': 'PL-15 x6, YJ-83 x4'},
    {'type': 'H-6K', 'role': 'BOMBER', 'max_speed': 650, 'weapons': 'CJ-10A x6, YJ-12 x4'},
]

IRANIAN_AIRCRAFT = [
    {'type': 'F-14A Tomcat', 'role': 'AIR_SUPERIORITY', 'max_speed': 1500, 'weapons': 'AIM-54 Phoenix x4, AIM-9 x2'},
    {'type': 'MiG-29A Fulcrum', 'role': 'MULTIROLE', 'max_speed': 1500, 'weapons': 'R-27 x2, R-73 x4'},
    {'type': 'Su-24MK Fencer', 'role': 'STRIKE', 'max_speed': 1100, 'weapons': 'Kh-29 x4, FAB-500 x6'},
    {'type': 'Shahed-136', 'role': 'ATTACK_UAV', 'max_speed': 115, 'weapons': 'Warhead 40kg'},
    {'type': 'F-4E Phantom II', 'role': 'STRIKE', 'max_speed': 1400, 'weapons': 'AGM-65 x4, Mk-82 x6'},
]

# Naval vessels
US_NAVAL = [
    {'type': 'Nimitz-class CVN', 'class': 'CARRIER', 'displacement': 100000, 'weapons': 'RIM-7, RIM-116, Phalanx x3', 'aircraft': 90},
    {'type': 'Gerald R. Ford CVN', 'class': 'CARRIER', 'displacement': 100000, 'weapons': 'RIM-162, RIM-116, Phalanx x3', 'aircraft': 75},
    {'type': 'Arleigh Burke DDG', 'class': 'DESTROYER', 'displacement': 9700, 'weapons': 'Mk-41 VLS x96, Harpoon x8'},
    {'type': 'Ticonderoga CG', 'class': 'CRUISER', 'displacement': 9800, 'weapons': 'Mk-41 VLS x122, Harpoon x8'},
    {'type': 'Virginia SSN', 'class': 'SUBMARINE', 'displacement': 7900, 'weapons': 'Mk-48 Torpedo x26, TLAM x12'},
]

RUSSIAN_NAVAL = [
    {'type': 'Admiral Kuznetsov CV', 'class': 'CARRIER', 'displacement': 58500, 'weapons': 'P-700 Granit x12', 'aircraft': 40},
    {'type': 'Slava-class CG', 'class': 'CRUISER', 'displacement': 12000, 'weapons': 'P-500 Bazalt x16, S-300F'},
    {'type': 'Kilo-class SSK', 'class': 'SUBMARINE', 'displacement': 3100, 'weapons': 'Torpedoes x18, Kalibr x4'},
]

CHINESE_NAVAL = [
    {'type': 'Shandong CV', 'class': 'CARRIER', 'displacement': 70000, 'weapons': 'HQ-10, Type 1130 CIWS', 'aircraft': 44},
    {'type': 'Type 055 DDG', 'class': 'DESTROYER', 'displacement': 13000, 'weapons': 'VLS x112'},
    {'type': 'Type 039A SSK', 'class': 'SUBMARINE', 'displacement': 3600, 'weapons': 'Torpedoes x18, YJ-82 x6'},
]

IRANIAN_NAVAL = [
    {'type': 'Alvand-class FFG', 'class': 'FRIGATE', 'displacement': 1350, 'weapons': 'C-802 x4, 76mm gun'},
    {'type': 'Moudge-class FFG', 'class': 'FRIGATE', 'displacement': 1500, 'weapons': 'Noor AShM x4'},
    {'type': 'Ghadir-class SSK', 'class': 'SUBMARINE', 'displacement': 120, 'weapons': 'Torpedoes x2'},
    {'type': 'Sina-class FAC', 'class': 'MISSILE_BOAT', 'displacement': 310, 'weapons': 'C-802 x4'},
]

# Cyber threat actors
CYBER_ACTORS = [
    {'name': 'APT28 (Fancy Bear)', 'country': 'RU', 'sophistication': 'ADVANCED', 'ttps': ['T1566', 'T1190', 'T1078']},
    {'name': 'APT29 (Cozy Bear)', 'country': 'RU', 'sophistication': 'ADVANCED', 'ttps': ['T1195', 'T1027', 'T1071']},
    {'name': 'Sandworm Team', 'country': 'RU', 'sophistication': 'ADVANCED', 'ttps': ['T1059', 'T1486', 'T1561']},
    {'name': 'APT41 (Double Dragon)', 'country': 'CN', 'sophistication': 'ADVANCED', 'ttps': ['T1190', 'T1055', 'T1021']},
    {'name': 'APT40 (Leviathan)', 'country': 'CN', 'sophistication': 'ADVANCED', 'ttps': ['T1566', 'T1203', 'T1048']},
    {'name': 'Lazarus Group', 'country': 'KP', 'sophistication': 'ADVANCED', 'ttps': ['T1566', 'T1059', 'T1486']},
    {'name': 'MuddyWater', 'country': 'IR', 'sophistication': 'MODERATE', 'ttps': ['T1566', 'T1059', 'T1105']},
    {'name': 'OilRig (APT34)', 'country': 'IR', 'sophistication': 'MODERATE', 'ttps': ['T1566', 'T1078', 'T1048']},
    {'name': 'Charming Kitten', 'country': 'IR', 'sophistication': 'ADVANCED', 'ttps': ['T1566', 'T1528', 'T1114']},
]

CYBER_ATTACKS = [
    {'type': 'NETWORK_INTRUSION', 'severity': 'CRITICAL', 'desc': 'Unauthorized network access detected'},
    {'type': 'MALWARE_DETECTED', 'severity': 'CRITICAL', 'desc': 'APT malware identified in system'},
    {'type': 'DATA_EXFILTRATION', 'severity': 'CRITICAL', 'desc': 'Sensitive data transfer to external IP'},
    {'type': 'C2_COMMUNICATION', 'severity': 'CRITICAL', 'desc': 'Command and control beacon detected'},
    {'type': 'RANSOMWARE_ATTEMPT', 'severity': 'CRITICAL', 'desc': 'Ransomware deployment blocked'},
    {'type': 'ZERO_DAY_EXPLOIT', 'severity': 'CRITICAL', 'desc': 'Unknown vulnerability exploited'},
    {'type': 'PHISHING_CAMPAIGN', 'severity': 'HIGH', 'desc': 'Targeted spearphishing detected'},
    {'type': 'CREDENTIAL_THEFT', 'severity': 'HIGH', 'desc': 'Credential harvesting attempt'},
    {'type': 'LATERAL_MOVEMENT', 'severity': 'HIGH', 'desc': 'Attacker moving laterally in network'},
    {'type': 'DNS_TUNNELING', 'severity': 'HIGH', 'desc': 'Data exfiltration via DNS queries'},
]

# Ground units with home bases
GROUND_UNITS = [
    {'id': 'ALPHA-1', 'name': '1st Armored Brigade', 'type': 'ARMOR', 'base': 'FAC-003', 'personnel': 4500, 'equipment': ['M1A2 Abrams x58', 'M2 Bradley x120']},
    {'id': 'BRAVO-2', 'name': '2nd Infantry Division', 'type': 'INFANTRY', 'base': 'FAC-006', 'personnel': 3200, 'equipment': ['MRAP x80', 'Stryker x45']},
    {'id': 'CHARLIE-3', 'name': '3rd CAB', 'type': 'AVIATION', 'base': 'FAC-001', 'personnel': 2800, 'equipment': ['AH-64E x24', 'UH-60M x36']},
    {'id': 'DELTA-4', 'name': '24th MEU', 'type': 'MARINES', 'base': 'FAC-012', 'personnel': 2200, 'equipment': ['LAV-25 x28', 'AAV-7 x12']},
    {'id': 'ECHO-5', 'name': '75th Ranger Rgt', 'type': 'SOF', 'base': 'FAC-012', 'personnel': 650, 'equipment': ['GMV x24', 'MH-6 x8']},
    {'id': 'FOXTROT-6', 'name': '101st Airborne', 'type': 'AIR_ASSAULT', 'base': 'FAC-002', 'personnel': 3800, 'equipment': ['UH-60 x48', 'CH-47 x16']},
]


class EntityState:
    """Base class for tracking entity state"""
    def __init__(self, entity_id, entity_type, lat, lon, country='US'):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.lat = lat
        self.lon = lon
        self.country = country
        self.heading = random.uniform(0, 360)
        self.speed = 0
        self.altitude = 0

    def get_gps_record(self):
        """Generate GPS record for unified tracking"""
        return {
            'unit_id': self.entity_id,
            'unit_name': getattr(self, 'name', self.entity_id),
            'unit_type': self.entity_type,
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'lat': round(self.lat, 6),
            'lon': round(self.lon, 6),
            'heading_deg': round(self.heading, 1),
            'speed_kph': round(self.speed * 1.852, 1),  # knots to kph
            'altitude_m': round(self.altitude * 0.3048, 1),  # ft to m
            'fuel_level_pct': getattr(self, 'fuel', 80.0),
            'ammo_level_pct': getattr(self, 'ammo', 85.0),
            'personnel_count': getattr(self, 'personnel', 0),
            'equipment_status': 'FULLY_OPERATIONAL',
            'mission_status': getattr(self, 'status', 'STANDBY'),
            'fuel_consumption_rate': 2.5,
            'comms_status': 'NOMINAL',
        }


class AircraftState(EntityState):
    def __init__(self, config, country, tail, base_lat, base_lon):
        super().__init__(tail, f"AIRCRAFT_{config['role']}", base_lat, base_lon, country)
        self.name = f"{config['type']} ({tail})"
        self.type = config['type']
        self.role = config['role']
        self.max_speed = config['max_speed']
        self.weapons = config.get('weapons', 'Unknown')
        self.tail = tail
        self.lat = base_lat + random.uniform(-2, 2)
        self.lon = base_lon + random.uniform(-3, 3)
        self.speed = random.uniform(250, self.max_speed * 0.5)
        self.altitude = random.uniform(8000, 42000) if 'UAV' not in self.role else random.uniform(3000, 15000)
        self.status = random.choice(['PATROL', 'PATROL', 'CAP', 'TRANSIT'])
        self.fuel = random.uniform(50, 95)
        self.iff = 'FRIENDLY' if country == 'US' else 'HOSTILE'

    def update(self, dt):
        self.heading = (self.heading + random.uniform(-3, 3)) % 360
        speed_deg = (self.speed / 3600) / 60 * 0.08
        self.lat += speed_deg * math.cos(math.radians(self.heading)) * dt
        self.lon += speed_deg * math.sin(math.radians(self.heading)) * dt
        self.fuel = max(15, self.fuel - 0.008 * dt)
        if random.random() < 0.003:
            self.status = random.choice(['PATROL', 'INTERCEPT', 'STRIKE', 'CAP', 'RTB'])
        return {
            'aircraft_id': self.tail, 'tail_number': self.tail, 'aircraft_type': self.type,
            'role': self.role, 'country': self.country, 'weapons': self.weapons,
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'lat': round(self.lat, 6), 'lon': round(self.lon, 6),
            'altitude_ft': round(self.altitude, 0), 'heading_deg': round(self.heading, 1),
            'speed_kts': round(self.speed * 0.54, 1), 'status': self.status,
            'fuel_pct': round(self.fuel, 1), 'iff_mode': self.iff,
        }


class NavalState(EntityState):
    def __init__(self, config, country, hull, lat, lon):
        super().__init__(hull, f"NAVAL_{config['class']}", lat, lon, country)
        self.name = f"{config['type']} ({hull})"
        self.type = config['type']
        self.ship_class = config['class']
        self.displacement = config['displacement']
        self.weapons = config.get('weapons', 'Unknown')
        self.aircraft_cap = config.get('aircraft', 0)
        self.hull = hull
        self.speed = random.uniform(10, 22) if 'PATROL' not in self.ship_class else random.uniform(25, 40)
        self.status = random.choice(['PATROL', 'TRANSIT', 'STATION'])
        self.iff = 'FRIENDLY' if country == 'US' else 'HOSTILE'
        self.personnel = config['displacement'] // 10

    def update(self, dt):
        self.heading = (self.heading + random.uniform(-1.5, 1.5)) % 360
        speed_deg = (self.speed * 1.852 / 3600) / 60 * 0.05
        self.lat += speed_deg * math.cos(math.radians(self.heading)) * dt
        self.lon += speed_deg * math.sin(math.radians(self.heading)) * dt
        return {
            'vessel_id': self.hull, 'hull_number': self.hull, 'vessel_type': self.type,
            'vessel_class': self.ship_class, 'displacement_tons': self.displacement,
            'country': self.country, 'weapons': self.weapons, 'aircraft_capacity': self.aircraft_cap,
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'lat': round(self.lat, 6), 'lon': round(self.lon, 6),
            'heading_deg': round(self.heading, 1), 'speed_kts': round(self.speed, 1),
            'status': self.status, 'iff_mode': self.iff,
        }


class GroundUnitState(EntityState):
    def __init__(self, config, facility):
        super().__init__(config['id'], config['type'], facility['lat'], facility['lon'], 'US')
        self.name = config['name']
        self.unit_type = config['type']
        self.personnel = config['personnel']
        self.equipment = config['equipment']
        self.base_facility = facility
        self.lat = facility['lat'] + random.uniform(-0.1, 0.1)
        self.lon = facility['lon'] + random.uniform(-0.1, 0.1)
        self.fuel = random.uniform(60, 95)
        self.ammo = random.uniform(65, 95)
        self.status = random.choice(['STANDBY', 'PATROL', 'TRAINING', 'ALERT'])

    def update(self, dt):
        # Small movements around base
        self.lat += random.uniform(-0.005, 0.005)
        self.lon += random.uniform(-0.005, 0.005)
        self.fuel = max(20, min(100, self.fuel + random.uniform(-0.5, 0.3)))
        self.ammo = max(30, min(100, self.ammo + random.uniform(-0.2, 0.1)))
        if random.random() < 0.01:
            self.status = random.choice(['STANDBY', 'PATROL', 'TRAINING', 'ALERT', 'DEPLOYED'])
        return self.get_gps_record()


class Generator:
    def __init__(self):
        self.producer = None
        self.aircraft = []
        self.naval = []
        self.ground_units = []
        self.running = False
        self._init_forces()

    def _init_forces(self):
        # US Aircraft from various bases
        us_air_bases = [f for f in US_FACILITIES if 'AIR' in f['type'] or f['type'] == 'UAV_BASE']
        for base in us_air_bases:
            for _ in range(random.randint(1, 3)):
                ac = random.choice(US_AIRCRAFT)
                self.aircraft.append(AircraftState(ac, 'US', f"{random.randint(70,99)}-{random.randint(1000,9999)}", base['lat'], base['lon']))
        
        # Russian Aircraft - Syria
        ru_syria = [{'lat': 35.401, 'lon': 35.949}, {'lat': 34.889, 'lon': 35.887}]
        for _ in range(8):
            ac = random.choice(RUSSIAN_AIRCRAFT)
            base = random.choice(ru_syria)
            self.aircraft.append(AircraftState(ac, 'RU', f"RF-{random.randint(81000,95999)}", base['lat'], base['lon']))
        
        # Russian Aircraft - Ukraine/Belarus theater
        ru_ukraine = [
            {'lat': 50.4, 'lon': 30.5},   # Kyiv area
            {'lat': 48.5, 'lon': 37.5},   # Donbas
            {'lat': 44.6, 'lon': 33.5},   # Crimea
            {'lat': 53.9, 'lon': 27.5},   # Belarus/Minsk
            {'lat': 55.0, 'lon': 30.0},   # Belarus/Vitebsk
        ]
        for base in ru_ukraine:
            for _ in range(3):
                ac = random.choice(RUSSIAN_AIRCRAFT)
                self.aircraft.append(AircraftState(ac, 'RU', f"RF-{random.randint(81000,95999)}", base['lat'], base['lon']))
        
        # Chinese Aircraft - Indian Ocean patrol
        cn_patrol = [(18.0, 60.0), (15.0, 55.0), (12.0, 52.0)]
        for pos in cn_patrol:
            for _ in range(2):
                ac = random.choice(CHINESE_AIRCRAFT)
                self.aircraft.append(AircraftState(ac, 'CN', f"{random.randint(10000,99999)}", pos[0], pos[1]))
        
        # Iranian Aircraft
        ir_bases = [{'lat': 35.689, 'lon': 51.311}, {'lat': 32.751, 'lon': 51.861}, {'lat': 29.540, 'lon': 52.589}, {'lat': 27.218, 'lon': 56.378}]
        for base in ir_bases:
            for _ in range(3):
                ac = random.choice(IRANIAN_AIRCRAFT)
                self.aircraft.append(AircraftState(ac, 'IR', f"IR-{random.randint(1000,9999)}", base['lat'], base['lon']))
        
        # Venezuelan Aircraft
        vz_bases = [{'lat': 10.5, 'lon': -66.9}, {'lat': 10.2, 'lon': -67.9}]  # Caracas area
        for base in vz_bases:
            for _ in range(2):
                # Venezuela has Su-30 and F-16
                ac = random.choice(RUSSIAN_AIRCRAFT[:2])  # Su-35 or Su-57 stand-in
                self.aircraft.append(AircraftState(ac, 'VZ', f"VZ-{random.randint(1000,9999)}", base['lat'], base['lon']))
        
        # US Naval - Persian Gulf CSG
        self.naval.append(NavalState(US_NAVAL[0], 'US', 'CVN-72', 25.8, 54.5))
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-89', 25.2, 55.0))
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-91', 26.1, 54.0))
        self.naval.append(NavalState(US_NAVAL[3], 'US', 'CG-62', 25.5, 55.5))
        self.naval.append(NavalState(US_NAVAL[4], 'US', 'SSN-784', 24.0, 57.0))
        # Red Sea CSG
        self.naval.append(NavalState(US_NAVAL[1], 'US', 'CVN-78', 14.5, 42.5))
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-105', 14.0, 42.8))
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-108', 15.0, 42.2))
        # Med
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-112', 35.5, 18.0))
        self.naval.append(NavalState(US_NAVAL[3], 'US', 'CG-70', 34.8, 25.0))
        # Caribbean (Venezuela monitoring)
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-115', 12.0, -65.0))
        self.naval.append(NavalState(US_NAVAL[3], 'US', 'CG-72', 13.5, -67.0))
        # Black Sea approaches
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-119', 41.0, 29.0))
        
        # Russian Naval - Eastern Med
        self.naval.append(NavalState(RUSSIAN_NAVAL[0], 'RU', '063', 35.2, 34.5))
        self.naval.append(NavalState(RUSSIAN_NAVAL[1], 'RU', '121', 34.8, 35.2))
        self.naval.append(NavalState(RUSSIAN_NAVAL[2], 'RU', 'B-237', 33.5, 34.0))
        self.naval.append(NavalState(RUSSIAN_NAVAL[2], 'RU', 'B-261', 34.2, 33.5))
        # Russian Naval - Black Sea
        self.naval.append(NavalState(RUSSIAN_NAVAL[1], 'RU', '810', 44.6, 33.5))  # Sevastopol
        self.naval.append(NavalState(RUSSIAN_NAVAL[2], 'RU', 'B-871', 43.5, 34.0))
        
        # Chinese Naval - Indian Ocean
        self.naval.append(NavalState(CHINESE_NAVAL[0], 'CN', '17', 12.0, 52.0))
        self.naval.append(NavalState(CHINESE_NAVAL[1], 'CN', '101', 11.5, 51.5))
        self.naval.append(NavalState(CHINESE_NAVAL[2], 'CN', '332', 10.5, 53.0))
        
        # Iranian Naval - Strait of Hormuz
        self.naval.append(NavalState(IRANIAN_NAVAL[0], 'IR', 'F71', 26.8, 56.3))
        self.naval.append(NavalState(IRANIAN_NAVAL[1], 'IR', 'F77', 27.0, 56.0))
        self.naval.append(NavalState(IRANIAN_NAVAL[2], 'IR', 'IS901', 26.5, 56.5))
        self.naval.append(NavalState(IRANIAN_NAVAL[2], 'IR', 'IS902', 26.3, 57.0))
        
        # Venezuelan Naval - Caribbean
        self.naval.append(NavalState(IRANIAN_NAVAL[0], 'VZ', 'F21', 10.5, -66.8))  # Re-use frigate type
        self.naval.append(NavalState(IRANIAN_NAVAL[3], 'IR', 'P221', 27.2, 56.2))
        self.naval.append(NavalState(IRANIAN_NAVAL[3], 'IR', 'P225', 26.9, 56.8))
        
        # Ground units
        for unit_cfg in GROUND_UNITS:
            facility = next((f for f in US_FACILITIES if f['id'] == unit_cfg['base']), US_FACILITIES[0])
            self.ground_units.append(GroundUnitState(unit_cfg, facility))

    def connect(self):
        for i in range(30):
            try:
                self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
                self.producer.list_topics(timeout=5)
                print("✓ Kafka connected")
                return True
            except Exception as e:
                print(f"Attempt {i+1}: {e}")
                time.sleep(2)
        return False

    def _produce(self, topic, key, value):
        self.producer.produce(topic, key=key, value=json.dumps(value))
        self.producer.poll(0)

    def _air_loop(self):
        """Aircraft updates - also emit to GPS stream"""
        last = time.time()
        while self.running:
            dt = time.time() - last
            last = time.time()
            for ac in self.aircraft:
                ac_data = ac.update(dt)
                self._produce(AIRCRAFT_TOPIC, ac.tail, ac_data)
                # Also emit to GPS stream for unified tracking
                self._produce(GPS_TOPIC, f"AC-{ac.tail}", ac.get_gps_record())
            time.sleep(2)

    def _naval_loop(self):
        """Naval updates - also emit to GPS stream"""
        last = time.time()
        while self.running:
            dt = time.time() - last
            last = time.time()
            for ship in self.naval:
                ship_data = ship.update(dt)
                self._produce(NAVAL_TOPIC, ship.hull, ship_data)
                # Also emit to GPS stream for unified tracking
                self._produce(GPS_TOPIC, f"SHIP-{ship.hull}", ship.get_gps_record())
            time.sleep(3)

    def _ground_loop(self):
        """Ground unit updates - primary GPS emitter"""
        last = time.time()
        while self.running:
            dt = time.time() - last
            last = time.time()
            for unit in self.ground_units:
                gps_data = unit.update(dt)
                self._produce(GPS_TOPIC, unit.entity_id, gps_data)
            time.sleep(4)

    def _cyber_loop(self):
        """Cyber threats - tied to US facilities"""
        while self.running:
            time.sleep(random.uniform(15, 35))
            
            # Select random US facility as target
            facility = random.choice(US_FACILITIES)
            network = random.choice(facility['networks'])
            
            actor = random.choice(CYBER_ACTORS)
            attack = random.choice(CYBER_ATTACKS)
            
            threat = {
                'incident_id': f"CYBER-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'threat_actor': actor['name'],
                'actor_country': actor['country'],
                'sophistication': actor['sophistication'],
                'attack_type': attack['type'],
                'severity': attack['severity'],
                'description': attack['desc'],
                # Target is always a US facility
                'target_facility_id': facility['id'],
                'target_facility': facility['name'],
                'target_system': network,
                'target_type': facility['type'],
                'target_region': facility['region'],
                'target_country': facility['country'],
                'target_lat': facility['lat'],
                'target_lon': facility['lon'],
                # Attack details
                'source_ip': f"{random.choice([185,91,45,103,194])}.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}",
                'destination_ip': f"10.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}",
                'protocol': random.choice(['TCP', 'HTTPS', 'DNS', 'SSH', 'RDP']),
                'port': random.choice([22, 443, 53, 3389, 445, 8080]),
                'mitre_attack': random.choice(actor['ttps']),
                'status': random.choice(['DETECTED', 'BLOCKED', 'INVESTIGATING', 'CONTAINED']),
            }
            self._produce(CYBER_TOPIC, threat['incident_id'], threat)
            print(f"[CYBER] {threat['severity']}: {threat['attack_type']} → {facility['name']} ({network})")

    def _sigint_loop(self):
        """Traditional SIGINT emissions from hostile forces"""
        while self.running:
            time.sleep(random.uniform(180, 360))  # Every 3-6 minutes
            
            # Pick a hostile aircraft or ship
            hostiles = [a for a in self.aircraft if a.country != 'US'] + [s for s in self.naval if s.country != 'US']
            if not hostiles:
                continue
            
            emitter = random.choice(hostiles)
            signal_type = random.choice(['RADAR', 'COMMS', 'DATALINK', 'IFF', 'JAMMER'])
            threat_level = random.choices(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'], weights=[50, 35, 12, 3])[0]
            
            actions_by_type = {
                'RADAR': {
                    'CRITICAL': ['Immediate evasive maneuvers', 'Deploy EA-18G for SEAD', 'Request ARM strike package'],
                    'HIGH': ['Reroute aircraft around threat ring', 'Alert airborne assets', 'Update threat library'],
                    'MEDIUM': ['Mark emitter location', 'Monitor for mode changes', 'Brief strike planners'],
                    'LOW': ['Continue monitoring', 'Log in threat database', 'Update EW picture']
                },
                'JAMMER': {
                    'CRITICAL': ['Switch to backup frequency', 'Request JSIR geolocation', 'Coordinate kinetic strike'],
                    'HIGH': ['Activate anti-jam protocols', 'Request EMBM-J support', 'Notify SATCOM operators'],
                    'MEDIUM': ['Document interference pattern', 'Test alternate channels', 'Report to JEMSOC'],
                    'LOW': ['Monitor jamming extent', 'Log spectrum activity', 'Continue mission']
                },
                'COMMS': {
                    'CRITICAL': ['Priority SIGINT exploitation', 'Forward to NSA/CSS', 'Request ELINT tasking'],
                    'HIGH': ['Analyze communication protocol', 'Cross-reference with tracks', 'Share with intel fusion'],
                    'MEDIUM': ['Continue monitoring', 'Forward to SIGINT cell', 'Update order of battle'],
                    'LOW': ['Log intercept', 'Catalog in database', 'Routine reporting']
                },
                'DATALINK': {
                    'CRITICAL': ['Priority signal exploitation', 'Attempt protocol analysis', 'Share with cyber ops'],
                    'HIGH': ['Analyze datalink protocol', 'Cross-reference with platforms', 'NSA coordination'],
                    'MEDIUM': ['Monitor link activity', 'Document message patterns', 'Intel fusion update'],
                    'LOW': ['Catalog datalink signature', 'Update EW library', 'Routine monitoring']
                },
                'IFF': {
                    'CRITICAL': ['Verify all friendly IFF codes', 'Check for spoofing', 'Alert air defense'],
                    'HIGH': ['Cross-check with known tracks', 'Update IFF database', 'Notify AWACS'],
                    'MEDIUM': ['Log IFF signature', 'Compare to known modes', 'Update threat picture'],
                    'LOW': ['Catalog IFF mode', 'Routine logging', 'Continue monitoring']
                }
            }
            
            corrective_actions = actions_by_type.get(signal_type, {}).get(threat_level, ['Monitor and report'])
            
            sigint = {
                'intercept_id': f"SIG-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'signal_type': signal_type,
                'category': 'TRADITIONAL_SIGINT',
                'frequency_mhz': round(random.uniform(100, 18000), 2),
                'bandwidth_khz': round(random.uniform(10, 500), 1),
                'modulation': random.choice(['PULSE', 'CW', 'FM', 'SPREAD_SPECTRUM', 'FHSS']),
                'bearing_deg': round(random.uniform(0, 360), 1),
                'signal_strength_dbm': round(random.uniform(-90, -30), 1),
                'lat': round(emitter.lat, 6),
                'lon': round(emitter.lon, 6),
                'elevation_deg': round(random.uniform(-5, 45), 1),
                'confidence_pct': round(random.uniform(70, 99), 1),
                'emitter_classification': f"{emitter.country}-{getattr(emitter, 'type', 'UNKNOWN')}",
                'threat_level': threat_level,
                'emission_pattern': random.choice(['SEARCH', 'TRACK', 'ACQUISITION', 'FIRE_CONTROL']),
                'collector_id': random.choice(['RC-135V', 'EP-3E', 'GROUND-STATION']),
                'corrective_actions': corrective_actions,
            }
            self._produce(SIGINT_TOPIC, sigint['intercept_id'], sigint)

    def _cyber_ew_convergence_loop(self):
        """Cyber-EW Convergence threats - RF-enabled cyber and side-channel attacks"""
        while self.running:
            time.sleep(random.uniform(240, 480))  # Every 4-8 minutes
            
            # Select a US facility as target
            facility = random.choice(US_FACILITIES)
            
            # Cyber-EW convergence event types
            event_types = [
                {
                    'signal_type': 'RF_CYBER_INJECTION',
                    'category': 'CYBER_EW_CONVERGENCE',
                    'description': 'RF-enabled malicious code injection via wireless network',
                    'attack_vector': 'Radio frequency signal carrying malicious payload',
                    'target_system': random.choice(['Tactical WiFi', 'SATCOM Terminal', 'Radio Gateway', 'Mesh Network Node']),
                    'bypass_method': 'Physical firewall bypass via RF injection',
                    'indicators': ['Anomalous RF patterns', 'Unexpected protocol headers', 'Malformed packets on air interface'],
                    'corrective_actions': [
                        'Isolate affected RF segment immediately',
                        'Enable RF shielding protocols',
                        'Scan for rogue transmitters',
                        'Coordinate with Cyber Protection Team',
                        'Report to JFHQ-DODIN'
                    ]
                },
                {
                    'signal_type': 'SIDE_CHANNEL_ATTACK',
                    'category': 'CYBER_EW_CONVERGENCE',
                    'description': 'Electromagnetic emanations exploitation (TEMPEST threat)',
                    'attack_vector': 'Passive collection of EM leakage from hardware',
                    'target_system': random.choice(['Crypto Device', 'KG-175 TACLANE', 'Server Rack', 'KIV-7M', 'HAIPE Device']),
                    'bypass_method': 'Non-invasive key extraction via EM analysis',
                    'indicators': ['Unusual TSCM readings', 'Unshielded emanations detected', 'Proximity sensor alerts'],
                    'corrective_actions': [
                        'Conduct immediate TSCM sweep',
                        'Verify TEMPEST shielding integrity',
                        'Rotate cryptographic keys',
                        'Increase physical security perimeter',
                        'Report to NSA/CSS TEMPEST office'
                    ]
                }
            ]
            
            event = random.choice(event_types)
            threat_level = random.choices(['MEDIUM', 'HIGH', 'CRITICAL'], weights=[30, 50, 20])[0]
            
            sigint = {
                'intercept_id': f"CEW-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'signal_type': event['signal_type'],
                'category': event['category'],
                'description': event['description'],
                'attack_vector': event['attack_vector'],
                'target_system': event['target_system'],
                'target_facility': facility['name'],
                'target_facility_id': facility['id'],
                'bypass_method': event['bypass_method'],
                'indicators': event['indicators'],
                'frequency_mhz': round(random.uniform(2400, 5800), 2),  # WiFi/tactical bands
                'signal_strength_dbm': round(random.uniform(-70, -40), 1),
                'lat': facility['lat'],
                'lon': facility['lon'],
                'threat_level': threat_level,
                'attribution': random.choice(['RU-GRU', 'CN-PLA-SSF', 'IR-IRGC-EC', 'Unknown APT']),
                'collector_id': random.choice(['TSCM-TEAM', 'CYBER-SENTINEL', 'RF-HUNTER']),
                'confidence_pct': round(random.uniform(65, 95), 1),
                'corrective_actions': event['corrective_actions'],
            }
            self._produce(SIGINT_TOPIC, sigint['intercept_id'], sigint)
            print(f"[CYBER-EW] {threat_level}: {event['signal_type']} targeting {facility['name']}")

    def _spectrum_management_loop(self):
        """Spectrum Management & Interference - DSO/DISA spectrum issues"""
        while self.running:
            time.sleep(random.uniform(300, 600))  # Every 5-10 minutes
            
            # Spectrum management event types
            event_types = [
                {
                    'signal_type': 'SPECTRUM_FRATRICIDE',
                    'category': 'SPECTRUM_MANAGEMENT',
                    'description': 'Friendly force electromagnetic interference',
                    'source_system': random.choice(['AN/APG-81 Radar', 'JTRS Radio', 'Link-16 Terminal', 'AESA Array', 'SATCOM Uplink']),
                    'affected_system': random.choice(['HF Comms Net', 'UHF SATCOM', 'Tactical Data Link', 'IFF Interrogator', 'GPS Receiver']),
                    'interference_type': 'Unintentional - Frequency Overlap',
                    'band_affected': random.choice(['L-Band', 'S-Band', 'C-Band', 'X-Band', 'Ku-Band']),
                    'corrective_actions': [
                        'Coordinate frequency deconfliction via JCEOI',
                        'Adjust source system operating frequency',
                        'Notify DSO Spectrum Manager',
                        'Update Joint Spectrum Interference Resolution (JSIR)',
                        'Implement time-sharing protocol'
                    ]
                },
                {
                    'signal_type': 'COMMERCIAL_ENCROACHMENT',
                    'category': 'SPECTRUM_MANAGEMENT',
                    'description': 'Commercial 5G/LTE interference with military bands',
                    'source_system': random.choice(['5G NR Tower', 'LTE Macro Cell', 'C-Band 5G', 'CBRS System']),
                    'affected_system': random.choice(['Radar Altimeter', 'Precision Approach Radar', 'Weather Radar', 'TACAN', 'ILS Glideslope']),
                    'interference_type': 'Commercial signal bleed into protected band',
                    'band_affected': random.choice(['3.7-3.98 GHz (C-Band)', '4.2-4.4 GHz (Altimeter)', 'AWS-3 Band']),
                    'corrective_actions': [
                        'Document interference for FCC coordination',
                        'Implement geographic exclusion zone',
                        'Activate interference mitigation filters',
                        'Coordinate with NTIA on spectrum sharing',
                        'Report to DSO Spectrum Operations'
                    ]
                },
                {
                    'signal_type': 'DYNAMIC_SPECTRUM_EXPLOIT',
                    'category': 'SPECTRUM_MANAGEMENT',
                    'description': 'Adversary frequency-hopping/agile radio detected',
                    'source_system': random.choice(['Near-peer agile radio', 'Cognitive EW system', 'Adaptive jammer', 'FHSS tactical net']),
                    'affected_system': 'Multiple - Unpredictable interference pattern',
                    'interference_type': 'Intentional - Adaptive frequency exploitation',
                    'band_affected': random.choice(['VHF Tactical', 'UHF Tactical', 'L-Band', 'S-Band']),
                    'hop_rate': f"{random.randint(100, 1000)} hops/sec",
                    'corrective_actions': [
                        'Activate Cognitive EW countermeasures',
                        'Enable adaptive frequency management',
                        'Deploy spectrum sensing assets',
                        'Coordinate with EC-130H Compass Call',
                        'Request JEMSOC support for pattern analysis'
                    ]
                }
            ]
            
            event = random.choice(event_types)
            # Fratricide is usually MEDIUM, others can be higher
            if event['signal_type'] == 'SPECTRUM_FRATRICIDE':
                threat_level = random.choices(['LOW', 'MEDIUM', 'HIGH'], weights=[30, 50, 20])[0]
            else:
                threat_level = random.choices(['MEDIUM', 'HIGH', 'CRITICAL'], weights=[40, 40, 20])[0]
            
            # Pick a location (facility for fratricide, random for others)
            if event['signal_type'] == 'SPECTRUM_FRATRICIDE':
                facility = random.choice(US_FACILITIES)
                lat, lon = facility['lat'], facility['lon']
                location_name = facility['name']
            else:
                # Random location in theater
                lat = round(random.uniform(20, 40), 6)
                lon = round(random.uniform(30, 60), 6)
                location_name = f"Grid {int(lat)}{int(lon)}"
            
            sigint = {
                'intercept_id': f"SPM-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'signal_type': event['signal_type'],
                'category': event['category'],
                'description': event['description'],
                'source_system': event['source_system'],
                'affected_system': event['affected_system'],
                'interference_type': event['interference_type'],
                'band_affected': event['band_affected'],
                'hop_rate': event.get('hop_rate'),
                'location_name': location_name,
                'frequency_mhz': round(random.uniform(225, 5800), 2),
                'signal_strength_dbm': round(random.uniform(-60, -30), 1),
                'lat': lat,
                'lon': lon,
                'threat_level': threat_level,
                'collector_id': random.choice(['DSO-MONITOR', 'JSIR-SYSTEM', 'SPECTRUM-ANALYZER']),
                'confidence_pct': round(random.uniform(75, 98), 1),
                'corrective_actions': event['corrective_actions'],
            }
            self._produce(SIGINT_TOPIC, sigint['intercept_id'], sigint)
            print(f"[SPECTRUM] {threat_level}: {event['signal_type']} - {event['affected_system']}")

    def _navwar_pnt_loop(self):
        """Navigation Warfare (NAVWAR) / PNT Threats - GPS jamming and spoofing"""
        while self.running:
            time.sleep(random.uniform(360, 720))  # Every 6-12 minutes
            
            # Known GPS threat zones
            threat_zones = [
                {'name': 'Eastern Mediterranean', 'lat': 35.0, 'lon': 34.0, 'source': 'RU-Syria'},
                {'name': 'Black Sea', 'lat': 44.0, 'lon': 34.0, 'source': 'RU-Crimea'},
                {'name': 'Baltic Approaches', 'lat': 55.0, 'lon': 20.0, 'source': 'RU-Kaliningrad'},
                {'name': 'Persian Gulf', 'lat': 27.0, 'lon': 52.0, 'source': 'IR-IRGC'},
                {'name': 'South China Sea', 'lat': 16.0, 'lon': 112.0, 'source': 'CN-PLA'},
                {'name': 'Ukraine Theater', 'lat': 48.5, 'lon': 37.5, 'source': 'RU-EW-Unit'},
            ]
            
            zone = random.choice(threat_zones)
            
            # NAVWAR event types
            event_types = [
                {
                    'signal_type': 'GNSS_JAMMING',
                    'category': 'NAVWAR_PNT',
                    'description': 'Broad-spectrum GNSS jamming detected',
                    'attack_method': 'High-power noise jamming across L1/L2 bands',
                    'gnss_affected': random.choice(['GPS L1/L2', 'GPS + GLONASS', 'GPS + Galileo', 'All GNSS']),
                    'jamming_radius_km': random.randint(20, 150),
                    'jammer_type': random.choice(['R-330Zh Zhitel', 'Pole-21', 'RP-377', 'Unknown Ground-Based']),
                    'affected_operations': random.choice(['Drone ISR', 'Precision Strike', 'Logistics Convoy', 'Aircraft Navigation', 'Ship Navigation']),
                    'corrective_actions': [
                        'Switch to inertial navigation backup',
                        'Activate M-Code GPS receivers',
                        'Enable anti-jam antenna systems',
                        'Coordinate alternate PNT source',
                        'Report to NAVWAR Fusion Center'
                    ]
                },
                {
                    'signal_type': 'GNSS_SPOOFING',
                    'category': 'NAVWAR_PNT',
                    'description': 'GPS spoofing attack - false position/timing',
                    'attack_method': 'Counterfeit GPS signal broadcast',
                    'gnss_affected': 'GPS L1 C/A (civil signal)',
                    'spoofing_type': random.choice(['Position Offset', 'Time Manipulation', 'Trajectory Deviation']),
                    'position_error_m': random.randint(100, 5000),
                    'time_error_ns': random.randint(100, 10000),
                    'spoofer_type': random.choice(['Vehicle-mounted', 'Fixed Site', 'Drone-deployed', 'Ship-based']),
                    'affected_operations': random.choice(['Crypto Timing Sync', 'Network Time Protocol', 'Precision Munitions', 'UAV Navigation', 'Tactical Data Links']),
                    'crypto_impact': random.choice(['Key rollover failure risk', 'HAIPE timing drift', 'Link encryption desync', 'None detected']),
                    'corrective_actions': [
                        'Enable GPS spoofing detection algorithms',
                        'Cross-check with alternative PNT sources',
                        'Verify timing against WWVB/Cesium backup',
                        'Alert crypto custodians of timing anomaly',
                        'Implement chipscale atomic clock backup',
                        'Report to USCYBERCOM for attribution'
                    ]
                }
            ]
            
            event = random.choice(event_types)
            # Spoofing is generally more dangerous than jamming
            if event['signal_type'] == 'GNSS_SPOOFING':
                threat_level = random.choices(['HIGH', 'CRITICAL'], weights=[40, 60])[0]
            else:
                threat_level = random.choices(['MEDIUM', 'HIGH', 'CRITICAL'], weights=[30, 50, 20])[0]
            
            # Add some position variance around the threat zone
            lat = round(zone['lat'] + random.uniform(-2, 2), 6)
            lon = round(zone['lon'] + random.uniform(-2, 2), 6)
            
            sigint = {
                'intercept_id': f"NAV-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'signal_type': event['signal_type'],
                'category': event['category'],
                'description': event['description'],
                'attack_method': event['attack_method'],
                'gnss_affected': event['gnss_affected'],
                'threat_zone': zone['name'],
                'attribution': zone['source'],
                'affected_operations': event['affected_operations'],
                'frequency_mhz': round(random.choice([1575.42, 1227.60, 1176.45]), 2),  # GPS frequencies
                'signal_strength_dbm': round(random.uniform(-50, -20), 1),
                'lat': lat,
                'lon': lon,
                'threat_level': threat_level,
                'collector_id': random.choice(['GPS-MONITOR', 'PNT-SENTINEL', 'NAVWAR-SENSOR']),
                'confidence_pct': round(random.uniform(80, 99), 1),
                'corrective_actions': event['corrective_actions'],
            }
            
            # Add event-specific fields
            if event['signal_type'] == 'GNSS_JAMMING':
                sigint['jamming_radius_km'] = event['jamming_radius_km']
                sigint['jammer_type'] = event['jammer_type']
            else:
                sigint['spoofing_type'] = event['spoofing_type']
                sigint['position_error_m'] = event['position_error_m']
                sigint['time_error_ns'] = event['time_error_ns']
                sigint['crypto_impact'] = event['crypto_impact']
                sigint['spoofer_type'] = event['spoofer_type']
            
            self._produce(SIGINT_TOPIC, sigint['intercept_id'], sigint)
            print(f"[NAVWAR] {threat_level}: {event['signal_type']} in {zone['name']} - {event['affected_operations']}")

    def _satellite_loop(self):
        """Satellite imagery metadata"""
        while self.running:
            time.sleep(random.uniform(5, 15))
            
            sat = {
                'image_id': f"SAT-{uuid.uuid4().hex[:8].upper()}",
                'source_id': random.choice(['WV-3', 'GE-1', 'PL-1', 'ICEYE-X1']),
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'acquisition_time': datetime.utcnow().isoformat(),
                'sensor_type': random.choice(['EO', 'IR', 'SAR', 'MSI']),
                'resolution_m': round(random.uniform(0.3, 5.0), 2),
                'cloud_cover_pct': round(random.uniform(0, 30), 1),
                'sun_elevation_deg': round(random.uniform(20, 70), 1),
                'off_nadir_angle_deg': round(random.uniform(0, 30), 1),
                'center_lat': round(random.uniform(10, 40), 6),
                'center_lon': round(random.uniform(25, 65), 6),
                'classification': random.choice(['UNCLASSIFIED', 'SECRET', 'TOP_SECRET']),
                'quality_score': round(random.uniform(70, 99), 1),
                'processing_status': 'COMPLETE',
            }
            self._produce(SATELLITE_TOPIC, sat['image_id'], sat)

    def _jsir_loop(self):
        """JSIR - Joint Spectrum Interference Resolution events
        Simulates SATCOM jamming incidents and DISA JSIR workflow
        """
        while self.running:
            time.sleep(random.uniform(45, 120))  # Jamming incidents every 45-120 seconds
            
            # Select jammer, target satellite, and affected units
            jammer_site = random.choice(JAMMER_SITES)
            jammer_system = random.choice([j for j in HOSTILE_JAMMERS if j['country'] == jammer_site['country']])
            target_sat = random.choice(US_SATCOM_SYSTEMS)
            affected = [u for u in AFFECTED_UNITS if target_sat['name'] in u['satcom_deps']]
            
            if not affected:
                affected = [random.choice(AFFECTED_UNITS)]
            
            # Generate TDOA/FDOA geolocation data
            # Uncertainty ellipse based on jammer power and satellite geometry
            cep_km = round(random.uniform(0.5, 5.0) * (10 / jammer_system['power_kw']), 2)
            ellipse_major_km = round(cep_km * random.uniform(1.5, 3.0), 2)
            ellipse_minor_km = round(cep_km * random.uniform(0.8, 1.2), 2)
            ellipse_orientation = round(random.uniform(0, 180), 1)
            
            # Adjacent satellite used for TDOA
            adjacent_sat = random.choice([s for s in US_SATCOM_SYSTEMS if s['name'] != target_sat['name']])
            tdoa_ms = round(random.uniform(0.1, 5.0), 3)
            fdoa_hz = round(random.uniform(-50, 50), 2)
            
            incident_id = f"JSIR-{uuid.uuid4().hex[:8].upper()}"
            
            jsir_event = {
                'incident_id': incident_id,
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'event_type': 'SATCOM_INTERFERENCE',
                'workflow_stage': random.choice(['DETECTION', 'CHARACTERIZATION', 'GEOLOCATION', 'MITIGATION']),
                
                # Jammer information
                'jammer_id': jammer_site['id'],
                'jammer_location': jammer_site['name'],
                'jammer_country': jammer_site['country'],
                'jammer_lat': jammer_site['lat'] + random.uniform(-0.01, 0.01),  # Slight position variation
                'jammer_lon': jammer_site['lon'] + random.uniform(-0.01, 0.01),
                'jammer_terrain': jammer_site['terrain'],
                'jammer_system': jammer_system['system'],
                'jammer_type': jammer_system['type'],
                'jammer_power_kw': jammer_system['power_kw'],
                'jammer_bands': jammer_system['bands'],
                
                # Target satellite
                'target_satellite': target_sat['name'],
                'target_sat_type': target_sat['type'],
                'target_freq_ghz': target_sat['freq_ghz'],
                'target_lon_slot': target_sat['lon_slot'],
                
                # Interference characteristics
                'interference_type': random.choice(['BROADBAND_NOISE', 'SWEPT_CW', 'PULSED', 'SPOT_JAMMING']),
                'noise_floor_increase_db': round(random.uniform(10, 40), 1),
                'signal_to_noise_degradation_db': round(random.uniform(5, 25), 1),
                'affected_bandwidth_mhz': round(random.uniform(5, 50), 1),
                'spectral_signature': random.choice(['HAYSTACK', 'SWEPT', 'MULTI_TONE', 'BARRAGE']),
                
                # TDOA/FDOA geolocation
                'geolocation_method': 'TDOA_FDOA',
                'primary_satellite': target_sat['name'],
                'adjacent_satellite': adjacent_sat['name'],
                'tdoa_milliseconds': tdoa_ms,
                'fdoa_hertz': fdoa_hz,
                'cep_km': cep_km,
                'uncertainty_ellipse_major_km': ellipse_major_km,
                'uncertainty_ellipse_minor_km': ellipse_minor_km,
                'uncertainty_ellipse_orientation_deg': ellipse_orientation,
                'geolocation_confidence_pct': round(random.uniform(75, 98), 1),
                
                # Affected units
                'affected_units': [u['callsign'] for u in affected],
                'affected_unit_types': [u['type'] for u in affected],
                'affected_region': affected[0]['region'],
                'mission_impact': random.choice(['DEGRADED_COMMS', 'TOTAL_BLACKOUT', 'INTERMITTENT', 'BACKUP_ACTIVATED']),
                'mission_impact_severity': random.choice(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']),
                
                # JSIR workflow status
                'jsir_ticket_status': random.choice(['SUBMITTED', 'ANALYZING', 'GEOLOCATING', 'MITIGATING', 'RESOLVED']),
                'friendly_fire_ruled_out': True,
                'recommended_action': random.choice([
                    'FREQUENCY_HOP',
                    'BEAM_REALLOCATION', 
                    'ALTERNATE_SATELLITE',
                    'INCREASE_EIRP',
                    'KINETIC_STRIKE_COORDS',
                    'ELECTRONIC_ATTACK'
                ]),
                
                # Response options
                'mitigation_option_a': 'BEAM_REALLOCATION',
                'mitigation_option_b': 'ALTERNATE_SATELLITE',
                'strike_coords_available': True,
                'strike_coords_lat': jammer_site['lat'],
                'strike_coords_lon': jammer_site['lon'],
            }
            
            self._produce(SPECTRUM_TOPIC, incident_id, jsir_event)
            print(f"[JSIR] {jsir_event['mission_impact_severity']}: {jammer_system['system']} jamming {target_sat['name']} - Affects {', '.join([u['callsign'] for u in affected])}")
            
            # Also generate EMBM track for the jammer
            self._generate_embm_track(jsir_event)
    
    def _generate_embm_track(self, jsir_event):
        """Generate EMBM-J track from JSIR geolocation"""
        embm_track = {
            'track_id': f"EMBM-{jsir_event['jammer_id']}-{uuid.uuid4().hex[:4].upper()}",
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'source_incident': jsir_event['incident_id'],
            
            # Track classification
            'track_type': 'HOSTILE_EMITTER',
            'track_category': 'EW_JAMMER',
            'emitter_function': 'SATCOM_JAMMER',
            
            # Position from geolocation
            'lat': jsir_event['jammer_lat'],
            'lon': jsir_event['jammer_lon'],
            'position_accuracy_m': jsir_event['cep_km'] * 1000,
            'terrain': jsir_event['jammer_terrain'],
            
            # Emitter characteristics
            'emitter_system': jsir_event['jammer_system'],
            'emitter_country': jsir_event['jammer_country'],
            'emitter_type': jsir_event['jammer_type'],
            'estimated_power_kw': jsir_event['jammer_power_kw'],
            'operating_bands': jsir_event['jammer_bands'],
            'spectral_signature': jsir_event['spectral_signature'],
            
            # Threat assessment
            'threat_level': jsir_event['mission_impact_severity'],
            'confidence': jsir_event['geolocation_confidence_pct'],
            'first_detected': datetime.utcnow().isoformat(),
            'last_updated': datetime.utcnow().isoformat(),
            
            # Targeting data
            'targetable': True,
            'target_coord_lat': jsir_event['strike_coords_lat'],
            'target_coord_lon': jsir_event['strike_coords_lon'],
            'target_coord_accuracy_m': jsir_event['cep_km'] * 1000,
            
            # Correlation with other INT
            'correlated_sigint': True,
            'correlated_imagery': random.choice([True, False]),
            'nearby_assets': random.choice([
                'MOBILE_RADAR',
                'SAM_BATTERY', 
                'COMMAND_POST',
                'LOGISTICS_HUB',
                'ISOLATED'
            ]),
            
            # Display properties for EMBM-J
            'display_icon': 'JAMMER',
            'display_color': 'RED',
            'display_priority': 'HIGH' if jsir_event['mission_impact_severity'] in ['HIGH', 'CRITICAL'] else 'MEDIUM',
        }
        
        self._produce(EMBM_TOPIC, embm_track['track_id'], embm_track)

    def _facility_gps_loop(self):
        """Emit GPS records for all US facilities"""
        while self.running:
            for fac in US_FACILITIES:
                gps_record = {
                    'unit_id': fac['id'],
                    'unit_name': fac['name'],
                    'unit_type': fac['type'],
                    'timestamp': int(datetime.utcnow().timestamp() * 1000),
                    'lat': fac['lat'],
                    'lon': fac['lon'],
                    'heading_deg': 0,
                    'speed_kph': 0,
                    'altitude_m': 0,
                    'fuel_level_pct': 100.0,
                    'ammo_level_pct': 100.0,
                    'personnel_count': random.randint(1000, 10000),
                    'equipment_status': 'FULLY_OPERATIONAL',
                    'mission_status': 'OPERATIONAL',
                    'fuel_consumption_rate': 0,
                    'comms_status': 'NOMINAL',
                }
                self._produce(GPS_TOPIC, fac['id'], gps_record)
            time.sleep(10)

    def _stats_loop(self):
        while self.running:
            time.sleep(30)
            print(f"[STATS] Aircraft: {len(self.aircraft)} | Ships: {len(self.naval)} | Ground: {len(self.ground_units)} | Facilities: {len(US_FACILITIES)}")
            self.producer.flush()

    def start(self):
        if not self.connect():
            return False
        self.running = True
        threads = [
            self._air_loop, self._naval_loop, self._ground_loop,
            self._cyber_loop, self._sigint_loop, self._satellite_loop,
            self._facility_gps_loop, self._jsir_loop, self._stats_loop,
            # New DISA-specific EW/Spectrum/NAVWAR threads
            self._cyber_ew_convergence_loop,
            self._spectrum_management_loop,
            self._navwar_pnt_loop,
        ]
        for fn in threads:
            threading.Thread(target=fn, daemon=True).start()
        return True

    def stop(self):
        self.running = False
        if self.producer:
            self.producer.flush()


def main():
    print("\n" + "="*60)
    print("JADC2 COP - Multi-Domain Generator")
    print("All entities emit GPS coordinates for unified tracking")
    print("Cyber events tied to US Military facilities")
    print("="*60 + "\n")
    gen = Generator()
    if not gen.start():
        return 1
    print(f"✓ Running - {len(gen.aircraft)} aircraft, {len(gen.naval)} ships, {len(gen.ground_units)} ground units\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        gen.stop()
    return 0

if __name__ == '__main__':
    exit(main())
