#!/usr/bin/env python3
"""JADC2 Mission Readiness Service - Atlas cloud, Kafka real-time, action endpoints"""

import os, json, threading, time
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from confluent_kafka import Consumer, Producer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ATLAS_URI = os.environ.get('ATLAS_URI', 'mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/?appName=workshop')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

app = Flask(__name__)
CORS(app)

db = None  # Atlas only
aircraft_cache, naval_cache, cyber_cache, gps_cache, sigint_cache = {}, {}, {}, {}, {}
action_log = []
CYBER_RETENTION_SEC = 600

US_FACILITIES = [
    {'id': 'FAC-001', 'name': 'Al Udeid AB', 'lat': 25.117, 'lon': 51.315, 'type': 'AIR_BASE', 'region': 'CENTCOM'},
    {'id': 'FAC-002', 'name': 'Al Dhafra AB', 'lat': 24.248, 'lon': 54.547, 'type': 'AIR_BASE', 'region': 'CENTCOM'},
    {'id': 'FAC-003', 'name': 'Camp Arifjan', 'lat': 28.933, 'lon': 48.100, 'type': 'ARMY_BASE', 'region': 'CENTCOM'},
    {'id': 'FAC-004', 'name': 'NSA Bahrain', 'lat': 26.237, 'lon': 50.652, 'type': 'NAVAL_BASE', 'region': 'CENTCOM'},
    {'id': 'FAC-005', 'name': 'Diego Garcia', 'lat': -7.313, 'lon': 72.411, 'type': 'NAVAL_BASE', 'region': 'INDOPACOM'},
    {'id': 'FAC-006', 'name': 'Incirlik AB', 'lat': 37.002, 'lon': 35.425, 'type': 'AIR_BASE', 'region': 'EUCOM'},
    {'id': 'FAC-007', 'name': 'Camp Lemonnier', 'lat': 11.547, 'lon': 43.145, 'type': 'ARMY_BASE', 'region': 'AFRICOM'},
]

def init_atlas():
    global db
    for i in range(30):
        try:
            client = MongoClient(ATLAS_URI, serverSelectionTimeoutMS=10000)
            client.admin.command('ping')
            db = client.jadc2_cop
            colls = db.list_collection_names()
            logger.info(f"✓ Atlas connected — {len(colls)} collections")
            return True
        except Exception as e:
            logger.warning(f"Atlas attempt {i+1}: {e}")
            time.sleep(3)
    logger.error("✗ Could not connect to Atlas after 30 attempts")
    return False

def kafka_consumer_thread():
    time.sleep(10)
    try:
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'mission-readiness',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(['ground-force-gps', 'military-aircraft', 'naval-vessels', 'cyber-threats', 'sigint-feeds', 'spectrum-interference', 'embm-tracks'])
        logger.info("✓ Kafka consumer started")
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                topic = msg.topic()
                if topic == 'ground-force-gps':
                    uid = data.get('unit_id')
                    if uid: gps_cache[uid] = {**data, 'last_update': time.time()}
                elif topic == 'military-aircraft':
                    aid = data.get('aircraft_id')
                    if aid: aircraft_cache[aid] = {**data, 'last_update': time.time()}
                elif topic == 'naval-vessels':
                    vid = data.get('vessel_id')
                    if vid: naval_cache[vid] = {**data, 'last_update': time.time()}
                elif topic == 'cyber-threats':
                    cid = data.get('incident_id') or data.get('source_ip', 'unknown') + '_' + str(data.get('timestamp', time.time()))
                    cyber_cache[cid] = {**data, 'last_update': time.time()}
                elif topic == 'sigint-feeds':
                    sid = data.get('intercept_id') or data.get('collector_id', 'unknown') + '_' + str(data.get('frequency_mhz', ''))
                    sigint_cache[sid] = {**data, 'last_update': time.time()}
            except Exception as e:
                logger.debug(f"Message parse error: {e}")
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")

def clean_docs(docs):
    """Remove MongoDB _id, convert datetimes, normalize UPPER keys to lower"""
    import hashlib
    result = []
    for d in docs:
        d.pop('_id', None)
        d.pop('last_update', None)
        # Normalize uppercase keys from ksqlDB/Atlas to lowercase
        lowered = {}
        for k, v in d.items():
            lk = k.lower()
            if isinstance(v, datetime):
                lowered[lk] = v.isoformat()
            elif hasattr(v, '__int__'):
                # Handle Int64, Decimal128 etc from pymongo
                try:
                    lowered[lk] = int(v) if float(v) == int(float(v)) else float(v)
                except:
                    lowered[lk] = str(v)
            else:
                lowered[lk] = v
        # Generate IDs if missing (ksqlDB sinks don't always include them)
        if 'incident_id' not in lowered and 'attack_type' in lowered:
            lowered['incident_id'] = 'CY-' + hashlib.md5((str(lowered.get('timestamp',''))+str(lowered.get('source_ip',''))).encode()).hexdigest()[:8]
        if 'intercept_id' not in lowered and 'signal_type' in lowered:
            lowered['intercept_id'] = 'SIG-' + hashlib.md5((str(lowered.get('timestamp',''))+str(lowered.get('frequency_mhz',''))).encode()).hexdigest()[:8]
        result.append(lowered)
    return result

# ============================================================
# CORE API ENDPOINTS
# ============================================================

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'atlas': db is not None})

@app.route('/api/facilities')
def get_facilities():
    return jsonify({'facilities': US_FACILITIES})

@app.route('/api/readiness')
def get_readiness():
    try:
        friendly = [v for v in gps_cache.values() if v.get('unit_id', '').startswith(('US-', 'UK-'))]
        total = len(friendly) or 1
        fuel_avg = sum(v.get('fuel_level_pct', 50) for v in friendly) / total
        ammo_avg = sum(v.get('ammo_level_pct', 50) for v in friendly) / total
        comms_up = sum(1 for v in friendly if v.get('comms_status') == 'NOMINAL') / total * 100
        equip_ok = sum(1 for v in friendly if v.get('equipment_status') == 'FULLY_OPERATIONAL') / total * 100
        overall = (fuel_avg * 0.3 + ammo_avg * 0.3 + comms_up * 0.2 + equip_ok * 0.2)
        return jsonify({
            'overall': round(overall, 1), 'fuel_avg': round(fuel_avg, 1),
            'ammo_avg': round(ammo_avg, 1), 'comms_up_pct': round(comms_up, 1),
            'equip_ok_pct': round(equip_ok, 1), 'friendly_count': len(friendly),
            'total_tracked': len(gps_cache)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/aircraft')
def get_aircraft():
    try:
        now = time.time()
        planes = [v for v in aircraft_cache.values() if now - v.get('last_update', 0) < 30]
        if not planes:
            planes = list(db.aircraft_enriched.find({}, {'_id': 0}).sort('timestamp', -1).limit(50))
        return jsonify({'aircraft': clean_docs(planes)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/naval')
def get_naval():
    try:
        now = time.time()
        ships = [v for v in naval_cache.values() if now - v.get('last_update', 0) < 30]
        if not ships:
            ships = list(db.naval_enriched.find({}, {'_id': 0}).sort('timestamp', -1).limit(50))
        return jsonify({'vessels': clean_docs(ships)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/cyber')
def get_cyber():
    try:
        now = time.time()
        # Always get from Atlas (enriched data)
        atlas_threats = list(db.cyber_enriched.find({}, {'_id': 0}).sort('timestamp', -1).limit(20))
        # Also get any recent Kafka cache items not yet in Atlas
        cache_threats = [v for v in cyber_cache.values() if now - v.get('last_update', 0) < CYBER_RETENTION_SEC]
        # Merge: Atlas first, then any cache items with unique IDs
        seen_ids = set()
        merged = []
        for t in atlas_threats:
            tid = str(t.get('INCIDENT_ID', '') or '') + str(t.get('incident_id', '') or '') + str(t.get('SOURCE_IP', '') or '') + str(t.get('TIMESTAMP', '') or '')
            if tid not in seen_ids:
                seen_ids.add(tid)
                merged.append(t)
        for t in cache_threats:
            tid = str(t.get('incident_id', '') or '') + str(t.get('source_ip', '') or '') + str(t.get('timestamp', '') or '')
            if tid not in seen_ids:
                seen_ids.add(tid)
                merged.append(t)
        return jsonify({'threats': clean_docs(merged)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/gps')
def get_gps():
    try:
        now = time.time()
        entities = [v for v in gps_cache.values() if now - v.get('last_update', 0) < 60]
        return jsonify({'entities': clean_docs(entities), 'count': len(entities)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sigint')
def get_sigint():
    try:
        now = time.time()
        # Always get from Atlas (enriched data)
        atlas_sigint = list(db.sigint_enriched.find({}, {'_id': 0}).sort('timestamp', -1).limit(30))
        # Also get any recent Kafka cache items
        cache_sigint = [v for v in sigint_cache.values() if now - v.get('last_update', 0) < 120]
        # Merge: Atlas first, then unique cache items
        seen_ids = set()
        merged = []
        for s in atlas_sigint:
            sid = str(s.get('INTERCEPT_ID', '') or '') + str(s.get('intercept_id', '') or '') + str(s.get('FREQUENCY_MHZ', '') or '') + str(s.get('TIMESTAMP', '') or '')
            if sid not in seen_ids:
                seen_ids.add(sid)
                merged.append(s)
        for s in cache_sigint:
            sid = str(s.get('intercept_id', '') or '') + str(s.get('frequency_mhz', '') or '') + str(s.get('timestamp', '') or '')
            if sid not in seen_ids:
                seen_ids.add(sid)
                merged.append(s)
        return jsonify({'intercepts': clean_docs(merged)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/alerts')
def get_alerts():
    try:
        alerts = list(db.operational_alerts.find({}, {'_id': 0}).sort('timestamp', -1).limit(25))
        return jsonify({'alerts': clean_docs(alerts)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# ENRICHED DATA ENDPOINTS (Atlas)
# ============================================================

@app.route('/api/cyber/enriched')
def get_cyber_enriched():
    try:
        return jsonify({'threats': clean_docs(list(db.cyber_enriched.find({}, {'_id': 0}).sort('timestamp', -1).limit(25)))})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sigint/air-defense')
def get_sigint_air_defense():
    try:
        return jsonify({'detections': clean_docs(list(db.sigint_air_defense.find({}, {'_id': 0}).sort('timestamp', -1).limit(20)))})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sigint/jamming')
def get_sigint_jamming():
    try:
        return jsonify({'detections': clean_docs(list(db.sigint_jamming.find({}, {'_id': 0}).sort('timestamp', -1).limit(20)))})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sigint/hostile-comms')
def get_sigint_hostile_comms():
    try:
        return jsonify({'intercepts': clean_docs(list(db.sigint_hostile_comms.find({}, {'_id': 0}).sort('timestamp', -1).limit(20)))})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/embm/strike-candidates')
def get_embm_strikes():
    try:
        return jsonify({'targets': clean_docs(list(db.embm_strike_candidates.find({}, {'_id': 0}).sort('timestamp', -1).limit(20)))})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/embm/active-jammers')
def get_embm_jammers():
    try:
        return jsonify({'jammers': clean_docs(list(db.embm_active_jammers.find({}, {'_id': 0}).sort('timestamp', -1).limit(20)))})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/satellite/passes')
def get_satellite_passes():
    try:
        return jsonify({'passes': clean_docs(list(db.satellite_pass_enriched.find({}, {'_id': 0}).sort('timestamp', -1).limit(20)))})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/jsir')
def get_jsir():
    try:
        return jsonify({'incidents': clean_docs(list(db.jsir_enriched.find({}, {'_id': 0}).sort('timestamp', -1).limit(20)))})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# ACTION ENDPOINTS - Cyber Response
# ============================================================

@app.route('/api/cyber/action', methods=['POST'])
def cyber_action():
    try:
        data = request.json
        incident_id = data.get('incident_id')
        action_type = data.get('action')
        operator = data.get('operator', 'COP_OPERATOR')

        if not incident_id or not action_type:
            return jsonify({'error': 'incident_id and action required'}), 400

        action_record = {
            'incident_id': incident_id,
            'action': action_type,
            'operator': operator,
            'timestamp': datetime.utcnow().isoformat(),
            'epoch_ms': int(time.time() * 1000),
            'status': 'EXECUTED',
            'domain': 'CYBER',
            'details': {}
        }

        if action_type == 'block_ip':
            source_ip = data.get('source_ip', 'UNKNOWN')
            action_record['details'] = {
                'blocked_ip': source_ip,
                'firewall_rule': f'BLOCK-{incident_id}',
                'scope': 'THEATER_WIDE',
                'message': f'IP {source_ip} blocked across all CENTCOM firewalls'
            }
        elif action_type == 'isolate_system':
            target_system = data.get('target_system', 'UNKNOWN')
            action_record['details'] = {
                'isolated_system': target_system,
                'network_segment': 'QUARANTINE_VLAN',
                'message': f'{target_system} isolated to quarantine VLAN. Forensic snapshot initiated.'
            }
        elif action_type == 'alert_team':
            teams = data.get('teams', ['CYBER_OPS'])
            action_record['details'] = {
                'notified_teams': teams,
                'channels': ['SIPR_CHAT', 'VOICE_NET', 'EMAIL'],
                'priority': data.get('severity', 'HIGH'),
                'message': f'Alert dispatched to {", ".join(teams)} via SIPR chat, voice net, and encrypted email'
            }
        elif action_type == 'escalate':
            action_record['details'] = {
                'escalated_to': 'USCYBERCOM',
                'ticket_id': f'CYBERCOM-{incident_id}',
                'message': 'Incident escalated to USCYBERCOM J3 watch floor. OPREP-3 initiated.'
            }
        elif action_type == 'contain':
            action_record['details'] = {
                'containment_actions': ['Network segment isolated', 'DNS sinkhole activated', 'IOCs distributed'],
                'message': 'Full containment protocol executed. All IOCs pushed to endpoint protection.'
            }
        elif action_type == 'investigate':
            action_record['details'] = {
                'investigation_id': f'INV-{incident_id}',
                'assigned_to': 'CIRT-ALPHA',
                'message': 'Investigation opened. CIRT-ALPHA team assigned. Full packet capture enabled.'
            }

        action_log.append(action_record)
        try:
            db.cyber_actions.insert_one({**action_record})
        except Exception as e:
            logger.warning(f"Failed to log cyber action to Atlas: {e}")

        try:
            producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
            producer.produce('cop-cyber-actions', key=incident_id, value=json.dumps(action_record))
            producer.produce('cop-actions', key=incident_id, value=json.dumps(action_record))
            producer.flush(timeout=5)
        except Exception as e:
            logger.warning(f"Failed to produce cyber action to Kafka: {e}")

        return jsonify({'success': True, 'action': action_record})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# ACTION ENDPOINTS - SIGINT Response
# ============================================================

@app.route('/api/sigint/action', methods=['POST'])
def sigint_action():
    try:
        data = request.json
        intercept_id = data.get('intercept_id')
        action_type = data.get('action')
        operator = data.get('operator', 'COP_OPERATOR')

        if not intercept_id or not action_type:
            return jsonify({'error': 'intercept_id and action required'}), 400

        action_record = {
            'intercept_id': intercept_id,
            'action': action_type,
            'operator': operator,
            'timestamp': datetime.utcnow().isoformat(),
            'epoch_ms': int(time.time() * 1000),
            'status': 'EXECUTED',
            'domain': 'SIGINT',
            'details': {}
        }

        if action_type == 'jam':
            action_record['details'] = {
                'jamming_platform': 'EA-18G GROWLER',
                'technique': 'RESPONSIVE_JAMMING',
                'freq_band': data.get('frequency_band', 'X-BAND'),
                'message': 'EA-18G Growler tasked for responsive jamming. ETA to target: 8 minutes.'
            }
        elif action_type == 'geolocate':
            action_record['details'] = {
                'method': 'TDOA/FDOA',
                'platforms': ['RC-135V RIVET JOINT', 'EP-3E ARIES II'],
                'estimated_cep': '< 500m',
                'message': 'TDOA/FDOA geolocation tasked. RC-135V and EP-3E converging for triangulation.'
            }
        elif action_type == 'alert_ew':
            action_record['details'] = {
                'notified': ['JEMSOC', 'EW_COORDINATOR', 'SPECTRUM_MANAGER'],
                'channels': ['HAVE_QUICK', 'SIPR_CHAT'],
                'message': 'EW coordinator and JEMSOC notified via HAVE QUICK. Spectrum management alert issued.'
            }
        elif action_type == 'task_collection':
            action_record['details'] = {
                'collection_platform': 'RC-135V RIVET JOINT',
                'collection_type': 'FULL_SPECTRUM_CAPTURE',
                'duration_min': 30,
                'message': 'RC-135V tasked for 30-min full spectrum capture. Data feed to NSA/CSS RT.'
            }
        elif action_type == 'classify':
            action_record['details'] = {
                'classification_request': 'EMITTER_ID',
                'database': 'EWIR/ELINT_PARAM',
                'message': 'Emitter submitted to EWIR database for classification. Cross-referencing ELINT parameters.'
            }
        elif action_type == 'share_intel':
            action_record['details'] = {
                'shared_with': ['NSA/CSS', 'DIA', 'COMBATANT_COMMAND_J2'],
                'format': 'TIPPER/CRITIC',
                'message': 'Intelligence shared via TIPPER report to NSA/CSS, DIA, and J2. CRITIC criteria evaluated.'
            }

        action_log.append(action_record)
        try:
            db.sigint_actions.insert_one({**action_record})
        except Exception as e:
            logger.warning(f"Failed to log sigint action to Atlas: {e}")

        try:
            producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
            producer.produce('cop-sigint-actions', key=intercept_id, value=json.dumps(action_record))
            producer.produce('cop-actions', key=intercept_id, value=json.dumps(action_record))
            producer.flush(timeout=5)
        except Exception as e:
            logger.warning(f"Failed to produce sigint action to Kafka: {e}")

        return jsonify({'success': True, 'action': action_record})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# ACTION ENDPOINTS - EMBM Strike
# ============================================================

@app.route('/api/embm/action', methods=['POST'])
def embm_action():
    try:
        data = request.json
        track_id = data.get('track_id')
        action_type = data.get('action')
        operator = data.get('operator', 'COP_OPERATOR')

        if not track_id or not action_type:
            return jsonify({'error': 'track_id and action required'}), 400

        action_record = {
            'track_id': track_id,
            'action': action_type,
            'operator': operator,
            'timestamp': datetime.utcnow().isoformat(),
            'epoch_ms': int(time.time() * 1000),
            'status': 'EXECUTED',
            'domain': 'EMBM',
            'details': {}
        }

        if action_type == 'kinetic_strike':
            action_record['details'] = {
                'weapon': 'AGM-88G AARGM-ER',
                'platform': 'F/A-18E SUPER HORNET',
                'strike_package': 'SEAD FLIGHT ALPHA',
                'message': 'SEAD strike package ALPHA tasked. F/A-18E with AARGM-ER. Time on target: 12 minutes.'
            }
        elif action_type == 'ew_attack':
            action_record['details'] = {
                'technique': 'PRECISION_SPOT_JAMMING',
                'platform': 'EA-18G GROWLER',
                'message': 'Precision spot jamming initiated by EA-18G. Target frequency suppressed.'
            }
        elif action_type == 'refine_geo':
            action_record['details'] = {
                'method': 'MULTI_SENSOR_FUSION',
                'sensors': ['SIGINT', 'SAR_IMAGERY', 'ELINT'],
                'message': 'Multi-sensor geolocation refinement tasked. SIGINT + SAR + ELINT fusion in progress.'
            }
        elif action_type == 'monitor':
            action_record['details'] = {
                'collection_plan': 'CONTINUOUS',
                'message': 'Continuous monitoring established. Alert on mode change or movement.'
            }

        action_log.append(action_record)
        try:
            db.embm_actions.insert_one({**action_record})
        except Exception as e:
            logger.warning(f"Failed to log embm action to Atlas: {e}")

        try:
            producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
            producer.produce('cop-embm-actions', key=track_id, value=json.dumps(action_record))
            producer.produce('cop-actions', key=track_id, value=json.dumps(action_record))
            producer.flush(timeout=5)
        except Exception as e:
            logger.warning(f"Failed to produce embm action to Kafka: {e}")

        return jsonify({'success': True, 'action': action_record})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# Action History
# ============================================================

@app.route('/api/actions')
def get_actions():
    limit = int(request.args.get('limit', 50))
    domain = request.args.get('domain')
    actions = action_log[-limit:]
    if domain:
        actions = [a for a in actions if a.get('domain') == domain.upper()]
    return jsonify({'actions': list(reversed(actions)), 'total': len(action_log)})

# ============================================================
# STARTUP
# ============================================================

if __name__ == '__main__':
    if not init_atlas():
        logger.error("Cannot start without Atlas connection")
        exit(1)
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    logger.info("✓ Mission Readiness Service ready on port 5001")
    app.run(host='0.0.0.0', port=5001, debug=False)
