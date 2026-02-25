#!/usr/bin/env python3
"""
JADC2 RAG Query Service
Semantic search and natural language queries against military COP data
Supports vector embeddings and MongoDB Atlas Search
"""

import os
import json
import hashlib
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://admin:jadc2secret@localhost:27017/jadc2_cop?authSource=admin')
EMBEDDING_DIM = 1536  # OpenAI ada-002 compatible

app = Flask(__name__)
CORS(app)

mongo_client = None
db = None


def init_mongo():
    global mongo_client, db
    for i in range(30):
        try:
            mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            mongo_client.admin.command('ping')
            db = mongo_client.jadc2_cop
            logger.info("✓ Connected to MongoDB")
            return True
        except Exception as e:
            logger.info(f"Waiting for MongoDB ({i+1}/30): {e}")
            time.sleep(2)
    return False


# ============================================================
# RAG DOCUMENT GENERATORS
# Transform raw data into RAG-optimized documents with metadata
# ============================================================

def generate_aircraft_rag_doc(aircraft: Dict) -> Dict:
    """Generate RAG document for aircraft with rich metadata"""
    country_names = {'US': 'United States', 'RU': 'Russia', 'CN': 'China', 'IR': 'Iran'}
    iff_status = 'friendly' if aircraft.get('iff_mode') == 'FRIENDLY' else 'hostile'
    
    # Generate natural language description
    rag_text = f"""
    {aircraft.get('aircraft_type', 'Unknown')} ({aircraft.get('tail_number', 'N/A')}) is a {iff_status} {aircraft.get('role', 'unknown role')} aircraft 
    operated by {country_names.get(aircraft.get('country'), aircraft.get('country', 'Unknown'))}. 
    Currently at altitude {aircraft.get('altitude_ft', 0):,.0f} feet, heading {aircraft.get('heading_deg', 0):.0f} degrees 
    at {aircraft.get('speed_kts', 0):.0f} knots. Position: {aircraft.get('lat', 0):.4f}°N, {aircraft.get('lon', 0):.4f}°E.
    Mission status: {aircraft.get('status', 'Unknown')}. Fuel: {aircraft.get('fuel_pct', 0):.0f}%.
    Weapons: {aircraft.get('weapons', 'Unknown')}.
    """.strip()
    
    return {
        'aircraft_id': aircraft.get('aircraft_id'),
        'tail_number': aircraft.get('tail_number'),
        'aircraft_type': aircraft.get('aircraft_type'),
        'role': aircraft.get('role'),
        'country': aircraft.get('country'),
        'iff_mode': aircraft.get('iff_mode'),
        'threat_level': 'HOSTILE' if iff_status == 'hostile' else 'FRIENDLY',
        'position': {
            'lat': aircraft.get('lat'),
            'lon': aircraft.get('lon'),
            'altitude_ft': aircraft.get('altitude_ft'),
            'heading_deg': aircraft.get('heading_deg'),
            'speed_kts': aircraft.get('speed_kts')
        },
        'location': {
            'type': 'Point',
            'coordinates': [aircraft.get('lon', 0), aircraft.get('lat', 0)]
        },
        'status': aircraft.get('status'),
        'fuel_pct': aircraft.get('fuel_pct'),
        'weapons': aircraft.get('weapons'),
        'rag_text': rag_text,
        'rag_metadata': {
            'doc_type': 'aircraft',
            'domain': 'AIR',
            'classification': 'UNCLASSIFIED',
            'source': 'real-time-tracking',
            'confidence': 0.95,
            'keywords': [aircraft.get('aircraft_type'), aircraft.get('role'), aircraft.get('country'), iff_status]
        },
        'timestamp': datetime.utcnow(),
        'embedding': None  # Placeholder for vector embedding
    }


def generate_naval_rag_doc(vessel: Dict) -> Dict:
    """Generate RAG document for naval vessel"""
    country_names = {'US': 'United States', 'RU': 'Russia', 'CN': 'China', 'IR': 'Iran'}
    iff_status = 'friendly' if vessel.get('iff_mode') == 'FRIENDLY' else 'hostile'
    
    rag_text = f"""
    {vessel.get('vessel_type', 'Unknown')} ({vessel.get('hull_number', 'N/A')}) is a {iff_status} {vessel.get('vessel_class', 'unknown class')} vessel
    operated by the {country_names.get(vessel.get('country'), vessel.get('country', 'Unknown'))} Navy.
    Displacement: {vessel.get('displacement_tons', 0):,} tons. Currently heading {vessel.get('heading_deg', 0):.0f} degrees
    at {vessel.get('speed_kts', 0):.0f} knots. Position: {vessel.get('lat', 0):.4f}°N, {vessel.get('lon', 0):.4f}°E.
    Status: {vessel.get('status', 'Unknown')}. Weapons: {vessel.get('weapons', 'Unknown')}.
    {f"Aircraft capacity: {vessel.get('aircraft_capacity')} aircraft." if vessel.get('aircraft_capacity') else ''}
    """.strip()
    
    return {
        'vessel_id': vessel.get('vessel_id'),
        'hull_number': vessel.get('hull_number'),
        'vessel_type': vessel.get('vessel_type'),
        'vessel_class': vessel.get('vessel_class'),
        'country': vessel.get('country'),
        'iff_mode': vessel.get('iff_mode'),
        'threat_level': 'HOSTILE' if iff_status == 'hostile' else 'FRIENDLY',
        'displacement_tons': vessel.get('displacement_tons'),
        'position': {
            'lat': vessel.get('lat'),
            'lon': vessel.get('lon'),
            'heading_deg': vessel.get('heading_deg'),
            'speed_kts': vessel.get('speed_kts')
        },
        'location': {
            'type': 'Point',
            'coordinates': [vessel.get('lon', 0), vessel.get('lat', 0)]
        },
        'status': vessel.get('status'),
        'weapons': vessel.get('weapons'),
        'aircraft_capacity': vessel.get('aircraft_capacity'),
        'rag_text': rag_text,
        'rag_metadata': {
            'doc_type': 'naval_vessel',
            'domain': 'MARITIME',
            'classification': 'UNCLASSIFIED',
            'source': 'real-time-tracking',
            'confidence': 0.92,
            'keywords': [vessel.get('vessel_type'), vessel.get('vessel_class'), vessel.get('country'), iff_status]
        },
        'timestamp': datetime.utcnow(),
        'embedding': None
    }


def generate_cyber_rag_doc(threat: Dict) -> Dict:
    """Generate RAG document for cyber threat"""
    country_names = {'RU': 'Russia', 'CN': 'China', 'IR': 'Iran', 'KP': 'North Korea'}
    
    rag_text = f"""
    CYBER INCIDENT {threat.get('incident_id')}: {threat.get('attack_type', 'Unknown')} attack detected.
    Threat actor: {threat.get('threat_actor', 'Unknown')} ({country_names.get(threat.get('actor_country'), 'Unknown')} state-sponsored).
    Sophistication level: {threat.get('sophistication', 'Unknown')}.
    Target: {threat.get('target_facility', 'Unknown')} - {threat.get('target_system', 'Unknown')} ({threat.get('target_type', '')} in {threat.get('target_region', '')}).
    Severity: {threat.get('severity', 'Unknown')}. Status: {threat.get('status', 'Unknown')}.
    Source IP: {threat.get('source_ip', 'Unknown')}. Destination: {threat.get('destination_ip', 'Unknown')}:{threat.get('port', '')}.
    MITRE ATT&CK: {threat.get('mitre_attack', 'Unknown')}.
    Target coordinates: {threat.get('target_lat', 0):.4f}°N, {threat.get('target_lon', 0):.4f}°E.
    """.strip()
    
    return {
        'incident_id': threat.get('incident_id'),
        'attack_type': threat.get('attack_type'),
        'severity': threat.get('severity'),
        'threat_actor': threat.get('threat_actor'),
        'actor_country': threat.get('actor_country'),
        'sophistication': threat.get('sophistication'),
        'target_facility_id': threat.get('target_facility_id'),
        'target_facility': threat.get('target_facility'),
        'target_system': threat.get('target_system'),
        'target_type': threat.get('target_type'),
        'target_region': threat.get('target_region'),
        'target_location': {
            'type': 'Point',
            'coordinates': [threat.get('target_lon', 0), threat.get('target_lat', 0)]
        },
        'source_ip': threat.get('source_ip'),
        'destination_ip': threat.get('destination_ip'),
        'protocol': threat.get('protocol'),
        'port': threat.get('port'),
        'mitre_attack': threat.get('mitre_attack'),
        'status': threat.get('status'),
        'rag_text': rag_text,
        'rag_metadata': {
            'doc_type': 'cyber_incident',
            'domain': 'CYBER',
            'classification': 'SECRET',
            'source': 'cyber-defense-center',
            'confidence': 0.88,
            'keywords': [threat.get('attack_type'), threat.get('threat_actor'), threat.get('target_facility'), threat.get('severity')]
        },
        'timestamp': datetime.utcnow(),
        'embedding': None
    }


def generate_facility_rag_doc(facility: Dict) -> Dict:
    """Generate RAG document for military facility"""
    rag_text = f"""
    US Military Facility: {facility.get('name', 'Unknown')} ({facility.get('id', '')}).
    Type: {facility.get('type', 'Unknown')}. Region: {facility.get('region', 'Unknown')}.
    Location: {facility.get('lat', 0):.4f}°N, {facility.get('lon', 0):.4f}°E in {facility.get('country', 'Unknown')}.
    Networks: {', '.join(facility.get('networks', []))}.
    This facility supports US military operations in the {facility.get('region', '')} theater.
    """.strip()
    
    return {
        'facility_id': facility.get('id'),
        'facility_name': facility.get('name'),
        'facility_type': facility.get('type'),
        'region': facility.get('region'),
        'country': facility.get('country'),
        'location': {
            'type': 'Point',
            'coordinates': [facility.get('lon', 0), facility.get('lat', 0)]
        },
        'networks': facility.get('networks', []),
        'rag_text': rag_text,
        'rag_metadata': {
            'doc_type': 'facility',
            'domain': 'INFRASTRUCTURE',
            'classification': 'UNCLASSIFIED',
            'source': 'facility-registry',
            'confidence': 1.0,
            'keywords': [facility.get('name'), facility.get('type'), facility.get('region')]
        },
        'timestamp': datetime.utcnow(),
        'embedding': None
    }


def generate_sigint_rag_doc(sigint: Dict) -> Dict:
    """Generate RAG document for SIGINT intercept"""
    rag_text = f"""
    SIGINT Intercept {sigint.get('intercept_id')}: {sigint.get('signal_type', 'Unknown')} signal detected.
    Frequency: {sigint.get('frequency_mhz', 0):.2f} MHz, Bandwidth: {sigint.get('bandwidth_khz', 0):.1f} kHz.
    Modulation: {sigint.get('modulation', 'Unknown')}. Signal strength: {sigint.get('signal_strength_dbm', 0):.1f} dBm.
    Bearing: {sigint.get('bearing_deg', 0):.1f}°. Position: {sigint.get('lat', 0):.4f}°N, {sigint.get('lon', 0):.4f}°E.
    Emitter classification: {sigint.get('emitter_classification', 'Unknown')}.
    Threat level: {sigint.get('threat_level', 'Unknown')}. Emission pattern: {sigint.get('emission_pattern', 'Unknown')}.
    Collected by: {sigint.get('collector_id', 'Unknown')}. Confidence: {sigint.get('confidence_pct', 0):.0f}%.
    """.strip()
    
    return {
        'intercept_id': sigint.get('intercept_id'),
        'signal_type': sigint.get('signal_type'),
        'frequency_mhz': sigint.get('frequency_mhz'),
        'bandwidth_khz': sigint.get('bandwidth_khz'),
        'modulation': sigint.get('modulation'),
        'signal_strength_dbm': sigint.get('signal_strength_dbm'),
        'bearing_deg': sigint.get('bearing_deg'),
        'location': {
            'type': 'Point',
            'coordinates': [sigint.get('lon', 0), sigint.get('lat', 0)]
        },
        'emitter_classification': sigint.get('emitter_classification'),
        'emitter_country': sigint.get('emitter_classification', '').split('-')[0] if sigint.get('emitter_classification') else None,
        'threat_level': sigint.get('threat_level'),
        'emission_pattern': sigint.get('emission_pattern'),
        'collector_id': sigint.get('collector_id'),
        'confidence_pct': sigint.get('confidence_pct'),
        'rag_text': rag_text,
        'rag_metadata': {
            'doc_type': 'sigint',
            'domain': 'SIGINT',
            'classification': 'TOP_SECRET',
            'source': 'sigint-collection',
            'confidence': sigint.get('confidence_pct', 0) / 100,
            'keywords': [sigint.get('signal_type'), sigint.get('emitter_classification'), sigint.get('threat_level')]
        },
        'timestamp': datetime.utcnow(),
        'embedding': None
    }


# ============================================================
# SIMPLE EMBEDDING GENERATOR (for demo without external API)
# Replace with OpenAI/Cohere/etc for production
# ============================================================

def generate_simple_embedding(text: str) -> List[float]:
    """
    Generate a deterministic pseudo-embedding for demo purposes.
    In production, replace with actual embedding API call.
    """
    # Create deterministic hash-based embedding
    text_lower = text.lower()
    hash_bytes = hashlib.sha256(text_lower.encode()).digest()
    
    # Expand hash to embedding dimension using multiple hashes
    embedding = []
    for i in range(EMBEDDING_DIM // 32 + 1):
        chunk_hash = hashlib.sha256(f"{text_lower}_{i}".encode()).digest()
        for byte in chunk_hash:
            if len(embedding) < EMBEDDING_DIM:
                # Normalize to [-1, 1]
                embedding.append((byte - 128) / 128.0)
    
    return embedding[:EMBEDDING_DIM]


# ============================================================
# RAG INDEXING - Ingest data into RAG collections
# ============================================================

@app.route('/api/rag/index/aircraft', methods=['POST'])
def index_aircraft():
    """Index aircraft data into RAG collection"""
    try:
        data = request.get_json()
        aircraft_list = data.get('aircraft', [])
        
        indexed = 0
        for ac in aircraft_list:
            rag_doc = generate_aircraft_rag_doc(ac)
            rag_doc['embedding'] = generate_simple_embedding(rag_doc['rag_text'])
            
            db.rag_aircraft.update_one(
                {'aircraft_id': rag_doc['aircraft_id']},
                {'$set': rag_doc},
                upsert=True
            )
            indexed += 1
        
        return jsonify({'indexed': indexed, 'collection': 'rag_aircraft'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/rag/index/naval', methods=['POST'])
def index_naval():
    """Index naval vessel data into RAG collection"""
    try:
        data = request.get_json()
        vessels = data.get('vessels', [])
        
        indexed = 0
        for vessel in vessels:
            rag_doc = generate_naval_rag_doc(vessel)
            rag_doc['embedding'] = generate_simple_embedding(rag_doc['rag_text'])
            
            db.rag_naval.update_one(
                {'vessel_id': rag_doc['vessel_id']},
                {'$set': rag_doc},
                upsert=True
            )
            indexed += 1
        
        return jsonify({'indexed': indexed, 'collection': 'rag_naval'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/rag/index/cyber', methods=['POST'])
def index_cyber():
    """Index cyber threat data into RAG collection"""
    try:
        data = request.get_json()
        threats = data.get('threats', [])
        
        indexed = 0
        for threat in threats:
            rag_doc = generate_cyber_rag_doc(threat)
            rag_doc['embedding'] = generate_simple_embedding(rag_doc['rag_text'])
            
            db.rag_cyber_incidents.update_one(
                {'incident_id': rag_doc['incident_id']},
                {'$set': rag_doc},
                upsert=True
            )
            indexed += 1
        
        return jsonify({'indexed': indexed, 'collection': 'rag_cyber_incidents'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# RAG SEARCH - Query across all domains
# ============================================================

def keyword_search(query: str, collections: List[str] = None) -> List[Dict]:
    """Perform keyword-based search across RAG collections"""
    if collections is None:
        collections = ['rag_aircraft', 'rag_naval', 'rag_cyber_incidents', 'kb_equipment', 'kb_threat_actors']
    
    results = []
    query_lower = query.lower()
    keywords = re.findall(r'\b\w+\b', query_lower)
    
    # Build regex pattern for flexible matching
    patterns = [re.compile(kw, re.IGNORECASE) for kw in keywords if len(kw) > 2]
    
    for coll_name in collections:
        try:
            coll = db[coll_name]
            # Text search on rag_text field
            docs = list(coll.find(
                {'rag_text': {'$regex': '|'.join(keywords), '$options': 'i'}},
                {'_id': 0, 'embedding': 0}
            ).limit(10))
            
            for doc in docs:
                # Score based on keyword matches
                score = sum(1 for p in patterns if p.search(doc.get('rag_text', '')))
                doc['_search_score'] = score / len(patterns) if patterns else 0
                doc['_source_collection'] = coll_name
                results.append(doc)
        except Exception as e:
            logger.error(f"Error searching {coll_name}: {e}")
    
    # Sort by score
    results.sort(key=lambda x: x.get('_search_score', 0), reverse=True)
    return results[:20]


def semantic_search(query: str, collections: List[str] = None, limit: int = 10) -> List[Dict]:
    """
    Perform semantic search using vector embeddings.
    Note: For production, use MongoDB Atlas Search with vector indexes.
    This is a simplified cosine similarity implementation for demo.
    """
    if collections is None:
        collections = ['rag_aircraft', 'rag_naval', 'rag_cyber_incidents', 'kb_equipment', 'kb_threat_actors']
    
    query_embedding = generate_simple_embedding(query)
    results = []
    
    for coll_name in collections:
        try:
            coll = db[coll_name]
            docs = list(coll.find({'embedding': {'$exists': True}}, {'_id': 0}).limit(100))
            
            for doc in docs:
                if doc.get('embedding'):
                    # Cosine similarity
                    dot_product = sum(a * b for a, b in zip(query_embedding, doc['embedding']))
                    magnitude_q = sum(a * a for a in query_embedding) ** 0.5
                    magnitude_d = sum(a * a for a in doc['embedding']) ** 0.5
                    
                    if magnitude_q > 0 and magnitude_d > 0:
                        similarity = dot_product / (magnitude_q * magnitude_d)
                        doc['_vector_score'] = similarity
                        doc['_source_collection'] = coll_name
                        del doc['embedding']  # Don't return embedding
                        results.append(doc)
        except Exception as e:
            logger.error(f"Error in semantic search on {coll_name}: {e}")
    
    results.sort(key=lambda x: x.get('_vector_score', 0), reverse=True)
    return results[:limit]


# ============================================================
# NATURAL LANGUAGE QUERY INTERFACE
# ============================================================

def extract_query_intent(query: str) -> Dict:
    """Extract intent and entities from natural language query"""
    query_lower = query.lower()
    
    intent = {
        'domain': None,
        'action': 'search',
        'filters': {},
        'entities': []
    }
    
    # Domain detection
    if any(kw in query_lower for kw in ['aircraft', 'plane', 'jet', 'fighter', 'bomber', 'uav', 'drone', 'flying']):
        intent['domain'] = 'AIR'
    elif any(kw in query_lower for kw in ['ship', 'vessel', 'naval', 'carrier', 'destroyer', 'submarine', 'fleet']):
        intent['domain'] = 'MARITIME'
    elif any(kw in query_lower for kw in ['cyber', 'attack', 'hacker', 'apt', 'intrusion', 'malware', 'threat actor']):
        intent['domain'] = 'CYBER'
    elif any(kw in query_lower for kw in ['base', 'facility', 'camp', 'airfield', 'station']):
        intent['domain'] = 'INFRASTRUCTURE'
    elif any(kw in query_lower for kw in ['signal', 'sigint', 'radar', 'emission', 'intercept']):
        intent['domain'] = 'SIGINT'
    
    # Country filters
    for country, names in [('US', ['us', 'american', 'united states']), 
                           ('RU', ['russian', 'russia']), 
                           ('CN', ['chinese', 'china']),
                           ('IR', ['iranian', 'iran'])]:
        if any(n in query_lower for n in names):
            intent['filters']['country'] = country
    
    # Threat level
    if 'hostile' in query_lower:
        intent['filters']['iff_mode'] = 'HOSTILE'
    elif 'friendly' in query_lower:
        intent['filters']['iff_mode'] = 'FRIENDLY'
    
    # Severity
    if 'critical' in query_lower:
        intent['filters']['severity'] = 'CRITICAL'
    elif 'high' in query_lower:
        intent['filters']['severity'] = 'HIGH'
    
    return intent


def generate_response(query: str, search_results: List[Dict], intent: Dict) -> str:
    """Generate natural language response from search results"""
    if not search_results:
        return f"No results found for your query: '{query}'. Try asking about aircraft, naval vessels, cyber threats, or facilities."
    
    response_parts = []
    domain = intent.get('domain', 'MULTI-DOMAIN')
    
    response_parts.append(f"**{domain} Intelligence Summary**\n")
    response_parts.append(f"_Query: {query}_\n")
    response_parts.append(f"_Found {len(search_results)} relevant items_\n\n")
    
    # Group by source collection
    by_collection = {}
    for r in search_results[:10]:
        coll = r.get('_source_collection', 'unknown')
        if coll not in by_collection:
            by_collection[coll] = []
        by_collection[coll].append(r)
    
    for coll, items in by_collection.items():
        coll_name = coll.replace('rag_', '').replace('kb_', '').replace('_', ' ').title()
        response_parts.append(f"**{coll_name}:**\n")
        
        for item in items[:5]:
            if 'aircraft_type' in item:
                response_parts.append(f"• {item.get('aircraft_type')} ({item.get('tail_number')}) - {item.get('country')} - {item.get('status')}\n")
            elif 'vessel_type' in item:
                response_parts.append(f"• {item.get('vessel_type')} ({item.get('hull_number')}) - {item.get('country')} - {item.get('status')}\n")
            elif 'incident_id' in item:
                response_parts.append(f"• {item.get('attack_type')} [{item.get('severity')}] - {item.get('threat_actor')} → {item.get('target_facility')}\n")
            elif 'equipment_id' in item:
                response_parts.append(f"• {item.get('name')} ({item.get('equipment_type')}) - {item.get('country_of_origin')}\n")
            elif 'actor_id' in item:
                response_parts.append(f"• {item.get('name')} ({item.get('country')}) - {item.get('sophistication')}\n")
            else:
                response_parts.append(f"• {item.get('rag_text', '')[:200]}...\n")
        
        response_parts.append("\n")
    
    return "".join(response_parts)


@app.route('/api/query', methods=['POST'])
def query():
    """Natural language query endpoint"""
    try:
        data = request.get_json()
        query_text = data.get('query', '')
        search_type = data.get('search_type', 'hybrid')  # keyword, semantic, hybrid
        
        if not query_text:
            return jsonify({'error': 'No query provided'}), 400
        
        # Extract intent
        intent = extract_query_intent(query_text)
        
        # Determine collections based on domain
        collections = None
        if intent['domain'] == 'AIR':
            collections = ['rag_aircraft', 'kb_equipment']
        elif intent['domain'] == 'MARITIME':
            collections = ['rag_naval', 'kb_equipment']
        elif intent['domain'] == 'CYBER':
            collections = ['rag_cyber_incidents', 'kb_threat_actors']
        elif intent['domain'] == 'INFRASTRUCTURE':
            collections = ['rag_facilities']
        elif intent['domain'] == 'SIGINT':
            collections = ['rag_sigint']
        
        # Perform search
        if search_type == 'keyword':
            results = keyword_search(query_text, collections)
        elif search_type == 'semantic':
            results = semantic_search(query_text, collections)
        else:  # hybrid
            kw_results = keyword_search(query_text, collections)
            sem_results = semantic_search(query_text, collections)
            # Merge and deduplicate
            seen = set()
            results = []
            for r in kw_results + sem_results:
                key = r.get('aircraft_id') or r.get('vessel_id') or r.get('incident_id') or r.get('equipment_id') or str(r)
                if key not in seen:
                    seen.add(key)
                    results.append(r)
        
        # Generate response
        response_text = generate_response(query_text, results, intent)
        
        # Log query
        db.rag_query_history.insert_one({
            'query': query_text,
            'intent': intent,
            'search_type': search_type,
            'result_count': len(results),
            'timestamp': datetime.utcnow()
        })
        
        return jsonify({
            'query': query_text,
            'intent': intent,
            'result_count': len(results),
            'results': results[:10],
            'response': response_text,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Query error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/rag/stats', methods=['GET'])
def rag_stats():
    """Get RAG collection statistics"""
    try:
        stats = {
            'collections': {},
            'total_documents': 0,
            'knowledge_base': {}
        }
        
        for coll_name in ['rag_aircraft', 'rag_naval', 'rag_cyber_incidents', 'rag_facilities', 'rag_sigint']:
            count = db[coll_name].count_documents({})
            stats['collections'][coll_name] = count
            stats['total_documents'] += count
        
        for kb_name in ['kb_equipment', 'kb_threat_actors', 'kb_geopolitical']:
            count = db[kb_name].count_documents({})
            stats['knowledge_base'][kb_name] = count
        
        stats['recent_queries'] = db.rag_query_history.count_documents({
            'timestamp': {'$gte': datetime.utcnow() - timedelta(hours=24)}
        })
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/rag/knowledge/<kb_type>', methods=['GET'])
def get_knowledge_base(kb_type):
    """Retrieve knowledge base entries"""
    try:
        valid_types = ['equipment', 'threat_actors', 'geopolitical']
        if kb_type not in valid_types:
            return jsonify({'error': f'Invalid type. Use: {valid_types}'}), 400
        
        coll_name = f'kb_{kb_type}'
        docs = list(db[coll_name].find({}, {'_id': 0, 'embedding': 0}).limit(50))
        
        return jsonify({
            'type': kb_type,
            'count': len(docs),
            'items': docs
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'service': 'rag-query'})


if __name__ == '__main__':
    if not init_mongo():
        exit(1)
    logger.info("Starting RAG Query Service on port 5002...")
    app.run(host='0.0.0.0', port=5002, debug=False)
