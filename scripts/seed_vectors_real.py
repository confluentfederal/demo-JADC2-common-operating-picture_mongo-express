#!/usr/bin/env python3
"""
Generate Real Vector Embeddings using Free Models
==================================================
Uses sentence-transformers (all-MiniLM-L6-v2) for real semantic embeddings.
This model is free, runs locally, and produces 384-dimensional vectors.

Usage:
    python3 seed_vectors_real.py <atlas-connection-string>

First install sentence-transformers:
    pip install sentence-transformers
"""

import sys
from datetime import datetime

try:
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pymongo not installed. Run: pip install pymongo")
    sys.exit(1)

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("ERROR: sentence-transformers not installed.")
    print("Run: pip install sentence-transformers")
    sys.exit(1)

DATABASE = "jadc2_cop"
MODEL_NAME = "all-MiniLM-L6-v2"  # Free, fast, 384 dimensions
EMBEDDING_DIMENSIONS = 384


def create_embeddings_for_collection(db, model, collection_name, text_fields, source_type, id_field):
    """Create embeddings for documents in a collection."""
    embeddings = []
    docs = list(db[collection_name].find({}))
    
    for doc in docs:
        # Build text from specified fields
        text_parts = []
        for field in text_fields:
            if field in doc:
                val = doc[field]
                if isinstance(val, list):
                    text_parts.append(" ".join(str(v) for v in val))
                elif isinstance(val, dict):
                    text_parts.append(" ".join(str(v) for v in val.values()))
                else:
                    text_parts.append(str(val))
        
        text = " ".join(text_parts)
        if not text.strip():
            continue
        
        # Generate embedding
        vector = model.encode(text).tolist()
        
        source_id = doc.get(id_field, str(doc.get('_id', 'unknown')))
        
        embedding_doc = {
            "embedding_id": f"emb-{source_type.lower()}-{source_id}",
            "source_collection": collection_name,
            "source_id": source_id,
            "source_type": source_type,
            "text_content": text[:1000],
            "embedding": vector,
            "embedding_model": MODEL_NAME,
            "dimensions": EMBEDDING_DIMENSIONS,
            "created_at": datetime.utcnow(),
        }
        embeddings.append(embedding_doc)
    
    return embeddings


def create_operational_embeddings(db, model):
    """Create embeddings for real-time operational data."""
    embeddings = []
    
    # Aircraft
    print("  Processing military_aircraft...")
    for doc in db.military_aircraft.find({}).limit(50):
        text = f"{doc.get('aircraft_type', '')} {doc.get('country', '')} {doc.get('role', '')} {doc.get('status', '')} {doc.get('weapons', '')}"
        if text.strip():
            vector = model.encode(text).tolist()
            embeddings.append({
                "embedding_id": f"emb-aircraft-{doc.get('aircraft_id', doc.get('tail_number', 'unknown'))}",
                "source_collection": "military_aircraft",
                "source_id": doc.get('aircraft_id', doc.get('tail_number')),
                "source_type": "AIRCRAFT",
                "text_content": text,
                "embedding": vector,
                "embedding_model": MODEL_NAME,
                "dimensions": EMBEDDING_DIMENSIONS,
                "created_at": datetime.utcnow(),
                "metadata": {
                    "country": doc.get('country'),
                    "aircraft_type": doc.get('aircraft_type'),
                    "iff_mode": doc.get('iff_mode')
                }
            })
    
    # Naval vessels
    print("  Processing naval_vessels...")
    for doc in db.naval_vessels.find({}).limit(50):
        text = f"{doc.get('vessel_type', '')} {doc.get('vessel_class', '')} {doc.get('country', '')} {doc.get('status', '')} {doc.get('weapons', '')}"
        if text.strip():
            vector = model.encode(text).tolist()
            embeddings.append({
                "embedding_id": f"emb-naval-{doc.get('vessel_id', doc.get('hull_number', 'unknown'))}",
                "source_collection": "naval_vessels",
                "source_id": doc.get('vessel_id', doc.get('hull_number')),
                "source_type": "NAVAL",
                "text_content": text,
                "embedding": vector,
                "embedding_model": MODEL_NAME,
                "dimensions": EMBEDDING_DIMENSIONS,
                "created_at": datetime.utcnow(),
                "metadata": {
                    "country": doc.get('country'),
                    "vessel_class": doc.get('vessel_class')
                }
            })
    
    # Cyber threats
    print("  Processing cyber_threats...")
    for doc in db.cyber_threats.find({}).sort("timestamp", -1).limit(50):
        text = f"{doc.get('attack_type', '')} {doc.get('severity', '')} {doc.get('threat_actor', '')} {doc.get('description', '')} {doc.get('target_system', '')} targeting {doc.get('target_location', '')}"
        if text.strip():
            vector = model.encode(text).tolist()
            embeddings.append({
                "embedding_id": f"emb-cyber-{doc.get('incident_id', 'unknown')}",
                "source_collection": "cyber_threats",
                "source_id": doc.get('incident_id'),
                "source_type": "CYBER_THREAT",
                "text_content": text,
                "embedding": vector,
                "embedding_model": MODEL_NAME,
                "dimensions": EMBEDDING_DIMENSIONS,
                "created_at": datetime.utcnow(),
                "metadata": {
                    "severity": doc.get('severity'),
                    "attack_type": doc.get('attack_type'),
                    "threat_actor": doc.get('threat_actor')
                }
            })
    
    # SIGINT intercepts
    print("  Processing sigint_intercepts...")
    for doc in db.sigint_intercepts.find({}).sort("timestamp", -1).limit(50):
        text = f"{doc.get('signal_type', '')} {doc.get('category', '')} threat level {doc.get('threat_level', '')} {doc.get('description', '')} {doc.get('emitter_classification', '')}"
        if text.strip():
            vector = model.encode(text).tolist()
            embeddings.append({
                "embedding_id": f"emb-sigint-{doc.get('intercept_id', 'unknown')}",
                "source_collection": "sigint_intercepts",
                "source_id": doc.get('intercept_id'),
                "source_type": "SIGINT",
                "text_content": text,
                "embedding": vector,
                "embedding_model": MODEL_NAME,
                "dimensions": EMBEDDING_DIMENSIONS,
                "created_at": datetime.utcnow(),
                "metadata": {
                    "signal_type": doc.get('signal_type'),
                    "threat_level": doc.get('threat_level'),
                    "category": doc.get('category')
                }
            })
    
    # JSIR incidents
    print("  Processing jsir_incidents...")
    for doc in db.jsir_incidents.find({}).sort("timestamp", -1).limit(50):
        text = f"SATCOM jamming {doc.get('jammer_system', '')} {doc.get('jammer_country', '')} targeting {doc.get('target_satellite', '')} mission impact {doc.get('mission_impact', '')}"
        if text.strip():
            vector = model.encode(text).tolist()
            embeddings.append({
                "embedding_id": f"emb-jsir-{doc.get('incident_id', 'unknown')}",
                "source_collection": "jsir_incidents",
                "source_id": doc.get('incident_id'),
                "source_type": "JSIR",
                "text_content": text,
                "embedding": vector,
                "embedding_model": MODEL_NAME,
                "dimensions": EMBEDDING_DIMENSIONS,
                "created_at": datetime.utcnow(),
                "metadata": {
                    "jammer_country": doc.get('jammer_country'),
                    "mission_impact": doc.get('mission_impact'),
                    "target_satellite": doc.get('target_satellite')
                }
            })
    
    # Operational alerts
    print("  Processing operational_alerts...")
    for doc in db.operational_alerts.find({}).sort("timestamp", -1).limit(50):
        text = f"{doc.get('alert_type', '')} {doc.get('severity', '')} {doc.get('alert_message', '')}"
        if text.strip():
            vector = model.encode(text).tolist()
            embeddings.append({
                "embedding_id": f"emb-alert-{doc.get('alert_id', 'unknown')}",
                "source_collection": "operational_alerts",
                "source_id": doc.get('alert_id'),
                "source_type": "ALERT",
                "text_content": text,
                "embedding": vector,
                "embedding_model": MODEL_NAME,
                "dimensions": EMBEDDING_DIMENSIONS,
                "created_at": datetime.utcnow(),
                "metadata": {
                    "alert_type": doc.get('alert_type'),
                    "severity": doc.get('severity')
                }
            })
    
    return embeddings


# Vector index definition for Atlas
VECTOR_INDEX_DEFINITION = {
    "index_name": "vector_search_index",
    "collection": "vector_embeddings",
    "type": "vectorSearch",
    "definition": {
        "fields": [
            {
                "type": "vector",
                "path": "embedding",
                "numDimensions": EMBEDDING_DIMENSIONS,
                "similarity": "cosine"
            },
            {
                "type": "filter",
                "path": "source_type"
            },
            {
                "type": "filter",
                "path": "source_collection"
            }
        ]
    },
    "description": f"Vector search index using {MODEL_NAME} embeddings ({EMBEDDING_DIMENSIONS} dimensions)",
    "status": "PENDING_CREATION",
    "created_at": datetime.utcnow()
}


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    atlas_uri = sys.argv[1]
    
    print("=" * 70)
    print("Generating Real Vector Embeddings")
    print("=" * 70)
    print(f"Model: {MODEL_NAME}")
    print(f"Dimensions: {EMBEDDING_DIMENSIONS}")
    print(f"Database: {DATABASE}")
    print("")
    
    # Load embedding model
    print("Loading embedding model (this may take a moment)...")
    try:
        model = SentenceTransformer(MODEL_NAME)
        print(f"✓ Model loaded: {MODEL_NAME}")
    except Exception as e:
        print(f"✗ Failed to load model: {e}")
        sys.exit(1)
    
    # Connect to Atlas
    print("\nConnecting to Atlas...")
    try:
        client = MongoClient(atlas_uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        print("✓ Connected to Atlas")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        sys.exit(1)
    
    db = client[DATABASE]
    
    all_embeddings = []
    
    # Knowledge Base embeddings
    print("\n" + "=" * 70)
    print("Generating Knowledge Base Embeddings")
    print("=" * 70)
    
    # Equipment
    print("  Processing kb_equipment...")
    kb_equip = create_embeddings_for_collection(
        db, model, "kb_equipment",
        ["name", "category", "description", "sensors", "weapons"],
        "EQUIPMENT", "equipment_id"
    )
    all_embeddings.extend(kb_equip)
    print(f"    ✓ {len(kb_equip)} embeddings")
    
    # Threat actors
    print("  Processing kb_threat_actors...")
    kb_actors = create_embeddings_for_collection(
        db, model, "kb_threat_actors",
        ["names", "attribution", "description", "typical_targets", "malware_families"],
        "THREAT_ACTOR", "actor_id"
    )
    all_embeddings.extend(kb_actors)
    print(f"    ✓ {len(kb_actors)} embeddings")
    
    # Facilities
    print("  Processing kb_facilities...")
    kb_facilities = create_embeddings_for_collection(
        db, model, "kb_facilities",
        ["name", "type", "primary_mission", "description", "units"],
        "FACILITY", "facility_id"
    )
    all_embeddings.extend(kb_facilities)
    print(f"    ✓ {len(kb_facilities)} embeddings")
    
    # Geopolitical
    print("  Processing kb_geopolitical...")
    kb_geo = create_embeddings_for_collection(
        db, model, "kb_geopolitical",
        ["name", "classification", "threat_assessment", "key_capabilities"],
        "GEOPOLITICAL", "country_code"
    )
    all_embeddings.extend(kb_geo)
    print(f"    ✓ {len(kb_geo)} embeddings")
    
    # RAG chunks
    print("  Processing rag_chunks...")
    rag_chunks = create_embeddings_for_collection(
        db, model, "rag_chunks",
        ["content"],
        "RAG_CHUNK", "chunk_id"
    )
    all_embeddings.extend(rag_chunks)
    print(f"    ✓ {len(rag_chunks)} embeddings")
    
    # Operational data embeddings
    print("\n" + "=" * 70)
    print("Generating Operational Data Embeddings")
    print("=" * 70)
    
    op_embeddings = create_operational_embeddings(db, model)
    all_embeddings.extend(op_embeddings)
    print(f"  ✓ Total operational embeddings: {len(op_embeddings)}")
    
    # Insert embeddings
    print("\n" + "=" * 70)
    print("Inserting Embeddings into Atlas")
    print("=" * 70)
    
    try:
        col = db.vector_embeddings
        col.drop()
        
        if all_embeddings:
            # Insert in batches
            batch_size = 100
            total_inserted = 0
            for i in range(0, len(all_embeddings), batch_size):
                batch = all_embeddings[i:i+batch_size]
                result = col.insert_many(batch)
                total_inserted += len(result.inserted_ids)
                print(f"  Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} embeddings")
            
            print(f"  ✓ Total inserted: {total_inserted} embeddings")
        
        # Create indexes
        col.create_index("embedding_id", unique=True)
        col.create_index("source_type")
        col.create_index("source_collection")
        col.create_index("source_id")
        print("  ✓ Created indexes")
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Insert index definition
    print("\n" + "=" * 70)
    print("Saving Vector Index Definition")
    print("=" * 70)
    
    try:
        col = db.vector_index_definitions
        col.drop()
        col.insert_one(VECTOR_INDEX_DEFINITION)
        print("  ✓ Index definition saved")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"  Total embeddings: {db.vector_embeddings.count_documents({})}")
    print(f"  Model: {MODEL_NAME}")
    print(f"  Dimensions: {EMBEDDING_DIMENSIONS}")
    
    # Show breakdown by type
    pipeline = [
        {"$group": {"_id": "$source_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    print("\n  By source type:")
    for doc in db.vector_embeddings.aggregate(pipeline):
        print(f"    {doc['_id']}: {doc['count']}")
    
    print("\n" + "=" * 70)
    print("NEXT: Create Atlas Search Vector Index")
    print("=" * 70)
    print(f"""
To enable vector search in Atlas:

1. Go to Atlas -> Your Cluster -> Atlas Search -> Create Index
2. Select "JSON Editor"
3. Select collection: vector_embeddings  
4. Paste this index definition:

{{
  "fields": [
    {{
      "type": "vector",
      "path": "embedding",
      "numDimensions": {EMBEDDING_DIMENSIONS},
      "similarity": "cosine"
    }},
    {{
      "type": "filter",
      "path": "source_type"
    }}
  ]
}}

5. Name it: vector_search_index
6. Click "Create Search Index"

Then you can query with:

db.vector_embeddings.aggregate([
  {{
    "$vectorSearch": {{
      "index": "vector_search_index",
      "path": "embedding",
      "queryVector": <your_query_vector>,
      "numCandidates": 100,
      "limit": 10,
      "filter": {{"source_type": "CYBER_THREAT"}}
    }}
  }}
])
""")
    
    client.close()
    print("✓ Done!")


if __name__ == "__main__":
    main()
