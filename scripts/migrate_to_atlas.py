#!/usr/bin/env python3
"""
MongoDB Migration Script: Local to Atlas
=========================================
Migrates all collections from local MongoDB to MongoDB Atlas.

Usage:
    python3 migrate_to_atlas.py <atlas-connection-string>
    
Example:
    python3 migrate_to_atlas.py "mongodb+srv://user:pass@cluster0.mongodb.net/"

Or run inside Docker:
    docker exec -it jadc2-mission-readiness python3 /app/migrate_to_atlas.py "mongodb+srv://..."
"""

import sys
import os
from datetime import datetime

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, OperationFailure
except ImportError:
    print("ERROR: pymongo not installed. Run: pip install pymongo")
    sys.exit(1)

# Configuration
LOCAL_URI = os.environ.get("LOCAL_MONGODB_URI", "mongodb://admin:jadc2secret@mongodb:27017")
DATABASE = "jadc2_cop"

# All collections to migrate
COLLECTIONS = [
    # Operational data (real-time)
    "common_operating_picture",
    "military_aircraft", 
    "naval_vessels",
    "cyber_threats",
    "sigint_intercepts",
    "jsir_incidents",
    "satellite_imagery",
    "operational_alerts",
    
    # Knowledge base (static reference data)
    "kb_equipment",
    "kb_geopolitical", 
    "kb_threat_actors",
    "kb_facilities",
    "kb_doctrine",
    "kb_units",
    
    # RAG/Vector data
    "rag_documents",
    "rag_chunks",
    "vector_embeddings",
    "vector_search_index",
]


def connect_mongodb(uri, name="MongoDB"):
    """Connect to MongoDB and return client."""
    print(f"Connecting to {name}...")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        print(f"  ✓ Connected to {name}")
        return client
    except ConnectionFailure as e:
        print(f"  ✗ Failed to connect to {name}: {e}")
        return None


def get_collection_stats(db):
    """Get stats for all collections in database."""
    stats = {}
    for col_name in db.list_collection_names():
        count = db[col_name].count_documents({})
        stats[col_name] = count
    return stats


def migrate_collection(local_db, atlas_db, collection_name, drop_existing=True):
    """Migrate a single collection from local to Atlas."""
    local_col = local_db[collection_name]
    atlas_col = atlas_db[collection_name]
    
    # Count documents
    count = local_col.count_documents({})
    
    if count == 0:
        return 0, "empty"
    
    # Get all documents
    docs = list(local_col.find({}))
    
    # Drop existing in Atlas if requested
    if drop_existing:
        atlas_col.drop()
    
    # Insert documents
    try:
        result = atlas_col.insert_many(docs, ordered=False)
        return len(result.inserted_ids), "success"
    except Exception as e:
        return 0, str(e)


def create_indexes(atlas_db):
    """Create indexes on Atlas collections."""
    print("\nCreating indexes...")
    
    indexes = {
        "military_aircraft": [
            ("aircraft_id", 1),
            ("country", 1),
            ("iff_mode", 1),
        ],
        "naval_vessels": [
            ("vessel_id", 1),
            ("country", 1),
        ],
        "common_operating_picture": [
            ("unit_id", 1),
        ],
        "cyber_threats": [
            ("incident_id", 1),
            ("severity", 1),
            ("timestamp", -1),
        ],
        "sigint_intercepts": [
            ("intercept_id", 1),
            ("threat_level", 1),
            ("signal_type", 1),
        ],
        "jsir_incidents": [
            ("incident_id", 1),
            ("jammer_country", 1),
        ],
        "operational_alerts": [
            ("alert_id", 1),
            ("severity", 1),
            ("timestamp", -1),
        ],
        "rag_chunks": [
            ("document_id", 1),
            ("chunk_index", 1),
        ],
        "vector_embeddings": [
            ("source_id", 1),
            ("embedding_model", 1),
        ],
    }
    
    for collection, idx_fields in indexes.items():
        try:
            col = atlas_db[collection]
            for field, direction in idx_fields:
                col.create_index([(field, direction)])
            print(f"  ✓ {collection}: indexes created")
        except Exception as e:
            print(f"  ✗ {collection}: {e}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nERROR: Atlas connection string required")
        print("\nExample:")
        print('  python3 migrate_to_atlas.py "mongodb+srv://user:pass@cluster0.mongodb.net/"')
        sys.exit(1)
    
    atlas_uri = sys.argv[1]
    
    print("=" * 70)
    print("MongoDB Migration: Local -> Atlas")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Database: {DATABASE}")
    print(f"Local URI: {LOCAL_URI[:40]}...")
    print(f"Atlas URI: {atlas_uri[:50]}...")
    print("")
    
    # Connect to both databases
    local_client = connect_mongodb(LOCAL_URI, "Local MongoDB")
    if not local_client:
        sys.exit(1)
    
    atlas_client = connect_mongodb(atlas_uri, "MongoDB Atlas")
    if not atlas_client:
        local_client.close()
        sys.exit(1)
    
    local_db = local_client[DATABASE]
    atlas_db = atlas_client[DATABASE]
    
    # Show local stats
    print("\n" + "=" * 70)
    print("Local MongoDB Collections")
    print("=" * 70)
    local_stats = get_collection_stats(local_db)
    for col, count in sorted(local_stats.items()):
        print(f"  {col}: {count} documents")
    print(f"\nTotal collections: {len(local_stats)}")
    print(f"Total documents: {sum(local_stats.values())}")
    
    # Migrate collections
    print("\n" + "=" * 70)
    print("Migrating Collections")
    print("=" * 70)
    
    results = {}
    total_migrated = 0
    
    # First migrate collections that exist locally
    for collection in COLLECTIONS:
        if collection in local_stats and local_stats[collection] > 0:
            count, status = migrate_collection(local_db, atlas_db, collection)
            results[collection] = (count, status)
            if status == "success":
                total_migrated += count
                print(f"  ✓ {collection}: {count} documents")
            else:
                print(f"  ✗ {collection}: {status}")
        elif collection in local_stats:
            print(f"  - {collection}: (empty)")
    
    # Also migrate any collections not in our list
    for collection in local_stats:
        if collection not in COLLECTIONS and local_stats[collection] > 0:
            count, status = migrate_collection(local_db, atlas_db, collection)
            results[collection] = (count, status)
            if status == "success":
                total_migrated += count
                print(f"  ✓ {collection}: {count} documents (extra)")
            else:
                print(f"  ✗ {collection}: {status}")
    
    # Create indexes
    create_indexes(atlas_db)
    
    # Show Atlas stats
    print("\n" + "=" * 70)
    print("Atlas Collections (After Migration)")
    print("=" * 70)
    atlas_stats = get_collection_stats(atlas_db)
    for col, count in sorted(atlas_stats.items()):
        print(f"  {col}: {count} documents")
    print(f"\nTotal collections: {len(atlas_stats)}")
    print(f"Total documents: {sum(atlas_stats.values())}")
    
    # Summary
    print("\n" + "=" * 70)
    print("Migration Summary")
    print("=" * 70)
    print(f"Collections migrated: {len([r for r in results.values() if r[1] == 'success'])}")
    print(f"Documents migrated: {total_migrated}")
    print(f"Errors: {len([r for r in results.values() if r[1] != 'success' and r[1] != 'empty'])}")
    
    # Cleanup
    local_client.close()
    atlas_client.close()
    
    print("\n" + "=" * 70)
    print("Next Steps")
    print("=" * 70)
    print("""
1. Verify data in Atlas:
   - Go to Atlas -> Browse Collections -> jadc2_cop

2. Update services to use Atlas:
   export MONGODB_URI="mongodb+srv://user:pass@cluster.mongodb.net/"
   docker compose -f docker/docker-compose.yml up -d

3. Configure sink connectors for real-time sync:
   ./scripts/setup-atlas-sinks.sh "$MONGODB_URI"

4. (Optional) Create Atlas Search indexes for RAG:
   - Go to Atlas -> Search -> Create Index
   - Select vector_embeddings collection
   - Configure vector search index
""")
    
    print("Migration complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
