#!/bin/bash
# ============================================================
# Migrate Local MongoDB Data to Atlas
# ============================================================
# Usage: ./migrate-to-atlas.sh <atlas-connection-string>
# Example: ./migrate-to-atlas.sh "mongodb+srv://user:pass@cluster.mongodb.net/"
# ============================================================

set -e

# Check if connection string provided
if [ -z "$1" ]; then
    echo "============================================================"
    echo "MongoDB Local to Atlas Migration"
    echo "============================================================"
    echo ""
    echo "Usage: $0 <atlas-connection-string>"
    echo ""
    echo "Example:"
    echo "  $0 \"mongodb+srv://myuser:mypassword@cluster0.abc123.mongodb.net/\""
    echo ""
    echo "This will migrate ALL collections from local MongoDB to Atlas:"
    echo "  - common_operating_picture"
    echo "  - military_aircraft"
    echo "  - naval_vessels"
    echo "  - cyber_threats"
    echo "  - sigint_intercepts"
    echo "  - operational_alerts"
    echo "  - kb_equipment"
    echo "  - kb_geopolitical"
    echo "  - kb_threat_actors"
    echo "  - rag_documents"
    echo "  - rag_chunks"
    echo "  - vector_embeddings"
    echo "============================================================"
    exit 1
fi

ATLAS_URI="$1"
LOCAL_URI="mongodb://admin:jadc2secret@localhost:27017"
DATABASE="jadc2_cop"

echo "============================================================"
echo "MongoDB Migration: Local -> Atlas"
echo "============================================================"
echo "Source: Local MongoDB (localhost:27017)"
echo "Target: Atlas (${ATLAS_URI:0:50}...)"
echo "Database: $DATABASE"
echo ""

# Collections to migrate
COLLECTIONS=(
    "common_operating_picture"
    "military_aircraft"
    "naval_vessels"
    "cyber_threats"
    "sigint_intercepts"
    "jsir_incidents"
    "satellite_imagery"
    "operational_alerts"
    "kb_equipment"
    "kb_geopolitical"
    "kb_threat_actors"
    "kb_facilities"
    "rag_documents"
    "rag_chunks"
    "vector_embeddings"
)

echo "Collections to migrate:"
for col in "${COLLECTIONS[@]}"; do
    echo "  - $col"
done
echo ""

# Check if mongodump/mongorestore are available
if command -v mongodump &> /dev/null && command -v mongorestore &> /dev/null; then
    echo "Using mongodump/mongorestore method..."
    echo ""
    
    DUMP_DIR="/tmp/mongodb_migration_$$"
    mkdir -p "$DUMP_DIR"
    
    echo "Step 1: Dumping from local MongoDB..."
    mongodump \
        --uri="$LOCAL_URI" \
        --db="$DATABASE" \
        --out="$DUMP_DIR" \
        --authenticationDatabase=admin
    
    echo ""
    echo "Step 2: Restoring to Atlas..."
    mongorestore \
        --uri="$ATLAS_URI" \
        --db="$DATABASE" \
        --dir="$DUMP_DIR/$DATABASE" \
        --drop
    
    echo ""
    echo "Step 3: Cleanup..."
    rm -rf "$DUMP_DIR"
    
    echo ""
    echo "✓ Migration complete using mongodump/mongorestore!"
    
else
    echo "mongodump/mongorestore not found, using Python method..."
    echo ""
    
    # Use Python script inside Docker container
    docker exec -i jadc2-mission-readiness python3 << PYTHON_SCRIPT
import sys
from pymongo import MongoClient
from bson import json_util
import json

LOCAL_URI = "mongodb://admin:jadc2secret@mongodb:27017"
ATLAS_URI = "$ATLAS_URI"
DATABASE = "$DATABASE"

COLLECTIONS = [
    "common_operating_picture",
    "military_aircraft",
    "naval_vessels",
    "cyber_threats",
    "sigint_intercepts",
    "jsir_incidents",
    "satellite_imagery",
    "operational_alerts",
    "kb_equipment",
    "kb_geopolitical",
    "kb_threat_actors",
    "kb_facilities",
    "rag_documents",
    "rag_chunks",
    "vector_embeddings"
]

print("Connecting to local MongoDB...")
try:
    local_client = MongoClient(LOCAL_URI, serverSelectionTimeoutMS=5000)
    local_client.admin.command('ping')
    local_db = local_client[DATABASE]
    print("✓ Connected to local MongoDB")
except Exception as e:
    print(f"✗ Failed to connect to local MongoDB: {e}")
    sys.exit(1)

print("Connecting to Atlas...")
try:
    atlas_client = MongoClient(ATLAS_URI, serverSelectionTimeoutMS=10000)
    atlas_client.admin.command('ping')
    atlas_db = atlas_client[DATABASE]
    print("✓ Connected to Atlas")
except Exception as e:
    print(f"✗ Failed to connect to Atlas: {e}")
    sys.exit(1)

print("")
print("Migrating collections...")
print("=" * 60)

total_docs = 0
for collection_name in COLLECTIONS:
    try:
        local_col = local_db[collection_name]
        atlas_col = atlas_db[collection_name]
        
        # Get count
        count = local_col.count_documents({})
        
        if count == 0:
            print(f"  {collection_name}: (empty, skipping)")
            continue
        
        # Get all documents
        docs = list(local_col.find({}))
        
        # Drop existing collection in Atlas and insert
        atlas_col.drop()
        
        if docs:
            result = atlas_col.insert_many(docs)
            print(f"  ✓ {collection_name}: {len(result.inserted_ids)} documents migrated")
            total_docs += len(result.inserted_ids)
        
    except Exception as e:
        print(f"  ✗ {collection_name}: Error - {e}")

print("=" * 60)
print(f"Migration complete! Total documents: {total_docs}")

# Close connections
local_client.close()
atlas_client.close()
PYTHON_SCRIPT

fi

echo ""
echo "============================================================"
echo "Migration Summary"
echo "============================================================"
echo ""
echo "Data has been migrated to Atlas database: $DATABASE"
echo ""
echo "Verify in Atlas:"
echo "  1. Go to Atlas -> Browse Collections"
echo "  2. Select database: $DATABASE"
echo "  3. Check each collection has data"
echo ""
echo "Next steps:"
echo "  1. Update your application to use Atlas URI"
echo "  2. Set MONGODB_URI environment variable"
echo "  3. Restart services:"
echo "     MONGODB_URI=\"$ATLAS_URI\" docker compose up -d"
echo ""
