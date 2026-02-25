#!/usr/bin/env python3
"""
Seed Vector Embeddings and Index Definitions into MongoDB Atlas
===============================================================
Creates sample vector embeddings and Atlas Search index definitions.

Usage:
    python3 seed_vectors.py <atlas-connection-string>
"""

import sys
import random
from datetime import datetime

try:
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pymongo not installed")
    sys.exit(1)

DATABASE = "jadc2_cop"
EMBEDDING_DIMENSIONS = 1536  # OpenAI ada-002 dimensions


def generate_mock_embedding(seed_text):
    """Generate a deterministic mock embedding based on text."""
    random.seed(hash(seed_text) % (2**32))
    return [random.uniform(-1, 1) for _ in range(EMBEDDING_DIMENSIONS)]


# ============================================================
# VECTOR EMBEDDINGS - Equipment
# ============================================================
def create_equipment_embeddings(db):
    """Create embeddings for equipment documents."""
    embeddings = []
    
    equipment_docs = list(db.kb_equipment.find({}))
    for doc in equipment_docs:
        # Create text to embed
        text = f"{doc.get('name', '')} {doc.get('category', '')} {doc.get('description', '')}"
        
        embedding = {
            "embedding_id": f"emb-equip-{doc.get('equipment_id', 'unknown')}",
            "source_collection": "kb_equipment",
            "source_id": doc.get('equipment_id'),
            "source_type": "EQUIPMENT",
            "text_content": text[:500],  # Truncate for storage
            "embedding": generate_mock_embedding(text),
            "embedding_model": "text-embedding-ada-002",
            "dimensions": EMBEDDING_DIMENSIONS,
            "created_at": datetime.utcnow(),
            "metadata": {
                "name": doc.get('name'),
                "category": doc.get('category'),
                "country": doc.get('country')
            }
        }
        embeddings.append(embedding)
    
    return embeddings


# ============================================================
# VECTOR EMBEDDINGS - Threat Actors
# ============================================================
def create_threat_actor_embeddings(db):
    """Create embeddings for threat actor documents."""
    embeddings = []
    
    actor_docs = list(db.kb_threat_actors.find({}))
    for doc in actor_docs:
        # Create text to embed
        names = " ".join(doc.get('names', []))
        text = f"{names} {doc.get('attribution', '')} {doc.get('description', '')}"
        
        embedding = {
            "embedding_id": f"emb-actor-{doc.get('actor_id', 'unknown')}",
            "source_collection": "kb_threat_actors",
            "source_id": doc.get('actor_id'),
            "source_type": "THREAT_ACTOR",
            "text_content": text[:500],
            "embedding": generate_mock_embedding(text),
            "embedding_model": "text-embedding-ada-002",
            "dimensions": EMBEDDING_DIMENSIONS,
            "created_at": datetime.utcnow(),
            "metadata": {
                "names": doc.get('names'),
                "country": doc.get('country'),
                "sophistication": doc.get('sophistication')
            }
        }
        embeddings.append(embedding)
    
    return embeddings


# ============================================================
# VECTOR EMBEDDINGS - Facilities
# ============================================================
def create_facility_embeddings(db):
    """Create embeddings for facility documents."""
    embeddings = []
    
    facility_docs = list(db.kb_facilities.find({}))
    for doc in facility_docs:
        text = f"{doc.get('name', '')} {doc.get('type', '')} {doc.get('primary_mission', '')} {doc.get('description', '')}"
        
        embedding = {
            "embedding_id": f"emb-facility-{doc.get('facility_id', 'unknown')}",
            "source_collection": "kb_facilities",
            "source_id": doc.get('facility_id'),
            "source_type": "FACILITY",
            "text_content": text[:500],
            "embedding": generate_mock_embedding(text),
            "embedding_model": "text-embedding-ada-002",
            "dimensions": EMBEDDING_DIMENSIONS,
            "created_at": datetime.utcnow(),
            "metadata": {
                "name": doc.get('name'),
                "type": doc.get('type'),
                "region": doc.get('region'),
                "country": doc.get('country')
            }
        }
        embeddings.append(embedding)
    
    return embeddings


# ============================================================
# VECTOR EMBEDDINGS - RAG Chunks
# ============================================================
def create_rag_chunk_embeddings(db):
    """Create embeddings for RAG chunks."""
    embeddings = []
    
    chunk_docs = list(db.rag_chunks.find({}))
    for doc in chunk_docs:
        text = doc.get('content', '')
        
        embedding = {
            "embedding_id": f"emb-chunk-{doc.get('chunk_id', 'unknown')}",
            "source_collection": "rag_chunks",
            "source_id": doc.get('chunk_id'),
            "source_type": "RAG_CHUNK",
            "text_content": text[:500],
            "embedding": generate_mock_embedding(text),
            "embedding_model": "text-embedding-ada-002",
            "dimensions": EMBEDDING_DIMENSIONS,
            "created_at": datetime.utcnow(),
            "metadata": {
                "document_id": doc.get('document_id'),
                "document_title": doc.get('document_title'),
                "chunk_index": doc.get('chunk_index')
            }
        }
        embeddings.append(embedding)
    
    return embeddings


# ============================================================
# VECTOR INDEX DEFINITIONS
# ============================================================
VECTOR_INDEX_DEFINITIONS = [
    {
        "index_name": "vector_search_equipment",
        "collection": "vector_embeddings",
        "type": "vectorSearch",
        "definition": {
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": 1536,
                    "similarity": "cosine"
                },
                {
                    "type": "filter",
                    "path": "source_type"
                },
                {
                    "type": "filter",
                    "path": "metadata.category"
                },
                {
                    "type": "filter",
                    "path": "metadata.country"
                }
            ]
        },
        "description": "Vector search index for all embeddings with filters",
        "status": "PENDING_CREATION",
        "created_at": datetime.utcnow()
    },
    {
        "index_name": "vector_search_rag",
        "collection": "rag_chunks",
        "type": "vectorSearch",
        "definition": {
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": 1536,
                    "similarity": "cosine"
                },
                {
                    "type": "filter",
                    "path": "document_id"
                }
            ]
        },
        "description": "Vector search for RAG document chunks",
        "status": "PENDING_CREATION",
        "created_at": datetime.utcnow()
    },
    {
        "index_name": "text_search_equipment",
        "collection": "kb_equipment",
        "type": "search",
        "definition": {
            "mappings": {
                "dynamic": False,
                "fields": {
                    "name": {"type": "string", "analyzer": "lucene.standard"},
                    "description": {"type": "string", "analyzer": "lucene.standard"},
                    "category": {"type": "stringFacet"},
                    "country": {"type": "stringFacet"}
                }
            }
        },
        "description": "Full-text search for equipment",
        "status": "PENDING_CREATION",
        "created_at": datetime.utcnow()
    },
    {
        "index_name": "text_search_threat_actors",
        "collection": "kb_threat_actors",
        "type": "search",
        "definition": {
            "mappings": {
                "dynamic": False,
                "fields": {
                    "names": {"type": "string", "analyzer": "lucene.standard"},
                    "description": {"type": "string", "analyzer": "lucene.standard"},
                    "attribution": {"type": "string", "analyzer": "lucene.standard"},
                    "country": {"type": "stringFacet"},
                    "sophistication": {"type": "stringFacet"}
                }
            }
        },
        "description": "Full-text search for threat actors",
        "status": "PENDING_CREATION",
        "created_at": datetime.utcnow()
    }
]


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    atlas_uri = sys.argv[1]
    
    print("=" * 70)
    print("Seeding Vector Embeddings and Index Definitions")
    print("=" * 70)
    print(f"Database: {DATABASE}")
    print(f"Embedding dimensions: {EMBEDDING_DIMENSIONS}")
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
    
    # Create embeddings from existing documents
    print("\n" + "=" * 70)
    print("Generating Vector Embeddings")
    print("=" * 70)
    
    all_embeddings = []
    
    # Equipment embeddings
    equip_embeddings = create_equipment_embeddings(db)
    all_embeddings.extend(equip_embeddings)
    print(f"  ✓ Equipment: {len(equip_embeddings)} embeddings")
    
    # Threat actor embeddings
    actor_embeddings = create_threat_actor_embeddings(db)
    all_embeddings.extend(actor_embeddings)
    print(f"  ✓ Threat Actors: {len(actor_embeddings)} embeddings")
    
    # Facility embeddings
    facility_embeddings = create_facility_embeddings(db)
    all_embeddings.extend(facility_embeddings)
    print(f"  ✓ Facilities: {len(facility_embeddings)} embeddings")
    
    # RAG chunk embeddings
    chunk_embeddings = create_rag_chunk_embeddings(db)
    all_embeddings.extend(chunk_embeddings)
    print(f"  ✓ RAG Chunks: {len(chunk_embeddings)} embeddings")
    
    # Insert embeddings
    print("\n" + "=" * 70)
    print("Inserting into vector_embeddings collection")
    print("=" * 70)
    
    try:
        col = db.vector_embeddings
        col.drop()
        if all_embeddings:
            result = col.insert_many(all_embeddings)
            print(f"  ✓ Inserted {len(result.inserted_ids)} embeddings")
        
        # Create index on embedding_id
        col.create_index("embedding_id", unique=True)
        col.create_index("source_type")
        col.create_index("source_id")
        print("  ✓ Created indexes")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Insert index definitions
    print("\n" + "=" * 70)
    print("Inserting Vector Index Definitions")
    print("=" * 70)
    
    try:
        col = db.vector_index_definitions
        col.drop()
        result = col.insert_many(VECTOR_INDEX_DEFINITIONS)
        print(f"  ✓ Inserted {len(result.inserted_ids)} index definitions")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"  vector_embeddings: {db.vector_embeddings.count_documents({})} documents")
    print(f"  vector_index_definitions: {db.vector_index_definitions.count_documents({})} documents")
    
    print("\n" + "=" * 70)
    print("IMPORTANT: Create Atlas Search Indexes Manually")
    print("=" * 70)
    print("""
To enable vector search, create indexes in Atlas UI:

1. Go to Atlas -> Your Cluster -> Search -> Create Index
2. Use JSON Editor and paste:

For vector_embeddings collection:
{
  "fields": [
    {
      "type": "vector",
      "path": "embedding",
      "numDimensions": 1536,
      "similarity": "cosine"
    },
    {
      "type": "filter",
      "path": "source_type"
    }
  ]
}

3. Name it: vector_search_index
4. Select collection: vector_embeddings
5. Click Create

Note: These are MOCK embeddings for demo purposes.
For production, use a real embedding model like OpenAI ada-002.
""")
    
    client.close()
    print("✓ Done!")


if __name__ == "__main__":
    main()
