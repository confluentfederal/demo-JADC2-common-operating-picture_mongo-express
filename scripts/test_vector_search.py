#!/usr/bin/env python3
"""Test vector search on Atlas"""

import sys
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer

# Load model
print("Loading model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

# Query
query = sys.argv[1] if len(sys.argv) > 1 else "Russian cyber attack on military systems"
print(f"Query: {query}")
query_vector = model.encode(query).tolist()

# Connect
client = MongoClient('mongodb+srv://workshop_db_user:N7ycY3fTi7wZtrIq@workshop.gmk6gb.mongodb.net/jadc2_cop?appName=workshop')
db = client['jadc2_cop']

# Vector search
print("\nSearching...")
results = db.vector_embeddings.aggregate([
    {
        "$vectorSearch": {
            "index": "vector_search_index",
            "path": "embedding",
            "queryVector": query_vector,
            "numCandidates": 100,
            "limit": 5
        }
    },
    {"$project": {"embedding": 0, "score": {"$meta": "vectorSearchScore"}}}
])

print("\nTop 5 matches:")
print("=" * 70)
for r in results:
    score = r.get('score', 0)
    src_type = r.get('source_type', 'UNKNOWN')
    text = r.get('text_content', '')[:80]
    print(f"[{score:.3f}] {src_type}: {text}...")

client.close()
