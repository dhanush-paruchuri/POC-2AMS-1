#!/usr/bin/env python3
"""
Enhanced query script that shows column information in responses
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams

load_dotenv()

def connect_to_weaviate():
    """Connect to Weaviate"""
    weaviate_url = os.getenv('WEAVIATE_URL', 'http://localhost:8080')
    grpc_port = int(os.getenv('WEAVIATE_GRPC_PORT', '8081'))
    
    connection_params = ConnectionParams.from_url(
        url=weaviate_url,
        grpc_port=grpc_port
    )
    
    client = WeaviateClient(connection_params=connection_params)
    client.connect()
    
    if client.is_ready():
        return client
    else:
        return None

def search_with_columns(query, show_all_columns=False):
    """Search and show relevant column information"""
    print(f"\n🔍 Query: '{query}'")
    print("-" * 50)
    
    client = connect_to_weaviate()
    if not client:
        print("❌ Could not connect to Weaviate")
        return
    
    try:
        collection = client.collections.get("DatasetMetadata")
        
        # Semantic search
        response = collection.query.near_text(
            query=query,
            limit=3
        )
        
        if len(response.objects) == 0:
            print("❌ No datasets found")
            return
        
        print(f"✅ Found {len(response.objects)} relevant datasets:")
        
        for i, obj in enumerate(response.objects, 1):
            props = obj.properties
            table_name = props.get('tableName', 'Unknown')
            description = props.get('description', '')[:100]
            columns = props.get('columnsArray', [])
            record_count = props.get('recordCount', 0)
            
            print(f"\n📊 [{i}] {table_name}")
            print(f"    📝 Description: {description}...")
            print(f"    📈 Records: {record_count:,}")
            print(f"    📋 Total Columns: {len(columns)}")
            
            if show_all_columns:
                print(f"    📋 All Columns:")
                for j, col in enumerate(columns, 1):
                    print(f"        {j:2d}. {col}")
            else:
                # Show columns that might be relevant to the query
                query_words = query.lower().split()
                relevant_columns = []
                
                # Simple relevance matching
                for col in columns:
                    col_lower = col.lower()
                    if any(word in col_lower for word in query_words):
                        relevant_columns.append(col)
                
                # Also check for common patterns
                if 'email' in query.lower():
                    relevant_columns.extend([col for col in columns if 'email' in col.lower() or 'mail' in col.lower()])
                if 'phone' in query.lower():
                    relevant_columns.extend([col for col in columns if 'phone' in col.lower() or 'tel' in col.lower()])
                if 'id' in query.lower():
                    relevant_columns.extend([col for col in columns if col.lower().endswith('id') or col.lower().startswith('id')])
                if 'date' in query.lower():
                    relevant_columns.extend([col for col in columns if 'date' in col.lower() or 'time' in col.lower()])
                if 'address' in query.lower():
                    relevant_columns.extend([col for col in columns if 'address' in col.lower() or 'location' in col.lower()])
                if 'price' in query.lower() or 'cost' in query.lower():
                    relevant_columns.extend([col for col in columns if 'price' in col.lower() or 'cost' in col.lower() or 'charge' in col.lower()])
                
                # Remove duplicates and limit
                relevant_columns = list(dict.fromkeys(relevant_columns))[:10]
                
                if relevant_columns:
                    print(f"    🎯 Relevant Columns:")
                    for col in relevant_columns:
                        print(f"        • {col}")
                else:
                    # Show first few columns as fallback
                    print(f"    📋 Sample Columns:")
                    for col in columns[:5]:
                        print(f"        • {col}")
                    if len(columns) > 5:
                        print(f"        ... and {len(columns)-5} more")
    
    except Exception as e:
        print(f"❌ Search failed: {e}")
    
    finally:
        client.close()

def main():
    print("🔍 COLUMN-AWARE SEMANTIC SEARCH")
    print("="*50)
    print("This tool finds datasets and shows relevant column information")
    print()
    
    # Predefined useful queries
    sample_queries = [
        "what is the email address of the customer with the id 1234567890",
    ]
    
    print("📋 Sample Queries:")
    for i, query in enumerate(sample_queries, 1):
        print(f"   {i}. {query}")
    
    print("\n" + "="*50)
    
    # Run sample searches
    for query in sample_queries[:4]:  # Show first 4 as examples
        search_with_columns(query)
    
    print(f"\n💡 Usage Examples:")
    print(f"   python3 column_aware_query.py")
    print(f"   Then modify the queries in the script or add interactive input")

if __name__ == "__main__":
    main()