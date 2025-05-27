#!/usr/bin/env python3
"""
Check what column information is stored in Weaviate DatasetMetadata objects
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
        print(f"✅ Connected to Weaviate at {weaviate_url}")
        return client
    else:
        print(f"❌ Could not connect to Weaviate")
        return None

def check_column_information():
    """Check what column information is stored in DatasetMetadata objects"""
    print("🔍 CHECKING COLUMN INFORMATION IN WEAVIATE")
    print("="*60)
    
    client = connect_to_weaviate()
    if not client:
        return
    
    try:
        collection = client.collections.get("DatasetMetadata")
        
        # Get all DatasetMetadata objects
        response = collection.query.fetch_objects(limit=10)
        
        print(f"📊 Found {len(response.objects)} DatasetMetadata objects")
        
        for i, obj in enumerate(response.objects, 1):
            props = obj.properties
            table_name = props.get('tableName', 'Unknown')
            
            print(f"\n📋 [{i}] Dataset: {table_name}")
            print(f"    UUID: {obj.uuid}")
            
            # Check column-related properties
            columns_array = props.get('columnsArray', [])
            detailed_column_info = props.get('detailedColumnInfo', '')
            column_semantics = props.get('columnSemanticsConcatenated', '')
            
            print(f"    📝 Column Count: {len(columns_array)}")
            
            if columns_array:
                print(f"    📋 Column Names ({len(columns_array)} total):")
                # Show first 10 columns to avoid cluttering
                for j, col in enumerate(columns_array[:10]):
                    print(f"       {j+1:2d}. {col}")
                if len(columns_array) > 10:
                    print(f"       ... and {len(columns_array)-10} more columns")
            else:
                print(f"    ❌ No columns in columnsArray")
            
            # Check detailed column info
            if detailed_column_info:
                try:
                    detailed_info = json.loads(detailed_column_info)
                    if 'columns' in detailed_info:
                        detailed_columns = detailed_info['columns']
                        print(f"    📊 Detailed Column Info: {len(detailed_columns)} columns with metadata")
                        
                        # Show first few detailed columns
                        for j, col_info in enumerate(detailed_columns[:3]):
                            col_name = col_info.get('name', 'Unknown')
                            col_type = col_info.get('type', 'Unknown')
                            col_desc = col_info.get('description', 'No description')[:50]
                            print(f"       • {col_name} ({col_type}): {col_desc}...")
                        if len(detailed_columns) > 3:
                            print(f"       ... and {len(detailed_columns)-3} more detailed columns")
                    else:
                        print(f"    📊 Detailed Column Info: Raw JSON data available")
                except json.JSONDecodeError:
                    print(f"    📊 Detailed Column Info: Raw text data available")
            else:
                print(f"    ❌ No detailed column information")
            
            # Check column semantics (for vectorized search)
            if column_semantics:
                semantics_length = len(column_semantics)
                print(f"    🎯 Column Semantics: {semantics_length} characters")
                if semantics_length > 100:
                    print(f"       Preview: {column_semantics[:100]}...")
                else:
                    print(f"       Content: {column_semantics}")
            else:
                print(f"    ❌ No column semantics for search")
        
        # Test semantic search for specific columns
        print(f"\n🔍 TESTING COLUMN-SPECIFIC SEMANTIC SEARCH:")
        print("-" * 50)
        
        column_queries = [
            "email address",
            "customer ID", 
            "phone number",
            "pricing information",
            "date columns",
            "location data"
        ]
        
        for query in column_queries:
            try:
                response = collection.query.near_text(
                    query=query,
                    limit=2
                )
                
                if len(response.objects) > 0:
                    print(f"✅ Query '{query}':")
                    for obj in response.objects:
                        table_name = obj.properties.get('tableName', 'Unknown')
                        columns = obj.properties.get('columnsArray', [])
                        print(f"   📋 {table_name}: {len(columns)} columns")
                        
                        # Show relevant columns (simple text search in column names)
                        relevant_cols = [col for col in columns if any(word in col.lower() for word in query.lower().split())]
                        if relevant_cols:
                            print(f"      🎯 Relevant columns: {', '.join(relevant_cols[:5])}")
                else:
                    print(f"❌ Query '{query}': No results found")
                    
            except Exception as e:
                print(f"❌ Query '{query}' failed: {e}")
        
        print(f"\n🎯 COLUMN SEARCH RECOMMENDATIONS:")
        print("-" * 40)
        print("For better column-level semantic search, ensure:")
        print("1. columnSemanticsConcatenated contains column descriptions")
        print("2. detailedColumnInfo has structured column metadata") 
        print("3. Column names are descriptive and searchable")
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        client.close()
        print(f"\n🔌 Disconnected from Weaviate")

if __name__ == "__main__":
    check_column_information()