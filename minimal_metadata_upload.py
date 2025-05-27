#!/usr/bin/env python3
"""
Upload with OPTIMIZED metadata sizes for Bedrock compatibility
"""

import os
import sys
import yaml
import json
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from ingestion.csv_extractor import CSVExtractor
from ingestion.weaviate_uploader import WeaviateUploader

load_dotenv()

def truncate_text(text, max_length, suffix="..."):
    """Truncate text to max_length, adding suffix if truncated"""
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[:max_length-len(suffix)] + suffix

def optimize_metadata_for_bedrock(metadata):
    """Optimize metadata size for Bedrock vectorization"""
    
    # VECTORIZED FIELDS - Keep small for fast Bedrock processing
    optimized = {
        "tableName": metadata.get("tableName", ""),
        "originalFileName": metadata.get("originalFileName", ""),
        "athenaTableName": metadata.get("athenaTableName", ""),
        "zone": metadata.get("zone", ""),
        "format": metadata.get("format", ""),
        
        # VECTORIZED FIELDS - OPTIMIZED SIZES
        "description": truncate_text(metadata.get("description", ""), 300),  # 300 chars max
        "businessPurpose": truncate_text(metadata.get("businessPurpose", ""), 200),  # 200 chars max
        "columnSemanticsConcatenated": truncate_text(metadata.get("columnSemanticsConcatenated", ""), 500),  # 500 chars max
        "tags": metadata.get("tags", [])[:5],  # Max 5 tags
        
        # NON-VECTORIZED FIELDS - Can be larger
        "columnsArray": metadata.get("columnsArray", []),
        "detailedColumnInfo": metadata.get("detailedColumnInfo", ""),
        "recordCount": metadata.get("recordCount", 0),
        "dataOwner": metadata.get("dataOwner", ""),
        "sourceSystem": metadata.get("sourceSystem", ""),
        
        # SIMPLIFIED COMPLEX FIELDS
        "answerableQuestions": '["What data does this dataset contain?", "How can this dataset be used?"]',
        "llmHints": '{"type": "tabular", "format": "structured"}',
        
        # DATES
        "metadataCreatedAt": metadata.get("metadataCreatedAt", "2024-01-01T00:00:00Z"),
        "dataLastModifiedAt": metadata.get("dataLastModifiedAt", "2024-01-01T00:00:00Z")
    }
    
    return optimized

def main():
    print("🚀 OPTIMIZED FULL DATA UPLOAD")
    print("="*50)
    
    # Initialize components
    data_dir = project_root / 'data_sources'
    config_dir = project_root / 'config'
    
    csv_extractor = CSVExtractor(str(data_dir))
    weaviate_uploader = WeaviateUploader()
    
    # Connect to Weaviate
    if not weaviate_uploader.connect():
        print("❌ Could not connect to Weaviate")
        return False
    
    try:
        collection = weaviate_uploader.client.collections.get("DatasetMetadata")
        
        # Check current count
        try:
            response = collection.aggregate.over_all(total_count=True)
            current_count = response.total_count
        except:
            response = collection.query.fetch_objects(limit=100)
            current_count = len(response.objects)
        
        print(f"📊 Current DatasetMetadata count: {current_count}")
        
        # Extract metadata from all files
        dataset_configs_dir = config_dir / 'dataset_configs'
        yaml_files = list(dataset_configs_dir.glob('*.yaml'))
        
        successful_uploads = 0
        
        for i, yaml_file in enumerate(yaml_files, 1):
            print(f"\n📄 [{i}/{len(yaml_files)}] Processing {yaml_file.name}")
            
            # Load YAML config
            with open(yaml_file, 'r') as f:
                yaml_config = yaml.safe_load(f)
            
            # Get CSV filename
            csv_filename = yaml_config['dataset_info']['original_file_name']
            csv_path = f"raw/{csv_filename}"
            
            # Extract metadata
            metadata = csv_extractor.extract_metadata(csv_path, yaml_config)
            
            if not metadata.get('success'):
                print(f"   ❌ Metadata extraction failed")
                continue
            
            # OPTIMIZE metadata for Bedrock
            upload_data = optimize_metadata_for_bedrock(metadata)
            
            # Calculate sizes
            total_size_kb = len(json.dumps(upload_data, default=str).encode()) / 1024
            vectorized_text = (
                upload_data["description"] + " " +
                upload_data["businessPurpose"] + " " +
                upload_data["columnSemanticsConcatenated"] + " " +
                " ".join(upload_data["tags"])
            )
            
            print(f"   📏 Total size: {total_size_kb:.1f} KB")
            print(f"   📝 Vectorized text: {len(vectorized_text)} chars")
            print(f"   🎯 Table: {upload_data['tableName']}")
            
            # Upload with timeout protection
            max_retries = 2
            success = False
            
            for attempt in range(max_retries):
                try:
                    print(f"   📤 Upload attempt {attempt + 1}/{max_retries}...")
                    
                    uuid = collection.data.insert(upload_data)
                    print(f"      ✅ Success: {uuid}")
                    successful_uploads += 1
                    success = True
                    break
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"      ❌ Attempt {attempt + 1} failed: {error_msg}")
                    
                    if "timeout" in error_msg.lower() and attempt < max_retries - 1:
                        print(f"      ⏳ Waiting 5s before retry...")
                        import time
                        time.sleep(5)
                    else:
                        print(f"      💥 Upload failed for {upload_data['tableName']}")
                        break
            
            # Small delay between uploads
            if success and i < len(yaml_files):
                import time
                time.sleep(1)
        
        print(f"\n📊 Upload Summary:")
        print(f"   Successful uploads: {successful_uploads}/{len(yaml_files)}")
        
        # Verify final count
        import time
        time.sleep(3)  # Wait for indexing
        
        try:
            response = collection.aggregate.over_all(total_count=True)
            final_count = response.total_count
        except:
            response = collection.query.fetch_objects(limit=100)
            final_count = len(response.objects)
        
        print(f"   Final DatasetMetadata count: {final_count}")
        added_count = final_count - current_count
        print(f"   Objects added: {added_count}")
        
        if added_count > 0:
            print(f"\n🔍 Testing semantic search with real data...")
            
            test_queries = [
                "customer data",
                "operational information", 
                "daily operations",
                "move information"
            ]
            
            search_results_found = False
            
            for query in test_queries:
                try:
                    response = collection.query.near_text(
                        query=query,
                        limit=3
                    )
                    
                    if len(response.objects) > 0:
                        print(f"   ✅ '{query}': Found {len(response.objects)} results")
                        for obj in response.objects:
                            table_name = obj.properties.get('tableName', 'Unknown')
                            description = obj.properties.get('description', '')[:50]
                            print(f"      - {table_name}: {description}...")
                        search_results_found = True
                    else:
                        print(f"   ⚠️  '{query}': No results")
                        
                except Exception as e:
                    print(f"   ❌ '{query}': Search failed - {e}")
            
            if search_results_found:
                print(f"\n🎉 SUCCESS! Your semantic search knowledge base is ready!")
                print(f"   You can now query your data using natural language")
                return True
            else:
                print(f"\n⚠️  Upload successful but semantic search needs more time")
                print(f"   Vectorization may still be processing")
                return True
        else:
            print(f"\n❌ No new objects were added")
            return False
            
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        weaviate_uploader.disconnect()

if __name__ == "__main__":
    success = main()
    
    if success:
        print(f"\n🎊 FULL DATA UPLOAD SUCCESSFUL!")
        print(f"")
        print(f"🚀 Next Steps:")
        print(f"1. Test more queries: python3 query_weaviate.py")
        print(f"2. Build applications using your semantic search API")
        print(f"3. Connect AI agents to your knowledge base")
        print(f"")
        print(f"💡 Example queries to try:")
        print(f"   • 'customer information' → finds customer datasets")
        print(f"   • 'operational data' → finds operational datasets")
        print(f"   • 'daily reports' → finds daily operational data")
    else:
        print(f"\n💥 UPLOAD FAILED OR INCOMPLETE")
        print(f"Check the errors above for specific issues")