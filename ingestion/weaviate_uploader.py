#!/usr/bin/env python3
"""
Weaviate Uploader for Knowledge Base

This module handles uploading metadata objects to Weaviate collections,
including DatasetMetadata, DataRelationship, and DomainTag objects.

Usage:
    from ingestion.weaviate_uploader import WeaviateUploader
    uploader = WeaviateUploader()
    success = uploader.upload_dataset_metadata(metadata_list)
"""

import os
import json
import uuid as uuid_lib
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from dotenv import load_dotenv

try:
    from weaviate import WeaviateClient
    from weaviate.connect import ConnectionParams
    print("✅ Weaviate imports successful")
except ImportError as e:
    print(f"❌ Weaviate import error: {e}")
    raise

load_dotenv()

class WeaviateUploader:
    """Handles uploading metadata to Weaviate collections."""
    
    def __init__(self):
        """Initialize Weaviate uploader."""
        self.weaviate_url = os.getenv('WEAVIATE_URL', 'http://localhost:8080')
        self.grpc_port = int(os.getenv('WEAVIATE_GRPC_PORT', '8081'))
        self.client = None
        
        # Track uploaded objects for relationship building
        self.uploaded_datasets = {}  # tableName -> uuid mapping
        
        print(f"🚀 Weaviate Uploader initialized")
        print(f"   Weaviate URL: {self.weaviate_url}")
    
    def connect(self) -> bool:
        """Connect to Weaviate instance."""
        try:
            connection_params = ConnectionParams.from_url(
                url=self.weaviate_url,
                grpc_port=self.grpc_port
            )
            
            self.client = WeaviateClient(connection_params=connection_params)
            self.client.connect()
            
            if self.client.is_ready():
                print(f"✅ Connected to Weaviate at {self.weaviate_url}")
                return True
            else:
                print(f"❌ Weaviate not ready at {self.weaviate_url}")
                return False
                
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from Weaviate."""
        if self.client:
            self.client.close()
            print("🔌 Disconnected from Weaviate")
    
    def generate_consistent_uuid(self, table_name: str, zone: str) -> str:
        """
        Generate consistent UUID for a dataset based on table name and zone.
        
        Args:
            table_name: Name of the table
            zone: Zone (Raw, Cleansed, Curated)
            
        Returns:
            Consistent UUID string
        """
        # Create a consistent identifier
        identifier = f"{table_name}_{zone}".lower()
        
        # Generate UUID5 from identifier (consistent across runs)
        namespace = uuid_lib.NAMESPACE_DNS
        consistent_uuid = uuid_lib.uuid5(namespace, identifier)
        
        return str(consistent_uuid)
    
    def validate_metadata_object(self, metadata: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate metadata object before uploading.
        
        Args:
            metadata: Metadata dictionary
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        required_fields = [
            'tableName', 'zone', 'originalFileName', 'recordCount',
            'columnsArray', 'detailedColumnInfo'
        ]
        
        # Check required fields
        for field in required_fields:
            if field not in metadata or metadata[field] is None:
                errors.append(f"Missing required field: {field}")
            elif field == 'columnsArray' and not isinstance(metadata[field], list):
                errors.append(f"Field {field} must be a list")
            elif field == 'recordCount' and not isinstance(metadata[field], (int, float)):
                errors.append(f"Field {field} must be a number")
        
        # Validate JSON fields
        json_fields = ['detailedColumnInfo', 'answerableQuestions', 'llmHints']
        for field in json_fields:
            if field in metadata and metadata[field]:
                try:
                    if isinstance(metadata[field], str):
                        json.loads(metadata[field])
                    elif not isinstance(metadata[field], (dict, list)):
                        errors.append(f"Field {field} must be JSON string or dict/list")
                except json.JSONDecodeError:
                    errors.append(f"Field {field} contains invalid JSON")
        
        # Validate zone
        valid_zones = ['Raw', 'Cleansed', 'Curated']
        if metadata.get('zone') not in valid_zones:
            errors.append(f"Zone must be one of: {valid_zones}")
        
        return len(errors) == 0, errors
    
    def prepare_dataset_metadata_for_weaviate(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare metadata object for Weaviate ingestion.
        
        Args:
            metadata: Raw metadata dictionary
            
        Returns:
            Weaviate-ready properties dictionary
        """
        # Ensure JSON fields are strings
        def ensure_json_string(value):
            if isinstance(value, str):
                return value
            elif isinstance(value, (dict, list)):
                return json.dumps(value)
            else:
                return str(value) if value is not None else ""
        
        # Ensure lists are properly formatted
        def ensure_list(value):
            if isinstance(value, list):
                return value
            elif isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    return parsed if isinstance(parsed, list) else [value]
                except:
                    return [value]
            else:
                return [str(value)] if value is not None else []
        
        weaviate_props = {
            # Core identity
            "tableName": str(metadata.get('tableName', '')),
            "originalFileName": str(metadata.get('originalFileName', '')),
            "athenaTableName": str(metadata.get('athenaTableName', '')),
            "zone": str(metadata.get('zone', 'Raw')),
            "format": str(metadata.get('format', 'CSV')),
            
            # Semantic content
            "description": str(metadata.get('description', '')),
            "businessPurpose": str(metadata.get('businessPurpose', '')),
            "tags": ensure_list(metadata.get('tags', [])),
            "columnSemanticsConcatenated": str(metadata.get('columnSemanticsConcatenated', '')),
            
            # Structure
            "columnsArray": ensure_list(metadata.get('columnsArray', [])),
            "detailedColumnInfo": ensure_json_string(metadata.get('detailedColumnInfo', '{}')),
            "recordCount": int(metadata.get('recordCount', 0)),
            
            # Governance
            "dataOwner": str(metadata.get('dataOwner', '')),
            "sourceSystem": str(metadata.get('sourceSystem', '')),
            
            # Timestamps - ensure proper format
            "metadataCreatedAt": metadata.get('metadataCreatedAt', datetime.now(timezone.utc).isoformat()),
            "dataLastModifiedAt": metadata.get('dataLastModifiedAt', datetime.now(timezone.utc).isoformat()),
            
            # LLM enhancement
            "llmHints": ensure_json_string(metadata.get('llmHints', '{}')),
            "answerableQuestions": ensure_json_string(metadata.get('answerableQuestions', '[]'))
        }
        
        return weaviate_props
    
    def upload_dataset_metadata(self, metadata_list: List[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
        """
        Upload DatasetMetadata objects to Weaviate.
        
        Args:
            metadata_list: List of metadata dictionaries
            
        Returns:
            Tuple of (success, results_summary)
        """
        if not self.client:
            return False, {"error": "Not connected to Weaviate"}
        
        print(f"\n📤 Uploading {len(metadata_list)} DatasetMetadata objects...")
        
        try:
            collection = self.client.collections.get("DatasetMetadata")
            
            successful_uploads = []
            failed_uploads = []
            
            # Use batch for efficiency
            with collection.batch.dynamic() as batch:
                for i, metadata in enumerate(metadata_list):
                    try:
                        # Validate metadata
                        is_valid, errors = self.validate_metadata_object(metadata)
                        if not is_valid:
                            failed_uploads.append({
                                "index": i,
                                "table_name": metadata.get('tableName', 'Unknown'),
                                "errors": errors
                            })
                            continue
                        
                        # Prepare for Weaviate
                        weaviate_props = self.prepare_dataset_metadata_for_weaviate(metadata)
                        
                        # Generate consistent UUID
                        table_name = weaviate_props['tableName']
                        zone = weaviate_props['zone']
                        consistent_uuid = self.generate_consistent_uuid(table_name, zone)
                        
                        # Add to batch
                        batch.add_object(
                            properties=weaviate_props,
                            uuid=consistent_uuid
                        )
                        
                        # Track for relationship building
                        self.uploaded_datasets[table_name] = consistent_uuid
                        
                        successful_uploads.append({
                            "index": i,
                            "table_name": table_name,
                            "zone": zone,
                            "uuid": consistent_uuid,
                            "record_count": weaviate_props['recordCount']
                        })
                        
                        print(f"   📊 Queued: {table_name} ({zone}) - {weaviate_props['recordCount']:,} records")
                        
                    except Exception as e:
                        failed_uploads.append({
                            "index": i,
                            "table_name": metadata.get('tableName', 'Unknown'),
                            "errors": [f"Upload error: {str(e)}"]
                        })
            
            # Batch results
            print(f"✅ Batch upload completed")
            print(f"   Successful: {len(successful_uploads)}")
            print(f"   Failed: {len(failed_uploads)}")
            
            # Print details
            for upload in successful_uploads:
                print(f"   ✅ {upload['table_name']}: {upload['uuid']}")
            
            for failure in failed_uploads:
                print(f"   ❌ {failure['table_name']}: {'; '.join(failure['errors'])}")
            
            results = {
                "total_attempted": len(metadata_list),
                "successful": len(successful_uploads),
                "failed": len(failed_uploads),
                "successful_uploads": successful_uploads,
                "failed_uploads": failed_uploads,
                "uploaded_datasets_map": self.uploaded_datasets.copy()
            }
            
            return len(failed_uploads) == 0, results
            
        except Exception as e:
            print(f"❌ Batch upload failed: {e}")
            return False, {"error": str(e)}
    
    def upload_relationships(self, relationships_config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Upload DataRelationship objects to Weaviate.
        
        Args:
            relationships_config: Relationships configuration from YAML
            
        Returns:
            Tuple of (success, results_summary)
        """
        if not self.client:
            return False, {"error": "Not connected to Weaviate"}
        
        relationships = relationships_config.get('relationships', [])
        print(f"\n🔗 Uploading {len(relationships)} DataRelationship objects...")
        
        try:
            collection = self.client.collections.get("DataRelationship")
            
            successful_uploads = []
            failed_uploads = []
            
            with collection.batch.dynamic() as batch:
                for i, relationship in enumerate(relationships):
                    try:
                        # Prepare relationship properties
                        rel_props = {
                            "fromTableName": str(relationship.get('from_table', '')),
                            "fromColumn": str(relationship.get('from_column', '')),
                            "toTableName": str(relationship.get('to_table', '')),
                            "toColumn": str(relationship.get('to_column', '')),
                            "relationshipType": str(relationship.get('relationship_type', 'foreign_key')),
                            "cardinality": str(relationship.get('cardinality', 'many-to-one')),
                            "suggestedJoinType": str(relationship.get('suggested_join_type', 'INNER')),
                            "businessMeaning": str(relationship.get('business_meaning', ''))
                        }
                        
                        # Generate UUID for relationship
                        rel_id = f"{rel_props['fromTableName']}.{rel_props['fromColumn']}_to_{rel_props['toTableName']}.{rel_props['toColumn']}"
                        rel_uuid = str(uuid_lib.uuid5(uuid_lib.NAMESPACE_DNS, rel_id))
                        
                        batch.add_object(
                            properties=rel_props,
                            uuid=rel_uuid
                        )
                        
                        successful_uploads.append({
                            "index": i,
                            "relationship": f"{rel_props['fromTableName']} -> {rel_props['toTableName']}",
                            "uuid": rel_uuid
                        })
                        
                        print(f"   🔗 Queued: {rel_props['fromTableName']}.{rel_props['fromColumn']} -> {rel_props['toTableName']}.{rel_props['toColumn']}")
                        
                    except Exception as e:
                        failed_uploads.append({
                            "index": i,
                            "error": str(e)
                        })
            
            print(f"✅ Relationship upload completed")
            print(f"   Successful: {len(successful_uploads)}")
            print(f"   Failed: {len(failed_uploads)}")
            
            results = {
                "total_attempted": len(relationships),
                "successful": len(successful_uploads),
                "failed": len(failed_uploads),
                "successful_uploads": successful_uploads,
                "failed_uploads": failed_uploads
            }
            
            return len(failed_uploads) == 0, results
            
        except Exception as e:
            print(f"❌ Relationship upload failed: {e}")
            return False, {"error": str(e)}
    
    def upload_domain_tags(self, domain_tags_config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Upload DomainTag objects to Weaviate.
        
        Args:
            domain_tags_config: Domain tags configuration from YAML
            
        Returns:
            Tuple of (success, results_summary)
        """
        if not self.client:
            return False, {"error": "Not connected to Weaviate"}
        
        domain_tags = domain_tags_config.get('domain_tags', [])
        print(f"\n🏷️  Uploading {len(domain_tags)} DomainTag objects...")
        
        try:
            collection = self.client.collections.get("DomainTag")
            
            successful_uploads = []
            failed_uploads = []
            
            with collection.batch.dynamic() as batch:
                for i, tag in enumerate(domain_tags):
                    try:
                        tag_props = {
                            "tagName": str(tag.get('tag_name', '')),
                            "tagDescription": str(tag.get('tag_description', '')),
                            "businessPriority": str(tag.get('business_priority', 'Medium')),
                            "dataSensitivity": str(tag.get('data_sensitivity', 'Internal'))
                        }
                        
                        # Generate UUID for tag
                        tag_uuid = str(uuid_lib.uuid5(uuid_lib.NAMESPACE_DNS, tag_props['tagName']))
                        
                        batch.add_object(
                            properties=tag_props,
                            uuid=tag_uuid
                        )
                        
                        successful_uploads.append({
                            "index": i,
                            "tag_name": tag_props['tagName'],
                            "uuid": tag_uuid
                        })
                        
                        print(f"   🏷️  Queued: {tag_props['tagName']}")
                        
                    except Exception as e:
                        failed_uploads.append({
                            "index": i,
                            "tag_name": tag.get('tag_name', 'Unknown'),
                            "error": str(e)
                        })
            
            print(f"✅ Domain tag upload completed")
            print(f"   Successful: {len(successful_uploads)}")
            print(f"   Failed: {len(failed_uploads)}")
            
            results = {
                "total_attempted": len(domain_tags),
                "successful": len(successful_uploads),
                "failed": len(failed_uploads),
                "successful_uploads": successful_uploads,
                "failed_uploads": failed_uploads
            }
            
            return len(failed_uploads) == 0, results
            
        except Exception as e:
            print(f"❌ Domain tag upload failed: {e}")
            return False, {"error": str(e)}
    
    def verify_upload_success(self) -> Dict[str, Any]:
        """
        Verify that uploads were successful by querying Weaviate.
        
        Returns:
            Dictionary with verification results
        """
        if not self.client:
            return {"error": "Not connected to Weaviate"}
        
        print(f"\n🔍 Verifying upload success...")
        
        verification = {}
        
        try:
            # Check each collection
            collections = ["DatasetMetadata", "DataRelationship", "DomainTag"]
            
            for collection_name in collections:
                try:
                    collection = self.client.collections.get(collection_name)
                    result = collection.query.fetch_objects(limit=100)
                    count = len(result.objects)
                    
                    verification[collection_name] = {
                        "count": count,
                        "status": "success"
                    }
                    
                    print(f"   ✅ {collection_name}: {count} objects")
                    
                    # Show sample objects
                    if result.objects:
                        first_obj = result.objects[0]
                        if collection_name == "DatasetMetadata":
                            sample_name = first_obj.properties.get('tableName', 'Unknown')
                        elif collection_name == "DataRelationship":
                            sample_name = f"{first_obj.properties.get('fromTableName', '?')} -> {first_obj.properties.get('toTableName', '?')}"
                        elif collection_name == "DomainTag":
                            sample_name = first_obj.properties.get('tagName', 'Unknown')
                        
                        print(f"      Sample: {sample_name}")
                    
                except Exception as e:
                    verification[collection_name] = {
                        "count": 0,
                        "status": "error",
                        "error": str(e)
                    }
                    print(f"   ❌ {collection_name}: {e}")
            
            return verification
            
        except Exception as e:
            print(f"❌ Verification failed: {e}")
            return {"error": str(e)}


def main():
    """Test the uploader with sample data."""
    uploader = WeaviateUploader()
    
    if uploader.connect():
        try:
            # Test with sample metadata
            sample_metadata = [{
                "tableName": "test_table",
                "originalFileName": "test.csv",
                "zone": "Raw",
                "recordCount": 100,
                "columnsArray": ["col1", "col2"],
                "detailedColumnInfo": '{"columns": [{"name": "col1"}]}',
                "description": "Test table",
                "businessPurpose": "Testing"
            }]
            
            success, results = uploader.upload_dataset_metadata(sample_metadata)
            print(f"Upload test: {'SUCCESS' if success else 'FAILED'}")
            
            # Verify
            verification = uploader.verify_upload_success()
            print(f"Verification: {verification}")
            
        finally:
            uploader.disconnect()
    else:
        print("❌ Could not connect to Weaviate")


if __name__ == "__main__":
    main()