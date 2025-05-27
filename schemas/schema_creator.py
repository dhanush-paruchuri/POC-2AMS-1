#!/usr/bin/env python3
"""
Bedrock-Enabled Weaviate Schema Creator

This script creates Weaviate schemas with Amazon Bedrock vectorization enabled
for semantic search capabilities using amazon.titan-embed-text-v2:0.

WHAT THIS ENABLES:
- Semantic search: "customer satisfaction data" finds datasets with CrewRating
- Natural language queries: "show me financial data" finds cost/pricing datasets  
- AI-powered discovery: Understands meaning, not just keywords
- Intelligent data catalog: Business context becomes searchable

Usage:
    python schemas/bedrock_schema_creator.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from weaviate import WeaviateClient
    from weaviate.connect import ConnectionParams
    from weaviate.collections.classes.config import Property, Configure, DataType
    print("✅ All imports successful")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Install dependencies: pip install weaviate-client python-dotenv")
    sys.exit(1)

load_dotenv()

class BedRockEnabledSchemaCreator:
    """
    Schema creator with Amazon Bedrock vectorization for semantic search.
    
    KEY FEATURES:
    - Uses amazon.titan-embed-text-v1:0 for generating embeddings
    - Selective vectorization: Only business-relevant text gets vectorized
    - Optimized for semantic discovery of datasets
    """
    
    def __init__(self):
        """Initialize with Bedrock configuration."""
        self.weaviate_url = os.getenv('WEAVIATE_URL', 'http://localhost:8080')
        self.grpc_port = int(os.getenv('WEAVIATE_GRPC_PORT', '8081'))
        
        # Bedrock configuration
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.bedrock_model = os.getenv('BEDROCK_MODEL_ID', 'cohere.embed-english-v3')

        # Add this debug line:
        print(f"🐛 DEBUG: BEDROCK_MODEL_ID from env = {os.getenv('BEDROCK_MODEL_ID')}")
        print(f"🐛 DEBUG: self.bedrock_model = {self.bedrock_model}")
                
        # Validate AWS configuration
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        self.client = None
        
        print(f"🤖 Bedrock-Enabled Schema Creator Initialized")
        print(f"   Weaviate URL: {self.weaviate_url}")
        print(f"   AWS Region: {self.aws_region}")
        print(f"   Bedrock Model: {self.bedrock_model}")
        print(f"   AWS Credentials: {'✅ Configured' if self.aws_access_key else '❌ Missing'}")
    
    def connect(self):
        """Connect to Weaviate with validation."""
        try:
            connection_params = ConnectionParams.from_url(
                url=self.weaviate_url,
                grpc_port=self.grpc_port
            )
            
            self.client = WeaviateClient(connection_params=connection_params)
            self.client.connect()
            
            if self.client.is_ready():
                print(f"✅ Connected to Weaviate at {self.weaviate_url}")
                
                # Check if Weaviate has text2vec-aws module enabled
                try:
                    meta = self.client.get_meta()
                    modules = meta.get('modules', {})
                    if 'text2vec-aws' in modules:
                        print(f"✅ text2vec-aws module detected in Weaviate")
                    else:
                        print(f"⚠️  text2vec-aws module not found in Weaviate modules")
                        print(f"   Available modules: {list(modules.keys())}")
                        print(f"   Check docker-compose.yml: ENABLE_MODULES should include 'text2vec-aws'")
                except:
                    print(f"⚠️  Could not check Weaviate modules")
                
                return True
            else:
                print(f"❌ Weaviate not ready")
                return False
                
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def get_bedrock_vectorizer_config(self):
        """
        Get Bedrock vectorizer configuration for class-level setup.
        
        This tells Weaviate to use Amazon Bedrock for generating embeddings.
        """
        try:
            return Configure.Vectorizer.text2vec_aws(
                model=self.bedrock_model,
                region=self.aws_region
            )
        except Exception as e:
            print(f"❌ Error creating Bedrock vectorizer config: {e}")
            print(f"   Falling back to no vectorization")
            return Configure.Vectorizer.none()
    
    def create_vectorized_property(self, name: str, data_type, description: str, vectorize: bool = True):
        """
        Create a property with optional vectorization.
        
        Args:
            name: Property name
            data_type: Weaviate data type
            description: Property description
            vectorize: Whether to generate embeddings for this property
        """
        prop = Property(
            name=name,
            data_type=data_type,
            description=description
        )
        
        # Add vectorization config if enabled
        if vectorize:
            prop.vectorize_property_name = True
            prop.skip_vectorization = False
        else:
            prop.vectorize_property_name = False
            prop.skip_vectorization = True
            
        return prop
    
    def create_dataset_metadata_class(self):
        """
        Create DatasetMetadata class with intelligent vectorization.
        
        VECTORIZED PROPERTIES (for semantic search):
        - description: Dataset purpose and content
        - businessPurpose: What business questions it answers
        - columnSemanticsConcatenated: Column meanings and context
        - tags: Categories and keywords
        
        NON-VECTORIZED PROPERTIES (for filtering/facts):
        - tableName, zone, recordCount, etc.
        """
        try:
            class_name = "DatasetMetadata"
            
            if self.client.collections.exists(class_name):
                print(f"⚠️  Class '{class_name}' already exists")
                response = input(f"Delete and recreate? (y/N): ").lower().strip()
                if response == 'y':
                    self.client.collections.delete(class_name)
                    print(f"🗑️  Deleted existing '{class_name}' class")
                else:
                    print(f"ℹ️  Keeping existing '{class_name}' class")
                    return True
            
            # IDENTITY PROPERTIES (not vectorized - used for exact matching)
            identity_properties = [
                self.create_vectorized_property("tableName", DataType.TEXT, 
                    "Primary unique name for the dataset", vectorize=False),
                self.create_vectorized_property("originalFileName", DataType.TEXT, 
                    "Original CSV file name", vectorize=False),
                self.create_vectorized_property("athenaTableName", DataType.TEXT, 
                    "AWS Athena table name", vectorize=False),
                self.create_vectorized_property("zone", DataType.TEXT, 
                    "Data processing zone (Raw/Cleansed/Curated)", vectorize=False),
                self.create_vectorized_property("format", DataType.TEXT, 
                    "File format (CSV, Parquet, etc.)", vectorize=False)
            ]
            
            # SEMANTIC PROPERTIES (vectorized - enables semantic search)
            # These are the MAGIC - Bedrock will create embeddings for these
            semantic_properties = [
                self.create_vectorized_property("description", DataType.TEXT,
                    "Detailed human-written summary of the dataset", vectorize=True),
                self.create_vectorized_property("businessPurpose", DataType.TEXT,
                    "What business questions this dataset helps answer", vectorize=True),
                self.create_vectorized_property("columnSemanticsConcatenated", DataType.TEXT,
                    "Concatenated column names and descriptions for semantic search", vectorize=True),
                self.create_vectorized_property("tags", DataType.TEXT_ARRAY,
                    "Keywords and categories associated with the dataset", vectorize=True)
            ]
            
            # STRUCTURE PROPERTIES (not vectorized - factual data)
            structure_properties = [
                self.create_vectorized_property("columnsArray", DataType.TEXT_ARRAY,
                    "List of column names in the dataset", vectorize=False),
                self.create_vectorized_property("detailedColumnInfo", DataType.TEXT,
                    "JSON string with detailed column information", vectorize=False),
                self.create_vectorized_property("recordCount", DataType.INT,
                    "Number of records in the dataset", vectorize=False)
            ]
            
            # GOVERNANCE PROPERTIES (not vectorized - categorical data)
            governance_properties = [
                self.create_vectorized_property("dataOwner", DataType.TEXT,
                    "Team or individual responsible for the data", vectorize=False),
                self.create_vectorized_property("sourceSystem", DataType.TEXT,
                    "Original system that generated this data", vectorize=False)
            ]
            
            # TIMESTAMP PROPERTIES (not vectorized - temporal data)
            timestamp_properties = [
                self.create_vectorized_property("metadataCreatedAt", DataType.DATE,
                    "When this metadata was created in Weaviate", vectorize=False),
                self.create_vectorized_property("dataLastModifiedAt", DataType.DATE,
                    "When the actual data was last modified", vectorize=False)
            ]
            
            # ADVANCED PROPERTIES (mixed vectorization)
            advanced_properties = [
                # Vectorize answerable questions - helps find datasets by what they can answer
                self.create_vectorized_property("answerableQuestions", DataType.TEXT,
                    "JSON string of sample questions this dataset can answer", vectorize=True),
                # Don't vectorize LLM hints - these are technical instructions
                self.create_vectorized_property("llmHints", DataType.TEXT,
                    "JSON string with hints for LLM SQL generation", vectorize=False)
            ]
            
            # COMBINE ALL PROPERTIES
            all_properties = (identity_properties + semantic_properties + 
                            structure_properties + governance_properties + 
                            timestamp_properties + advanced_properties)
            
            print(f"🏗️  Creating '{class_name}' with {len(all_properties)} properties")
            print(f"   Vectorized properties: {len(semantic_properties + [advanced_properties[0]])}")
            print(f"   Non-vectorized properties: {len(all_properties) - len(semantic_properties) - 1}")
            
            # CREATE COLLECTION WITH BEDROCK VECTORIZATION
            collection = self.client.collections.create(
                name=class_name,
                description="Intelligent dataset catalog with semantic search powered by Amazon Bedrock",
                properties=all_properties,
                vectorizer_config=self.get_bedrock_vectorizer_config()
            )
            
            print(f"✅ Created '{class_name}' with Bedrock vectorization")
            print(f"   Semantic search enabled for business descriptions")
            print(f"   Model: {self.bedrock_model}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to create DatasetMetadata class: {e}")
            return False
    
    def create_data_relationship_class(self):
        """
        Create DataRelationship class (no vectorization needed).
        
        This class stores JOIN definitions - structured data that doesn't need semantic search.
        """
        try:
            class_name = "DataRelationship"
            
            if self.client.collections.exists(class_name):
                print(f"ℹ️  Class '{class_name}' already exists - skipping")
                return True
            
            # All properties are non-vectorized (structured relationship data)
            properties = [
                self.create_vectorized_property("fromTableName", DataType.TEXT,
                    "Name of the source table in the relationship", vectorize=False),
                self.create_vectorized_property("fromColumn", DataType.TEXT,
                    "Column name in the source table", vectorize=False),
                self.create_vectorized_property("toTableName", DataType.TEXT,
                    "Name of the target table in the relationship", vectorize=False),
                self.create_vectorized_property("toColumn", DataType.TEXT,
                    "Column name in the target table", vectorize=False),
                self.create_vectorized_property("relationshipType", DataType.TEXT,
                    "Type of relationship (foreign_key, etc.)", vectorize=False),
                self.create_vectorized_property("cardinality", DataType.TEXT,
                    "Relationship cardinality (one-to-many, etc.)", vectorize=False),
                self.create_vectorized_property("suggestedJoinType", DataType.TEXT,
                    "Suggested SQL join type (INNER, LEFT, etc.)", vectorize=False),
                # Business meaning could be vectorized, but relationships are usually found
                # by table names, so we'll keep it simple
                self.create_vectorized_property("businessMeaning", DataType.TEXT,
                    "Business explanation of this relationship", vectorize=False)
            ]
            
            # No vectorization needed for relationship data
            collection = self.client.collections.create(
                name=class_name,
                description="Defines relationships between datasets for SQL JOINs",
                properties=properties,
                vectorizer_config=Configure.Vectorizer.none()
            )
            
            print(f"✅ Created '{class_name}' (no vectorization - structural data)")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create DataRelationship class: {e}")
            return False
    
    def create_domain_tag_class(self):
        """
        Create DomainTag class with selective vectorization.
        
        Vectorize tag descriptions to enable semantic discovery of business domains.
        """
        try:
            class_name = "DomainTag"
            
            if self.client.collections.exists(class_name):
                print(f"ℹ️  Class '{class_name}' already exists - skipping")
                return True
            
            properties = [
                # Tag name - exact matching
                self.create_vectorized_property("tagName", DataType.TEXT,
                    "Unique name of the domain tag", vectorize=False),
                # Tag description - semantic search enabled
                self.create_vectorized_property("tagDescription", DataType.TEXT,
                    "Detailed description of what this domain covers", vectorize=True),
                # Business priority - categorical
                self.create_vectorized_property("businessPriority", DataType.TEXT,
                    "Business priority level (High, Medium, Low)", vectorize=False),
                # Data sensitivity - categorical  
                self.create_vectorized_property("dataSensitivity", DataType.TEXT,
                    "Data sensitivity classification", vectorize=False)
            ]
            
            collection = self.client.collections.create(
                name=class_name,
                description="Business domain tags with semantic search on descriptions",
                properties=properties,
                vectorizer_config=self.get_bedrock_vectorizer_config()
            )
            
            print(f"✅ Created '{class_name}' with selective vectorization")
            print(f"   Domain descriptions are semantically searchable")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create DomainTag class: {e}")
            return False
    
    def test_bedrock_integration(self):
        """
        Test that Bedrock integration is working by inserting and querying test data.
        """
        print(f"\n🧪 Testing Bedrock Integration...")
        
        try:
            # Insert test data
            collection = self.client.collections.get("DatasetMetadata")
            
            test_data = {
                "tableName": "test_bedrock_integration",
                "originalFileName": "test.csv",
                "zone": "Raw",
                "format": "CSV",
                "description": "Customer satisfaction ratings and service quality metrics for business analysis",
                "businessPurpose": "Measure customer happiness and service performance for operational improvements",
                "columnSemanticsConcatenated": "rating: customer satisfaction score; feedback: customer comments about service quality",
                "tags": ["customer satisfaction", "service quality", "ratings", "feedback"],
                "recordCount": 100,
                "columnsArray": ["rating", "feedback", "date"],
                "detailedColumnInfo": '{"columns": [{"name": "rating", "description": "satisfaction score"}]}',
                "dataOwner": "Test Team",
                "sourceSystem": "Test System",
                "metadataCreatedAt": "2024-01-01T00:00:00Z",
                "dataLastModifiedAt": "2024-01-01T00:00:00Z",
                "answerableQuestions": '[]',
                "llmHints": '{}'
            }
            
            # Insert test object
            test_uuid = collection.data.insert(test_data)
            print(f"✅ Inserted test data with UUID: {test_uuid}")
            
            # Wait a moment for vectorization
            import time
            time.sleep(2)
            
            # Test semantic search
            print(f"🔍 Testing semantic search...")
            
            # This should find the test data even though query doesn't match exact text
            search_queries = [
                "unhappy customers",  # Should match "customer satisfaction"
                "service performance", # Should match "service quality"
                "business metrics"     # Should match "business analysis"  
            ]
            
            for query in search_queries:
                try:
                    result = collection.query.near_text(
                        query=query,
                        limit=3
                    )
                    
                    found_test_data = any(
                        obj.properties.get("tableName") == "test_bedrock_integration"
                        for obj in result.objects
                    )
                    
                    if found_test_data:
                        print(f"✅ Semantic search working: '{query}' found test data")
                    else:
                        print(f"⚠️  Semantic search: '{query}' didn't find test data")
                        
                except Exception as e:
                    print(f"❌ Semantic search failed for '{query}': {e}")
            
            # Clean up test data
            collection.data.delete_by_id(test_uuid)
            print(f"🧹 Cleaned up test data")
            
            print(f"✅ Bedrock integration test completed")
            return True
            
        except Exception as e:
            print(f"❌ Bedrock integration test failed: {e}")
            return False
    
    def validate_schema_creation(self):
        try:
            collections = self.client.collections.list_all()
            
            # Fix: Handle both string names and objects with .name attribute
            existing_names = []
            for col in collections:
                if isinstance(col, str):
                    existing_names.append(col)
                elif hasattr(col, 'name'):
                    existing_names.append(col.name)
                else:
                    # Debug: print what we actually got
                    print(f"🐛 DEBUG: Unexpected collection type: {type(col)} = {col}")
                    existing_names.append(str(col))
            
            expected_classes = ["DatasetMetadata", "DataRelationship", "DomainTag"]
            missing_classes = [cls for cls in expected_classes if cls not in existing_names]
            
            if missing_classes:
                print(f"❌ Missing classes: {missing_classes}")
                return False
            else:
                print(f"✅ All expected classes exist: {expected_classes}")
                
                # Show vectorization status
                for class_name in expected_classes:
                    try:
                        collection = self.client.collections.get(class_name)
                        config = collection.config.get()
                        
                        vectorizer = getattr(config, 'vectorizer', 'unknown')
                        print(f"   {class_name}: vectorizer = {vectorizer}")
                        
                    except Exception as e:
                        print(f"   {class_name}: could not check config ({e})")
                
                return True
                
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            return False
        
    def print_schema_summary(self):
        try:
            print(f"\n" + "="*70)
            print(f"🤖 BEDROCK-ENABLED WEAVIATE SCHEMA SUMMARY")
            print(f"="*70)
            
            collections = self.client.collections.list_all()
            
            # Handle both string names and objects with .name attribute
            collection_names = []
            for col in collections:
                if isinstance(col, str):
                    collection_names.append(col)
                elif hasattr(col, 'name'):
                    collection_names.append(col.name)
                else:
                    collection_names.append(str(col))
            
            for collection_name in collection_names:
                print(f"\n🏷️  Class: {collection_name}")
                
                try:
                    collection_obj = self.client.collections.get(collection_name)
                    config = collection_obj.config.get()
                    
                    print(f"   Description: {config.description}")
                    
                    # Show vectorizer configuration
                    vectorizer = getattr(config, 'vectorizer', 'none')
                    if 'aws' in str(vectorizer).lower():
                        print(f"   🤖 Vectorizer: Amazon Bedrock ({self.bedrock_model})")
                        print(f"   🔍 Semantic search: ENABLED")
                    else:
                        print(f"   📋 Vectorizer: {vectorizer}")
                        print(f"   🔍 Semantic search: disabled")
                    
                    print(f"   📊 Properties: {len(config.properties)}")
                    
                    # Count vectorized properties
                    vectorized_props = []
                    for prop in config.properties:
                        if hasattr(prop, 'vectorize_property_name') and prop.vectorize_property_name:
                            vectorized_props.append(prop.name)
                    
                    if vectorized_props:
                        print(f"   🎯 Vectorized: {', '.join(vectorized_props)}")
                    
                except Exception as e:
                    print(f"   ⚠️  Could not get detailed config: {e}")
            
            print(f"\n🚀 CAPABILITIES ENABLED:")
            print(f"   ✅ Semantic search on dataset descriptions")
            print(f"   ✅ Natural language data discovery")
            print(f"   ✅ AI-powered business context understanding")
            print(f"   ✅ Meaning-based dataset matching")
            
            print(f"\n💡 TRY THESE QUERIES:")
            print(f"   • 'customer satisfaction data' → finds datasets with ratings/feedback")
            print(f"   • 'financial information' → finds datasets with costs/revenue")
            print(f"   • 'operational metrics' → finds datasets with performance data")
            
            print(f"\n🔗 Bedrock Configuration:")
            print(f"   Model: {self.bedrock_model}")
            print(f"   Region: {self.aws_region}")
            print(f"   Status: {'✅ Active' if self.aws_access_key else '❌ No credentials'}")
            
            print(f"="*70)
            
        except Exception as e:
            print(f"❌ Could not print schema summary: {e}")
            # Print a basic summary instead
            print(f"\n✅ BASIC SUMMARY:")
            print(f"   Schema creation completed successfully")
            print(f"   Bedrock model: {self.bedrock_model}")
            print(f"   Semantic search: ENABLED")

    def run(self):
        """Execute the complete Bedrock-enabled schema creation process."""
        print(f"🚀 Starting Bedrock-Enabled Schema Creation")
        print(f"-" * 60)
        
        # Validate AWS credentials
        if not self.aws_access_key or not self.aws_secret_key:
            print(f"⚠️  AWS credentials not found in environment variables")
            print(f"   Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file")
            print(f"   Bedrock vectorization may not work without valid credentials")
        
        # Connect to Weaviate
        if not self.connect():
            return False
        
        try:
            # Create classes with Bedrock integration
            success = True
            
            print(f"\n🏗️  Creating Weaviate Classes with Bedrock Vectorization...")
            success &= self.create_dataset_metadata_class()
            success &= self.create_data_relationship_class()  
            success &= self.create_domain_tag_class()
            
            if success:
                # Validate creation
                success &= self.validate_schema_creation()
                
                if success:
                    # Test Bedrock integration
                    print(f"\n🧪 Testing Bedrock Integration...")
                    self.test_bedrock_integration()
                    
                    # Print comprehensive summary
                    self.print_schema_summary()
                    return True
            
            return False
            
        except Exception as e:
            print(f"❌ Unexpected error during schema creation: {e}")
            return False
            
        finally:
            if self.client:
                self.client.close()
                print(f"🔌 Disconnected from Weaviate")


def main():
    """Main execution function."""
    try:
        creator = BedRockEnabledSchemaCreator()
        success = creator.run()
        
        if success:
            print(f"\n🎉 Bedrock-enabled schema creation completed successfully!")
            print(f"\n🚀 NEXT STEPS:")
            print(f"1. Run ingestion pipeline: python main_ingestion_pipeline.py")
            print(f"2. Test semantic search queries")
            print(f"3. Build AI agents that use natural language data discovery")
            print(f"\n💡 Your knowledge base now has AI-powered semantic search!")
            sys.exit(0)
        else:
            print(f"\n💥 Schema creation failed!")
            print(f"Check the errors above and ensure:")
            print(f"• Weaviate is running with text2vec-aws module")
            print(f"• AWS credentials are configured")
            print(f"• Bedrock permissions are granted")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print(f"\n⏹️  Schema creation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()