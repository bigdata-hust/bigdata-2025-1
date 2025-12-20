"""
Elasticsearch Integration Module
Save Spark DataFrame Analysis Results to Elasticsearch
"""

import os
import json
import requests
from datetime import datetime
from pyspark.sql import DataFrame


class ElasticsearchSaver:
    """
    Save Spark DataFrames to Elasticsearch using Bulk API
    Compatible with both local and Docker deployments
    """

    def __init__(self, es_host="localhost", es_port=9200):
        """
        Initialize Elasticsearch connection

        Args:
            es_host: Elasticsearch hostname (default: localhost)
            es_port: Elasticsearch port (default: 9200)
        """
        # Check environment variables first (for Docker)
        self.es_host = os.getenv("ELASTICSEARCH_HOST", es_host)
        self.es_port = os.getenv("ELASTICSEARCH_PORT", str(es_port))
        self.es_url = f"http://{self.es_host}:{self.es_port}"

        print(f"\n{'='*60}")
        print(f"Elasticsearch Saver Initialized")
        print(f"URL: {self.es_url}")
        print(f"{'='*60}\n")

    def test_connection(self):
        """Test Elasticsearch connection"""
        try:
            response = requests.get(self.es_url)
            if response.status_code == 200:
                info = response.json()
                print(f"✓ Connected to Elasticsearch {info['version']['number']}")
                return True
            else:
                print(f"✗ Elasticsearch connection failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ Cannot connect to Elasticsearch: {str(e)}")
            return False

    def create_index(self, index_name, mapping=None):
        """
        Create Elasticsearch index with optional mapping

        Args:
            index_name: Name of the index
            mapping: Index mapping configuration (optional)
        """
        try:
            # Check if index exists
            response = requests.head(f"{self.es_url}/{index_name}")

            if response.status_code == 200:
                print(f"ℹ Index '{index_name}' already exists")
                return True

            # Create index
            if mapping:
                response = requests.put(
                    f"{self.es_url}/{index_name}",
                    json=mapping,
                    headers={"Content-Type": "application/json"}
                )
            else:
                response = requests.put(f"{self.es_url}/{index_name}")

            if response.status_code in [200, 201]:
                print(f"✓ Index '{index_name}' created successfully")
                return True
            else:
                print(f"✗ Failed to create index '{index_name}': {response.text}")
                return False

        except Exception as e:
            print(f"✗ Error creating index: {str(e)}")
            return False

    def delete_index(self, index_name):
        """Delete an index (useful for testing)"""
        try:
            response = requests.delete(f"{self.es_url}/{index_name}")
            if response.status_code in [200, 404]:
                print(f"✓ Index '{index_name}' deleted")
                return True
            else:
                print(f"✗ Failed to delete index: {response.text}")
                return False
        except Exception as e:
            print(f"✗ Error deleting index: {str(e)}")
            return False

    def save_dataframe(self, df, index_name, batch_id=None, mode="append"):
        """
        Save Spark DataFrame to Elasticsearch

        Args:
            df: Spark DataFrame to save
            index_name: Elasticsearch index name
            batch_id: Optional batch identifier
            mode: 'append' or 'overwrite' (default: append)
        """
        print(f"\n{'='*60}")
        print(f"Saving to Elasticsearch: {index_name}")
        if batch_id is not None:
            print(f"Batch ID: {batch_id}")
        print(f"Mode: {mode}")
        print(f"{'='*60}\n")

        # If overwrite mode, delete existing index
        if mode == "overwrite":
            self.delete_index(index_name)
            self.create_index(index_name)

        # Save using foreachPartition for better performance
        es_url = self.es_url

        def send_partition(rows):
            """Send a partition of data to Elasticsearch using Bulk API"""
            bulk_data = ""

            for row in rows:
                # Convert row to dictionary
                doc = row.asDict(recursive=True)

                # Add metadata for indexing
                bulk_data += json.dumps({"index": {}}) + "\n"

                # Add document (handle datetime serialization)
                bulk_data += json.dumps(
                    doc,
                    default=lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x)
                ) + "\n"

            # Send bulk request if we have data
            if bulk_data.strip():
                try:
                    response = requests.post(
                        f"{es_url}/{index_name}/_bulk",
                        data=bulk_data,
                        headers={"Content-Type": "application/x-ndjson"}
                    )

                    if response.status_code >= 300:
                        print(f"✗ ES bulk error for {index_name}: {response.text}")
                    else:
                        result = response.json()
                        if result.get("errors"):
                            print(f"⚠ Some documents failed to index in {index_name}")
                        else:
                            print(f"✓ Bulk insert successful for {index_name}")

                except Exception as e:
                    print(f"✗ Error sending bulk data: {str(e)}")

        # Execute save
        try:
            df.foreachPartition(send_partition)
            print(f"\n✓ Successfully saved to index: {index_name}\n")
            return True
        except Exception as e:
            print(f"\n✗ Error saving DataFrame: {str(e)}\n")
            return False

    def get_index_count(self, index_name):
        """Get document count in an index"""
        try:
            response = requests.get(f"{self.es_url}/{index_name}/_count")
            if response.status_code == 200:
                count = response.json()['count']
                print(f"Index '{index_name}' has {count} documents")
                return count
            else:
                print(f"✗ Failed to get count: {response.text}")
                return None
        except Exception as e:
            print(f"✗ Error getting count: {str(e)}")
            return None


# ============================================================================
# ELASTICSEARCH MAPPINGS FOR 9 ANALYSES
# ============================================================================

# Analysis 1: Top Selling Products (Recent Reviews)
ANALYSIS_1_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "business_id": {"type": "keyword"},
            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "city": {"type": "keyword"},
            "state": {"type": "keyword"},
            "categories": {"type": "text"},
            "review_count": {"type": "long"},
            "avg_stars": {"type": "float"},
            "total_reviews": {"type": "long"},
            "rank": {"type": "integer"}
        }
    }
}

# Analysis 2: User Purchase Patterns
ANALYSIS_2_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "user_id": {"type": "keyword"},
            "total_reviews": {"type": "long"},
            "avg_stars": {"type": "float"},
            "review_frequency": {"type": "text"}
        }
    }
}

# Analysis 3: Top Users by Reviews
ANALYSIS_3_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "user_id": {"type": "keyword"},
            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "review_count": {"type": "long"},
            "average_stars": {"type": "float"},
            "useful_votes": {"type": "long"},
            "funny_votes": {"type": "long"},
            "cool_votes": {"type": "long"},
            "yelping_since": {"type": "date"}
        }
    }
}

# Analysis 4: Category Trends Over Time
ANALYSIS_4_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "category": {"type": "keyword"},
            "year": {"type": "integer"},
            "month": {"type": "integer"},
            "review_count": {"type": "long"},
            "avg_stars": {"type": "float"}
        }
    }
}

# Analysis 5: High Rating Low Review Count Businesses
ANALYSIS_5_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "business_id": {"type": "keyword"},
            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "city": {"type": "keyword"},
            "state": {"type": "keyword"},
            "stars": {"type": "float"},
            "review_count": {"type": "integer"},
            "categories": {"type": "text"}
        }
    }
}

# Analysis 6: Geographic Distribution
ANALYSIS_6_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "city": {"type": "keyword"},
            "state": {"type": "keyword"},
            "business_count": {"type": "long"},
            "avg_stars": {"type": "float"},
            "total_reviews": {"type": "long"},
            "avg_review_count_per_business": {"type": "float"}
        }
    }
}

# Analysis 7: Seasonal Trends
ANALYSIS_7_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "season": {"type": "keyword"},
            "month": {"type": "integer"},
            "review_count": {"type": "long"},
            "avg_stars": {"type": "float"},
            "peak_category": {"type": "keyword"}
        }
    }
}

# Analysis 8: Trending Businesses (Window Functions)
ANALYSIS_8_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "business_id": {"type": "keyword"},
            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "city": {"type": "keyword"},
            "categories": {"type": "text"},
            "week_start": {"type": "date"},
            "weekly_count": {"type": "long"},
            "prev_week_count": {"type": "long"},
            "growth_rate": {"type": "float"},
            "avg_last_4_weeks": {"type": "float"},
            "trend_rank": {"type": "integer"}
        }
    }
}

# Analysis 9: Category Performance Matrix (Pivot/Unpivot)
ANALYSIS_9_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "category": {"type": "keyword"},
            "city": {"type": "keyword"},
            "avg_stars": {"type": "float"},
            "review_count": {"type": "long"},
            "metrics": {"type": "text"}
        }
    }
}

# Mapping dictionary for easy access
ANALYSIS_MAPPINGS = {
    "yelp-analysis-1-top-selling": ANALYSIS_1_MAPPING,
    "yelp-analysis-2-user-patterns": ANALYSIS_2_MAPPING,
    "yelp-analysis-3-top-users": ANALYSIS_3_MAPPING,
    "yelp-analysis-4-category-trends": ANALYSIS_4_MAPPING,
    "yelp-analysis-5-high-rating-low-review": ANALYSIS_5_MAPPING,
    "yelp-analysis-6-geographic": ANALYSIS_6_MAPPING,
    "yelp-analysis-7-seasonal": ANALYSIS_7_MAPPING,
    "yelp-analysis-8-trending": ANALYSIS_8_MAPPING,
    "yelp-analysis-9-performance-matrix": ANALYSIS_9_MAPPING,
}


def initialize_all_indices(es_host="localhost", es_port=9200):
    """
    Initialize all Elasticsearch indices with mappings
    Useful for first-time setup
    """
    saver = ElasticsearchSaver(es_host, es_port)

    if not saver.test_connection():
        print("✗ Cannot connect to Elasticsearch. Make sure it's running.")
        return False

    print("\nInitializing all indices...")
    for index_name, mapping in ANALYSIS_MAPPINGS.items():
        saver.create_index(index_name, mapping)

    print("\n✓ All indices initialized successfully!\n")
    return True


if __name__ == "__main__":
    # Test the connection and create indices
    print("Testing Elasticsearch Integration...")
    initialize_all_indices()
