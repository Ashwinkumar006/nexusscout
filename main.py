# data_harvester/main.py
import os
import requests
import json
import datetime
import uuid
from google.cloud import storage

# --- Configuration (UPDATE THESE PLACEHOLDERS CAREFULLY!) ---
# Replace 'nexusscout' with your actual GCP Project ID.
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nexusscout")
# Replace 'your-nexus-raw-data-bucket-unique' with the GLOBALLY UNIQUE name
# of the Cloud Storage bucket you created/will create for raw data.
CLOUD_STORAGE_BUCKET = os.environ.get("CLOUD_STORAGE_BUCKET", "nexusscout-raw-data-sriramo-23jun2025-1")

# --- Mock Data Source (This pulls dummy posts from a public API) ---
MOCK_DATA_URL = "https://jsonplaceholder.typicode.com/posts"

def ingest_data_entry(entry_data: dict, client: storage.Client):
    """
    Uploads a single data entry as a JSON file to Cloud Storage.
    Each file will be named with a timestamp and a unique ID.
    """
    timestamp = datetime.datetime.now().isoformat()
    # Files will be stored in a 'raw_data/' folder within your bucket.
    file_name = f"raw_data/{timestamp}-{uuid.uuid4().hex[:8]}.json"
    
    # Get the Cloud Storage bucket object.
    bucket = client.bucket(CLOUD_STORAGE_BUCKET)
    # Create a new blob (file) object within the bucket.
    blob = bucket.blob(file_name)

    # Convert the Python dictionary data to a JSON string.
    json_data = json.dumps(entry_data, indent=2)

    # Upload the JSON string to the Cloud Storage blob.
    blob.upload_from_string(json_data, content_type="application/json")
    print(f"Uploaded {file_name} to Cloud Storage.")
    return file_name # Return the file path for logging/tracking

def chronicle_harvester_agent(request):
    """
    Cloud Function entry point for the Chronicle Harvester Agent.
    This function is designed to be triggered by HTTP (manually or via Cloud Scheduler).
    It fetches data from a mock source and uploads it to Cloud Storage.
    """
    print("Chronicle Harvester Agent: Initiating data collection...")
    
    # Initialize the Cloud Storage client.
    storage_client = storage.Client(project=GCP_PROJECT_ID)

    try:
        # Make an HTTP GET request to the mock data source.
        response = requests.get(MOCK_DATA_URL)
        # Raise an exception for bad HTTP status codes (4xx or 5xx).
        response.raise_for_status() 
        # Parse the JSON response into a Python list of dictionaries.
        mock_data_entries = response.json()

        uploaded_files = []
        # CRITICAL COST/SPEED OPTIMIZATION for hackathon:
        # Process only a very small number of entries (e.g., first 3) to minimize LLM calls later.
        for entry in mock_data_entries[:3]: 
            # Add metadata to the entry before ingesting.
            entry['harvest_timestamp'] = datetime.datetime.now().isoformat()
            entry['source_url'] = MOCK_DATA_URL
            entry['agent'] = 'ChronicleHarvester' # Identify which agent processed this.
            
            # Ingest the single data entry into Cloud Storage.
            uploaded_file_name = ingest_data_entry(entry, storage_client)
            uploaded_files.append(uploaded_file_name)

        print(f"Chronicle Harvester Agent: Successfully harvested and uploaded {len(uploaded_files)} entries.")
        # Return a success response to the Cloud Function caller.
        return {"status": "success", "uploaded_files": uploaded_files}, 200

    except requests.exceptions.RequestException as e:
        # Handle HTTP request errors.
        print(f"Chronicle Harvester Agent: HTTP request failed: {e}")
        return {"status": "error", "message": f"HTTP request failed: {e}"}, 500
    except Exception as e:
        # Handle any other unexpected errors during the process.
        print(f"Chronicle Harvester Agent: An unexpected error occurred: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {e}"}, 500