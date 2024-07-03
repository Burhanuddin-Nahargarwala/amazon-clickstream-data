## E-commerce Real-time Streaming Data API

This project provides a FastAPI-based backend for generating real-time e-commerce streaming data. The API allows users to start and stop data generation processes, supporting both Azure and GCP configurations. Authentication is handled using access tokens.

### Features

- **Start Data Generation**: Begin generating real-time e-commerce data.
- **Stop Processing**: Stop the data generation process and update the configuration.

### Endpoints

1. **`/start_data_generation`**: Starts the data generation process.
2. **`/stop_processing`**: Stops the data generation process.

### Prerequisites

- Python 3.7+
- FastAPI
- Uvicorn
- boto3 (for S3 and DynamoDB)

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/Burhanuddin-Nahargarwala/amazon-clickstream-data.git
   ```
2. Navigate to the project directory:
   ```sh
   cd amazon-clickstream-data
   ```
3. Install the dependencies:
   ```sh
   pip install -r requirements.txt
   ```

### Configuration

Configure your connection details in `api_config.py` and `constant.py`.

Update the credentials in .env file. Refer the .env.example file. The .env file should be same as .env.example. 

.env
```
ACCESS_TOKEN=<api_access_token>

aws-access-key-id=<your_aws_access_key>
aws-secret-access-key=<your_aws_secret_access_key>
aws-region=<your_aws_region>
dynamodb-region=<your_aws_dynamodb_region>
```

Replace `<api_access_token>`, `<your_aws_access_key>`, `<your_aws_secret_access_key>`, `<your_aws_region>`, `<your_aws_dynamodb_region>` with the actual credentials

Note: .env.example and .env are separate files. Store the actual credentials in .env file

### Authentication

The `auth.py` file handles the authentication using access tokens. Ensure your access token is valid and included in your requests.

### Usage

1. **Start the API server:**
   ```sh
   uvicorn main:app --reload
   ```

2. **Endpoints:**

   - **Start Data Generation**
     ```http
     POST /start_data_generation
     ```

     **Example Requests for Azure:**
     ```bash
        curl -X POST "http://127.0.0.1:8000/start_data_generation" -H "accept: application/json" -H "Content-Type: application/json" -d '{
        "cloud_provider": "Azure",
        "connection_string": "your_connection_string",
        "hub_name": "your_hub",
        "email_id": "your_email_id",
        }'
     ```

   - **Stop Processing**
     ```http
     POST /stop_processing
     ```

     **Parameters:**
     - `access_token` (str): Access token for authentication.
     - `stop_flag` (bool): Flag to stop the process.
     - `stop_email_id` (str): Email ID to notify.

     **Example Request:**
     ```bash
     curl -X POST "http://127.0.0.1:8000/stop_processing" -H "accept: application/json" -H "Content-Type: application/json" -d '{"email_id": "your_email_id"}'
     ```


### Asynchronous Event Generation and Uploading

This project utilizes asynchronous programming to generate real-time e-commerce streaming data and upload it to Azure Event Hubs and Google Cloud Pub/Sub. Asynchronous processing allows for efficient handling of concurrent tasks and improves overall performance by leveraging non-blocking operations.

#### Azure Event Hubs

Azure Event Hubs are used to ingest and process large volumes of events. Here’s how the asynchronous data generation and uploading to Azure Event Hubs are implemented:

1. **Azure Data Generation Function** (`azure_data_generation`):
   
   ```python
   async def azure_data_generation(
       dynamodb, email_id, called_at, hub_name, connection_string, data_dict
   ):
       try:
           producer = EventHubProducerClient.from_connection_string(
               connection_string, eventhub_name=hub_name
           )

           async with producer:
               event_data_batch = await producer.create_batch()
               event_data_batch.add(EventData(json.dumps(data_dict)))
               await producer.send_batch(event_data_batch)
               print(f"Event sent to Azure Event Hub: {data_dict}")

       except Exception:
           dynamodb.update_data(
               email_id=email_id, called_at=called_at, status_value="failed"
           )
           raise HTTPException(
               status_code=403,
               detail="Check whether the connection string or event hub name is correct or not",
           )
   ```

   - **Explanation**: This function creates an `EventHubProducerClient` using the provided connection string and hub name. It asynchronously creates a batch of events (`event_data_batch`) and sends them to Azure Event Hub. Error handling ensures proper status updates and exception raising on failure.

2. **Integration in `generate_events` Function**:
   
   The `generate_events` function orchestrates event generation based on the chosen cloud platform (`Azure` or `GCP`). For Azure, it calls `azure_data_generation` asynchronously:

   ```python
   async def generate_events(
       events_data, email_id, dynamodb, called_at, cloud_platform, cloud_parameters
   ):
       for event in events_data["Events"]:
           # Event preparation logic
           if cloud_platform == "Azure":
               await azure_data_generation(
                   dynamodb, email_id, called_at, hub_name, connection_string, event
               )
           # GCP event generation logic similarly implemented
           ...
   ```

#### Google Cloud Platform (GCP) Pub/Sub

Google Cloud Pub/Sub provides a scalable messaging service for event-driven systems. Here’s how events are asynchronously generated and uploaded to GCP Pub/Sub:

1. **GCP Data Generation Function** (`gcp_data_generation`):
   
   ```python
   async def gcp_data_generation(
       dynamodb, email_id, called_at, credentials, project_id, topic_id, data_dict
   ):
       try:
           publisher = pubsub_v1.PublisherClient.from_service_account_info(credentials)
           topic_path = publisher.topic_path(project_id, topic_id)

           data_str = json.dumps(data_dict)
           data_encoded_str = data_str.encode("utf-8")
           future = publisher.publish(topic_path, data_encoded_str)
           await future  # Wait for the publish operation to complete asynchronously

           print(f"Published {data_encoded_str} to {topic_path}: {future.result()}")

       except Exception:
           dynamodb.update_data(
               email_id=email_id, called_at=called_at, status_value="failed"
           )
           raise HTTPException(
               status_code=403,
               detail="Check whether the connection string or event hub name is correct or not",
           )
   ```

   - **Explanation**: This function initializes a `PublisherClient` using GCP credentials and publishes serialized data (`data_encoded_str`) to the specified Pub/Sub topic (`topic_path`). It waits asynchronously for the publishing operation (`future`) to complete.

2. **Integration in `generate_events` Function**:
   
   The `generate_events` function handles event generation for both Azure and GCP platforms based on the configuration. For GCP, it calls `gcp_data_generation`:

   ```python
   async def generate_events(
       events_data, email_id, dynamodb, called_at, cloud_platform, cloud_parameters
   ):
       for event in events_data["Events"]:
           # Event preparation logic
           if cloud_platform == "GCP":
               await gcp_data_generation(
                   dynamodb, email_id, called_at, credentials, project_id, topic_id, event
               )
           # Azure event generation logic similarly implemented
           ...
   ```

Asynchronous event generation and uploading are critical for handling real-time data streams efficiently. This approach ensures scalability and responsiveness in processing large volumes of e-commerce data across Azure and GCP environments.


### File Structure

- `main.py`: Main FastAPI file.
- `before_data_generation.py`: Preliminary data generation logic.
- `data_generation.py`: Main data generation logic.
- `constant.py`: Constants used across the project.
- `api_config.py`: File to update config.json to stop data generation, in case when user requests to stop data generation
- `s3.py`: Functions for interacting with AWS S3.
- `dynamod_db.py`: Functions for interacting with AWS DynamoDB.
- `auth.py`: Authentication logic.
