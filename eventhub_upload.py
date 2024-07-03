from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
import asyncio


# async def send_to_eventhub_async(event, producer):
#     loop = asyncio.get_event_loop()
#     await loop.run_in_executor(None, send_to_eventhub, event, producer)


def send_to_eventhub(event: str, producer: EventHubProducerClient):
    try:
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(event))
        producer.send_batch(event_data_batch)
        print("Event sent: %s", event)
    except Exception as ex:
        print("Exception: %s", ex)


def create_event_producer(connection_string: str, event_hub_name: str):
    try:
        producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string, eventhub_name=event_hub_name
        )
    except EventHubError as e:
        # Handle EventHubError exceptions
        raise Exception(f"An EventHubError occurred: {str(e)}")

    except Exception as ex:
        # Handle other exceptions
        raise Exception(f"An unexpected error occurred: {str(ex)}")
    
    return producer