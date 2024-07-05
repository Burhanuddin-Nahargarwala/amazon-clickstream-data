import boto3
from datetime import datetime
from botocore.exceptions import ClientError


class DynamoDB:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, table_name: str='amazon-ecommerce-streaming-api'):
        self.table_name = table_name

        self.dynamodb_client = boto3.client(
                service_name="dynamodb",
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )


    def write(self, email_id, called_at, status):
        """Writes the data to the dynamo db

        Args:
            email_id (str): Email of the user that calls the API
            status (str): Status of the API, wheher it is in_progress, done, failed, etc
            broker_id (str): The broker_id, which will be received by the SQS when the request call is 
            aligned in the queue
        """
        # Define the item attributes
        item_data = {
            'email_id': {'S': email_id},
            'called_at': {'S': called_at},
            'status': {'S': status},
            # 'broker_id': {'S': broker_id}
        }

        try:
            # Perform the put_item operation
            response = self.dynamodb_client.put_item(
                TableName=self.table_name,
                Item=item_data
            )
        except ClientError as err:
            print("Client Error:", err)
        
        return response


    def read_data(self):
        """
        This function fetches whole data from the table. It uses the scan operation, which is more 
        general-purpose operation that scans the entire table or a specified segment of the table, 
        applying filter conditions if needed. It's useful when you need to search for items without specifying 
        the primary key or when you need to filter items based on non-key attributes.
        """

        # Define the scan parameters
        scan_params = {
            'TableName': self.table_name,
        }

        # Initialize an empty list to store all items
        all_items = []

        # Perform the scan operation in a loop to retrieve all items
        while True:
            response = self.dynamodb_client.scan(**scan_params)
            
            # Add the items from the current page to the list
            items = response.get('Items', [])
            all_items.extend(items)
            
            # Check if there are more pages to scan
            if 'LastEvaluatedKey' in response:
                scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            else:
                # All items have been retrieved
                break

        return all_items

    
    def read_data_by_keys(self, email_id: str, status: str="in_progress"):
        """
        This method fetches the data from the table using query operation. The query operation is designed 
        to retrieve items from a DynamoDB table based on the primary key or indexed attributes. It's more 
        efficient for querying specific items or a range of items within a single partition.

        Args:
            email_id (str): _description_
            status (str, optional): _description_. Defaults to "in_progress".
        """

        query_params = {
            'TableName': self.table_name,
            'KeyConditionExpression': '#pk = :pkValue',
            'FilterExpression': '#status = :statusValue',
            'ExpressionAttributeNames': {
                '#pk': 'email_id',
                '#status': 'status'
            },
            'ExpressionAttributeValues': {
                ':pkValue': {'S': email_id},
                ':statusValue': {'S': status}
            }
        }

        # Perform the query
        # **query_params unpacks the dictionary, so its key-value pairs become keyword arguments.
        # dynamod_db_client.query('TableName' = self.table_name, ...)
        response = self.dynamodb_client.query(**query_params)

        # process the query results
        items = response.get('Items', [])

        return items # should return only single record


    def update_data(self, email_id, called_at, status_value):
        # Define the update expression and attribute values
        update_expression = 'SET #status = :statusValue'
        expression_attribute_names = {
            '#status': 'status'
        }
        expression_attribute_values = {
            ':statusValue': {'S': status_value},
        }

        # Define the update parameters
        update_params = {
            'TableName': self.table_name,
            'Key': {
                'email_id': {'S': email_id},
                'called_at': {'S': called_at}
            },
            'UpdateExpression': update_expression,
            'ExpressionAttributeNames': expression_attribute_names,
            'ExpressionAttributeValues': expression_attribute_values
        }

        # Perform the update_item operation
        response = self.dynamodb_client.update_item(**update_params)

        return response
    

    def delete_data(self, email_id, called_at):
        # Define the delete parameters
        delete_params = {
            'TableName': self.table_name,
            'Key': {
                'email_id': {'S': email_id},
                'called_at': {'S': called_at}
            }
        }

        # Perform the delete_item operation
        response = self.dynamodb_client.delete_item(**delete_params)

        return response

        

if __name__ == "__main__":
    # Create an instance of dynamodb
    dynamodb = DynamoDB(endpoint_url='http://localhost:8000')

    # To update the item we require the called_at, thus store the called_at in a variable
    called_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")