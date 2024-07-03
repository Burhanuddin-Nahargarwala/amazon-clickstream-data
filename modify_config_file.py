import json

# Function to read the JSON configuration file
def read_config(file_path):
    """
    Reads a JSON configuration file and returns its contents.

    Parameters:
    - `file_path` (str): The path to the JSON configuration file.

    Returns:
    - dict: A dictionary containing the contents of the JSON configuration file.

    Note:
    This function opens and reads the specified JSON file and returns its contents as a dictionary.

    """

    # This function opens and reads the specified JSON file and returns its 
    # contents as a dictionary.
    with open(file_path, 'r') as file:
        config = json.load(file)
    
    return config


# Function to update the JSON configuration file
def update_config(file_path, config):
    """
    Updates a JSON configuration file with the provided configuration.

    Parameters:
    - `file_path` (str): The path to the JSON configuration file.
    - `config` (dict): The configuration to be written to the file.

    Returns:
    - None

    """

    # The file is overwritten with the new configuration, and the indentation 
    # is set to 4 spaces.
    with open(file_path, 'w') as file:
        json.dump(config, file, indent=4)