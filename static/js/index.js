document.addEventListener('DOMContentLoaded', () => {
    const azureModal = document.getElementById('azureModal');
    const gcpModal = document.getElementById('gcpModal');
    const awsModal = document.getElementById('awsModal');

    const awsButton = document.getElementById('aws');
    const azureButton = document.getElementById('azure');
    const gcpButton = document.getElementById('gcp');

    const azureCloseButton = document.getElementById('azureClose');
    const gcpCloseButton = document.getElementById('gcpClose');
    const awsCloseButton = document.getElementById('awsClose');

    // Inputs of Azure Modal
    const hubName = document.getElementById('Hub Name');
    const connectionString = document.getElementById('connectionString');
    const emailId = document.getElementById('emailId');
    const azureAccessToken = document.getElementById('azureAccessToken');
    // Button of Azure Modal
    const azureGenerateDataButton = document.getElementById('azureGenerateData');
    const azureStopGenerateData = document.getElementById('azureStopGenerateData');

    // Inputs of GCP Modal
    const projectId = document.getElementById("projectId");
    const topicId = document.getElementById("topicId");
    const gcpEmailId = document.getElementById("gcpEmailId");
    const gcpAccessToken = document.getElementById("gcpAccessToken");
    // Button of GCP Modal
    const gcpGenerateDataButton = document.getElementById('gcpGenerateData');
    const gcpStopGenerateData = document.getElementById('gcpStopGenerateData');

    let cloudProvider;
    let credentialFileContent;

    document.getElementById("credentialsFile").addEventListener("change", handleFileSelect);


    /**
     * Create a toast notification
     * @param {string} message - The message to display in the toast.
     */
    function createToast(message) {
        const toast = document.createElement('div');
        toast.className = 'toast';
        toast.innerText = message;
        document.body.appendChild(toast);

        setTimeout(() => {
            toast.classList.add('show');
        }, 100);

        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => {
                document.body.removeChild(toast);
            }, 300);
        }, 3000);
    }

    /**
     * Handles the file selection event for the GCP modal. Fetch the content of the credentials file
     * @param {Event} event - The file selection event.
     */
    function handleFileSelect(event) {
        const fileInput = event.target;
        const file = fileInput.files[0]; // FileList {0: File, length: 1}. We are selecting File from 0th index
        // console.log(file);
        // if no file is selected then in that case the if condition won't be executed
        if (file) {
            const reader = new FileReader();

            // Sets up an event handler for the onload event of the FileReader. When the file reading 
            // operation is complete, this function will be executed. It extracts the content of the file 
            // from the event and stores it in the fileContent variable. It then logs the content to the console.
            reader.onload = function (e) {
                const content = e.target.result;
                credentialFileContent = content;
                // console.log(`File Content: ${fileContent}`);
            }

            // Initiates the reading of the selected file as text. This asynchronous operation will trigger 
            // the onload event when it's completed.
            reader.readAsText(file);
        }
    }


    /**
     * Event listener for the click event on the "AWS" button.
     * Opens the AWS modal and sets the cloudProvider variable.
     */
    awsButton.addEventListener('click', () => {
        cloudProvider = 'AWS';
        showModal('AWS');
    });


    /**
     * Event listener for the click event on the "Azure" button.
     * Opens the Azure modal and sets the cloudProvider variable.
     */
    azureButton.addEventListener('click', () => {
        cloudProvider = 'Azure';
        showModal('Azure');
    });


    /**
    * Event listener for the click event on the "GCP" button.
    * Opens the GCP modal and sets the cloudProvider variable.
    */
    gcpButton.addEventListener('click', () => {
        cloudProvider = 'GCP';
        showModal('GCP');
    });


    /**
    * Event listener for the click event on the "Azure" close button.
    * Closed the Azure modal.
    */
    azureCloseButton.addEventListener('click', () => {
        closeModal('Azure');
    });


    /**
    * Event listener for the click event on the "GCP" close button.
    * Closed the GCP modal.
    */
    gcpCloseButton.addEventListener('click', () => {
        closeModal('GCP');
    });


    /**
    * Event listener for the click event on the "AWS" close button.
    * Closed the AWS modal.
    */
    awsCloseButton.addEventListener('click', () => {
        closeModal('AWS'); // No parameter is passed means the function parameter will contain undefined and the coming soon model will be closed
    })

    /**
     * Stops the data generation process by sending a GET request to the provided API URL with headers.
     * If the request is successful, it logs the data; otherwise, it logs the error to the console.
     * @param {string} apiURL - The URL to send the API request to.
     * @param {object} headers - The headers to include in the API request.
     */
    function stopGenerateData(apiURL, headers, body) {
        fetch(apiURL, {
            method: 'POST',
            headers: headers,
            body: JSON.stringify(body)
        })
            .then(response => {
                if (!response.ok) {
                    console.log("Error: ", response.json())
                }
                return response.json();
            })
            .then(data => {
                // Handle the API response data here
                createToast(`✅ ${data.message}`);
                console.log(data);
            })
            .catch(error => {
                // Handle errors here
                createToast("❌ Error stopping data generation!");
                console.log('There was a problem with the fetch operation:', error);
            });
    }


    /**
     * Event listener for the click event on the "Stop Generate Data" button in the Azure modal.
     * Sends a GET request to stop data generation.
     */
    azureStopGenerateData.addEventListener('click', () => {
        // Request headers with the access token
        const headers = {
            'access_token': azureAccessToken.value,
            'Content-Type': 'application/json', // Adjust content type as needed
        };

        // Send a POST request with headers and payload
        const body = {
            "cloud_provider": cloudProvider,
            "email_id": emailId.value
        };
        const apiURL = `/stop_processing`;
        stopGenerateData(apiURL, headers, body);
        closeModal('Azure');
    })


    /**
     * Event listener for the click event on the "Stop Generate Data" button in the GCP modal.
     * Sends a GET request to stop data generation.
     */
    gcpStopGenerateData.addEventListener('click', () => {
        // Request headers with the access token
        const headers = {
            'access_token': gcpAccessToken.value,
            'Content-Type': 'application/json', // Adjust content type as needed
        };

        // Send a POST request with headers and payload
        const body = {
            "cloud_provider": cloudProvider,
            "email_id": gcpEmailId.value
        };
        const apiURL = `/stop_processing`;
        stopGenerateData(apiURL, headers, body);
        closeModal('GCP');
    })

    /**
     * Sends a request to the API to generate data. If the request is successful, it logs the data;
     * otherwise, it logs the error to the console.
     * @param {string} apiURL - The URL to send the API request to.
     * @param {object} headers - The headers to include in the API request.
     */
    function requestToGenerateData(apiURL, headers, body) {
        fetch(apiURL, {
            method: 'POST',
            headers: headers,
            body: JSON.stringify(body)
        })
            .then(response => {
                if (!response.ok) {
                    console.log("Error: ", response.json())
                }
                return response.json();
            })
            .then(data => {
                // Handle the API response data here
                createToast(`✅ ${data.message}`);
                console.log(data);
            })
            .catch(error => {
                // Handle errors here
                createToast("❌ Error starting data generation!");
                console.log('There was a problem with the fetch operation:', error);
            });
    }


    /**
     * Event listener for the click event on the "Generate Data" button in the Azure modal.
     * Sends a GET request to generate data.
     */
    azureGenerateDataButton.addEventListener('click', () => {
        // URL-encode the string
        // const encodedConnectionString = encodeURIComponent(connectionString.value);

        // Request headers with the access token
        const headers = {
            'access_token': azureAccessToken.value,
            'Content-Type': 'application/json', // Adjust content type as needed
        };

        // Send a POST request with headers and payload
        const body = {
            "cloud_provider": cloudProvider,
            "connection_string": connectionString.value,
            "hub_name": hubName.value,
            "email_id": emailId.value
        };
        const apiURL = `/data_generation`;

        requestToGenerateData(apiURL, headers, body);

        closeModal('Azure');
    });


    /**
     * Event listener for the click event on the "Generate Data" button in the GCP modal.
     * Sends a GET request to generate data.
     */
    gcpGenerateDataButton.addEventListener('click', () => {
        console.log(credentialFileContent);

        // Encode the credentials
        // const encodedCredentialsString = encodeURIComponent(credentialFileContent);

        // Request headers with the access token
        const headers = {
            'access_token': gcpAccessToken.value,
            'Content-Type': 'application/json', // Adjust content type as needed
        };

        // Send a POST request with headers and payload
        const body = {
            "cloud_provider": cloudProvider,
            "credentials": credentialFileContent,
            "project_id": projectId.value,
            "topic_id": topicId.value,
            "email_id": gcpEmailId.value
        };
        const apiURL = `/data_generation`;

        requestToGenerateData(apiURL, headers, body);
        // alert(response);

        closeModal('GCP');
    })


    /**
     * Opens a modal based on the cloud provider selected.
     * @param {string} modelName - The name of the modal to be opened.
     */
    function showModal(modelName) {
        if (modelName == 'Azure') {
            azureModal.style.display = 'flex';
        } else if (modelName == 'GCP') {
            gcpModal.style.display = 'flex';
        } else {
            awsModal.style.display = 'flex';
        }
    }


    /**
     * Closes a modal based on the cloud provider selected.
     * @param {string} modelName - The name of the modal to be closed.
     */
    function closeModal(modelName) {
        if (modelName == 'Azure') {
            azureModal.style.display = 'none';
        } else if (modelName == 'GCP') {
            gcpModal.style.display = 'none';
        } else {
            awsModal.style.display = 'none';
        }
    }
});