# impression-digital-task

## Clone Repo

Clone this Git repository with the following command:

```bash
git clone https://gitlab.com/ah70768/impression-digital-task
```

## Installation

This ETL pipeline is designed to be deployed using Google Cloud Functions. To deploy this pipeline, you will need to download and set up the Google Cloud SDK. Follow the steps below:

### Step 1: Install Google Cloud SDK

To install the Google Cloud SDK, run the following commands based on your operating system:

#### For Windows:
1. Download the installer from [Google Cloud SDK for Windows](https://cloud.google.com/sdk/docs/install).
2. Run the installer and follow the on-screen instructions to complete the setup.

Download the installer from Google Cloud SDK for Windows.
Run the installer and follow the on-screen instructions to complete the setup.

#### For macOS:
```bash
brew install --cask google-cloud-sdk
```

#### For linux:
```bash
curl https://sdk.cloud.google.com | bash
```

After installation, restart your terminal and initialize the Google Cloud SDK by running:

#### 
```bash
gcloud init
```

### Step 2: Authenticate with Google Cloud


To authenticate your Google Cloud account, use the following command:
####

```bash
gcloud auth login
```

### Step 3: Set Your Google Cloud Project

Make sure your desired Google Cloud project is set:

 
```bash
gcloud config set project <YOUR_PROJECT_ID>
```

For Example:

```bash
gcloud config set project impression-digital
```

### Step 4: Change Project Parameters:

The following parameters need to be changed to your desired settings as per your Google Cloud Project, etc:

#### 01_code/config/profiles.yml

```yaml
shopify:
  outputs:
    dev:
      dataset: staging
      job_execution_timeout_seconds: 540
      job_retries: 1
      location: EU
      method: oauth
      priority: interactive
      project: <YOUR_PROJECT_NAME>
      threads: 1
      type: bigquery

  target: dev
```

#### 01_code/config/env.yml
```yaml
SHOP_URL: 'https://impression-digital-test-store.myshopify.com'
SHOPIFY_API_KEY: <API_KEY>
SHOPIFY_API_SECRET_KEY: <API_SECRET_KEY>
SHOPIFY_ADMIN_API_ACCESS_TOKEN: <ADMIN_API_ACCESS_TOKEN>
SHOPIFY_API_VERSION: '2024-07'
GCP_PROJECT_ID: <YOUR_PROJECT_NAME>
```

 
## Usage

### Step 1: To deploy the cloud function first navigate to the project directory:

```bash
cd <PATH_TO_REPO>\01_code
```

For Example:

```bash
cd "Impression Digital\impression-digital-task\01_code"
```


### Step 2: Deploy the function using the following command:

```bash
gcloud functions deploy <CLOUD_FUNCTION_NAME> \
  --runtime python312 \
  --trigger-http \
  --entry-point main \
  --region europe-west2
  --env-vars-file config/env.yml \
  --allow-unauthenticated \
  --memory 512MB \
  --timeout 540s
```
 
For example:

```bash
gcloud functions deploy shopify-etl --runtime python312 --trigger-http --entry-point main --region europe-west2 --env-vars-file config/env.yml --allow-unauthenticated --memory 512MB --timeout 540s
```


### Step 3: Test the deployment

Once the function is deployed, you can test it by making a GET HTTP request using the URL provided by Google Cloud Functions. You can use curl or any HTTP client to trigger the function:

```bash
curl <CLOUD_FUNCTION_URL>
```

The function URL can be retrieved from the CLI using the following:

```bash
gcloud functions describe <CLOUD_FUNCTION_NAME> --region <YOUR_FUNCTION_REGION>
```

For Example:

```bash
curl https://europe-west2-impression-digital.cloudfunctions.net/shopify-etl
```

### Step 4: View Logs

You can review the logs for your Google Cloud Function by using the following command:

```bash
gcloud functions logs read <CLOUD_FUNCTION_NAME> --region <YOUR_FUNCTION_REGION>
```

Alternatively, you can view logs in the Google Cloud Console. Select the appropriate project and navigate to the "Cloud Functions" section to access the logs for the deployed function.


## Schedule

Once deployed the cloud function can be scheduled using google cloud scheduler. 

The following provides a comprehensive follow through on how to do this: 
 [Cloud Scheduler - Time Triggers for Cloud Functions](https://www.youtube.com/watch?v=WUPEUjvSBW8).
