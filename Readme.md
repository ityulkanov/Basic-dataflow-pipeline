### Dataflow pipeline project 

#### To run it first set up environment variables: 

```bash
export TF_VAR_credentials_path=... # path to google_credentials.json file
export TF_VAR_project_id=... # project id 
```

#### Then run terraform commands within terraform folder: 

```bash
terraform init && terraform apply
```

Add sample file (if run from terraform folder): 
```bash
gsutil cp ../sample/sales_file.json gs://sales_data_32345543/

```

if any changes within avro scheme is made, please rerun: 
```makefile
make generate-avro
```

#### Then run the pipeline: 
First start takes substantial amount of time (over 20 mins), since it needs to fill stage folder with required jar files.

Make sure you have all the required api enabled): 


```bash
bigquery.googleapis.com              BigQuery API
bigquerymigration.googleapis.com     BigQuery Migration API
bigquerystorage.googleapis.com       BigQuery Storage API
cloudapis.googleapis.com             Google Cloud APIs
cloudresourcemanager.googleapis.com  Cloud Resource Manager API
cloudscheduler.googleapis.com        Cloud Scheduler API
cloudtrace.googleapis.com            Cloud Trace API
compute.googleapis.com               Compute Engine API
dataflow.googleapis.com              Dataflow API
datapipelines.googleapis.com         Data pipelines API
datastore.googleapis.com             Cloud Datastore API
deploymentmanager.googleapis.com     Cloud Deployment Manager V2 API
logging.googleapis.com               Cloud Logging API
monitoring.googleapis.com            Cloud Monitoring API
osconfig.googleapis.com              OS Config API
oslogin.googleapis.com               Cloud OS Login API
servicemanagement.googleapis.com     Service Management API
storage-api.googleapis.com           Google Cloud Storage JSON API
storage-component.googleapis.com     Cloud Storage
storage.googleapis.com               Cloud Storage API

```
to check enabled apis: 
    
```bash
gcloud services list --enabled --project <PROJECT>
```
