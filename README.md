# dataflow-tfrecord
This repository is a reference ETL Pipeline for creating TF-Records using Apache Beam Python SDK on Google Cloud Dataflow.
You may find the blog for his repo [here](https://medium.com/@swapnil3597/etl-pipeline-for-creating-tf-records-using-apache-beam-python-sdk-on-google-cloud-dataflow-93ec2879e524?source=friends_link&sk=3db753be2f7ded476d169265707ad8b7)

To run this pipeline: 
### Step 1:
First have a csv_file in format in the GCS Bucket,
```
gs://path/img.png,label1
gs://path/img.png,label12
...
```
and corresponding dummy square images of same size stored in the GCS bucket at correct path.

### Step 2:
Before running the pipeline make sure you initialize the following variables in `create_tfrecords/create_tfrecords.py`:
```python
# TODO: Initialize below variables
LABEL_DICT = {
    'label1':0,
    'label2':1,
    'label3':2}
NUM_CLASS = len(LABEL_DICT)
IMG_SIZE = 28 # TODO: Enter your own int value for square image

PROJ_NAME = 'Your Project Name'

CSV_PATH = 'gs://<bucket-name>/path-to.csv'
RUNNER = 'DataflowRunner'
STAGING_LOCATION = 'gs://<bucket-name>/staging/'
TEMP_LOCATION = 'gs://<bucket-name>/temp/'
TEMPLATE_LOCATION = 'gs://<bucket-name>/path/to/template_location/template_name'
JOB_NAME = 'random-job-name'
OUTPUT_PATH = 'gs://<bucket-name>/output_path/'
```

### Step 3:
Now, inorder to run the pipeline on GOOGLE VM Instance you may run,
```bash
bash run.sh
```
