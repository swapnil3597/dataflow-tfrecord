# dataflow-tfrecord
This repository is a reference ETL Pipeline for creating TF-Records using Apache Beam Python SDK on Google Cloud Dataflow

Inorder to run the pipeline on GOOGLE VM Instance you may run,
```bash
bash run.sh
```
But before running the pipeline make sure you initialize the following variables in `create_tfrecords/create_tfrecords.py`:
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
