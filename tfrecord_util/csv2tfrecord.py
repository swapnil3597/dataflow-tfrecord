"""
  Author: Swapnil Masurekar

  Create tfrecords from csv
  csv format:
  
  path,label
  gs://<bucket-name>/path-to-img1.jpg,label1
  gs://<bucket-name>/path-to-img2.jpg,label2
  gs://<bucket-name>/path-to-img3.jpg,label3
  gs://<bucket-name>/path-to-img4.jpg,label2
  ...
"""

import argparse
import gcsfs
import numpy as np

import tensorflow as tf

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# TODO: Initialize below variables
LABEL_DICT = {
    'label1':0,
    'label2':1,
    'label3':2}
NUM_CLASS = len(LABEL_DICT)
IMG_SIZE = 28 # TODO: Enter your own int value for square image

PROJ_NAME = 'Your Project Name'

RUNNER = 'DataflowRunner'
STAGING_LOCATION = 'gs://<bucket-name>/staging/'
TEMP_LOCATION = 'gs://<bucket-name>/temp/'
JOB_NAME = 'random-job-name'
OUTPUT_PATH = 'gs://<bucket-name>/output_path/'


def get_args():
  """
    Takes arguments from command line
    ARGS: None
    Returns: Dictionary
  """
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--csv-path',
      type=str,
      required=True,
      help='name of csv file')

  return parser.parse_args()

class DecodeFromTextLineDoFn(beam.DoFn):
  """
    Decode text line to path and label
  """
  def __init__(self,
               num_class,
               delimiter=','):
    """
      Initialization
    """
    self.delimiter = delimiter
    self.num_class = num_class

  def _decode_label(self, label):
    """
      Decode string label to categorical using LABEL_DICT
    """
    label = LABEL_DICT[label]
    label = tf.keras.utils.to_categorical(label, self.num_class)
    return label

  def process(self, text_line):
    """
      Beam function to Get path and label from text line
    """
    path_and_labels = text_line.split(self.delimiter)
    path = path_and_labels[0]
    label = path_and_labels[1]
    label = self._decode_label(label)
    yield path, label

class LoadImageDoFn(beam.DoFn):
  """
    Load image from gcs
  """
  def __init__(self, img_size, project_name):
    """
      Initialization
    """
    self.img_size = img_size
    self.file_system = gcsfs.GCSFileSystem(project=project_name)

  def process(self, path_label_tuple):
    """
      Load image from gcs
      Yields: Image, categorical label
    """
    path, label = path_label_tuple
    with self.file_system.open(path) as img_file:
      img = np.array(
          tf.python.keras.preprocessing.image.load_img(
              img_file,
              target_size=(self.img_size, self.img_size))
      )
    yield img, label

class ImageToTfExampleDoFn(beam.DoFn):
  """
    Convert Image to TFExample
  """
  def __init__(self, mode):
    """
      Initialization
    """
    self.mode = mode

  @staticmethod
  def _bytes_feature(value):
    """
        Get byte features
    """
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

  @staticmethod
  def _int64_feature(value):
    """
        Get int64 feature
    """
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

  def process(self, image_label_tuple):
    """
      Convert image to tf-example
    """
    img, label = image_label_tuple
    img = img.astype(np.float32)
    g_labels = label.astype(np.float32)
    example = tf.train.Example(features=tf.train.Features(
        feature={self.mode+'/image': self._bytes_feature(img.tostring()),
                 self.mode+'/label': self._bytes_feature(g_labels.tostring())
                 }))
    yield example



def main():
  """
    Main function to create tfrecords (ETL pipeline)
  """
  args = get_args()

  # --> EXTRACTION <--
  # Read line from csv
  read_text_line_from_csv = beam.io.ReadFromText(args.csv_path,
                                                 skip_header_lines=1)

  # Decode str line from csv to obtain path and label
  decode_from_text_line = DecodeFromTextLineDoFn(num_class=NUM_CLASS)

  # Load the data (in this case image data)
  load_img_from_path = LoadImageDoFn(img_size=IMG_SIZE,
                                     project_name=PROJ_NAME)


  # --> TRANSFORMATION <--
  # Convert image to tf-example
  # (before this step you can perform various transformation on image)
  img_to_tfexample = ImageToTfExampleDoFn(mode='train')


  # --> LOADING <--
  # Write serialized example to tfrecords
  write_to_tf_record = beam.io.tfrecordio.WriteToTFRecord(
      file_path_prefix=OUTPUT_PATH,
      num_shards=20)


  # TODO: Enter required pipeline arguments
  pipeline_args = [
      '--runner', RUNNER,
      '--project', PROJ_NAME,
      '--staging_location', STAGING_LOCATION,
      '--temp_location', TEMP_LOCATION,
      '--job_name', JOB_NAME,
      '--num_workers', '5',
      '--setup_file', './setup.py']
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True


  # Defining Apache Beam Pipeline
  with beam.Pipeline(options=pipeline_options) as pipe:
    _ = (pipe
         | 'ReadCsvAsText' >> read_text_line_from_csv
         | 'DecodeFromText' >> beam.ParDo(decode_from_text_line)
         | 'LoadImageData' >> beam.ParDo(load_img_from_path)
         | 'ImageToTfExample' >> beam.ParDo(img_to_tfexample)
         | 'SerializeProto' >> beam.Map(lambda x: x.SerializeToString())
         | 'WriteTfrecord' >> write_to_tf_record)

if __name__ == "__main__":
  main()
