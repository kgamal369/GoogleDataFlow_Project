# Kgamal 30-6-2018 #


from __future__ import absolute_import

import argparse
import logging
import re
import json
import pandas as pd
import matplotlib.pyplot as plt

import apache_beam as beam




import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

try:
  unicode           
except NameError:
  unicode = str

def run(argv=None):
  

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-kga369-207722/twitter_data_3.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                   
                      default='$BUCKET/output',
                      help='kga369-207722:MindValley_Project.Twitter')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
 
      '--runner=DirectRunner',
      '--project=$PROJECT',
      '--staging_location=$BUCKET/staging',
     '--temp_location=$BUCKET/temp',
      '--job_name=your-wordcount-job',
  ])



  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | ReadFromText(known_args.input)
	

    # Count the occurrences of each word.
    counts = (
        lines
        | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[#]+[A-Za-z\']+', x))
                      .with_output_types(unicode))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))
        

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    output = counts | 'Format' >> beam.Map(format_result)
     # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | WriteToText(known_args.output)


	# Save to BigQuery IO 
   #  output | 'Write' >> beam.io.WriteToBigQuery(
    #    known_args.output,
     #   schema='Trend:STRING, Count:INTEGER',
      #  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
       # write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

   
	
	 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  
  run()

