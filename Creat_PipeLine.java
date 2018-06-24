package org.apache.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineoOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;



public class MaxTrend {
	
	public static void main (String[] args) {
// Start by defining the options for the pipeline.
PipelineOptions options = PipelineOptionsFactory.create();

// Then create the pipeline.
Pipeline p = Pipeline.create(options);


tweets_data_path = '../MindValley_Project/twitter_data.txt'

	p.apply(TextIO.read().from(tweets_data_path))
			.apply (Count.<String>globally())
			.apply(MapElemens.via(new SimpleFuntion<Long, String>() {
				public String apply(Long input) {
					return Long.toString(input);
				}	
			}))
			.apply(TextIO.write().to("linecount"));
			
		p.run().waitUntilFinish();
	}
}
