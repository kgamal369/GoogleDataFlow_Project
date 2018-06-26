/* Kgama 24-06-2018 Mind Valley Task  */
package org.apache.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;



public class Kgamal {

  public interface WordCountOptions extends PipelineOptions {

    
    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-kga369-207722/twitter_data.txt")
    String getInputFile();
    void setInputFile(String value);

    
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {

    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
			  .as(WordCountOptions.class);

   
    Pipeline p = Pipeline.create(options);

   
    p.apply(TextIO.read().from(options.getInputFile()))

   
     .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                       @ProcessElement
                       public void processElement(ProcessContext c) {
                         for (String word : c.element().split(ExampleUtils.TOKENIZER_PATTERN)) {
                           if (!word.isEmpty()) {
                             c.output(word);
                           }
                         }
                       }
                     }))


     .apply(Count.<String>perElement())

   
     .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                       @Override
                       public String apply(KV<String, Long> input) {
                         return input.getKey() + ": " + input.getValue();
                       }
                     }))

    
     .apply(TextIO.write().to(options.getOutput()));

    
    p.run().waitUntilFinish();
  }
}
