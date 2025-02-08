// SamplePipeline.java
package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.TextIO;

public class SamplePipeline {
    public interface Options extends PipelineOptions {
        String getInputPath();
        void setInputPath(String value);
        
        String getOutputPath();
        void setOutputPath(String value);
    }
    
    static class ProcessFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toUpperCase());
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(Options.class);
            
        Pipeline pipeline = Pipeline.create();
        
        pipeline.apply(TextIO.read().from(options.getInputPath()))
               .apply(ParDo.of(new ProcessFn()))
               .apply(TextIO.write().to(options.getOutputPath()));
               
        pipeline.run();
    }
}

