package com.naman.apachebeam.student;

import com.google.pubsub.v1.TopicName;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;

public class TotalScoreComputation {

    private static final String CSV_HEADER =
            "ID,Name,Physics,Chemistry,Math,English,Biology,History";

    private static final String projectId = "gcp-learning-342413";
    private static final String topicId = "messages";

    public static void main(String[] args) throws IOException {

        TopicName topicName = TopicName.of(projectId, topicId);

        TotalScoreComputationOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(TotalScoreComputationOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        System.out.println("****Input File: " + options.getInputFile());
        System.out.println("****Output File: " + options.getOutputFile());

        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(PubsubIO.writeStrings().to(topicName.toString()));
        //.apply(TextIO.write().to(options.getOutputFile())
        // .withHeader("Name,Total").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        private FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            String row = context.element();

            assert row != null;
            if (!row.isEmpty() && !row.equals(this.header)) {
                context.output(row);
            }
        }
    }

    private static class ComputeTotalScoreFn extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(ProcessContext context) {

            String[] data = context.element().split(",");

            String name = data[1];
            Integer totalScore = Integer.parseInt(data[2]) + Integer.parseInt(data[3]) +
                    Integer.parseInt(data[4]) + Integer.parseInt(data[5]) +
                    Integer.parseInt(data[6]) + Integer.parseInt(data[7]);

            context.output(KV.of(name, totalScore));
        }
    }

    private static class ConvertToStringFn extends DoFn<KV<String, Integer>, String> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            context.output(context.element().getKey() + "," + context.element().getValue());
        }

    }

}
