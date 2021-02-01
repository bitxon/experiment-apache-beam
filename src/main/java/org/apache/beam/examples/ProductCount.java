package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.commons.lang3.StringUtils.trim;

public class ProductCount {

    private static final Logger LOG = LoggerFactory.getLogger(ProductCount.class);

    public static void main(String[] args) {
        LOG.info("Parsing args - started");
        final ProductCountOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ProductCountOptions.class);
        LOG.info("Parsing args - completed");


        final Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("ExtractProductAndPrice", ParDo.of(new ExtractProductTypePriceFn()))
                .apply("GroupByProductType", GroupByKey.create())
                .apply(MapElements.via(new FormatStringDoubleListAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        pipeline.run().waitUntilFinish();
    }

    static class ExtractProductTypePriceFn extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<String, Double>> receiver) {
            String[] parts = element.split(",");
            receiver.output(KV.of(trim(parts[2]), Double.parseDouble(parts[3]))
            );
        }
    }

    static class ExtractCustomerTypeFn extends DoFn<String, KV<String, Double>> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<String, Double>> receiver) {
            String[] parts = element.split(",");
            receiver.output(KV.of(trim(parts[2]), 1.0/** As one line represents one customer **/));
        }
    }

    static class FormatStringDoubleAsTextFn extends SimpleFunction<KV<String, Double>, String> {
        @Override
        public String apply(KV<String, Double> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }
    static class FormatStringDoubleListAsTextFn extends SimpleFunction<KV<String, Iterable<Double>>, String> {
        @Override
        public String apply(KV<String, Iterable<Double>> input) {
            String values = StreamSupport.stream(input.getValue().spliterator(), true)
                    .map(Object::toString)
                    .collect(Collectors.joining(","));

            return input.getKey() + ": " + values;
        }
    }


    public interface ProductCountOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("gs://learn-data-flow-bucket/sales_transactions.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }
}
