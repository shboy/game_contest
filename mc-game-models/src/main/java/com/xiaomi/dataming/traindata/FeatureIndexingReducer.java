package com.xiaomi.dataming.traindata;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class FeatureIndexingReducer extends Reducer<Text, Text, Text, Text> {
    private long groupCount;
    private Text keyOutput;
    private Text valueOutput;
    private MultipleOutputs<Text, Text> multipleOutputs;
    private long globalCount;
    // shenhao: set成UNK么
    private Boolean fixDefaultValue;  //是否把default值写死
    private static String OOV_VALUE = "9999999999999";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        fixDefaultValue = context.getConfiguration()
                .getBoolean("recommender.indexer.fix-default-value", false);
        groupCount = 1;
        globalCount = 1;
        multipleOutputs = new MultipleOutputs<>(context);
        keyOutput = new Text();
        valueOutput = new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value: values) {
            groupCount++;
            globalCount++;
            valueOutput.set(value);
            multipleOutputs.write("vocabulary", NullWritable.get(),
                    valueOutput, "vocabulary-" + key.toString());
        }
        if (fixDefaultValue) {
            // default值，由于num_oov_buckets有bug，暂时写死
            valueOutput.set(new Text(OOV_VALUE));
            multipleOutputs.write("vocabulary", NullWritable.get(),
                    valueOutput, "vocabulary-" + key.toString());
        }
        keyOutput.set(key);
        valueOutput.set("{\"feature_count\": " + Long.toString(groupCount) + "}");
        multipleOutputs.write("meta", keyOutput, valueOutput, ".meta");
        groupCount = 1;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        keyOutput.set("global");
        valueOutput.set("{\"feature_count\": " + Long.toString(globalCount) + "}");
        multipleOutputs.write("meta", keyOutput, valueOutput, ".meta");
        super.cleanup(context);
        multipleOutputs.close();
    }
}
