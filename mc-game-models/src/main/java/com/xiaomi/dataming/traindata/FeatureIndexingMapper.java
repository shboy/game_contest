package com.xiaomi.dataming.traindata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FeatureIndexingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private long featureCountThreshold;
    private Text keyOutput;
    private Text valueOutput;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        featureCountThreshold = context.getConfiguration()
                .getInt("recommender.indexer.feature-threshold", 15);
        keyOutput = new Text();
        valueOutput = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        String feature = parts[0];
        String groupId = parts[1];
        long count = Long.parseLong(parts[2]);
        if (count > featureCountThreshold) {
            keyOutput.set(groupId);
            valueOutput.set(feature);
            context.write(keyOutput, valueOutput);
        }
    }
}
