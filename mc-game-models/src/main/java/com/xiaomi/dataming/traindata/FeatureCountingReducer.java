package com.xiaomi.dataming.traindata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FeatureCountingReducer extends Reducer<Text, Text, Text, Text> {
    private Text keyOutput;
    private Text valueOutput;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        keyOutput = new Text();
        valueOutput = new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        String debug = null;
        for (Text value : values) {
            // 如果parts.length>1，则parts[1]为debug信息
            String[] parts = value.toString().split("\t");
            long count = Long.parseLong(parts[0]);
            if (parts.length > 1) {
                debug = parts[1];
            }
            total = total + count;
        }
        keyOutput.set(key);
        valueOutput.set(Long.toString(total) + "\t" + debug);
        context.write(keyOutput, valueOutput);
    }
}
