package com.xiaomi.dataming.traindata;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/29
 */
public class FeatureExtractorReducer extends Reducer<Text, BytesWritable, BytesWritable, NullWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // todo: load vocabulary
        String vocabularyPath = context.getConfiguration().get("recommender.extractor.vocabulary");
    }

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        for (BytesWritable value: values) {
            context.write(value, NullWritable.get());
        }
    }

}
