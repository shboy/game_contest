package com.xiaomi.dataming.traindata;


import com.google.protobuf.ByteString;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Int64List;

import java.io.IOException;
import java.util.Map;

public class FeatureCountingMapper extends Mapper<BytesWritable, NullWritable, Text, Text> {
    private Text keyOutput;
    private Text valueOutput;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        keyOutput = new Text();
        valueOutput = new Text();
    }

    @Override
    protected void map(BytesWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        Example example = Example.parseFrom(key.getBytes());
        Map<String, Feature> featuresMap = example.getFeatures().getFeatureMap();
        for (Map.Entry<String, Feature> entry : featuresMap.entrySet()) {
            String groupId = entry.getKey();
            Int64List features = entry.getValue().getInt64List();
            for (int i = 0; i < features.getValueCount(); ++i) {
                long feature = features.getValue(i);
                StringBuilder keyBuilder = new StringBuilder();
                keyBuilder.append(feature).append("\t");
                keyBuilder.append(groupId);
                // key: value+\t+groupId
                keyOutput.set(keyBuilder.toString());
                StringBuilder valueBuilder = new StringBuilder();
                valueBuilder.append(1);
                valueOutput.set(valueBuilder.toString());
                context.write(keyOutput, valueOutput);
            }

            // if entry value is not bytesList, the bytes_features.getValueCount() is 0!
            BytesList bytes_features = entry.getValue().getBytesList();
            for (int i = 0; i < bytes_features.getValueCount(); ++i) {
                ByteString feature = bytes_features.getValue(i);
                StringBuilder keyBuilder = new StringBuilder();
                keyBuilder.append(feature.toStringUtf8()).append("\t");
                keyBuilder.append(groupId);
                keyOutput.set(keyBuilder.toString());
                StringBuilder valueBuilder = new StringBuilder();
                valueBuilder.append(1);
                valueOutput.set(valueBuilder.toString());
                context.write(keyOutput, valueOutput);
            }
        }
    }
}
