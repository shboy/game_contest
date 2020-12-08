package com.xiaomi.dataming.traindata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.tensorflow.hadoop.io.TFRecordFileInputFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * 02. count
 */
public class FeatureCountingJob implements Tool {
    private Configuration configuration;

    @Override
    public int run(String[] strings) throws Exception {
        boolean fromTrain = configuration.getBoolean("recommender.counter.from-train", true);
        Job counter = Job.getInstance(configuration, "FeatureCountingJob[shenhao@xiaomi.com]");
        counter.setJarByClass(FeatureCountingJob.class);

        String outputPath = configuration.get("recommender.counter.output-base");
        if (!fromTrain) {
            DateTimeFormatter datePattern = DateTimeFormat.forPattern("yyyyMMdd");
            DateTime date = datePattern.parseDateTime(configuration.get("recommender.counter.base-date"));
            String dataPathBase = configuration.get("recommender.counter.input-base");
            int days = configuration.getInt("recommender.counter.days", 1);
            List<String> dataPath = new ArrayList<>();
            for (int i = 0; i < days; ++i) {
                dataPath.add(dataPathBase + "/date=" + date.minusDays(i).toString("yyyyMMdd"));
            }
            for (String path: dataPath) {
                FileInputFormat.addInputPath(counter, new Path(path));
            }
        } else {
            String dataPathBase = configuration.get("recommender.counter.input-base");
            FileInputFormat.addInputPath(counter, new Path(dataPathBase));
        }
        counter.setInputFormatClass(TFRecordFileInputFormat.class);
        counter.setMapperClass(FeatureCountingMapper.class);

        // features
        String featuresPathName = configuration.get("recommender.counter.features-path", "features");
        String featuresPath = outputPath + "/" + featuresPathName;
        FileOutputFormat.setOutputPath(counter, new Path(featuresPath));
        counter.setOutputFormatClass(TextOutputFormat.class);
        counter.setCombinerClass(FeatureCountingReducer.class);
        counter.setReducerClass(FeatureCountingReducer.class);

        counter.setMapOutputKeyClass(Text.class);
        counter.setMapOutputValueClass(Text.class);
        counter.setOutputKeyClass(Text.class);
        counter.setOutputValueClass(Text.class);
        counter.setNumReduceTasks(1000);

        if (!counter.waitForCompletion(true)) {
            return 1;
        }
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        ToolRunner.run(configuration, new FeatureCountingJob(), args);
    }
}
