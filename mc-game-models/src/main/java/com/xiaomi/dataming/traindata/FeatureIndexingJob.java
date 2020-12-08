package com.xiaomi.dataming.traindata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 03. index
 */
public class FeatureIndexingJob implements Tool {
    private Configuration configuration;

    @Override
    public int run(String[] strings) throws Exception {
        // /user/h_data_platform/platform/aiservice/recommend_model_data/${app}/training_examples/date=${curDate}
        String dataPathBase = configuration.get("recommender.indexer.data-base");
        Job indexer = Job.getInstance(configuration, "FeatureIndexingJob[shenhao@xiaomi.com]");
        indexer.setJarByClass(FeatureIndexingJob.class);
        String featuresPathName = configuration.get("recommender.indexer.features-path", "features");
        String vocabularyPathName = configuration.get("recommender.indexer.vocabulary-path", "vocabulary");
        String featuresPath = dataPathBase + "/" + featuresPathName;
        String vocabularyPath = dataPathBase + "/" + vocabularyPathName;
        FileInputFormat.setInputPaths(indexer, new Path(featuresPath));
        FileOutputFormat.setOutputPath(indexer, new Path(vocabularyPath));

        indexer.setInputFormatClass(TextInputFormat.class);
//        indexer.setOutputFormatClass(TextOutputFormat.class); // 默认情况下，输出目录会生成part-r-00000或者part-m-00000的空文件，需要如下设置后，才不会生成
        LazyOutputFormat.setOutputFormatClass(indexer, TextOutputFormat.class);

        indexer.setMapperClass(FeatureIndexingMapper.class);
        indexer.setReducerClass(FeatureIndexingReducer.class);

        MultipleOutputs.addNamedOutput(indexer, "meta", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(indexer, "vocabulary", TextOutputFormat.class, Text.class, Text.class);
        indexer.setMapOutputKeyClass(Text.class);
        indexer.setMapOutputValueClass(Text.class);
        indexer.setOutputKeyClass(Text.class);
        indexer.setOutputValueClass(Text.class);
        indexer.setNumReduceTasks(1);

        if (!indexer.waitForCompletion(true)) {
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
        ToolRunner.run(configuration, new FeatureIndexingJob(), args);
    }
}
