package com.xiaomi.dataming.traindata;

import com.xiaomi.data.aiservice.contest.MCRecommendSamplesBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.thrift.ParquetThriftInputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/29
 *
 * 01. extract
 */
public class FeatureExtractorJob implements Tool {
    private Configuration configuration;

    @Override
    public int run(String[] strings) throws Exception {
        String app = "game";
        DateTimeFormatter datePattern = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime curDate = datePattern.parseDateTime(configuration.get("recommender.extractor.base-date")); // ${date}
//            configuration.get("recommender.extractor.app-name", "");
        // /user/h_data_platform/platform/aiservice/recommend_samples_batch
        // /user/h_data_platform/platform/aiservice/m_c_user_attrib/recSamplesBatch/date=
        String dataPathBase = Constant.recSamplesBatchOutputPathPrefix();
//            configuration.get("recommender.extractor.input-base");
        // /user/h_data_platform/platform/aiservice/recommend_model_data/${app}/training_examples/date=${curDate}/train
        // /user/h_data_platform/platform/aiservice/m_c_user_attrib/training_examples/date="
        String outputPath  = String.format(Constant.recTfRecordOutputPathFormat(), curDate.toString("yyyyMMdd"));
//            configuration.get("recommender.extractor.output-path");
        System.err.println(String.format("dataPathBase : %s, output path: %s", dataPathBase, outputPath));
        int days = configuration.getInt("recommender.extractor.days", 1); // training_days=2
        List<String> dataPaths = new ArrayList<>();
        for (int i = 0; i < days; ++i) {
            dataPaths.add(dataPathBase + curDate.minusDays(i).toString("yyyyMMdd"));
        }
        System.err.println(String.format("input paths: %s", String.join("|", dataPaths)));

        Job job = Job.getInstance(configuration, "FeatureExtractorJob[shenhao@xiaomi.com]");
        job.setJarByClass(FeatureExtractorJob.class);

        job.setInputFormatClass(ParquetThriftInputFormat.class);
        ParquetThriftInputFormat.setThriftClass(configuration, MCRecommendSamplesBatch.class);
        for (String path: dataPaths) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TFRecordFileOutputFormat.class);

        job.setMapperClass(FeatureExtractorMapper.class);
        job.setReducerClass(FeatureExtractorReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(NullWritable.class);

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
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
        ToolRunner.run(configuration, new FeatureExtractorJob(), args);
    }
}
