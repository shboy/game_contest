package com.xiaomi.dataming.traindata;

import com.google.common.collect.Lists;
import com.xiaomi.data.aiservice.contest.*;
import com.xiaomi.dataming.scorers.config.ScorerConfigs;
import com.xiaomi.dataming.traindata.scorers.collector.Collector;
import com.xiaomi.dataming.traindata.scorers.collector.CollectorFactory;
import com.xiaomi.dataming.traindata.scorers.collector.TensorCollector;
import com.xiaomi.dataming.traindata.scorers.features.IFeatureExtractor;
import com.xiaomi.dataming.traindata.scorers.rankers.ExampleUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xiaomi.data.aiservice.contest.RecommendImpression;
import org.tensorflow.example.Example;


import java.io.IOException;
import java.util.*;
import java.util.function.Function;

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/29
 */
public class FeatureExtractorMapper extends Mapper<Void, MCRecommendSamplesBatch, Text, BytesWritable> {

    private Text keyOutput;
    private BytesWritable valueOutput;
    private double negativeRate;
    private double samplingRate;
    private Random random;
    private Set<String> queueNames;
    private boolean isDev;
    private long exposeTime;
    private double maskRate;
    private boolean debugOn;

    private List<IFeatureExtractor<MCRankerUserInfo, CandidateItem, RankerContextInfo>> featureExtractors;
    Map<String, Function<RecommendImpression, Long>> labelSupplies;
    private String mainLabel; //当有多个label时，负采样使用mainLabel。
    private CollectorFactory collectorFactory;
    private ISamplesBatchFilter samplesBatchFilter;

    private final static String FEATURE_EXTRACT_CLAZZ_PACKAGE_PREFIX = "com.xiaomi.dataming.traindata.scorers.features.";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        keyOutput = new Text();
        valueOutput = new BytesWritable();
        negativeRate = context.getConfiguration().getDouble("recommender.extractor.negative-rate", 1.0);
        samplingRate = context.getConfiguration().getDouble("recommender.extractor.sampling-rate", 1.0);
        random = new Random();
        queueNames = new HashSet<>();
        isDev = context.getConfiguration().getBoolean("recommender.extractor.dev", false);
        exposeTime = context.getConfiguration().getLong("recommender.extractor.expose-time", 2500); // ?
        maskRate = context.getConfiguration().getDouble("recommender.extractor.mask-rate", 0.15); // ?
        String[] queues = context.getConfiguration().getStrings("recommender.extractor.queues");
        if (queues != null && queues.length > 0) {
            queueNames.addAll(Lists.newArrayList(queues));
        }
        debugOn = context.getConfiguration().getBoolean("recommender.extractor.debugOn", false);

        String filterClazz  = context.getConfiguration().get("recommender.extractor.filter-clazz", null);

        // label and feature extractors
        labelSupplies = new HashMap<>();
        featureExtractors = new ArrayList<>();
        // LABEL:ACCEPTED
        // LABEL:ACTIVATED,LABEL:PAID
        String[] labelFieldMap = context.getConfiguration().getStrings("recommender.extractor.label2field", "LABEL:CLICKED");
        String[] featExtractors = context.getConfiguration().getStrings("recommender.extractor.feature-extractors",
            "BaseProfileContextFeatureExtractor");

        try {
            // feature extractors
            for (String extractor: featExtractors){
                String fullClazz = extractor.startsWith(FEATURE_EXTRACT_CLAZZ_PACKAGE_PREFIX) ? extractor:
                    FEATURE_EXTRACT_CLAZZ_PACKAGE_PREFIX + extractor;
                System.err.println("extractor: " + fullClazz);
                IFeatureExtractor ext = (IFeatureExtractor) Class.forName(fullClazz).newInstance();
                featureExtractors.add(ext);
            }
            System.err.println("featureExtractors: ");
            featureExtractors.forEach(System.err::println);

            // TODO: 这里需要处理一下
            // label setting
            for (String labelField: labelFieldMap) {
                // LABEL:ACCEPTED
                String[] labelAndField = labelField.split(":");
                Function<RecommendImpression, Long> LabelAccessFunc = (imp) -> {
                    // ACCEPTED
                    Object value = imp.getFieldValue(RecommendImpression._Fields.valueOf(labelAndField[1].toUpperCase()));
                    if (value instanceof Boolean) {
                        return ((Boolean) value) ? 1L : 0L;
                    }
                    else if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    else {
                        throw new RuntimeException("unknown value: " + value);
                    }
                };
                // LABEL
                labelSupplies.put(labelAndField[0], LabelAccessFunc);
            }

            // 下面的暂时不用
            // samplesBatchFilter
            if (StringUtils.isNotBlank(filterClazz)) {
                String fullFilterClazz = (filterClazz.startsWith("com.xiaomi.ai.recommender.framework.models.")) ?
                    filterClazz : "com.xiaomi.ai.recommender.framework.models." + filterClazz;
                samplesBatchFilter = (ISamplesBatchFilter) Class.forName(fullFilterClazz).newInstance();
                samplesBatchFilter.setup(context.getConfiguration());
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // TODO: 再建一个 activated_and_paid的label
        mainLabel = labelFieldMap[0].split(":")[0];

        String collectorType = context.getConfiguration().get("recommender.extractor.collector-type", "RAW");
        collectorFactory = new CollectorFactory() {
            @Override
            public Collector create() {
                return TensorCollector.builder().strCollectorType(ScorerConfigs.StrCollectorType.valueOf(collectorType)).debugMode(debugOn).build();
            }
        };

    }

    @Override
    protected void map(Void key, MCRecommendSamplesBatch value, Context context) throws IOException, InterruptedException {
        if (samplesBatchFilter != null && !samplesBatchFilter.filter(value)) {
            return;
        }

        List<Example> examples = ExampleUtils.offlineGenExamples(collectorFactory, value.user, value.items, value.context, featureExtractors, labelSupplies);
        for (Example example: examples) {
            long label = example.getFeatures().getFeatureOrThrow(mainLabel).getInt64List().getValue(0);
            boolean emit = ((label == 1) || (label == 0 && random.nextDouble() < negativeRate)) &&
                (random.nextDouble() < samplingRate);
            if (emit) {
                byte[] bytes = example.toByteArray();
                keyOutput.set(Integer.toString(example.hashCode()));
                valueOutput.set(bytes, 0, bytes.length);
                context.write(keyOutput, valueOutput);
            }
        }

    }



}
