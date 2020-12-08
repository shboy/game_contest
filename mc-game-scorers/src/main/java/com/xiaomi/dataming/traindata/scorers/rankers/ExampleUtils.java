package com.xiaomi.dataming.traindata.scorers.rankers;

import com.xiaomi.data.aiservice.contest.CandidateItem;
import com.xiaomi.data.aiservice.contest.MCRankerUserInfo;
import com.xiaomi.data.aiservice.contest.RankerContextInfo;
import com.xiaomi.data.aiservice.contest.RecommendImpression;
import com.xiaomi.dataming.traindata.scorers.collector.Collector;
import com.xiaomi.dataming.traindata.scorers.collector.CollectorFactory;
import com.xiaomi.dataming.traindata.scorers.collector.TensorCollector;
import com.xiaomi.dataming.traindata.scorers.features.ICrossFeatureExtractor;
import com.xiaomi.dataming.traindata.scorers.features.IFeatureExtractor;
import com.xiaomi.dataming.traindata.scorers.features.IItemFeatureExtractor;
import com.xiaomi.dataming.traindata.scorers.features.IUserContextFeatureExtractor;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExampleUtils {

    private final static Logger logger = LoggerFactory.getLogger(ExampleUtils.class);

    public static <U extends TBase, I extends TBase, C extends TBase> List<Example> toExamples(CollectorFactory collectorFactory, U user, List<I> items,
                                                                                               C context, List<IFeatureExtractor<U, I, C>> iFeatureExtractors, RankerDebugger debugger) {
        ArrayList<Example> examples = new ArrayList<>();
        double[] scores = new double[items.size()];
        Collector userContextCollector = collectorFactory.create();

        // user context
        for (IFeatureExtractor extractor: iFeatureExtractors) {
            if (extractor instanceof IUserContextFeatureExtractor) {
                extractor.extract(user, null, context, userContextCollector);
            }
        }
        debugger.putAllFeatureId2Names(userContextCollector.getDebug());
        logger.debug("userContextCollector: \n{}", userContextCollector.getFeatures());

        // item (or cross)
        for (int i=0; i < items.size(); i++) {
            Collector collector = collectorFactory.create();;
            for (IFeatureExtractor extractor: iFeatureExtractors) {
                if (extractor instanceof IItemFeatureExtractor) {
                    // TODO: 这里注意一下 我把cacheItem注释了
                    if (debugger == null || !debugger.isDebugOn()) {
//                        new CachedItemFeatureExtractor((IItemFeatureExtractor)extractor, collectorFactory).extract(user, items.get(i), context, collector);
                        extractor.extract(user, items.get(i), context, collector);
                    } else {
                        extractor.extract(user, items.get(i), context, collector);
                    }
                }
                else if (extractor instanceof ICrossFeatureExtractor) {
                    extractor.extract(user, items.get(i), context, collector);
                }
            }

            debugger.putAllFeatureId2Names(collector.getDebug());
            logger.debug("[item {}] itemCollector: \n{}", i, collector.getFeatures());
            // merge item user context
            collector.putFeatures(userContextCollector);
            logger.debug("item {} merged collector: \n{}", i, collector.getFeatures());

            examples.add(Example.newBuilder().setFeatures(((TensorCollector)collector).getFeatures()).build());
        }
        return examples;
    }


    public static List<Example> offlineGenExamples(CollectorFactory collectorFactory,
                                                   MCRankerUserInfo user,
                                                   List<RecommendImpression> impressions,
                                                   RankerContextInfo context,
                                                   List<IFeatureExtractor<MCRankerUserInfo, CandidateItem, RankerContextInfo>> featureExtractors,
                                                   Map<String, Function<RecommendImpression, Long>> labelSupplies) {

        List<CandidateItem> items = impressions.stream().map(imp -> imp.item).collect(Collectors.toList());
        List<Example> examples = toExamples(collectorFactory, user, items, context, featureExtractors, new RankerDebugger(false));

        ArrayList<Example> examplesWithLabels = new ArrayList<>();

        for (int i=0; i < impressions.size(); i++) {
            Example.Builder exampleBuilder = examples.get(i).toBuilder();
            for (Map.Entry<String, Function<RecommendImpression, Long>> entry: labelSupplies.entrySet()) {
                long label = entry.getValue().apply(impressions.get(i));
                Feature.Builder featureBuilder = Feature.newBuilder();
                featureBuilder.getInt64ListBuilder().addValue(label);
                exampleBuilder.getFeaturesBuilder().putFeature(entry.getKey(), featureBuilder.build());
            }
            examplesWithLabels.add(exampleBuilder.build());
        }
        return examplesWithLabels;
    }

}
