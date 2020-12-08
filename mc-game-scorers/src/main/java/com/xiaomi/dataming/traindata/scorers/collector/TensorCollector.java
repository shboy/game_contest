package com.xiaomi.dataming.traindata.scorers.collector;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.xiaomi.dataming.scorers.config.ScorerConfigs.StrCollectorType;
import com.xiaomi.dataming.traindata.scorers.features.BaseFeatureGroup;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import lombok.Builder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/***
  TensorCollector对string类型有三种处理方式：
    StrCollectorType.HASH: hashing（默认）。
    StrCollectorType.RAW: 使用原始字符串
    StrCollectorType.DICT: dict将string映射为id。
***/

@Builder
public class TensorCollector implements Collector<Features.Builder> {
    private final static Logger logger = LoggerFactory.getLogger(TensorCollector.class);
    private final static String WEIGHTED_CATEGORICAL_VALUE_SUFFIX = "_VALUE";
    private final static int seed = 17173;
    private final HashFunction hashFunction = Hashing.murmur3_128(seed);
    private final Features.Builder featuresBuilder = Features.newBuilder();

    // Attention: 以下类变量声明的顺序须和下面的全参构造函数形参顺序一致！！！
    @Builder.Default
    private StrCollectorType strCollectorType = StrCollectorType.HASH;
    private final boolean debugMode;
    private final Map<String, Map<String, Integer>> vocabulary;
    //没必要用双层hash
    private final Map<Long, String> debug;

    public TensorCollector(StrCollectorType type, boolean debugMode, Map<String, Map<String, Integer>> vocabulary,
                           Map<Long, String> debug) {
        this.strCollectorType = type;
        this.debugMode = debugMode;
        this.debug = (type == StrCollectorType.HASH && debugMode && debug == null) ? new Long2ObjectOpenHashMap<>() : null;
        this.vocabulary = vocabulary;
        if (type == StrCollectorType.DICT && vocabulary == null) {
            throw new RuntimeException("vocabulary is null in DICT mode!");
        }
    }

    public Map<Long, String> getDebug() {
        return this.debug;
    }

    public Map<String, Map<String, Integer>> getVocabulary() {
        return vocabulary;
    }

    public long hashFeature(BaseFeatureGroup group, String value) {
        long groupMask = ((long)group.getGroupId() & 0xFFL) << 48;
        long hashValue = Math.abs(hashFunction.hashString(value, Charset.defaultCharset()).asLong());
        return (hashValue & 0xFFFFFFFFFFFFL) | groupMask;
    }

    @Override
    public void setCategorical(BaseFeatureGroup group, String value) {
        if (StringUtils.isBlank(value)) {
            return ;
        }
        Feature.Builder featureBuilder = Feature.newBuilder();
        long feature = 0L;
        if (strCollectorType == StrCollectorType.HASH) {
            feature = hashFeature(group, value);
            featureBuilder.getInt64ListBuilder().addValue(feature);
        }
        else if (strCollectorType == StrCollectorType.DICT) {
            if (!vocabulary.containsKey(group.getGroupName())) {
                throw new RuntimeException(group + " not found in vocabulary.");
            }

            Map<String, Integer> featureVocabulary = vocabulary.get(group.getGroupName());
            Integer id = featureVocabulary.get(value);
            // TODO: default id
            if (id == null) {
                throw new RuntimeException(value + " not found in vocabulary group " + group + ".");
            }
            featureBuilder.getInt64ListBuilder().addValue(id);
        }
        else {
            ByteString bytes = ByteString.copyFromUtf8(value);
            featureBuilder.getBytesListBuilder().addValue(bytes);
        }
        featuresBuilder.putFeature(group.getGroupName(), featureBuilder.build());

        if (debug != null) {
            debug.put(feature, group.getGroupName() + ":" + value);
        }
    }

    public void setCategorical(BaseFeatureGroup group, List<String> values) {
        if (values.size() == 0) {
            return ;
        }
        Feature.Builder featureBuilder = Feature.newBuilder();
        switch (strCollectorType) {
            case HASH:
                values.forEach(value -> {
                    long feature = hashFeature(group, value);
                    featureBuilder.getInt64ListBuilder().addValue(feature);
                    if (debug != null) {
                        debug.put(feature, group.getGroupName() + ":" + value);
                    }
                });
                break;
            case DICT:
                values.forEach(value -> {
                    if (!vocabulary.containsKey(group.getGroupName())) {
                        // TODO: default id
                        throw new RuntimeException(group + " not found in vocabulary.");
                    }
                    Map<String, Integer> featureVocabulary = vocabulary.get(group.getGroupName());
                    long feature = featureVocabulary.get(value);
                    featureBuilder.getInt64ListBuilder().addValue(feature);
                    if (debug != null) {
                        debug.put(feature, group.getGroupName() + ":" + value);
                    }
                });
                break;
            default:
                values.forEach(value -> {
                    ByteString bytes = ByteString.copyFromUtf8(value);
                    featureBuilder.getBytesListBuilder().addValue(bytes);
                });
        }
        featuresBuilder.putFeature(group.getGroupName(), featureBuilder.build());
    }


    @Override
    public void setWeightedCategorical(BaseFeatureGroup group, List<String> values, List<Float> weights) {
        if (values.size() != weights.size()) {
            throw new RuntimeException(String.format("values size: %d not equals to weights size: %d", values.size(), weights.size()));
        }
        setCategorical(group, values);
        Feature.Builder featureBuilder = Feature.newBuilder();
        featureBuilder.getFloatListBuilder().addAllValue(weights);
        featuresBuilder.putFeature(group.getGroupName() + WEIGHTED_CATEGORICAL_VALUE_SUFFIX, featureBuilder.build());
    }

    @Override
    public void setNumerical(BaseFeatureGroup group, float value) {
        Feature.Builder featureBuilder = Feature.newBuilder();
        featureBuilder.getFloatListBuilder().addValue(value);
        featuresBuilder.putFeature(group.getGroupName(), featureBuilder.build());
    }

    @Override
    public void setNumerical(BaseFeatureGroup group, List<Float> values) {
        Feature.Builder featureBuilder = Feature.newBuilder();
        featureBuilder.getFloatListBuilder().addAllValue(values);
        featuresBuilder.putFeature(group.getGroupName(), featureBuilder.build());
    }

    @Override
    public void setInt64(BaseFeatureGroup group, long value) {
        Feature.Builder featureBuilder = Feature.newBuilder();
        featureBuilder.getInt64ListBuilder().addValue(value);
        featuresBuilder.putFeature(group.getGroupName(), featureBuilder.build());
    }

    @Override
    public void setInt64(BaseFeatureGroup group, List<Long> values) {
        Feature.Builder featureBuilder = Feature.newBuilder();
        featureBuilder.getInt64ListBuilder().addAllValue(values);
        featuresBuilder.putFeature(group.getGroupName(), featureBuilder.build());
    }

    @Override
    public Features.Builder getFeatures() {
        return this.featuresBuilder;
    }

    @Override
    public void putFeatures(Collector<Features.Builder> other) {
        if (other.getClass() != this.getClass()) {
            throw new RuntimeException("Two collector classes are not consistent: " + other.getClass());
        }
        if (other.getStrCollectorType() != this.getStrCollectorType()) {
            throw new RuntimeException("strCollectorTypes are not consistent: " + other.getStrCollectorType());
        }
        for (String groupName: other.getFeatures().getFeatureMap().keySet()) {
            if (this.featuresBuilder.containsFeature(groupName)) {
                throw new RuntimeException("Merging collectors with collides on group " + groupName);
            }
            this.featuresBuilder.putFeature(groupName, other.getFeatures().getFeatureMap().get(groupName));
        }
        if (this.debug != null) {
            debug.putAll(((TensorCollector)other).debug);
        }
    }

    @Override
    public StrCollectorType getStrCollectorType() {
        return strCollectorType;
    }
}
