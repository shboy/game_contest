package com.xiaomi.dataming.traindata.scorers.collector;

import com.xiaomi.dataming.scorers.config.ScorerConfigs.StrCollectorType;
import com.xiaomi.dataming.traindata.scorers.features.BaseFeatureGroup;

import java.util.List;
import java.util.Map;

public interface Collector<F> {
    //类别特征
    void setCategorical(BaseFeatureGroup name, String value);
    //类别特征数组
    void setCategorical(BaseFeatureGroup name, List<String> values);
    //带权重的类别特征
    void setWeightedCategorical(BaseFeatureGroup name, List<String> values, List<Float> weights);
    //浮点数特征
    void setNumerical(BaseFeatureGroup name, float value);
    //浮点数特征数组
    void setNumerical(BaseFeatureGroup name, List<Float> values);
    //整数特征
    void setInt64(BaseFeatureGroup name, long value);
    //整数特征数组
    void setInt64(BaseFeatureGroup name, List<Long> values);
    //返回所有特征
    F getFeatures();
    //把另外一个Collector的特征全部复制过来
    void putFeatures(Collector<F> otherCollector);
    //类别特征类型： hash、dict、raw
    StrCollectorType getStrCollectorType();
    //特征值映射debug信息
    Map<Long, String> getDebug();
}
