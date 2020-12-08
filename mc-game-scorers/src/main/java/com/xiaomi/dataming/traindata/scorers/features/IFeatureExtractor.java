package com.xiaomi.dataming.traindata.scorers.features;

import com.xiaomi.dataming.traindata.scorers.collector.Collector;
import org.apache.thrift.TBase;

public interface IFeatureExtractor<U extends TBase, I extends TBase, C extends TBase> {
    void extract(U user, I item, C context, Collector collector);
}


