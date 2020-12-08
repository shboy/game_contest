package com.xiaomi.dataming.traindata.scorers.collector;

public interface CollectorFactory<F> {
    Collector<F> create();
}
