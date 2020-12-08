package com.xiaomi.dataming.traindata;

import com.xiaomi.data.aiservice.contest.MCRecommendSamplesBatch;
import org.apache.hadoop.conf.Configuration;

public interface ISamplesBatchFilter {

    // parse configuration
    void setup(Configuration configuration);

    boolean filter(MCRecommendSamplesBatch batch);

}
