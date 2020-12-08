package com.xiaomi.dataming.traindata.scorers.rankers;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import lombok.Getter;

import java.util.Map;

public class RankerDebugger {
    @Getter
    private boolean isDebugOn;
    @Getter
    private Map<Long, String> featureId2Name;

    public RankerDebugger(boolean isDebugOn) {
        this.isDebugOn = isDebugOn;
        featureId2Name = new Long2ObjectOpenHashMap<>();
    }

    public void putFeatureId2Name(Long featureId, String featureName) {
        if (isDebugOn) {
            featureId2Name.put(featureId, featureName);
        }
    }

    public void putAllFeatureId2Names(Map<Long, String> featureId2Names) {
        if (isDebugOn && featureId2Names != null) {
            featureId2Name.putAll(featureId2Names);
        }
    }

}
