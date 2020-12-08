package com.xiaomi.dataming.traindata.scorers.features;

import com.xiaomi.data.aiservice.contest.Stats;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FeatureUtils {

    // 计算accept,reject,like,dislike的rate. eg: x rate = x/impression。
    public static float activityRate(Stats stats, Stats._Fields field) {
        if (0 == stats.impresssion) {
            return 0;
        }
        return (long) stats.getFieldValue(field) / (float) stats.impresssion;
    }

    public static float activityRateByCycle(List<Stats> cycleStats, short cycle, Stats._Fields field) {
        for (Stats stats: cycleStats) {
            if (cycle != stats.cycle) continue;
            return activityRate(stats, field);
        }
        throw new RuntimeException("Stats does not contain cycle " + cycle);
    }

    public static List<Float> activityByField(List<Stats> cycleStats, Stats._Fields field) {
        List<Float> activities = new ArrayList<>();
        for (Stats stats: cycleStats) {
            activities.add(((Double)stats.getFieldValue(field)).floatValue());
        }
        return activities;
    }

    public static long activityByCycle(List<Stats> cycleStats, short cycle, Stats._Fields field) {
        for (Stats stats: cycleStats) {
            if (cycle != stats.cycle) continue;
            return (long) stats.getFieldValue(field);
        }
        throw new RuntimeException("Stats does not contain cycle " + cycle);
    }

    public static long activityByKeyAndCycle(Map<String, List<Stats>> itemCycleStats, String key, short cycle, Stats._Fields field) {
        if (!itemCycleStats.containsKey(key)) {
            return 0L;
        }
        List<Stats> cycleStats = itemCycleStats.get(key);
        return activityByCycle(cycleStats, cycle, field);
    }

    // boundary: 升序排序的边界值。此处没有做判断！！！
    public static String bucketedStr(double value, double[] boundary) {
        String defaultRange = ">" + boundary[boundary.length-1];
        for (int i=0; i<boundary.length; i++) {
            if (value < boundary[i]) {
                if (i==0) {
                    return "<" + boundary[0];
                }
                else {
                    return "("+boundary[i-1]+", "+boundary[i]+"]";
                }
            }
        }
        return defaultRange;
    }

    // name -> Name
    // sex -> Sex
    public static String upperFirstChar(String str){
        if(StringUtils.isBlank(str)) return str;
        StringBuilder sb = new StringBuilder(str);
        sb.setCharAt(0, Character.toUpperCase(str.charAt(0)));
        return sb.toString();
    }


}
