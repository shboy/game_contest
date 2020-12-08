package com.xiaomi.dataming.traindata.scorers.features.game;

import com.xiaomi.data.aiservice.contest.CandidateItem;
import com.xiaomi.data.aiservice.contest.MCRankerUserInfo;
import com.xiaomi.data.aiservice.contest.RankerContextInfo;
import com.xiaomi.data.aiservice.xiaoai.Stats;
import com.xiaomi.dataming.traindata.scorers.collector.Collector;
import com.xiaomi.dataming.traindata.scorers.features.BaseFeatureGroup;
import com.xiaomi.dataming.traindata.scorers.features.IUserContextFeatureExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 抽取user和context特征
 */
public class GameUserContextFeatureExtractor implements IUserContextFeatureExtractor<MCRankerUserInfo, CandidateItem, RankerContextInfo> {

    @Override
    public void extract(MCRankerUserInfo user, CandidateItem item, RankerContextInfo context, Collector collector) {
        // context
        if (context != null) {
//            collector.setCategorical(BaseFeatureGroup.MASTER_DOMAIN, context.master_domain);
//            collector.setCategorical(BaseFeatureGroup.EXPOSE_TIME, Long.toString(context.timestamp));
        }

        // 用户对所有topic的行为特征。目前修改为对推荐topic的行为特征，属于cross feature。
        //if (user.activity_stats_info != null) {
        //    setCycleHistory(BaseFeatureGroup.USER_ALL_TOPICS_PV, user.activity_stats_info.item_stats, Stats._Fields.IMPRESSSION, (short) 30, collector);
        //    setCycleHistory(BaseFeatureGroup.USER_ALL_TOPICS_ACCEPT, user.activity_stats_info.item_stats, Stats._Fields.ACCEPT, (short) 30, collector);
        //}

    }


    public static void setCycleHistory(BaseFeatureGroup group, Map<String, List<Stats>> cycleStats, Stats._Fields field, short cycle, Collector collector) {
        if (cycleStats == null || cycleStats.isEmpty()) {
            return;
        }
        List keys = new ArrayList();
        List values = new ArrayList();
        for (String k: cycleStats.keySet()) {
            for (Stats stats: cycleStats.get(k)) {
                if (cycle != stats.cycle) continue;
                Long value = (Long) stats.getFieldValue(field);
                if (value > 0) {
                    keys.add(k);
                    values.add(value.floatValue());
                }
            }
        }
        if (keys.size() != 0) {
            collector.setWeightedCategorical(group, keys, values);
        }
    }

}
