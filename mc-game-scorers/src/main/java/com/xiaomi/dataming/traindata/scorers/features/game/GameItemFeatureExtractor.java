package com.xiaomi.dataming.traindata.scorers.features.game;

import com.xiaomi.data.aiservice.contest.CandidateItem;
import com.xiaomi.data.aiservice.contest.MCRankerUserInfo;
import com.xiaomi.data.aiservice.contest.RankerContextInfo;
import com.xiaomi.data.aiservice.contest.Stats;
import com.xiaomi.dataming.traindata.scorers.collector.Collector;
import com.xiaomi.dataming.traindata.scorers.features.BaseFeatureGroup;
import com.xiaomi.dataming.traindata.scorers.features.FeatureUtils;
import com.xiaomi.dataming.traindata.scorers.features.IItemFeatureExtractor;

import java.util.List;

public class GameItemFeatureExtractor implements IItemFeatureExtractor<MCRankerUserInfo, CandidateItem, RankerContextInfo> {
//    private static double[] impressionBoundary = new double[]{5000, 20000, 50000, 100000, 200000, 500000};
//    private static double[] acceptRateBoundary = new double[]{0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.1,0.11,0.12,
//                                                              0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.2,0.21,0.22,0.23,0.24,0.25};

    @Override
    public void extract(MCRankerUserInfo user, CandidateItem item, RankerContextInfo context, Collector collector) {
        if (item == null || item.recommend_item == null) {
            return;
        }

        // TODO: ---- 下面字段暂时无值或无有效值
        collector.setCategorical(BaseFeatureGroup.ITEM_ID, item.recommend_item.game_id_s);

        collector.setInt64(BaseFeatureGroup.CREATE_TIME_DEV, item.recommend_item.create_time_dev);
        collector.setInt64(BaseFeatureGroup.UPDATE_TIME_DEV, item.recommend_item.update_time_dev);

        collector.setNumerical(BaseFeatureGroup.USER_ITEM_RECALL_SCORE, Double.valueOf(item.recall_score).floatValue());



        if (item.item_stats_info != null && item.item_stats_info.item_activated_and_paid_rank_info != null) {

            List<Float> paidRankRadioList = FeatureUtils.activityByField(
                item.item_stats_info.item_activated_and_paid_rank_info, Stats._Fields.RANK_RADIO);
            if (paidRankRadioList.size() > 0) {
                // FIXME: 这个在tf中怎么用？
                collector.setNumerical(BaseFeatureGroup.PAID_MAX_RANK_RADIOS, paidRankRadioList);
            }
        }

//        if (item.item_stats_info != null && item.item_stats_info.domain_stats != null) {
//            long domainUv = FeatureUtils.activityByCycle(item.item_stats_info.domain_stats, (short) 7, Stats._Fields.UV);
//            collector.setInt64(BaseFeatureGroup.DOMAIN_UV, domainUv);
//        }

    }



}
