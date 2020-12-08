package com.xiaomi.dataming.traindata.scorers.features.game;

import com.xiaomi.data.aiservice.contest.Stats;
import com.xiaomi.dataming.traindata.scorers.collector.Collector;
import com.xiaomi.dataming.traindata.scorers.features.BaseFeatureGroup;
import com.xiaomi.dataming.traindata.scorers.features.FeatureUtils;
import com.xiaomi.dataming.traindata.scorers.features.ICrossFeatureExtractor;
import org.apache.commons.lang3.StringUtils;
import com.xiaomi.data.aiservice.contest.CandidateItem;
import com.xiaomi.data.aiservice.contest.MCRankerUserInfo;
import com.xiaomi.data.aiservice.contest.RankerContextInfo;

import java.util.List;

public class GameCrossFeatureExtractor implements ICrossFeatureExtractor<MCRankerUserInfo, CandidateItem, RankerContextInfo> {

    @Override
    public void extract(MCRankerUserInfo user, CandidateItem item, RankerContextInfo context, Collector collector) {
        if (user == null || item == null || item.recommend_item == null) return;

        /*
        // 用户对item各个维度的历史行为统计（不同时间周期，如：前一月、前二月、前三月）
            struct ActivityStatsInfo {
              1:optional list<MCRecommendItem.Stats> paid_rank_radio_stats;// user付费sum/average/min/max/std 排名比
              2:optional list<MCRecommendItem.Stats> paid_cnt_stats;// user对多少款游戏付过款
              3:optional list<MCRecommendItem.Stats> is_paid_stats;// user有没有对热门的激活付费游戏付过费
              // TODO add other, like: 用户对不同一级分类花了多少钱
            }
         */
        // user付费sum/average/min/max/std 排名比
        if (user.activity_stats_info != null && user.activity_stats_info.paid_rank_radio_stats != null) {
            String gameId = item.recommend_item.game_id_s;
            if (StringUtils.isNotBlank(gameId)) {
                // TODO: add sum average 等等
                List<Float> maxPaidRankRadioList = FeatureUtils.activityByField(
                    user.activity_stats_info.paid_rank_radio_stats, Stats._Fields.MAX_RANK_RADIO);
                if (maxPaidRankRadioList.size() > 0) {
                    // FIXME: 这个在tf中怎么用？
                    collector.setNumerical(BaseFeatureGroup.PAID_MAX_RANK_RADIOS, maxPaidRankRadioList);
                }
            }
        }

//        // 用户对推荐DOMAIN的历史行为统计
//        if (user.activity_stats_info != null && user.activity_stats_info.domain_stats != null) {
//            String slaveDomain = item.recommend_item.item_domain;
//            if (StringUtils.isNotBlank(slaveDomain) && user.activity_stats_info.domain_stats.containsKey(slaveDomain)) {
//                long domainPv = FeatureUtils.activityByKeyAndCycle(user.activity_stats_info.domain_stats, slaveDomain, (short) 30, Stats._Fields.IMPRESSSION);
//                if (domainPv > 0) {
//                    collector.setInt64(BaseFeatureGroup.USER_DOMAIN_PV, domainPv);
//                }
//            }
//        }

    }


}
