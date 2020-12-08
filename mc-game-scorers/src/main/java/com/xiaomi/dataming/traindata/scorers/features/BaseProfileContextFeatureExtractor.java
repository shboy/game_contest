package com.xiaomi.dataming.traindata.scorers.features;

import com.xiaomi.data.aiservice.contest.CandidateItem;
import com.xiaomi.data.aiservice.contest.MCRankerUserInfo;
import com.xiaomi.data.aiservice.contest.RankerContextInfo;
import com.xiaomi.data.aiservice.contest.UserProfileInfo;
import com.xiaomi.dataming.traindata.scorers.collector.Collector;

/**
 * 只抽取用户画像特征
 */
public class BaseProfileContextFeatureExtractor implements IUserContextFeatureExtractor<MCRankerUserInfo, CandidateItem, RankerContextInfo> {

    @Override
    public void extract(MCRankerUserInfo user, CandidateItem item, RankerContextInfo context, Collector collector) {
        UserProfileInfo profileInfo = user.profile_info;
        if (profileInfo == null) {
            return;
        }
        if (profileInfo.age > 0) {
            collector.setCategorical(BaseFeatureGroup.USER_AGE, Long.toString(profileInfo.age));
        }
        if (!profileInfo.sex.equals("0")) {
            collector.setCategorical(BaseFeatureGroup.USER_SEX, profileInfo.sex);
        }

        collector.setCategorical(BaseFeatureGroup.RESIDENT_CITY, profileInfo.city);
        collector.setCategorical(BaseFeatureGroup.USER_DEGREE, profileInfo.degree);
        collector.setCategorical(BaseFeatureGroup.PHONE_BRAND, profileInfo.phoneBrand);

    }
}
