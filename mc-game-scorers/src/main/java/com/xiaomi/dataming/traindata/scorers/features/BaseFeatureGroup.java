package com.xiaomi.dataming.traindata.scorers.features;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
没有定义成枚举类型，考虑到以后可能会自动抽取特征， 特征名称和ID在配置文件或者管理平台里面定义
 */
public class BaseFeatureGroup {
    private final int id;
    private final FeatureType type;
    private final String name;
    private static Map<String, BaseFeatureGroup> objMap = new ConcurrentHashMap();

    public static BaseFeatureGroup REQUEST_ID = newInstance(0, "REQUEST_ID", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup MIAI_VERSION = newInstance(1, "MIAI_VERSION", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup MIPHONE_SERIES = newInstance(2, "MIPHONE_SERIES", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PHONE_MODEL = newInstance(3, "PHONE_MODEL", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PHONE_BRAND = newInstance(255, "PHONE_BRAND", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup NETWORK_TYPE = newInstance(4, "NETWORK_TYPE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup HOUR = newInstance(5, "HOUR", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PART_OF_DAY = newInstance(6, "PART_OF_DAY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup WEEKDAY = newInstance(7, "WEEKDAY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup APP_PACKAGE_NAME = newInstance(8, "APP_PACKAGE_NAME", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PHONE_IS_INTERNATIONAL = newInstance(9, "PHONE_IS_INTERNATIONAL", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PHONE_TELECOM = newInstance(10, "PHONE_TELECOM", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup CHANNEL = newInstance(11, "CHANNEL", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup OS = newInstance(12, "OS", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup CURRENT_ORIGIN = newInstance(13, "CURRENT_ORIGIN", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup TIMING = newInstance(14, "TIMING", FeatureType.CATEGORICAL);

    // User
    public static BaseFeatureGroup DEVICE_ID = newInstance(27, "DEVICE_ID", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_IN_RESIDENT_CITY = newInstance(28, "USER_IN_RESIDENT_CITY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_TRUE_PERSON = newInstance(29, "USER_TRUE_PERSON", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_ID_TYPE = newInstance(30, "USER_ID_TYPE", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup USER_AGE = newInstance(31, "USER_AGE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_SEX = newInstance(32, "USER_SEX", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_DEGREE = newInstance(33, "USER_DEGREE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup HAVE_KID = newInstance(34, "HAVE_KID", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup CURRENT_PROVINCE = newInstance(35, "CURRENT_PROVINCE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup CURRENT_CITY = newInstance(36, "CURRENT_CITY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup CURRENT_DISTRICT = newInstance(37, "CURRENT_DISTRICT", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup CURRENT_CITY_TYPE = newInstance(38, "CURRENT_CITY_TYPE", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup AVG_CNT = newInstance(39, "AVG_CNT", FeatureType.NUMERIC);
    public static BaseFeatureGroup DAY_CNT = newInstance(40, "DAY_CNT", FeatureType.NUMERIC);
    public static BaseFeatureGroup RECENCY = newInstance(41, "RECENCY", FeatureType.NUMERIC);
    public static BaseFeatureGroup CYCLE_CNT = newInstance(42, "CYCLE_CNT", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup DOMAIN_CNT = newInstance(43, "DOMAIN_CNT", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup DOMAIN_CYCLE_CNT = newInstance(44, "DOMAIN_CYCLE_CNT", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup AVG_DAY_FREQUENCY_MOIT = newInstance(45, "AVG_DAY_FREQUENCY_MOIT", FeatureType.NUMERIC);
    public static BaseFeatureGroup RECENCY_MIOT = newInstance(46, "RECENCY_MIOT", FeatureType.NUMERIC);
    public static BaseFeatureGroup ACTIVE_DAY_NUM_MIOT = newInstance(47, "ACTIVE_DAY_NUM_MIOT", FeatureType.NUMERIC);
    public static BaseFeatureGroup ACTIVE_DAYS_MUSIC = newInstance(48, "ACTIVE_DAYS_MUSIC", FeatureType.NUMERIC);
    public static BaseFeatureGroup TAGS_MUSIC = newInstance(49, "TAGS_MUSIC", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup APP_USAGE = newInstance(50, "APP_USAGE", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup MUSIC_SCORE = newInstance(51, "MUSIC_SCORE", FeatureType.NUMERIC);
    public static BaseFeatureGroup MOBILEVIDEO_SCORE = newInstance(52, "MOBILEVIDEO_SCORE", FeatureType.NUMERIC);
    public static BaseFeatureGroup STATION_SCORE = newInstance(53, "STATION_SCORE", FeatureType.NUMERIC);
    public static BaseFeatureGroup USER_ITEM_RECALL_SCORE = newInstance(53, "USER_ITEM_RECALL_SCORE", FeatureType.NUMERIC);

    //history Query
    public static BaseFeatureGroup TERM_FREQ = newInstance(54, "TERM_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup APP_CLICK = newInstance(55, "APP_CLICK", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup USER_TYPE = newInstance(56, "USER_TYPE", FeatureType.CATEGORICAL);

    // Query
    public static BaseFeatureGroup ITEM_ID = newInstance(59, "ITEM_ID", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup CREATE_TIME_DEV = newInstance(60, "CREATE_TIME_DEV", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup UPDATE_TIME_DEV = newInstance(61, "UPDATE_TIME_DEV", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_INTENT = newInstance(62, "QUERY_INTENT", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup QUERY_CATEGORY = newInstance(64, "QUERY_CATEGORY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_MODULE = newInstance(65, "QUERY_MODULE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_FUNC = newInstance(66, "QUERY_FUNC", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup QUERY_REFRESH = newInstance(67, "QUERY_REFRESH", FeatureType.NUMERIC);

    public static BaseFeatureGroup QUERY_SUBJECT = newInstance(70, "QUERY_SUBJECT", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_SUBJECT_TYPE = newInstance(71, "QUERY_SUBJECT_TYPE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_FRESHNESS = newInstance(72, "QUERY_FRESHNESS", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_DESCRIPTION_TERMS = newInstance(73, "QUERY_DESCRIPTION_TERMS", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup QUERY_SUBCATEGORY = newInstance(74, "QUERY_SUBCATEGORY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_FUNCTION = newInstance(75, "QUERY_FUNCTION", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_CP = newInstance(76, "QUERY_CP", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup SIDEKICK_QUERY_UNIGRAM = newInstance(87, "SIDEKICK_QUERY_UNIGRAM", FeatureType.CATEGORICAL);

    // Query Contextual
    public static BaseFeatureGroup POSITION = newInstance(90, "POSITION", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup RESIDENT_PROVINCE = newInstance(77, "RESIDENT_PROVINCE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup RESIDENT_CITY = newInstance(78, "RESIDENT_CITY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup IN_RESIDENT_PROVINCE = newInstance(79, "IN_RESIDENT_PROVINCE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup RECENT_TERM_FREQ = newInstance(80, "RECENT_TERM_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_ORIGIN_FREQ = newInstance(81, "RECENT_ORIGIN_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SUBCATEGORY_FREQ = newInstance(82, "RECENT_SUBCATEGORY_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_QUERY_TERM = newInstance(83, "RECENT_CLICKED_QUERY_TERM", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_ORIGIN_FREQ = newInstance(84, "RECENT_CLICKED_ORIGIN_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_SUBJECT = newInstance(85, "RECENT_CLICKED_SUBJECT", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup INSTALLED_APPS = newInstance(86, "INSTALLED_APPS", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup EXAMPLE_AGE = newInstance(89, "EXAMPLE_AGE", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup TODAY_TERM_FREQ = newInstance(100, "TODAY_TERM_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup TODAY_ORIGIN_FREQ = newInstance(101, "TODAY_ORIGIN_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup TODAY_SUBCATEGORY_FREQ = newInstance(102, "TODAY_SUBCATEGORY_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup TODAY_SUBJECT_CTR = newInstance(103, "TODAY_SUBJECT_CTR", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup TODAY_IMPRESSION_QUERY_COUNT = newInstance(104, "TODAY_IMPRESSION_QUERY_COUNT", FeatureType.NUMERIC);

    // Cross Feature
    public static BaseFeatureGroup MIAI_VERSION_WITH_PHONE_MODEL = newInstance(120, "MIAI_VERSION_WITH_PHONE_MODEL", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup MIAI_VERSION_WITH_CHANNEL = newInstance(121, "MIAI_VERSION_WITH_CHANNEL", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup MIAI_VERSION_WITH_OS = newInstance(122, "MIAI_VERSION_WITH_OS", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PART_OF_DAY_WITH_WEEKDAY = newInstance(123, "PART_OF_DAY_WITH_WEEKDAY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup HOUR_WITH_WEEKDAY = newInstance(124, "HOUR_WITH_WEEKDAY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup APP_PACKAGE_NAME_WITH_WEEKDAY = newInstance(125, "APP_PACKAGE_NAME_WITH_WEEKDAY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup APP_PACKAGE_NAME_WITH_WEEKDAY_WITH_PART_OF_DAY = newInstance(126, "APP_PACKAGE_NAME_WITH_WEEKDAY_WITH_PART_OF_DAY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup APP_PACKAGE_NAME_WITH_PART_OF_DAY = newInstance(127, "APP_PACKAGE_NAME_WITH_PART_OF_DAY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_AGE_WITH_USER_SEX = newInstance(128, "USER_AGE_WITH_USER_SEX", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_AGE_WITH_PHONE_MODEL = newInstance(129, "USER_AGE_WITH_PHONE_MODEL", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_AGE_WITH_USER_SEX_WITH_PHONE_MODEL = newInstance(130, "USER_AGE_WITH_USER_SEX_WITH_PHONE_MODEL", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_AGE_WITH_USER_SEX_WITH_QUERY_SUBJECT = newInstance(131, "USER_AGE_WITH_USER_SEX_WITH_QUERY_SUBJECT", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_AGE_WITH_USER_SEX_WITH_QUERY_SUBJECT_TYPE = newInstance(132, "USER_AGE_WITH_USER_SEX_WITH_QUERY_SUBJECT_TYPE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_AGE_WITH_USER_SEX_WITH_QUERY_UNIGRAM = newInstance(133, "USER_AGE_WITH_USER_SEX_WITH_QUERY_UNIGRAM", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_AGE_WITH_USER_SEX_WITH_QUERY_SUBCATEGORY = newInstance(134, "USER_AGE_WITH_USER_SEX_WITH_QUERY_SUBCATEGORY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PART_OF_DAY_WITH_RECENT_CLICKED_SUBJECT = newInstance(135, "PART_OF_DAY_WITH_RECENT_CLICKED_SUBJECT", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PART_OF_DAY_WITH_RECENT_SUBCATEGORY = newInstance(136, "PART_OF_DAY_WITH_RECENT_SUBCATEGORY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup APP_PACKAGE_NAME_WITH_SCENE = newInstance(137, "APP_PACKAGE_NAME_WITH_SCENE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup USER_AGE_WITH_USER_SEX_WITH_SCENE = newInstance(138, "USER_AGE_WITH_USER_SEX_WITH_SCENE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup PART_OF_DAY_WITH_SCENE = newInstance(139, "PART_OF_DAY_WITH_SCENE", FeatureType.CATEGORICAL);

    // tyw new feature
    public static BaseFeatureGroup RECENT_SHORT_TIME_QUERY_FREQ = newInstance(150, "RECENT_SHORT_TIME_QUERY_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SHORT_TIME_QUERY_DOMAIN_FREQ = newInstance(151, "RECENT_SHORT_TIME_QUERY_DOMAIN_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SHORT_TIME_QUERY_CATAGORY_FREQ = newInstance(152, "RECENT_SHORT_TIME_QUERY_CATAGORY_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SHORT_TIME_QUERY_SUBCATAGORY_FREQ = newInstance(153, "RECENT_SHORT_TIME_QUERY_SUBCATAGORY_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SHORT_TIME_QUERY_MODULE_FREQ = newInstance(154, "RECENT_SHORT_TIME_QUERY_MODULE_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SHORT_TIME_QUERY_FUNC_FREQ = newInstance(155, "RECENT_SHORT_TIME_QUERY_FUNC_FREQ", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SHORT_TIME_QUERY_ORIGIN_FREQ = newInstance(156, "RECENT_SHORT_TIME_QUERY_ORIGIN_FREQ", FeatureType.WEIGHTED_CATEGORICAL);

    public static BaseFeatureGroup QUERY_CTR = newInstance(63, "QUERY_CTR", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_WILSON_CTR = newInstance(160, "QUERY_WILSON_CTR", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_EXPOSURE = newInstance(68, "QUERY_EXPOSURE", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_CLICKS = newInstance(69, "QUERY_CLICKS", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup QUERY_EXPOSURE_CROSS_CTR = newInstance(161, "QUERY_EXPOSURE_CROSS_CTR", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup RECENT_CLICKED_CATAGORY = newInstance(163, "RECENT_CLICKED_CATAGORY", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_SUBCATEGORY = newInstance(164, "RECENT_CLICKED_SUBCATEGORY", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_MODULE = newInstance(165, "RECENT_CLICKED_MODULE", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_FUNC = newInstance(166, "RECENT_CLICKED_FUNC", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_DOMAIN = newInstance(167, "RECENT_CLICKED_DOMAIN", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SHOW_COUNT = newInstance(168, "RECENT_SHOW_COUNT", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICK_COUNT = newInstance(169, "RECENT_CLICK_COUNT", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICK_RATIO = newInstance(170, "RECENT_CLICK_RATIO", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup RECENT_CLICKED_PUSH_TASK = newInstance(187, "RECENT_CLICKED_PUSH_TASK", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_PUSH_FUNC = newInstance(188, "RECENT_CLICKED_PUSH_FUNC", FeatureType.WEIGHTED_CATEGORICAL);

    public static BaseFeatureGroup RECENT_TERM_FREQ_3M = newInstance(189, "RECENT_TERM_FREQ_3M", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_ORIGIN_FREQ_3M = newInstance(190, "RECENT_ORIGIN_FREQ_3M", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_SUBCATEGORY_FREQ_3M = newInstance(191, "RECENT_SUBCATEGORY_FREQ_3M", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_QUERY_TERM_3M = newInstance(192, "RECENT_CLICKED_QUERY_TERM_3M", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_ORIGIN_FREQ_3M = newInstance(193, "RECENT_CLICKED_ORIGIN_FREQ_3M", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup RECENT_CLICKED_SUBJECT_3M = newInstance(194, "RECENT_CLICKED_SUBJECT_3M", FeatureType.WEIGHTED_CATEGORICAL);

    // Scene Feature
    public static BaseFeatureGroup SCENE_ID = newInstance(200, "SCENE_ID", FeatureType.CATEGORICAL);

    public static BaseFeatureGroup ITEM_VEC_NO_SEGMENT = newInstance(204, "ITEM_VEC_NO_SEGMENT", FeatureType.NUMERIC);
    public static BaseFeatureGroup ITEM_VEC = newInstance(205, "ITEM_VEC", FeatureType.NUMERIC);
    public static BaseFeatureGroup SIDEKICK_QUERY_VEC = newInstance(206, "SIDEKICK_QUERY_VEC", FeatureType.NUMERIC);
    public static BaseFeatureGroup RECENT_TERM_VEC = newInstance(207, "RECENT_TERM_VEC", FeatureType.NUMERIC);
    public static BaseFeatureGroup RECENT_CLICK_TERM_VEC = newInstance(208, "RECENT_CLICK_TERM_VEC", FeatureType.NUMERIC);
    public static BaseFeatureGroup RECENT_TERM_3M_VEC = newInstance(209, "RECENT_TERM_3M_VEC", FeatureType.NUMERIC);
    public static BaseFeatureGroup RECENT_CLICK_TERM_3M_VEC = newInstance(210, "RECENT_CLICK_TERM_3M_VEC", FeatureType.NUMERIC);
    public static BaseFeatureGroup LAST_QUERIES_VEC = newInstance(211, "LAST_QUERIES_VEC", FeatureType.NUMERIC);

    public static BaseFeatureGroup TASK_TOTAL_EXPOSURE = newInstance(212, "TASK_TOTAL_EXPOSURE", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_TOTAL_CLICK = newInstance(213, "TASK_TOTAL_CLICK", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_TOTAL_CONVERSION = newInstance(214, "TASK_TOTAL_CONVERSION", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_TOTAL_CTR = newInstance(215, "TASK_TOTAL_CTR", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_TOTAL_CVR = newInstance(216, "TASK_TOTAL_CVR", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_EXPLORE_EXPOSURE = newInstance(217, "TASK_EXPLORE_EXPOSURE", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_EXPLORE_CLICK = newInstance(218, "TASK_EXPLORE_CLICK", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_EXPLORE_CONVERSION = newInstance(219, "TASK_EXPLORE_CONVERSION", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_EXPLORE_CTR = newInstance(220, "TASK_EXPLORE_CTR", FeatureType.NUMERIC);
    public static BaseFeatureGroup TASK_EXPLORE_CVR = newInstance(221, "TASK_EXPLORE_CVR", FeatureType.NUMERIC);

    public static BaseFeatureGroup QUERY_4FUNCS = newInstance(232, "QUERY_4FUNCS", FeatureType.CATEGORICAL);

    /***************************
    * quanzhi FEATURES
     ***************************/
    // CONTEXT
    public static BaseFeatureGroup EXPOSE_TIME = newInstance(255, "EXPOSE_TIME", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup MASTER_DOMAIN = newInstance(255, "MASTER_DOMAIN", FeatureType.CATEGORICAL);

    // ITEM
    public static BaseFeatureGroup QUERY_DOMAIN = newInstance(255, "QUERY_DOMAIN", FeatureType.CATEGORICAL);
    // TOPIC
    public static BaseFeatureGroup QUERY_TOPIC = newInstance(255, "QUERY_TOPIC", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup TOPIC_CATEGORY = newInstance(255, "TOPIC_CATEGORY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup TOPIC_SUBCATEGORY = newInstance(255, "TOPIC_SUBCATEGORY", FeatureType.CATEGORICAL);
    public static BaseFeatureGroup TOPIC_RULE_ID = newInstance(255, "TOPIC_RULE_ID", FeatureType.CATEGORICAL);

    // TOPIC在一段时间内的统计值：pv/accept/accept_rate
    public static BaseFeatureGroup TOPIC_PV = newInstance(255, "TOPIC_PV", FeatureType.NUMERIC);
    public static BaseFeatureGroup TOPIC_ACCEPT = newInstance(255, "TOPIC_ACCEPT", FeatureType.NUMERIC);
    public static BaseFeatureGroup TOPIC_ACCEPT_RATE = newInstance(255, "TOPIC_ACCEPT_RATE", FeatureType.NUMERIC);

    // USER
    // 用户对推荐topic的历史行为统计
    public static BaseFeatureGroup USER_DOMAIN_PV = newInstance(255, "USER_DOMAIN_PV", FeatureType.NUMERIC);
    public static BaseFeatureGroup USER_TOPIC_PV = newInstance(255, "USER_TOPIC_PV", FeatureType.NUMERIC);
    // FIXME： 这要加一个numeric list么?
    public static BaseFeatureGroup PAID_MAX_RANK_RADIOS = newInstance(255, "PAID_MAX_RANK_RADIOS", FeatureType.NUMERIC);
    public static BaseFeatureGroup PAID_RANK_RADIOS = newInstance(255, "PAID_RANK_RADIOS", FeatureType.NUMERIC);
    public static BaseFeatureGroup USER_TOPIC_ACCEPT_RATE = newInstance(255, "USER_TOPIC_ACCEPT_RATE", FeatureType.NUMERIC);
    public static BaseFeatureGroup TOPIC_ACCEPT_RATE_X_PV = newInstance(255, "TOPIC_ACCEPT_RATE_X_PV", FeatureType.CATEGORICAL);

    // 用户对所有topic的历史行为统计
    public static BaseFeatureGroup USER_ALL_TOPICS_PV = newInstance(255, "USER_ALL_TOPICS_PV", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup USER_ALL_TOPICS_ACCEPT = newInstance(255, "USER_ALL_TOPICS_ACCEPT", FeatureType.WEIGHTED_CATEGORICAL);
    public static BaseFeatureGroup USER_ALL_TOPICS_ACCEPT_RATE = newInstance(255, "USER_ALL_TOPICS_ACCEPT_RATE", FeatureType.WEIGHTED_CATEGORICAL);


    public static BaseFeatureGroup DOMAIN_UV = newInstance(255, "DOMAIN_UV", FeatureType.NUMERIC);

    public static BaseFeatureGroup LABEL = newInstance(555, "LABEL", FeatureType.CATEGORICAL);

    private BaseFeatureGroup(int id, String name, FeatureType type) {
        this.id = id;
        this.name = name;
        this.type = type;
    }

    private static BaseFeatureGroup newInstance(int id, String name, FeatureType type) {
        BaseFeatureGroup featureGroup = new BaseFeatureGroup(id, name, type);
        objMap.put(featureGroup.name, featureGroup);
        return featureGroup;
    }

    public static BaseFeatureGroup valueOf(String name) throws Exception {
        if (objMap.containsKey(name)) {
            return objMap.get(name);
        } else {
            throw new Exception(name + "not found");
        }
    }

    public String getGroupName() {
        return name;
    }

    public int getGroupId() {
        return id;
    }

    public String toString() {
        return name;
    }



}
