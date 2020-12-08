package com.xiaomi.dataming.cf

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/9/11
 */
object Constants {
	val TAB = "\t"
	val SPACE = " "
	val USER_VISITED ="USER_VISITED" // 用户访问过的items
	val RECALL_PREDICT = "RECALL_PREDICT" // cf 召回的相似items

	val USER_PREDICTION_ITEM = "user_prediction_item"

	val PAY = "pay"
	val PAY_SLING = "pay-sling"

	val EVENT_ACTIVE = "EVENT_ACTIVE" // 激活游戏
	val EVENT_CLICK = "EVENT_CLICK" // 点击游戏
	val EVENT_DOWNLOAD = "EVENT_DOWNLOAD" // 下载游戏
	val EVENT_NOTICE = "EVENT_NOTICE" // 关注游戏
	val EVENT_RESERVE = "EVENT_RESERVE" // 预约游戏
	val EVENT_VIDEO = "EVENT_VIDEO" // 观看过该游戏视频
	val EVENT_VIEW = "EVENT_VIEW" // 浏览（被曝光该游戏）和信息平台推送，认为是用户被动的，非主动的action
	val EVENT_PUSH_THROUGH = "EVENT_PUSH_THROUGH"
	
	val VALID_ACTION_SET: Set[String] = Set(EVENT_ACTIVE, EVENT_CLICK, EVENT_DOWNLOAD,
		EVENT_NOTICE, EVENT_RESERVE, EVENT_VIDEO, EVENT_VIEW, EVENT_PUSH_THROUGH, PAY, PAY_SLING)
	
	// https://datum.xiaomi.com/#/metadetail/basicInfo/113238
	val userDoActionPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_user_do_action/"
	// https://datum.xiaomi.com/#/metadetail/basicInfo/113241
	val userPaidInfoPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_pay_info/"
	
	val gameInfoPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_game_info/"
 
	val paidGamesPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_game_paid_info/"
	
	// 验证集
	// https://datum.xiaomi.com/#/metadetail/basicInfo/114073
	val validationSetPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/"
 
	val activatedAndPaidGamesPathsPrefixes = List(
	  "hdfs://zjyprc-hadoop//user/h_data_platform/platform/aiservice/activated_and_paid_games_1d/",
	  "hdfs://zjyprc-hadoop//user/h_data_platform/platform/aiservice/activated_and_paid_games_2d/",
	  "hdfs://zjyprc-hadoop//user/h_data_platform/platform/aiservice/activated_and_paid_games_3d/",
	  "hdfs://zjyprc-hadoop//user/h_data_platform/platform/aiservice/activated_and_paid_games_4d/"
	)

}
