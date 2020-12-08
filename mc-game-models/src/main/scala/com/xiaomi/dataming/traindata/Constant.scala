package com.xiaomi.dataming.traindata

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/22
 */
object Constant {
  val USER_ATTRIB_PATH = "/user/h_data_platform/platform/aiservice/m_c_user_attrib/date=20200920"

  val MC_UserPayFee3DaysInfo_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_pay_fee_s_3_days_info/date="
  val MC_UserPayFee7DaysInfo_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_pay_fee_s_7_days_info/date="
  val MC_UserPayFee15DaysInfo_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_pay_fee_s_15_days_info/date="
  val MC_UserPayFee30DaysInfo_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_pay_fee_s_30_days_info/date="

  val MC_userPaidForNDistinctGames_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_paid_for_n_distinct_games/date="

  val MC_userActivatedAndPaidGamesPaidInfo1D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_activated_and_paid_games_paid_info_1_day/date="
  val MC_userActivatedAndPaidGamesPaidInfo3D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_activated_and_paid_games_paid_info_3_day/date="
  val MC_userActivatedAndPaidGamesPaidInfo7D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_activated_and_paid_games_paid_info_7_day/date="
  val MC_userActivatedAndPaidGamesPaidInfo15D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_activated_and_paid_games_paid_info_15_day/date="
  val MC_userActivatedAndPaidGamesPaidInfo30D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_user_activated_and_paid_games_paid_info_30_day/date="

  val userFeatureBasePathPrefix = "/user/h_data_platform/platform/aiservice/m_c_user_attrib/userFeature"

  val MC_GAME_INFO_PATH_PREFIX= "/user/h_data_platform/platform/aiservice/m_c_game_info/date="

  val MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_1D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_game_activated_and_paid_rank_radio_1d/date="
  val MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_3D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_game_activated_and_paid_rank_radio_3d/date="
  val MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_5D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_game_activated_and_paid_rank_radio_5d/date="
  val MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_15D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_game_activated_and_paid_rank_radio_15d/date="
  val MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_30D_PATH_PREFIX = "/user/h_data_platform/platform/aiservice/mc_game_activated_and_paid_rank_radio_30d/date="

//  val recallPath = "/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/multiRecall/activatedAndPaidGames/cf/userRecallItems/20200601_20200831_isUseTimeDecay_true_topSimiNum_20_recallNum_1000/"
  val recallPathFormat = "/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/multiRecall/activatedAndPaidGames/cf/userRecallItems/20200601_%s_isUseTimeDecay_true_topSimiNum_20_recallNum_1000/"

  val gameItemFeatureOutputPathPrefix = "/user/h_data_platform/platform/aiservice/m_c_game_info/gameItemFeats/date="

  val recSamplesBatchOutputPathPrefix = "/user/h_data_platform/platform/aiservice/m_c_user_attrib/recSamplesBatch/date="
  val recTfRecordOutputPathFormat = "/user/h_data_platform/platform/aiservice/m_c_user_attrib/training_examples/examples/date=%s/train"
  val recTfRecordCountOutputPathFormat = "/user/h_data_platform/platform/aiservice/m_c_user_attrib/training_examples/date=%s/features"
}
