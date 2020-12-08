package com.xiaomi.dataming.traindata

import com.xiaomi.data.aiservice.contest.Stats
import com.xiaomi.data.aiservice.quanzhiRecommend.{MCUserActivatedAndPaidGamesVisitedInfo, MCUserAttrib, MCUserPayFeeSRankInfo}
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/29
 */
object UserFeatsUtils {
  def getUserPaidFeeRankInfo(userAttrib: RDD[(String, MCUserAttrib)], date: String)(implicit sc: SparkContext): RDD[(String, List[Stats])] = {
	// user付费sum/average/min/max/std 排名比
	val mcUserPayFee3DaysInfo: RDD[(String, MCUserPayFeeSRankInfo)] =
	  sc.thriftParquetFile(Constant.MC_UserPayFee3DaysInfo_PATH_PREFIX+date, classOf[MCUserPayFeeSRankInfo]).keyBy(_.uid)
	val mcUserPayFee7DaysInfo: RDD[(String, MCUserPayFeeSRankInfo)] =
	  sc.thriftParquetFile(Constant.MC_UserPayFee7DaysInfo_PATH_PREFIX+date, classOf[MCUserPayFeeSRankInfo]).keyBy(_.uid)
	val mcUserPayFee15DaysInfo: RDD[(String, MCUserPayFeeSRankInfo)] =
	  sc.thriftParquetFile(Constant.MC_UserPayFee15DaysInfo_PATH_PREFIX+date, classOf[MCUserPayFeeSRankInfo]).keyBy(_.uid)
	val mcUserPayFee30DaysInfo: RDD[(String, MCUserPayFeeSRankInfo)] =
	  sc.thriftParquetFile(Constant.MC_UserPayFee30DaysInfo_PATH_PREFIX+date, classOf[MCUserPayFeeSRankInfo]).keyBy(_.uid)

	// userId -> stats
	val userPaidFeeRankInfo: RDD[(String, List[Stats])] =
	  userAttrib.map(userAttrib=> userAttrib._1 -> null)
		.leftOuterJoin(mcUserPayFee3DaysInfo)
		.mapValues { case (attrib, userPayFee3DaysInfoOpt) =>
		  userPayFee3DaysInfoOpt
		}
		.leftOuterJoin(mcUserPayFee7DaysInfo)
		.mapValues { case (userPayFee3DaysInfoOpt, userPayFee7DaysInfoOpt) =>
		  (userPayFee3DaysInfoOpt, userPayFee7DaysInfoOpt)
		}
		.leftOuterJoin(mcUserPayFee15DaysInfo)
		.mapValues { case ((userPayFee3DaysInfoOpt, userPayFee7DaysInfoOpt), userPayFee15DaysInfoOpt) =>
		  (userPayFee3DaysInfoOpt, userPayFee7DaysInfoOpt, userPayFee15DaysInfoOpt)
		}
		.leftOuterJoin(mcUserPayFee30DaysInfo)
		.mapValues { case ((userPayFee3DaysInfoOpt, userPayFee7DaysInfoOpt, userPayFee15DaysInfoOpt), userPayFee30DaysInfoOpt) =>
		  (userPayFee3DaysInfoOpt, userPayFee7DaysInfoOpt, userPayFee15DaysInfoOpt, userPayFee30DaysInfoOpt)
		}
		.mapValues { case (user3DayInfoOpt, user7DayInfoOpt, user15DayInfoOpt, user30DayInfoOpt) =>
		  ObjectTransformers.toStats(user3DayInfoOpt, user7DayInfoOpt, user15DayInfoOpt, user30DayInfoOpt)
		}
	userPaidFeeRankInfo
  }


  def getUserIsPaidsInfo(userAttrib: RDD[(String, MCUserAttrib)], date: String)(implicit sc: SparkContext): RDD[(String, List[Stats])] = {
	// is_paid_stats: user有没有对热门的激活付费游戏付过费
	val isPaid1D = sc.thriftParquetFile(Constant.MC_userActivatedAndPaidGamesPaidInfo1D_PATH_PREFIX+date, classOf[MCUserActivatedAndPaidGamesVisitedInfo])
	  .keyBy(_.uid)
	val isPaid3D =
	  sc.thriftParquetFile(Constant.MC_userActivatedAndPaidGamesPaidInfo3D_PATH_PREFIX+date, classOf[MCUserActivatedAndPaidGamesVisitedInfo])
		.keyBy(_.uid)
	val isPaid7D =
	  sc.thriftParquetFile(Constant.MC_userActivatedAndPaidGamesPaidInfo7D_PATH_PREFIX+date, classOf[MCUserActivatedAndPaidGamesVisitedInfo])
		.keyBy(_.uid)
	val isPaid15D =
	  sc.thriftParquetFile(Constant.MC_userActivatedAndPaidGamesPaidInfo15D_PATH_PREFIX+date, classOf[MCUserActivatedAndPaidGamesVisitedInfo])
		.keyBy(_.uid)
	val isPaid30D =
	  sc.thriftParquetFile(Constant.MC_userActivatedAndPaidGamesPaidInfo30D_PATH_PREFIX+date, classOf[MCUserActivatedAndPaidGamesVisitedInfo])
		.keyBy(_.uid)

	val userIsPaidsInfo: RDD[(String, List[Stats])] =
	  userAttrib.map(userAttrib=> userAttrib._1 -> null).leftOuterJoin(isPaid1D)
		.mapValues { case (attrib, isPaid1DOpt) => isPaid1DOpt }
		.leftOuterJoin(isPaid3D)
		.mapValues { case (isPaid1DOpt, isPaid3DOpt) => (isPaid1DOpt, isPaid3DOpt) }
		.leftOuterJoin(isPaid7D)
		.mapValues { case ((isPaid1DOpt, isPaid3DOpt), isPaid7DOpt) => (isPaid1DOpt, isPaid3DOpt, isPaid7DOpt) }
		.leftOuterJoin(isPaid15D)
		.mapValues { case ((isPaid1DOpt, isPaid3DOpt, isPaid7DOpt), isPaid15DOpt) => (isPaid1DOpt, isPaid3DOpt, isPaid7DOpt, isPaid15DOpt) }
		.leftOuterJoin(isPaid30D)
		.mapValues { case ((isPaid1DOpt, isPaid3DOpt, isPaid7DOpt, isPaid15DOpt), isPaid30DOpt) =>
		  (isPaid1DOpt, isPaid3DOpt, isPaid7DOpt, isPaid15DOpt, isPaid30DOpt)
		}
		.mapValues { case (isPaid1DOpt, isPaid3DOpt, isPaid7DOpt, isPaid15DOpt, isPaid30DOpt) =>
		  ObjectTransformers.toStats(isPaid1DOpt, isPaid3DOpt, isPaid7DOpt, isPaid15DOpt, isPaid30DOpt)
		}

	userIsPaidsInfo
  }

}
