package com.xiaomi.dataming.traindata

import java.lang.reflect.Method

import com.xiaomi.data.aiservice.contest.{CycleType, Stats}
import com.xiaomi.data.aiservice.quanzhiRecommend.{MCNDistinctGamesInNDays, MCUserActivatedAndPaidGamesVisitedInfo, MCUserPayFeeSRankInfo}

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/22
 */
object ObjectTransformers {
  def toStats(user3DayInfoOpt: Option[MCUserPayFeeSRankInfo],
              user7DayInfoOpt: Option[MCUserPayFeeSRankInfo],
              user15DayInfoOpt: Option[MCUserPayFeeSRankInfo],
              user30DayInfoOpt: Option[MCUserPayFeeSRankInfo]): List[Stats] = {

    val stats: List[Stats] = List(user3DayInfoOpt, user7DayInfoOpt, user15DayInfoOpt, user30DayInfoOpt).zip(List(3, 7, 15, 30))
      .map{ case (userPaidInfoOpt, day) =>
        val stat = new Stats().setCycleType(CycleType.DAY).setCycle(day.toShort)
        if (userPaidInfoOpt.isDefined) {
          val userPaidInfo: MCUserPayFeeSRankInfo = userPaidInfoOpt.get
          stat.setAvg_rank_radio(userPaidInfo.pay_fee_s_avg_rank_radio)
          stat.setMin_rank_radio(userPaidInfo.pay_fee_s_min_rank_radio)
          stat.setMax_rank_radio(userPaidInfo.pay_fee_s_max_rank_radio)
		      stat.setStd_rank_radio(userPaidInfo.pay_fee_s_stddev_rank_radio)
          stat.setMid_rank_radio(userPaidInfo.pay_fee_s_mid_rank_radio)
        }
        stat
      }
    stats
  }

  // FIXME: 可能会抛异常
  def toStats(userPaidGameCnt: MCNDistinctGamesInNDays): List[Stats] = {
    val stats: List[Stats] = List(3, 7, 15, 30).map { day =>
      // getGames_cnt_3d
      val getMethod: Method = classOf[MCNDistinctGamesInNDays].getDeclaredMethod(s"getGames_cnt_${day}d")
      val gamesCnt: Long = getMethod.invoke(userPaidGameCnt).asInstanceOf[Long]
      val stats = new Stats().setCycleType(CycleType.DAY).setCycle(day.toShort).setPaid_cnt(gamesCnt)
      stats
    }
    stats
  }

  def toStats(isPaid1DOpt: Option[MCUserActivatedAndPaidGamesVisitedInfo],
              isPaid3DOpt: Option[MCUserActivatedAndPaidGamesVisitedInfo],
              isPaid7DOpt: Option[MCUserActivatedAndPaidGamesVisitedInfo],
              isPaid15DOpt: Option[MCUserActivatedAndPaidGamesVisitedInfo],
              isPaid30DOpt: Option[MCUserActivatedAndPaidGamesVisitedInfo]): List[Stats] = {
    val stats: List[Stats] = List(isPaid1DOpt, isPaid3DOpt, isPaid7DOpt, isPaid15DOpt, isPaid30DOpt).zip(List(1, 3, 7, 15, 30))
      .map { case (isPaidOpt, day) =>
        val stat = new Stats().setCycleType(CycleType.DAY).setCycle(day.toShort)
        if (isPaidOpt.isDefined) {
          stat.setIs_paid(isPaidOpt.get.is_paid)
        }
        stat
      }
    stats
  }

}
