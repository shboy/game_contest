package com.xiaomi.dataming.traindata

import com.xiaomi.data.aiservice.contest.{CycleType, MCRecommendItem, Stats}
import com.xiaomi.data.aiservice.quanzhiRecommend.MCGameActivatedAndPaidRankRadio
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.xiaomi.data.commons.spark.HdfsIO._

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/29
 */
object ItemFeatsUtils {
  
  def getGameActivatedAndPaidRankRadio(recalledGameIds: RDD[(String, MCRecommendItem)], date: String)(implicit sc: SparkContext): RDD[(String, List[Stats])] = {
	val gameActivatedAndPaidRankRadio1d = sc.thriftParquetFile(Constant.MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_1D_PATH_PREFIX+date, classOf[MCGameActivatedAndPaidRankRadio])
	  .keyBy(_.game_id)
	val gameActivatedAndPaidRankRadio3d = sc.thriftParquetFile(Constant.MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_3D_PATH_PREFIX+date, classOf[MCGameActivatedAndPaidRankRadio])
	  .keyBy(_.game_id)
	val gameActivatedAndPaidRankRadio5d = sc.thriftParquetFile(Constant.MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_5D_PATH_PREFIX+date, classOf[MCGameActivatedAndPaidRankRadio])
	  .keyBy(_.game_id)
	val gameActivatedAndPaidRankRadio15d = sc.thriftParquetFile(Constant.MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_15D_PATH_PREFIX+date, classOf[MCGameActivatedAndPaidRankRadio])
	  .keyBy(_.game_id)
	val gameActivatedAndPaidRankRadio30d = sc.thriftParquetFile(Constant.MC_GAME_ACTIVATED_AND_PAID_RANK_RADIO_30D_PATH_PREFIX+date, classOf[MCGameActivatedAndPaidRankRadio])
	  .keyBy(_.game_id)
	
	recalledGameIds
  	  .map(gameId => gameId._1 -> null)
	  .leftOuterJoin(gameActivatedAndPaidRankRadio1d)
	  .mapValues { case (gameId, gameRank1DOpt) =>
		(gameId, gameRank1DOpt)
	  }
	  .leftOuterJoin(gameActivatedAndPaidRankRadio3d)
	  .mapValues { case ((gameId, gameRank1DOpt), gameRank3DOpt) => (gameId, gameRank1DOpt, gameRank3DOpt) }
	  .leftOuterJoin(gameActivatedAndPaidRankRadio5d)
	  .mapValues { case ((gameId, gameRank1DOpt, gameRank3DOpt), gameRank5DOpt) => (gameId, gameRank1DOpt, gameRank3DOpt, gameRank5DOpt) }
	  .leftOuterJoin(gameActivatedAndPaidRankRadio15d)
	  .mapValues { case ((gameId, gameRank1DOpt, gameRank3DOpt, gameRank5DOpt), gameRank15DOpt) =>
		(gameId, gameRank1DOpt, gameRank3DOpt, gameRank5DOpt, gameRank15DOpt)
	  }
	  .leftOuterJoin(gameActivatedAndPaidRankRadio30d)
	  .mapValues { case ((gameId, gameRank1DOpt, gameRank3DOpt, gameRank5DOpt, gameRank15DOpt), gameRank30DOpt) =>
		(gameId, gameRank1DOpt, gameRank3DOpt, gameRank5DOpt, gameRank15DOpt, gameRank30DOpt)
	  }
	  .mapValues { case (gameId, gameRank1DOpt, gameRank3DOpt, gameRank5DOpt, gameRank15DOpt, gameRank30DOpt) =>
      val stats: List[Stats] = Array(gameRank1DOpt, gameRank3DOpt, gameRank5DOpt, gameRank15DOpt, gameRank30DOpt).zip(Array(1, 3, 5, 15, 30))
        .map { case (gameRankOpt, day) =>
          val stat = new Stats().setCycle(day.toShort).setCycleType(CycleType.DAY)
          if (gameRankOpt.isDefined) {
            stat.setRank_radio(gameRankOpt.get.pv_rank_radio)
          }
          stat
		  }.toList
		  stats
	  }
  }

}
