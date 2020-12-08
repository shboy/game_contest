package com.xiaomi.dataming.util

import java.text.SimpleDateFormat

import com.xiaomi.data.aiservice.quanzhiRecomend.{MCItemBasedSimilarityInfo, MCItemBasedSimilarityUserItemInfo}
import com.xiaomi.data.aiservice.quanzhiRecommend.{MCMostPopularGames, MCUserDoAction}
import com.xiaomi.dataming.cf.Constants._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.rdd.RDD
import com.xiaomi.data.commons.spark.HdfsIO._

import scala.collection.JavaConverters._

/**
 * Authors: shenhao <shenhao@xiaomi.com>
 * created on 2020/9/10
 */
object CFUtils {
	
	def getMostActivatedAndPaidGamesSetWithCnt(trainEndDate: String,
											   predictDate: String,
											   activatedAndPaidGamesPathsPrefixes: List[String],
											   nDays: Int = 14,
											   recallNum: Int=140)(implicit sc: SparkContext): Set[String] = {
		val activatedAndPaidGamesSetWithCnt: Set[String] =
			sc.union(DateUtils.getDateRange(DateUtils.getDateFromAddNDays(predictDate, -nDays, new SimpleDateFormat("yyyyMMdd")), trainEndDate)
				.flatMap {
					date =>
						activatedAndPaidGamesPathsPrefixes.map {
							activatedAndPaidGamesPathPrefix =>
								val activatedAndPaidGamesPath = activatedAndPaidGamesPathPrefix + s"date=$date"
								sc.thriftParquetFile(activatedAndPaidGamesPath, classOf[MCMostPopularGames])
									.map(gameInfo => gameInfo.game_id -> gameInfo.pv)
						}
				}).reduceByKey(_+_).sortBy(_._2, ascending = false).zipWithIndex().filter(_._2<=recallNum).map(_._1._1).collect().toSet
		activatedAndPaidGamesSetWithCnt
	}
	
	/**
	 * nDays天热榜
	 * @param trainEndDate
	 * @param predictDate
	 * @param activatedAndPaidGamesPathsPrefixes
	 * @param popularRadio
	 * @param nDays
	 * @param sc
	 * @return
	 */
	def getMostActivatedAndPaidGames(trainEndDate: String,
									 predictDate: String,
									 activatedAndPaidGamesPathsPrefixes: List[String],
									 popularRadio: Double,
									 nDays: Int = 16)(implicit sc: SparkContext): Set[String] = {
		val activatedAndPaidGamesSet: Set[String] =
			sc.union(DateUtils.getDateRange(DateUtils.getDateFromAddNDays(predictDate, -nDays, new SimpleDateFormat("yyyyMMdd")), trainEndDate)
				.flatMap {
					date =>
						activatedAndPaidGamesPathsPrefixes.map {
							activatedAndPaidGamesPathPrefix =>
								val activatedAndPaidGamesPath = activatedAndPaidGamesPathPrefix + s"date=$date"
								val sumCnt: Long = sc.thriftParquetFile(activatedAndPaidGamesPath, classOf[MCMostPopularGames]).count()
								val headCnt: Long = (sumCnt * popularRadio).toLong // 每张表取前 headCnt 个
								sc.thriftParquetFile(activatedAndPaidGamesPath, classOf[MCMostPopularGames])
									.map(gameInfo => gameInfo.game_id -> gameInfo.pv)
									.sortBy(_._2, ascending = false).zipWithIndex().filter(_._2 <= headCnt).map(_._1._1) // 该表的gameId已经是去了重的
						}
				}).distinct().collect().toSet
		activatedAndPaidGamesSet
	}

	/**
	 *
	 * @param userActionLogDataRDD
	 * @param predDate 预测哪一天的
	 * @param isUseTimeDecay 加不加时间衰减
	 * @return
	 */
	def getItem(userActionLogDataRDD: MCUserDoAction, predDate: String, isUseTimeDecay: Boolean): Option[MCItemBasedSimilarityInfo] = {
		val gameIdS = userActionLogDataRDD.game_id_s
		val actionDate = DateUtils.getDateTimeByTimestamp(userActionLogDataRDD.time_stamp, new SimpleDateFormat("yyyyMMdd"))
		val dateDev: Option[Int] = DateUtils.getDateDiffDay(predDate, actionDate, new SimpleDateFormat("yyyyMMdd"))
        if (dateDev.isEmpty) return None // 若是actionDate >= predDate 舍弃
		val score: Double = userActionLogDataRDD.action match {
			case PAY => 10
			case PAY_SLING => 8.94
			case EVENT_ACTIVE => 14.63
			case EVENT_CLICK => 4.86
			case EVENT_DOWNLOAD => 2.73
			case EVENT_NOTICE => 20
			case EVENT_RESERVE => 50
			case EVENT_VIDEO => 6.53
			case EVENT_VIEW => 0.098
			case _ => 0
		}
		score match {
			case 0 => None
			case _ =>
				/*
					时间衰减函数： 2^(-n)、 e^(-n), log(-1)x
		 		*/
				if (isUseTimeDecay) {
					if (score != 0) Some(new MCItemBasedSimilarityInfo().setItemId(gameIdS).setScore(score * Math.exp(-0.1 * dateDev.get)).setScoreType(0)) else None
				} else {
					if (score != 0) Some(new MCItemBasedSimilarityInfo().setItemId(gameIdS).setScore(score).setScoreType(0)) else None
				}
		}
	}

	/**
	  * 统计信息见： hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_game_info/ActionsOnMostActivatedAndPaidGamesStatistic/startDate_20200819_endDate_20200826_result.txt
	  * @param userActionLogDataRDD
	  * @param predDate 预测哪一天的
	  * @param isUseTimeDecay 加不加时间衰减
	  * @return
	  */
	def getItemForActivatedAndPaidGames(userActionLogDataRDD: MCUserDoAction, predDate: String, isUseTimeDecay: Boolean): Option[MCItemBasedSimilarityInfo] = {
		val gameIdS = userActionLogDataRDD.game_id_s
		val actionDate = DateUtils.getDateTimeByTimestamp(userActionLogDataRDD.time_stamp, new SimpleDateFormat("yyyyMMdd"))
		val action = userActionLogDataRDD.action
		val dateDev: Option[Int] = DateUtils.getDateDiffDay(predDate, actionDate, new SimpleDateFormat("yyyyMMdd"))
		if (dateDev.isEmpty) return None // 若是actionDate >= predDate 舍弃
		val score: Double = action match {
			case EVENT_VIEW => 0.013557
			case EVENT_DOWNLOAD => 0.395598
			case EVENT_CLICK => 0.803652
			case EVENT_VIDEO => 1.197834
			case EVENT_ACTIVE => 2.542724
			case EVENT_NOTICE => 8.253222
			case PAY_SLING => 8.527655
			case PAY => 10
			case EVENT_PUSH_THROUGH => 209.685466
			case EVENT_RESERVE => 753.136439

			case _ => 0
		}
		score match {
			case 0 => None
			case _ =>
				/*
					时间衰减函数： 2^(-n)、 e^(-n), log(-1)x
		 		*/
				if (isUseTimeDecay) {
					if (score != 0) Some(new MCItemBasedSimilarityInfo().setItemId(gameIdS).setDetail(action).setScore(score * Math.exp(-0.1 * dateDev.get)).setScoreType(0)) else None
				} else {
					if (score != 0) Some(new MCItemBasedSimilarityInfo().setItemId(gameIdS).setDetail(action).setScore(score).setScoreType(0)) else None
				}
		}
	}

	/**
	  * 统计信息见： hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_game_info/ActionsOnMostActivatedAndPaidGamesStatistic/startDate_20200819_endDate_20200826_result.txt
	  * @param action
	  * @param actionDate
	  * @param predDate
	  * @param cnt 计数
	  * @param isUseTimeDecay
	  * @return
	  */
	def getItemForActivatedAndPaidGames(action: String, actionDate: String, predDate: String, cnt: Long, isUseTimeDecay: Boolean): Option[Double] = {
		val dateDev: Option[Int] = DateUtils.getDateDiffDay(predDate, actionDate, new SimpleDateFormat("yyyyMMdd"))
		if (dateDev.isEmpty) return None // 若是actionDate >= predDate 舍弃
		val score: Double = action match {
			case EVENT_VIEW => 0.013557
			case EVENT_DOWNLOAD => 0.395598
			case EVENT_CLICK => 0.803652
			case EVENT_VIDEO => 1.197834
			case EVENT_ACTIVE => 2.542724
			case EVENT_NOTICE => 8.253222
			case PAY_SLING => 8.527655
			case PAY => 10
			case EVENT_PUSH_THROUGH => 209.685466
			case EVENT_RESERVE => 753.136439

			case _ => 0
		}
        if (score != 0) {
			if (isUseTimeDecay) Some(cnt * score * Math.exp(-0.1 * dateDev.get)) else Some(cnt * score)
		} else None
	}

	/**
	  *
	  * @param action
	  * @param actionDate
	  * @param predDate
	  * @param cnt
	  * @param isUseTimeDecay
	  * @return
	  */
	def getUserItemScore(action: String, actionDate: String, predDate: String, cnt: Long, isUseTimeDecay: Boolean): Option[Double] = {
		val dateDev: Option[Int] = DateUtils.getDateDiffDay(predDate, actionDate, new SimpleDateFormat("yyyyMMdd"))
		if (dateDev.isEmpty) return None // 若是actionDate >= predDate 舍弃
		val score: Double = action match {
			case EVENT_VIEW => 0.008166
			case EVENT_DOWNLOAD => 0.182050
			case EVENT_CLICK => 0.356845
			case EVENT_VIDEO => 0.869698
			case EVENT_ACTIVE => 1.059143
			case EVENT_NOTICE => 7.156829
			case PAY_SLING => 5.149797
			case PAY => 10
			case EVENT_PUSH_THROUGH => 113.589010
			case EVENT_RESERVE => 59.734607
			
			case _ => 0
		}
		if (score != 0) {
			if (isUseTimeDecay)
				Some(cnt * score * Math.exp(-0.1 * dateDev.get))
			else
				Some(cnt * score)
		} else None
	}
	
	/**
	 *
	 * @param action
	 * @param actionDate
	 * @param predDate
	 * @param cnt
	 * @param isUseTimeDecay
	 * @param actionScoresMap
	 * @return
	 */
	def getUserItemScoreWithActionScoresMap(action: String,
											actionDate: String,
											predDate: String,
											cnt: Long,
											isUseTimeDecay: Boolean,
											actionScoresMap: Map[String, Double]): Option[Double] = {
		val dateDev: Option[Int] = DateUtils.getDateDiffDay(predDate, actionDate, new SimpleDateFormat("yyyyMMdd"))
		if (dateDev.isEmpty) return None // 若是actionDate >= predDate 舍弃
		val score: Double = actionScoresMap.getOrElse(action, 0.0)
		if (score != 0) {
			if (isUseTimeDecay)
				Some(cnt * score * Math.exp(-0.05 * dateDev.get))
			else
				Some(cnt * score)
		} else None
	}

	/**
	 * 只考虑用户 付费、提交订单、激活的行为
	 * @param userActionLogDataRDD
	 * @param predDate
	 * @param isUseTimeDecay
	 * @return
	 */
	def getItemForW2V(userActionLogDataRDD: MCUserDoAction, predDate: String, isUseTimeDecay: Boolean): Option[MCItemBasedSimilarityInfo] = {
		val gameIdS = userActionLogDataRDD.game_id_s
		val actionDate = DateUtils.getDateTimeByTimestamp(userActionLogDataRDD.time_stamp, new SimpleDateFormat("yyyyMMdd"))
		val dateDev: Option[Int] = DateUtils.getDateDiffDay(predDate, actionDate, new SimpleDateFormat("yyyyMMdd"))
		if (dateDev.isEmpty) return None // 若是actionDate >= predDate 舍弃
		val score: Double = userActionLogDataRDD.action match {
			case PAY => 10
			case PAY_SLING => 8.94
			case EVENT_ACTIVE => 14.63
			case _ => 0
		}
		score match {
			case 0.0 => None
			case _ =>
				/*
					时间衰减函数： 2^(-n)、 e^(-n), log(-1)x
		 		*/
				if (isUseTimeDecay) {
					if (score != 0) Some(new MCItemBasedSimilarityInfo().setItemId(gameIdS).setScore(score * Math.exp(-0.1 * dateDev.get)).setScoreType(0)) else None
				} else {
					if (score != 0) Some(new MCItemBasedSimilarityInfo().setItemId(gameIdS).setScore(score).setScoreType(0)) else None
				}
		}
	}

	/**
	 * 计算item之间的余弦相似度
	 * @param userItemsRankRDD
	 * @return cosineSimilarity 全是正数
	 */
	def calcSimilarity(userItemsRankRDD: RDD[MCItemBasedSimilarityUserItemInfo]): RDD[((String, String), Double)] = {
		val userItemRDD: RDD[(String, MCItemBasedSimilarityInfo)] = userItemsRankRDD
			.flatMap{ userItemsRank => userItemsRank.items.asScala.map(item => userItemsRank.userid -> item)} // 前面已经做了限制，一个用户只取他感兴趣的前 userMostLiketopK 个

		val allItemScoreRDD: RDD[((String, String), Double)] =
			userItemRDD.join(userItemRDD)
				.mapValues { case (item_i, item_j) =>
					val item_ij = item_i.itemId -> item_j.itemId
					val score = item_i.score * item_j.score
					item_ij -> score
				}
				.map(_._2) // (item_i -> item_j) -> score
				.reduceByKey(_ + _)

		val itemL2Norm: RDD[(String, Double)] = // 一阶范数，作为预先相似度的分母
			allItemScoreRDD.flatMap { case ((item_i, item_j), score) =>
				if (item_i == item_j) Some(item_i -> score) else None
			}

		val itemPairRDD: RDD[(String, (String, Double))] =
			allItemScoreRDD.flatMap { case ((item_i, item_j), score) =>
				if (item_i != item_j) Some(item_i -> (item_j -> score)) else None
			}

		val cosineSimilarity: RDD[((String, String), Double)] = itemPairRDD
			.join(itemL2Norm)
			.map { case (item_i, ((item_j, score), item_i_l2norm)) =>
				item_j -> (item_i, score, item_i_l2norm)
			}.join(itemL2Norm)
			.map { case (item_j, ((item_i, score, item_i_l2norm), item_j_l2norm)) =>
				val sim = score / Math.sqrt(item_i_l2norm * item_j_l2norm)
				val key = item_i -> item_j
				key -> sim
			}

		cosineSimilarity
	}

	/**
	 * 通过 w2v 得到 item-item相似矩阵
	 * @param word2VecPath word2vec path
	 * @param mostSimilarItemsTopK 每个item取最相似的mostSimilarItemsTopK个
	 * @param sc
	 * @return (item -> similarItem) -> score
	 */
	def calcSimilarityWithW2V(word2VecPath: String, mostSimilarItemsTopK: Int)(implicit sc:SparkContext): RDD[((String, String), Double)] = {
		val model = Word2VecModel.load(sc, path = word2VecPath)
		sc.parallelize(model.getVectors.keySet.flatMap {
			gameIds => model.findSynonyms(gameIds, mostSimilarItemsTopK).map {
				case (similarGameId, similarityScore) => (gameIds -> similarGameId) -> similarityScore
			}
		}.toSeq)
	}

	def getWord2VecModel(word2VecPath: String)(implicit sc:SparkContext): Word2VecModel = {
		val model = Word2VecModel.load(sc, path = word2VecPath)
        model
	}

	def getWord2VecModelVectors(word2VecPath: String)(implicit sc:SparkContext): Map[String, Array[Float]] = {
		val model = Word2VecModel.load(sc, path = word2VecPath)
		model.getVectors
	}

}
