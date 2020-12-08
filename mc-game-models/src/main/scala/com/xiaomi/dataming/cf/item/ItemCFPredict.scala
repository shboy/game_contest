package com.xiaomi.dataming.cf.item

import java.text.SimpleDateFormat

import com.xiaomi.data.aiservice.quanzhiRecomend.{MCItemBasedSimilarity, MCItemBasedSimilarityUserItemInfo}
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.util.{DateUtils, Util, Validation}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/8
 */
object ItemCFPredict {
  implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    Util.setLoggerLevel("warn")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    implicit val sc:SparkContext = new SparkContext(sparkConf)

    LOGGER.info("spark conf: {}", sparkConf)
    val argsMap = Util.getArgMap(args)

    val trainStartDate = argsMap("trainStartDate") // 20200601
    val trainEndDate = argsMap("trainEndDate") // 20200831
    val predictDate = DateUtils.getDateFromAddNDays(trainEndDate, 1, new SimpleDateFormat("yyyyMMdd")) // validation: 20200827 test: 20200901  这个为${trainEndDate}+1

    val isUseTimeDecay = argsMap("isUseTimeDecay").toBoolean // 是否使用时间衰减 true/false

    val nDays = argsMap.getOrElse("nDays", "10").toInt
    val userRankNum = argsMap.getOrElse("userRankNum", "-1").toInt // 20 30
    val topSimiNum = argsMap.getOrElse("topSimiNum", "-1").toInt // keep topSimiNum for popular items only (不包含自己)

    val popularRadio = argsMap("popularRadio").toDouble // (0, 1] 收录每天热榜的前popularRadio*100%

    val recallNum = argsMap("recallNum").toInt // 每个用户召回 recallNum 个 1000

    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/multiRecall/UIMatrix/
    val UIMatrixInputPath =
      if (userRankNum == -1) {
        argsMap("UIMatrixInputPathPrefix") +
          s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"
      } else {
        argsMap("UIMatrixInputPathPrefix") +
          s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}_userRankNum_${userRankNum}"
      }

    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_userid_paid_info/userPaidSortedSeq/final/activatedAndPaidGames/IIMatrix/
    val IIMatrixInputPath =
      if (topSimiNum == -1)
        argsMap("IIMatrixInputPathPrefix") + s"${trainStartDate}_${trainEndDate}_nDays_${nDays}_popularRadio_${popularRadio}"
      else
        argsMap("IIMatrixInputPathPrefix") + s"${trainStartDate}_${trainEndDate}_nDays_${nDays}_popularRadio_${popularRadio}_topSimiNum_${topSimiNum}"

    val isValuate = argsMap("isValuate").toBoolean // true/false

    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/multiRecall/activatedAndPaidGames/cf/userRecallItems/
    val userRecallItemsFromSimilarItemOutputPath = argsMap("userRecallItemsFromSimilarItemOutputPathPrefix") +
      s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}_topSimiNum_${topSimiNum}_recallNum_${recallNum}"

    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/multiRecall/activatedAndPaidGames/cf/userRecallItems_result/
    // 最优： 20200601_20200831_isUseTimeDecay_true_topSimiNum_20_recallNum_1000.txt
    val valuateResultPath = argsMap("valuateResultPathPrefix") +
      s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}_topSimiNum_${topSimiNum}_recallNum_${recallNum}.txt"

    LOGGER.warn(s"trainStartDate: $trainStartDate, trainEndDate: $trainEndDate, predictDate: $predictDate, " +
      s"recallNum: $recallNum, userRecallItemsFromSimilarItemOutputPath: $userRecallItemsFromSimilarItemOutputPath")

    val UIMatrix = sc.thriftParquetFile(UIMatrixInputPath, classOf[MCItemBasedSimilarityUserItemInfo])

    val IIMatrix = sc.thriftParquetFile(IIMatrixInputPath, classOf[MCItemBasedSimilarity])

    val recallResultRDD: RDD[String] = UIMatrix
      .cartesian(IIMatrix)
      .flatMap { case (userInfo, itemSimilarityInfo) =>
        val curItemInfo = itemSimilarityInfo.item
        val itemId2SimilarityScoreMap: Map[String, Double] = itemSimilarityInfo.similarItems.asScala
          .map { info =>
            val itemId: String = info.itemId
            val similarity: Double = info.score
            itemId -> similarity
          }.toMap
        val userId: String = userInfo.userid
        val userCurItemRateScore: Double = userInfo.items.asScala
          .flatMap {info =>
            val itemId: String = info.itemId
            val userRateSore: Double = info.score
            if (!itemId2SimilarityScoreMap.contains(itemId)) None
            else Some(itemId2SimilarityScoreMap(itemId)*userRateSore)
          }.sum
        if (userCurItemRateScore != 0) Some(userId, (curItemInfo.itemId, userCurItemRateScore))
        else None
      }.aggregateByKey(List.empty[(String, Double)])((u,v) => (v::u).sortBy(_._2).takeRight(recallNum), (u0, u1) => u0++u1)
      .map {case (userId, itemInfoArr) =>
        val recallItems = itemInfoArr.sortBy(_._2).takeRight(recallNum)
        userId + "\t" + recallItems.map {case (itemId, recallScore) =>
          Array(itemId, recallScore.formatted("%.6f")).mkString(":")
        }.mkString("|")
      }


    Util.deleteFile(userRecallItemsFromSimilarItemOutputPath)
    recallResultRDD
      .saveAsTextFile(userRecallItemsFromSimilarItemOutputPath)

    if (trainEndDate != "20200920" && isValuate)
      Validation.valuate(valuateResultPath,
        List(userRecallItemsFromSimilarItemOutputPath),
        predictDate, DateUtils.getDateFromAddNDays(predictDate, 6, new SimpleDateFormat("yyyyMMdd")))

    LOGGER.warn("done")
    sc.stop()
  }
  
}
