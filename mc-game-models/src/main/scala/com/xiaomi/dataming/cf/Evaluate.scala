package com.xiaomi.dataming.cf

import java.text.SimpleDateFormat

import com.xiaomi.dataming.util.{DateUtils, Util, Validation}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Authors: shenhao <shenhao@xiaomi.com>
  * created on 20-11-11
  */
object Evaluate {
  implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    Util.setLoggerLevel("warn")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    implicit val sc:SparkContext = new SparkContext(sparkConf)

    LOGGER.info("spark conf: {}", sparkConf)
    val argsMap = Util.getArgMap(args)
    val trainStartDate = argsMap("trainStartDate") // 20200601
    val trainEndDate = argsMap("trainEndDate") // 20200831
    val isUseTimeDecay = argsMap.getOrElse("isUseTimeDecay", "true").toBoolean // 是否使用时间衰减 true/false
    val topSimiNum = argsMap.getOrElse("topSimiNum", "10").toInt // keep topSimiNum for popular items only (不包含自己)
    val recallNum = argsMap("recallNum").toInt // 每个用户召回 recallNum 个 1000

    assert(recallNum <= 1000, "recall num must <= 1000")

    val predictDate = DateUtils.getDateFromAddNDays(trainEndDate, 1, new SimpleDateFormat("yyyyMMdd")) // validation: 20200827 test: 20200901  这个为${trainEndDate}+1
    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/multiRecall/activatedAndPaidGames/cf/userRecallItems/
    val userRecallItemsFromSimilarItemOutputPath = argsMap("userRecallItemsFromSimilarItemOutputPathPrefix") +
      s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}_topSimiNum_${topSimiNum}_recallNum_1000"

    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/multiRecall/activatedAndPaidGames/cf/userRecallItems_result/
    val valuateResultPath = argsMap("valuateResultPathPrefix") +
      s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}_topSimiNum_${topSimiNum}_recallNum_${recallNum}.txt"

    if (trainEndDate != "20200920")
      Validation.valuateWithRecallNum(valuateResultPath,
        List(userRecallItemsFromSimilarItemOutputPath),
        recallNum, predictDate, DateUtils.getDateFromAddNDays(predictDate, 6, new SimpleDateFormat("yyyyMMdd")))

    LOGGER.warn("done")
    sc.stop()

  }

}
