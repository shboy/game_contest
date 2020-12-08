package com.xiaomi.dataming.graphsage

import java.text.SimpleDateFormat

import com.xiaomi.dataming.util.{DateUtils, Util}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/15
 */
object UserIdGameIdScoreTriTupAnalysis {
  implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    Util.setLoggerLevel("warn")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    implicit val sc: SparkContext = new SparkContext(sparkConf)

    LOGGER.info("spark conf: {}", sparkConf)
    val argsMap = Util.getArgMap(args)
    // 以下三个日期的选择见 https://xiaomi.f.mioffice.cn/docs/dock4rGIxuFwofg36d0qsRflGec
    val trainStartDate = argsMap("trainStartDate") // 20200601
    val trainEndDate = argsMap("trainEndDate") // train: 20200826 test: 20200831
    // validation: 20200827 test: 20200901  这个为${trainEndDate}+1
    val predictDate = DateUtils.getDateFromAddNDays(trainEndDate, 1, new SimpleDateFormat("yyyyMMdd"))
    val isUseTimeDecay = argsMap("isUseTimeDecay").toBoolean // 是否使用时间衰减 true/false
    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/user2GameScores/
    val user2GameScoresInputpath = argsMap("user2GameScoresInputpathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"

    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/cntStat/
    val cntStatOutputPath = argsMap("cntStatOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"

    val cntStat: RDD[(Int, Long)] = sc.textFile(user2GameScoresInputpath).map { line =>
      val Array(userId, gameId2scores) = line.split("\t")
      gameId2scores.split("\\|").size -> 1L
    }.reduceByKey(_ + _)

    val cntStatStr = cntStat.map { case (cnt, cntCnt) => cnt + "\t" + cntCnt }.collect().mkString("\n")

    Util.writeString(cntStatStr, cntStatOutputPath, true)

    sc.stop()
  }
}
