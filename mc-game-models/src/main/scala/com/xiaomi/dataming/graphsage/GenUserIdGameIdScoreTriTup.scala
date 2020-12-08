package com.xiaomi.dataming.graphsage

import java.text.SimpleDateFormat

import com.xiaomi.data.aiservice.quanzhiRecommend.{MCPayInfo, MCUserDoAction, PaidInfo}
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.cf.Constants
import com.xiaomi.dataming.cf.Constants.{PAY, PAY_SLING, VALID_ACTION_SET}
import com.xiaomi.dataming.util.{CFUtils, DateUtils, Util}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * 产生 u,i,score三元组
 *
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/14
 */
object GenUserIdGameIdScoreTriTup {
  implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    Util.setLoggerLevel("warn")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    implicit val sc:SparkContext = new SparkContext(sparkConf)
  
    LOGGER.info("spark conf: {}", sparkConf)
    val argsMap = Util.getArgMap(args)
    // 以下三个日期的选择见 https://xiaomi.f.mioffice.cn/docs/dock4rGIxuFwofg36d0qsRflGec
    val trainStartDate = argsMap("trainStartDate") // 20200601
    val trainEndDate = argsMap("trainEndDate") // train: 20200826 test: 20200831
    // validation: 20200827 test: 20200901  这个为${trainEndDate}+1
    val predictDate = DateUtils.getDateFromAddNDays(trainEndDate, 1, new SimpleDateFormat("yyyyMMdd"))
  
    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_user_do_action/
    val userDoActionPathPrefix = Constants.userDoActionPathPrefix
  
    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_pay_info/
    val paidInfoPathPrefix = Constants.userPaidInfoPathPrefix
  
    val isUseTimeDecay = argsMap("isUseTimeDecay").toBoolean // 是否使用时间衰减 true/false
  
    // 用 [20200601， trainEndDate] 付过费的游戏就可以了, 证明如下
    // https://datum.xiaomi.com/#/adInquire/inquire?queryId=158088&pane=preview
    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_game_paid_info/
    val paidGamesPathPrefix = Constants.paidGamesPathPrefix
  
    // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/user2GameScores/
    val user2GameScoresOutputPath = argsMap("user2GameScoresOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"
  
    LOGGER.warn(s"trainStartDate: $trainStartDate, trainEndDate: $trainEndDate, predictDate: $predictDate, " +
      s"userDoActionPathPrefix: $userDoActionPathPrefix, isUseTimeDecay: $isUseTimeDecay, " +
      s"paidInfoPathPrefix: $paidInfoPathPrefix, user2GameScoresOutputPath: $user2GameScoresOutputPath"
    )
  
    // [20200601， trainEndDate] 付过费的游戏
    val paidGamesPath = paidGamesPathPrefix+s"date=${trainEndDate}"
    val paidGameIdSet: Set[String] = sc.thriftParquetFile(paidGamesPath, classOf[PaidInfo])
      .map(item => item.id).distinct().collect().toSet
    val paidGameIdSetBroadcast = sc.broadcast(paidGameIdSet)
  
    // 读取上游各action分数
    val actionsScoreFilePath = s"hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_user_do_action/userActionsStat/" +
      s"20200601_${trainEndDate}.txt"
    //        s"${trainStartDate}_${trainEndDate}.txt"
    val actionScoreMap: Map[String, Double] = sc.textFile(actionsScoreFilePath)
      .flatMap {line =>
        val arr = line.trim.split("\t")
        if (arr.length != 2) None
        else {
          val action = arr(0)
          val score = arr(1).toDouble
          Some(action -> score)
        }
      }.filter{ case (action, _) => VALID_ACTION_SET.contains(action) }
      .collectAsMap().toMap
    LOGGER.warn(s"actionScoreMap: ${actionScoreMap.map(item => item._1 + "\t" + item._2.toDouble).mkString("\n")}\n")
    val actionScoreMapBroadcast: Broadcast[Map[String, Double]] = sc.broadcast(actionScoreMap)
    
    val userId2GameScoresRDD: RDD[String] = sc.union(DateUtils.getDateRange(trainStartDate, trainEndDate).map { date =>
      val userDoActionPath = userDoActionPathPrefix+s"date=$date"
      val paidInfoPath = paidInfoPathPrefix+s"date=$date"
      val actionDate = date
      val paidGameIdSet = paidGameIdSetBroadcast.value
      val userDoActionRDD: RDD[((String, String), Double)] = sc.thriftParquetFile(userDoActionPath, classOf[MCUserDoAction])
        .filter(userAction =>
          StringUtils.isNotBlank(userAction.action)
            && StringUtils.isNotBlank(userAction.uid)
            && StringUtils.isNotBlank(userAction.game_id_s)
            && paidGameIdSet.contains(userAction.game_id_s)
        ).map { userAction =>
          val userId = userAction.uid
          val gameId = userAction.game_id_s
          val action = userAction.action
          (userId, gameId, action) -> 1L
        }.reduceByKey(_ + _)
        .flatMap {
          case ((userId, gameId, action), cnt) =>
            val actionScoresMap: Map[String, Double] = actionScoreMapBroadcast.value
            val scoreOpt: Option[Double] = CFUtils.getUserItemScoreWithActionScoresMap(
              action, actionDate, predictDate, cnt, isUseTimeDecay, actionScoresMap)
            if (scoreOpt.isDefined)
              Some((userId, gameId) -> scoreOpt.get)
            else None
        }
      val userPaidRDD: RDD[((String, String), Double)] = sc.thriftParquetFile(paidInfoPath, classOf[MCPayInfo])
        .filter(userPaid =>
          StringUtils.isNotBlank(userPaid.source)
            && StringUtils.isNotBlank(userPaid.uid)
            && StringUtils.isNotBlank(userPaid.game_id_s)
            && paidGameIdSet.contains(userPaid.game_id_s)
        )
        .flatMap { userPaid =>
          val userId = userPaid.uid
          val gameId = userPaid.game_id_s
          val action = userPaid.source
          val baseList: List[((String, String, String), Long)] = List((userId, gameId, PAY_SLING) -> 1L)
          action match {
            case PAY => ((userId, gameId, PAY) -> 1L) :: baseList
            case PAY_SLING => baseList
            case _ => None
          }
        }
        .reduceByKey(_ + _)
        .flatMap {
          case ((userId, gameId, action), cnt) =>
            val actionScoresMap: Map[String, Double] = actionScoreMapBroadcast.value
            val scoreOpt: Option[Double] = CFUtils.getUserItemScoreWithActionScoresMap(
              action, actionDate, predictDate, cnt, isUseTimeDecay, actionScoresMap)
            if (scoreOpt.isDefined)
              Some((userId, gameId) -> scoreOpt.get)
            else None
        }
//      Array(userDoActionRDD, userPaidRDD)
      userDoActionRDD.union(userPaidRDD).reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK_SER)
    }).reduceByKey(_ + _)
        .map { case ((userId, gameId), score) =>  userId -> Array(Array(gameId, score.formatted("%.6f").toString).mkString(","))}
        .reduceByKey(_ ++ _)
        .map { case (userId, gameId2ScorePairs) => userId+"\t"+gameId2ScorePairs.mkString("|") }
//      .map { case ((userId, gameId), score) => Array(userId, gameId, score.formatted("%.6f").toString).mkString(",") }
    
    Util.deleteFile(user2GameScoresOutputPath)
    userId2GameScoresRDD.saveAsTextFile(user2GameScoresOutputPath)
    
    LOGGER.warn(s"job done, save to $user2GameScoresOutputPath")
    
    sc.stop()
  }
  
}
