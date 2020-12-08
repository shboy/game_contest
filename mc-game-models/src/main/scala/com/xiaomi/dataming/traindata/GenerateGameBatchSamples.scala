package com.xiaomi.dataming.traindata

import java.util

import com.google.common.collect.Lists
import com.xiaomi.data.aiservice.contest.{MCRankerUserInfo, MCRecommendSamplesBatch, RankerContextInfo, RecommendImpression}
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.util.{Util, Validation}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.Random


/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/22
 *
 */
object GenerateGameBatchSamples {
  implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  case class GameItemRecallScorePair(gameId: String="",
                                      itemRecallScore: Double=0d,
                                      isActivated: Boolean=false,
                                      isPaid: Boolean=false)

  def main(args: Array[String]): Unit = {
    Util.setLoggerLevel("warn")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    implicit val sc: SparkContext = new SparkContext(sparkConf)

    LOGGER.warn("spark conf: {}", sparkConf.toDebugString)
    val argsMap = Util.getArgMap(args)

    val curDate = argsMap("date")
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val dateTime: DateTime = formatter.parseDateTime(curDate)
    val oneDayBefore: String = dateTime.minusDays(1).toString("yyyyMMdd")

//    val endDate = oneDayBefore

    val recallPath = Constant.recallPathFormat.format(oneDayBefore)

    val positiveLabelMap: Map[String, Set[String]] = Validation.getPositiveUsersAndGameMap(curDate, dateTime.plusDays(6).toString("yyyyMMdd"))
    val positiveLabelMapBroadcast = sc.broadcast(positiveLabelMap)

    val userFeatures = sc.thriftParquetFile(s"${Constant.userFeatureBasePathPrefix}/date=${oneDayBefore}", classOf[MCRankerUserInfo])
      .keyBy(_.uid)
    System.err.println(s"userFeats path: ${Constant.userFeatureBasePathPrefix}/date=${oneDayBefore}, userFeatures count: " + userFeatures.count())
    System.err.println(s"show some examples of userFeatures: ${userFeatures.map(_._2).take(10).mkString("\n")}")

    val recommendImpression: Map[String, RecommendImpression] = sc.thriftParquetFile(Constant.gameItemFeatureOutputPathPrefix + oneDayBefore, classOf[RecommendImpression])
      .keyBy(_.item.recommend_item.game_id_s).collectAsMap().toMap
    System.err.println(s"recImp path: ${Constant.gameItemFeatureOutputPathPrefix + oneDayBefore}, recommendImpression size: ${recommendImpression.size}")
    System.err.println(s"show some examples of recImp: ${recommendImpression.take(10).mkString("\n")}")

    val recommendImpressionBroadcast: Broadcast[Map[String, RecommendImpression]] = sc.broadcast(recommendImpression)

    val recommendSamplesBatch: RDD[MCRecommendSamplesBatch] = sc.textFile(recallPath).flatMap { line =>
        val Array(uid, game2scores) = line.split("\t")
        val positiveLabelMap: Map[String, Set[String]] = positiveLabelMapBroadcast.value
        var isActivated = false
        var isPaid = false
        var positiveCnt = 0
        val gameItemRecallScorePairs: Array[GameItemRecallScorePair] = game2scores.split("\\|").map { game2score =>
          val Array(gameId, score) = game2score.split(":")
          if (positiveLabelMap.contains(uid) && positiveLabelMap(uid).contains(gameId)) {
            isActivated = true
            isPaid = true
            positiveCnt += 1
          }
          GameItemRecallScorePair(gameId, score.toDouble, isActivated, isPaid)
        }
//         对负样本做千分之一采样, 大概有13w
//        if (positiveCnt == 0 && Random.nextFloat() < 0.001) {
//          Some(uid -> gameItemRecallScorePairs)
//        } else {
//          None
//        }
        Some(uid -> gameItemRecallScorePairs)
      }
//      .zipWithIndex()
//      .map { case ((uid, gameItemRecallScorePairs), cnt) =>
//        if (cnt < 100) {
//          System.err.println(s"01 cnt: $cnt, uid: $uid," +
//            s" gameids: ${gameItemRecallScorePairs.map(_.gameId).mkString("|")}," +
//            s" score: ${gameItemRecallScorePairs.map(_.itemRecallScore).mkString("|")}, " +
//            s"isActivated: ${gameItemRecallScorePairs.map(_.isActivated).mkString("|")}, " +
//            s"isPaid: ${gameItemRecallScorePairs.map(_.isPaid).mkString("|")}")
//        }
//        (uid, gameItemRecallScorePairs)
//      }
      .leftOuterJoin(userFeatures)
      .map { case (uid, (gameItemRecallScorePairs, userFeaturesOpt)) =>
        val userFeatures = userFeaturesOpt.getOrElse(new MCRankerUserInfo())
        userFeatures.setUid(uid)
        userFeatures -> gameItemRecallScorePairs
      }
//      .zipWithIndex()
//      .map { case ((userFeats, gameItemRecallScorePairs), cnt) =>
//        if (cnt < 100) {
//          System.err.println(s"02 cnt: $cnt, userFeats: $userFeats, gameItemRecallScorePairs: ${gameItemRecallScorePairs.mkString("|")}")
//        }
//        (userFeats, gameItemRecallScorePairs)
//      }
      .flatMap { case (userFeats, gameItemRecallScorePairs) =>
        val recommendImpressionMap: Map[String, RecommendImpression] = recommendImpressionBroadcast.value
        val recommendImpressions: Array[RecommendImpression] = gameItemRecallScorePairs.flatMap { gameItemRecallScorePair =>
          val gameId = gameItemRecallScorePair.gameId
          val itemRecallScore: Double = gameItemRecallScorePair.itemRecallScore
          val isActivated = gameItemRecallScorePair.isActivated
          val isPaid = gameItemRecallScorePair.isPaid
          if (recommendImpressionMap.contains(gameId)) {
            val recommendImpression: RecommendImpression = recommendImpressionMap(gameId)
            val recommendImpressionNew = recommendImpression.deepCopy()
            recommendImpressionNew.setActivated(isActivated).setPaid(isPaid)
              .getItem
                .setRecall_score(itemRecallScore)
//                .setRecommend_item(recommendImpressionNew.getItem.getRecommend_item)
            Some(recommendImpressionNew)
          } else {
            None // FIXME： 为何会出现这个情况
          }
        }
        if (recommendImpressions.length > 0)
          Some(new MCRecommendSamplesBatch()
            .setUser(userFeats)
//            .setContext(new RankerContextInfo().setDebug(""))
            .setItems(recommendImpressions.toList.asJava)
//            .setExpId("0")
//            .setDebug("debug")
        )
        else
          None
      }
//      .zipWithIndex()
//      .map{ case ((userFeats, recImps), cnt) =>
//        if (cnt < 100) {
//          System.err.println(s"03 cnt: $cnt, userFeats: ${userFeats}, recImps: ${recImps.mkString("|")}")
//        }
//        (userFeats, recImps)
//      }

    System.err.println(s"recommendSamplesBatch size: ${recommendSamplesBatch.count()}")
    System.err.println(s"show some example of recommendSamplesBatch: ${recommendSamplesBatch.take(100).mkString("\n")}")

    val savePath = Constant.recSamplesBatchOutputPathPrefix+curDate
    Util.deleteFile(savePath)
//    recommendSamplesBatch.map(_.user).saveAsParquetFile(savePath+curDate+"user")
    recommendSamplesBatch.saveAsParquetFile(savePath)

  }

}
