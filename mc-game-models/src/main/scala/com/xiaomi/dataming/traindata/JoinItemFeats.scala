package com.xiaomi.dataming.traindata

import com.xiaomi.data.aiservice.contest._
import com.xiaomi.data.aiservice.quanzhiRecommend.{MCGameActivatedAndPaidRankRadio, MCGameInfo}
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/28
 */
object JoinItemFeats {
  implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    Util.setLoggerLevel("warn")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    implicit val sc: SparkContext = new SparkContext(sparkConf)

    LOGGER.info("spark conf: {}", sparkConf)
    val argsMap = Util.getArgMap(args)

    val date = argsMap("date")

    val recallItemPath = Constant.recallPathFormat.format(date)
    LOGGER.warn(s"recallItemPath: $recallItemPath")

    val recalledGameIds: RDD[(String, MCRecommendItem)] = sc.textFile(recallItemPath).flatMap { line =>
      val Array(uid, game2scores) = line.split("\t")
      game2scores.split("\\|").map { game2score =>
        val Array(gameId, itemRecallScore) = game2score.split(":")
        gameId
      }
    }.distinct().map(gameId => gameId -> new MCRecommendItem().setGame_id_s(gameId)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val gameFeats: RDD[(String, MCGameInfo)] = sc.thriftParquetFile(Constant.MC_GAME_INFO_PATH_PREFIX + date, classOf[MCGameInfo])
      .keyBy(_.game_id_s)

    val gameActivatedAndPaidRankRadio: RDD[(String, List[Stats])] = ItemFeatsUtils.getGameActivatedAndPaidRankRadio(recalledGameIds, date)

    val recommendImpression: RDD[RecommendImpression] = recalledGameIds
      .join(gameFeats)
      .values
      .map { case (recommendItem, gameFeat) =>
      recommendItem
        .setCreate_time(gameFeat.create_time)
        .setUpdate_time(gameFeat.update_time)
        .setSource(gameFeat.source)
        .setApksize(gameFeat.apksize)
        .setPublisher_name_s(gameFeat.publisher_name_s)
        .setDeveloper_name(gameFeat.developer_name)
        .setGametype(gameFeat.gametype)
        .setApp_type(gameFeat.app_type)
        .setDeveloper_type(gameFeat.developer_type)
        .setCategory(gameFeat.category)
        .setTags(gameFeat.tags)
      val candidateItem: CandidateItem = new CandidateItem()
        .setRecommend_item(recommendItem)
        .setItem_stats_info(new StatsInfo())
      val recommendImpression = new RecommendImpression()
        .setItem(candidateItem)
      recommendItem.game_id_s -> recommendImpression
      }
      .leftOuterJoin(gameActivatedAndPaidRankRadio)
      .mapValues {
      case (impression, Some(stats)) =>
        impression.getItem.getItem_stats_info.setItem_activated_and_paid_rank_info(stats.asJava)
        impression
        case (impression, None) => impression
      }.values

    System.err.println(s"recommendImpression count: ${recommendImpression.count()}")
    System.err.println(s"show some recommendImpressions: ${recommendImpression.take(100).mkString("\n")}")

    val savePath = Constant.gameItemFeatureOutputPathPrefix+date
    Util.deleteFile(savePath)
    recommendImpression.saveAsParquetFile(savePath)

    LOGGER.warn("job done")
  }
}
