package com.xiaomi.dataming.traindata

import com.xiaomi.data.aiservice.contest.{ActivityStatsInfo, MCRankerUserInfo, Stats, UserProfileInfo}
import com.xiaomi.data.aiservice.quanzhiRecommend.{MCNDistinctGamesInNDays, MCUserAttrib}
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
 * @date 2020/11/22
 */
object JoinUserFeatures {
  implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    Util.setLoggerLevel("warn")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    implicit val sc:SparkContext = new SparkContext(sparkConf)

    LOGGER.warn("spark conf: {}", sparkConf)
    val argsMap = Util.getArgMap(args)

    val date = argsMap("date")

    val userAttrib: RDD[(String, MCUserAttrib)] =
      sc.thriftParquetFile(Constant.USER_ATTRIB_PATH, classOf[MCUserAttrib])
        .keyBy(_.uid)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // user付费sum/average/min/max/std 排名比
    val userPaidFeeRankInfo: RDD[(String, List[Stats])] = UserFeatsUtils.getUserPaidFeeRankInfo(userAttrib, date)

    // paid_cnt_stats: user对多少款游戏付过款
    val userPaidGameCnt: RDD[(String, MCNDistinctGamesInNDays)] =
      sc.thriftParquetFile(Constant.MC_userPaidForNDistinctGames_PATH_PREFIX+date, classOf[MCNDistinctGamesInNDays])
        .keyBy(_.uid)

    //  user有没有对热门的激活付费游戏付过费
    val userIsPaidsInfo: RDD[(String, List[Stats])] = UserFeatsUtils.getUserIsPaidsInfo(userAttrib, date)

    val res: RDD[MCRankerUserInfo] = userAttrib
      .map { case (uid, attrib) =>
        val profileInfo: UserProfileInfo = new UserProfileInfo()
          .setUid(uid)
          .setAge(attrib.age)
          .setSex(attrib.sex.toString)
          .setCity(attrib.city)
          .setDegree(attrib.degree)
          .setPhoneBrand(attrib.phone_brand)

        val userRankerInfo = new MCRankerUserInfo()
          .setUid(uid)
          .setProfile_info(profileInfo)
          .setActivity_stats_info(new ActivityStatsInfo())
//          .setItem_recall_scores()
//          .setCate1_recall_scores()
        uid -> userRankerInfo
      }
      .leftOuterJoin(userPaidFeeRankInfo)
      .mapValues {
        case (userInfo, Some(statsInfo)) => {
          userInfo.getActivity_stats_info.setPaid_rank_radio_stats(statsInfo.asJava)
          userInfo
        }
        case (userInfo, None) => userInfo
      }
      .leftOuterJoin(userPaidGameCnt)
      .mapValues {
        case (userInfo, Some(userPaidGameCnt)) => {
          val statsInfo: List[Stats] = ObjectTransformers.toStats(userPaidGameCnt)
          userInfo.getActivity_stats_info.setPaid_cnt_stats(statsInfo.asJava)
          userInfo
        }
        case (userInfo, None) => userInfo
      }
      .leftOuterJoin(userIsPaidsInfo)
      .mapValues {
        case (userInfo, Some(statsInfo)) =>
          userInfo.getActivity_stats_info.setIs_paid_stats(statsInfo.asJava)
          userInfo
        case (userInfo, None) => userInfo
      }
      .values

    val savePath = s"${Constant.userFeatureBasePathPrefix}/date=${date}"
    Util.deleteFile(savePath)
    res.saveAsParquetFile(savePath)
  }

}
