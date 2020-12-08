package com.xiaomi.dataming.cf.node2vec

import com.xiaomi.data.aiservice.quanzhiRecommend.MCGameInfo
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.cf.Constants
import com.xiaomi.dataming.util.Util
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Authors: shenhao <shenhao@xiaomi.com>
  * created on 20-10-9
  */
object GenNode2VecCate2 {
    implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

    def main(args: Array[String]): Unit = {
        Util.setLoggerLevel("warn")

        val sparkConf = new SparkConf().setAppName(this.getClass.getName)
        implicit val sc: SparkContext = new SparkContext(sparkConf)

        LOGGER.info("spark conf: {}", sparkConf)
        val argsMap = Util.getArgMap(args)
        val trainStartDate = argsMap("trainStartDate")
        val trainEndDate = argsMap("trainEndDate")

        // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_userid_paid_info/userPaidSortedSeq/
        val userPaidSeqPath = argsMap("userPaidSeqPathPrefix")+ s"${trainStartDate}_${trainEndDate}"

        // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_userid_paid_info/userPaidSortedSeq/node2vec/cate2/
        val userPaidNode2VecOutputPath = argsMap("userPaidNode2VecOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}"
        
        val gameId2Cate2: Map[String, Array[String]] =
            sc.thriftParquetFile(Constants.gameInfoPathPrefix+s"date=$trainEndDate", classOf[MCGameInfo]).flatMap { gameInfo =>
                val gameId = gameInfo.game_id_s
                val cate2: Array[String] = gameInfo.category.split(",")
                    .flatMap { item =>
                        val cates = item.split("#")
						if (cates.size == 2)
                            Some(cates(1))
                        else None
                    }.filter(StringUtils.isNotBlank(_)).distinct
                if (cate2.length > 0)
                    Some(gameId -> cate2)
                else None
            }.collectAsMap().toMap
        val gameId2Cate2Broadcast: Broadcast[Map[String, Array[String]]] = sc.broadcast(gameId2Cate2)

        // ((preGameId, afterGameId), 出现次数)
        val gameOrderedPairs: RDD[((String, String), Long)] = sc.textFile(userPaidSeqPath).flatMap { line =>
            val Array(userId, gameIDSeq) = line.split("\t")
            val gameIds = gameIDSeq.split(" ")
            if (gameIds.length > 1) Some(gameIds.toSeq) else None
        }.flatMap { gameIds =>
            val preGameIds = gameIds.drop(1)
            val afterGameIds = gameIds.dropRight(1)
            val t: Seq[((String, String), Long)] = preGameIds.zip(afterGameIds).flatMap { case (preGameId, afterGameId) =>
                val gameId2Cate2Map = gameId2Cate2Broadcast.value
                if (!gameId2Cate2Map.contains(preGameId) || !gameId2Cate2Map.contains(afterGameId)) None
                else {
                    val preGameId2Cate2Set = gameId2Cate2Map(preGameId)
                    val afterGameId2Cate2Set = gameId2Cate2Map(afterGameId)
                    val l: Array[((String, String), Long)] = preGameId2Cate2Set.flatMap { preGameId2Cate2 =>
                        afterGameId2Cate2Set.map(afterGameId2Cate2 => (preGameId2Cate2, afterGameId2Cate2) -> 1L)
                    }
                    l
                }
            }
            t
        }.reduceByKey(_ + _)

        val node2VecTrainData: RDD[String] = gameOrderedPairs
            .map{ case ((preGameId, afterGameId), cnt) => Array(preGameId, afterGameId, cnt.toString).mkString(" ") }

        Util.deleteFile(userPaidNode2VecOutputPath)
        LOGGER.info(s"node2VecTrainData save to: $userPaidNode2VecOutputPath")
        node2VecTrainData.repartition(100).saveAsTextFile(userPaidNode2VecOutputPath)

        LOGGER.info("done")

        sc.stop()

    }

}
