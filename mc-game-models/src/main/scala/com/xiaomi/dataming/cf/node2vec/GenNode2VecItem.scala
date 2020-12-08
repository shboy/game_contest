package com.xiaomi.dataming.cf.node2vec

import com.xiaomi.dataming.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Authors: shenhao <shenhao@xiaomi.com>
  * created on 20-10-9
  */
object GenNode2VecItem {
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

        // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_userid_paid_info/userPaidSortedSeq/node2vec/item/
        val userPaidNode2VecOutputPath = argsMap("userPaidNode2VecOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}"

        // ((preGameId, afterGameId), 出现次数)
        val gameOrderedPairs: RDD[((String, String), Long)] = sc.textFile(userPaidSeqPath).flatMap { line =>
            val Array(userId, gameIDSeq) = line.split("\t")
            val gameIds = gameIDSeq.split(" ")
            if (gameIds.length > 1) Some(gameIds.toSeq) else None
        }.flatMap { gameIds =>
            val preGameIds = gameIds.drop(1)
            val afterGameIds = gameIds.dropRight(1)
            preGameIds.zip(afterGameIds).map {case (preGameId, afterGameId) => (preGameId, afterGameId) -> 1L}
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
