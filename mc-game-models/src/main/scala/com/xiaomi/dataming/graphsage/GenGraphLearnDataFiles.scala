package com.xiaomi.dataming.graphsage

import java.text.SimpleDateFormat

import com.xiaomi.dataming.util.{DateUtils, Util}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/14
 */
object GenGraphLearnDataFiles {
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
	val isUseTimeDecay = argsMap("isUseTimeDecay").toBoolean // 是否使用时间衰减 true/false
	// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/user2GameScores/
	val user2GameScoresInputpath = argsMap("user2GameScoresInputpathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"
	
	// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/userId2Idx/
	val userId2IdxOutputPath = argsMap("userId2IdxOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"
	// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/userIdx2Idx/
	val userIdx2IdxOutputPath = argsMap("userIdx2IdxOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"

	// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/gameId2Idx/
	val gameId2IdxOutputPath = argsMap("gameId2IdxOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}.txt"
	// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/gameIdx2Idx/
	val gameIdx2IdxOutputPath = argsMap("gameIdx2IdxOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}.txt"

	// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/uidIdx2GameIdIdx/
	val uidIdx2GameIdxOutputPath = argsMap("uidIdx2GameIdxOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"

	// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/final/graphsage/data/gameIdx2UidIdx/
	val gameIdx2UidIdxOutputPath = argsMap("gameIdx2UidIdxOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_isUseTimeDecay_${isUseTimeDecay}"

	val userId2GameIdScoresRDD = sc.textFile(user2GameScoresInputpath).map { line =>
	  val Array(userId, gameId2scores) = line.split("\t")
	  val gameIdScorePairs: Array[(String, Double)] = gameId2scores.split("\\|").map { gameId2score =>
		val Array(gameId, score) = gameId2score.split(",")
		(gameId, score.toDouble)
	  }
	  userId -> gameIdScorePairs
	}.persist(StorageLevel.MEMORY_AND_DISK_SER)

	val uid2IdxRDD: RDD[(String, Long)] = userId2GameIdScoresRDD.map(_._1).distinct()
	  .sortBy(uid => uid)
	  .zipWithIndex()
	  .map{ case (uid, idx) => uid ->idx }.persist(StorageLevel.MEMORY_AND_DISK_SER)

	// uid -> idx
	val uidId2IdxStrRDD: RDD[String] = uid2IdxRDD.map{ case (uid, idx) => uid+"\t"+idx }
	Util.deleteFile(userId2IdxOutputPath)
	uidId2IdxStrRDD.repartition(100).saveAsTextFile(userId2IdxOutputPath)
	LOGGER.warn("uidId2IdxStrRDD done")
	// uidIdx -> uidIdx
	// id:int64\tfeature:string
	val uidx2IdxStrRDD: RDD[String] = uid2IdxRDD.map{ case (uid, idx) => idx+"\t"+idx }
	Util.deleteFile(userIdx2IdxOutputPath)
	uidx2IdxStrRDD.repartition(100).saveAsTextFile(userIdx2IdxOutputPath)
	LOGGER.warn("uidx2IdxStrRDD done")

	val gameId2IdxMap: Map[String, Long] = userId2GameIdScoresRDD.flatMap(_._2).map(_._1).distinct()
	  .sortBy(gameId =>gameId)
	  .zipWithIndex
	  .map{ case (gameId, idx) => gameId->idx }.collectAsMap().toMap
	val gameId2IdxMapBroadcast: Broadcast[Map[String, Long]] = sc.broadcast(gameId2IdxMap)
	// gameId -> idx
	val gameId2IdxStr = gameId2IdxMap.toArray.sortBy(_._2).map{ case (gameId, idx) => gameId+"\t"+idx}.mkString("gameId:int64\tidx:int64\n", "\n", "\n")
	Util.writeString(gameId2IdxStr, gameId2IdxOutputPath, true)
	LOGGER.warn("gameId2IdxStr done")
	// gameIdx -> gameIdx
	val gameIdx2IdxStr = gameId2IdxMap.toArray.sortBy(_._2).map{ case (gameId, idx) => idx+"\t"+idx}.mkString("id:int64\tfeature:string\n", "\n", "\n")
	Util.writeString(gameIdx2IdxStr, gameIdx2IdxOutputPath, true)
	LOGGER.warn("gameIdx2IdxStr done")

	val uidIdx2GameIdxRDD: RDD[String] = userId2GameIdScoresRDD.join(uid2IdxRDD).map{ case (uid, (gameIdScorePairs, uidIdx)) =>
	  val gameIdxScores: Array[(Long, Double)] = gameIdScorePairs.map{ case (gameId, score) => gameId2IdxMapBroadcast.value(gameId) -> score }
	  uidIdx -> gameIdxScores
	}.flatMap { case (uidIdx, gameIdxScores) =>
	  gameIdxScores.map{ case (gameIdx, score) => uidIdx + "\t" + gameIdx + "\t" + score.formatted("%.6f") }
	}

	userId2GameIdScoresRDD.unpersist()
	uid2IdxRDD.unpersist()

	uidIdx2GameIdxRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

	Util.deleteFile(uidIdx2GameIdxOutputPath)
	// src_id:int64	dst_id:int64	weight:double
	uidIdx2GameIdxRDD.saveAsTextFile(uidIdx2GameIdxOutputPath)
	LOGGER.warn("uidIdx2GameIdxRDD done")

	val gameIdx2uidIdx: RDD[String] = uidIdx2GameIdxRDD.map { line =>
	  	val Array(uidIdx, gameIdx, score) = line.split("\t")
	  	gameIdx + "\t" + uidIdx + "\t" + score
	}
	Util.deleteFile(gameIdx2UidIdxOutputPath)
	// src_id:int64	dst_id:int64	weight:double
	gameIdx2uidIdx.saveAsTextFile(gameIdx2UidIdxOutputPath)
	LOGGER.warn("gameIdx2uidIdx done")

	sc.stop()
  }
  
}
