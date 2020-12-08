package com.xiaomi.dataming.cf.item

import java.text.SimpleDateFormat

import com.xiaomi.data.aiservice.quanzhiRecomend.{MCItemBasedSimilarity, MCItemBasedSimilarityInfo}
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.cf.Constants
import com.xiaomi.dataming.util.{CFUtils, DateUtils, Util}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/10/1
 *
 *      生成item级别II矩阵
 */
object GenItemII {
	implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

	def main(args: Array[String]): Unit = {
		Util.setLoggerLevel("warn")
		val sparkConf = new SparkConf().setAppName(this.getClass.getName)
		implicit val sc:SparkContext = new SparkContext(sparkConf)

		LOGGER.info("spark conf: {}", sparkConf)
		val argsMap = Util.getArgMap(args)

		val trainStartDate = argsMap("trainStartDate") // 20200601
		val trainEndDate = argsMap("trainEndDate") // train: 20200826 test: 20200831
		val predictDate = DateUtils.getDateFromAddNDays(trainEndDate, 1, new SimpleDateFormat("yyyyMMdd"))
	  val nDays = argsMap.getOrElse("nDays", "10").toInt

		val topSimiNum = argsMap.getOrElse("topSimiNum", "-1").toInt // keep topSimiNum for popular items only (不包含自己)
		val popularRadio = argsMap("popularRadio").toDouble // (0, 1] 收录每天热榜的前popularRadio*100%

	  	// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_userid_paid_info/userPaidSortedSeq/node2vec_result/item/
		val word2VecPath = argsMap("word2VecPathPrefix") + s"${trainStartDate}_${trainEndDate}/${trainStartDate}_${trainEndDate}.emb/"

		// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_userid_paid_info/userPaidSortedSeq/final/activatedAndPaidGames/IIMatrix/
		val IIMatrixOutputPath =
			if (topSimiNum == -1)
				argsMap("IIMatrixOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_nDays_${nDays}_popularRadio_${popularRadio}"
			else
				argsMap("IIMatrixOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}_nDays_${nDays}_popularRadio_${popularRadio}_topSimiNum_${topSimiNum}"

		val activatedAndPaidGamesPathsPrefixes = Constants.activatedAndPaidGamesPathsPrefixes
	 
		// nDays天热榜
		val activatedAndPaidGamesSet: Set[String] = CFUtils.getMostActivatedAndPaidGames(
			trainEndDate, predictDate, activatedAndPaidGamesPathsPrefixes, popularRadio, nDays)
		val activatedAndPaidGamesSetBroadcast: Broadcast[Set[String]] = sc.broadcast(activatedAndPaidGamesSet)

//		val model = Word2VecModel.load(sc, path = word2VecPath)
		val vecsRDD: RDD[(String, Array[Float])] = sc.textFile(word2VecPath).map { line =>
		  val Array(wordId, vecs) = line.split("\t")
		  wordId -> vecs.split(",").map(_.toFloat)
		}.cache()
//		  .filter { case (gameID, _) =>
//				val activatedAndPaidGamesSet = activatedAndPaidGamesSetBroadcast.value
//				activatedAndPaidGamesSet.contains(gameID)
//		}
	  LOGGER.warn(s"show some examples: \n${vecsRDD.mapValues(_.mkString("\t", " ", "")).take(5).mkString("\n")}")
	  
//		val vecsRDD: RDD[(String, Array[Float])] = sc.parallelize(vecs.toList).cache()
	  	val vecsRDDFiltered: RDD[(String, Array[Float])] = vecsRDD.filter { case (gameID, _) =>
		  val activatedAndPaidGamesSet = activatedAndPaidGamesSetBroadcast.value
		  activatedAndPaidGamesSet.contains(gameID)
		}.cache()
	  	LOGGER.warn(s"vecsRDD size: ${vecsRDD.count()}, vecsRDDFiltered size: ${vecsRDDFiltered.count()}")

		val IIMatrix: RDD[MCItemBasedSimilarity] = vecsRDD
		  	// 列中只保留热门games
			.cartesian(vecsRDDFiltered)
//		  	.filter { case ((itemId0, vec0), (itemId1, vec1)) => itemId0.compareTo(itemId1) <= 0 }
			.map { case ((itemId0, vec0), (itemId1, vec1)) =>
				if (itemId0 == itemId1) itemId0 -> Array((itemId1, 1d)) // item与自己的相似度为1.0
				else {
					val numerator: Float = vec0.zip(vec1).map { case (ele0, ele1) => ele0 * ele1 }.sum
					val vec0_sqrt: Double = Math.sqrt(vec0.map(ele0 => Math.pow(ele0, 2)).sum)
					val vec1_sqrt: Double = Math.sqrt(vec1.map(ele1 => Math.pow(ele1, 2)).sum)
					val similarity: Double = numerator / (vec0_sqrt * vec1_sqrt)
					itemId0 -> Array((itemId1, similarity))
				}
			}
		  	.reduceByKey(_ ++ _)
			.map {item2ItemsimilarityArr =>
				val itemId = item2ItemsimilarityArr._1
				val similarItemsArr: List[(String, Double)] =
					if (topSimiNum == -1) item2ItemsimilarityArr._2.sortBy(_._2).toList
					else item2ItemsimilarityArr._2.sortBy(_._2).toList.takeRight(topSimiNum+1)
				val similarSum: Double = Math.sqrt(similarItemsArr.map(ele => Math.pow(ele._2, 2)).sum)
				new MCItemBasedSimilarity()
					.setItem(new MCItemBasedSimilarityInfo()
						.setItemId(itemId)
						.setScore(similarSum)
						.setScoreType(0)
					).setSimilarItems(
				  		similarItemsArr
						  .map { case (similarItemId, similarity) =>
							new MCItemBasedSimilarityInfo()
							  .setItemId(similarItemId)
							  .setScore(similarity/similarSum)
							  .setScoreType(1)
						  }.asJava
					)
			}

		Util.deleteFile(IIMatrixOutputPath)
		IIMatrix.repartition(100).saveAsParquetFile(IIMatrixOutputPath)
		sc.stop()
	}

}
