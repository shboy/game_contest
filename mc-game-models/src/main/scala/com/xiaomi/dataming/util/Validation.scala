package com.xiaomi.dataming.util

import com.xiaomi.data.aiservice.quanzhiRecommend.{MCUser2SatisfiedList, MCUserPaidRankForTOP100W}
import com.xiaomi.data.commons.spark.HdfsIO.SparkContextThriftFileWrapper
import com.xiaomi.dataming.cf.Constants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/9/21
 */
object Validation {
	implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)
	
	/**
	 * 返回 [validationStartTime, validationEndTime] 验证集中的userId set
	 * @param validationStartTime
	 * @param validationEndTime
	 * @param sc
	 * @return
	 */
	def getValidationUserSet(validationStartTime: String="20200901", validationEndTime: String="20200907") (implicit sc: SparkContext)= {
		sc.union(
			DateUtils.getDateRange(validationStartTime, validationEndTime).map {date =>
				val validationPath = Constants.validationSetPathPrefix + s"date=$date"
				val userIdSet = sc.thriftParquetFile(validationPath, classOf[MCUser2SatisfiedList]).map(_.user_id).distinct()
				userIdSet
			}
		).distinct()
	}
	/**
	  * 不会自动加_result.txt
	  * @param outputPath
	  * @param toValuatedPathList
	  * @param otherPrintStr
	  * @param sc
	  */
	def valuate(outputPath: String,
				toValuatedPathList: List[String],
				validationStartTime: String="20200912", validationEndTime: String="20200918",
				otherPrintStr: String="")(implicit sc: SparkContext) = {
		val inputPredRecallItemsPaths: List[String] = toValuatedPathList
		val inputValidationSetPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/"


		LOGGER.warn(s"inputPredRecallItemsPath: ${inputPredRecallItemsPaths.mkString(", ")}, inputValidationSetPathPrefix: $inputValidationSetPathPrefix, " +
			s"validationStartTime: $validationStartTime, validationEndTime: $validationEndTime, outputPath: $outputPath")

		// [validationStartTime, validationEndTime]
		val validationMap: Map[String, Set[String]] = sc.union(DateUtils.getDateRange(validationStartTime, validationEndTime).map {
			date =>
				val inputValidationSetPath = inputValidationSetPathPrefix+s"date=$date"
				LOGGER.warn(s"inputValidationSetPath: $inputValidationSetPath")
				sc.thriftParquetFile(inputValidationSetPath, classOf[MCUser2SatisfiedList])
					.map(x => x.user_id -> x.real_satisfied_game_ids.split("\\|").toSet)
		}).reduceByKey(_ ++ _).collectAsMap().toMap

		val validationSize: Int = validationMap.map(item => item._2.size).sum
		val validationMapBroadcast: Broadcast[Map[String, Set[String]]] = sc.broadcast(validationMap)

		val (successGameRecalledNum, recallGameSumNum): (Long, Long) = inputPredRecallItemsPaths.map { inputPredRecallItemsPath =>
			sc.textFile(inputPredRecallItemsPath).map { line =>
				val Array(userId, gameId2ScoreListStr) = line.split("\t")
				gameId2ScoreListStr.split("\\|").map{ gameId2ScoreStr =>
					val Array(gameId, score) = gameId2ScoreStr.split(":")
					gameId -> score.toDouble
				}.map { case (gameId, score) =>
          val validationMap = validationMapBroadcast.value
					if (validationMap.contains(userId) && validationMap(userId).contains(gameId)) (1L, 1L)
					else (0L, 1L)
				}.reduce {
					(tup0, tup1) =>
						val successGameRecalledNum = tup0._1 + tup1._1
						val recallGameSumNum = tup0._2 + tup1._2
						(successGameRecalledNum, recallGameSumNum)
				}
			}.reduce{
				(tup0, tup1) =>
					val successGameRecalledNum = tup0._1 + tup1._1
					val recallGameSumNum = tup0._2 + tup1._2
					(successGameRecalledNum, recallGameSumNum)
			}
		}.reduce{ (tup0, tup1) =>
			val successGameRecalledNum = tup0._1 + tup1._1
			val recallGameSumNum = tup0._2 + tup1._2
			(successGameRecalledNum, recallGameSumNum)
		}

		val recalledUserId2GameCntRDD: RDD[(String, Long)] = sc.union(inputPredRecallItemsPaths.map { inputPredRecallItemsPath =>
			sc.textFile(inputPredRecallItemsPath).map { line =>
				val Array(userId, gameId2ScoreListStr) = line.split("\t")
				userId -> gameId2ScoreListStr.split("\\|").size.toLong
			}.reduceByKey(_+_)
		}).reduceByKey(_+_)


		val minUserRecalledCntUserIdGameIdPair: (String, Long) = recalledUserId2GameCntRDD.min()(new Ordering[(String, Long)]() {
			override def compare(x: (String, Long), y: (String, Long)): Int =
				Ordering[Long].compare(x._2, y._2)
		})

		val maxUserRecalledCntUserIdGameIdPair: (String, Long) = recalledUserId2GameCntRDD.max()(new Ordering[(String, Long)]() {
			override def compare(x: (String, Long), y: (String, Long)): Int =
				Ordering[Long].compare(x._2, y._2)
		})

		val averageRecalledCnt: Double = recalledUserId2GameCntRDD.map(_._2).sum/recalledUserId2GameCntRDD.count()


		val writePath = outputPath
		Util.writeString(
			s"outputPath:\t$writePath\n" +
				otherPrintStr +
				s"recall radio:\t $successGameRecalledNum\t$validationSize\t${successGameRecalledNum.toDouble/validationSize}\n" +
				s"precise radio:\t $successGameRecalledNum\t$recallGameSumNum\t${successGameRecalledNum.toDouble/recallGameSumNum}\n" +
				s"minUserRecalledCntUserIdGameIdPair: ${minUserRecalledCntUserIdGameIdPair}\n" +
				s"maxUserRecalledCntUserIdGameIdPair: ${maxUserRecalledCntUserIdGameIdPair}\n" +
				s"averageRecalledCnt: $averageRecalledCnt\n"
			, writePath, true)

		LOGGER.warn("job done")
	}

	def valuateWithRecallNum(valuateResultOutputPath: String,
													 toValuatedPathList: List[String],
													 recallNum: Int,
													 validationStartTime: String="20200912", validationEndTime: String="20200918",
													 otherPrintStr: String="")(implicit sc: SparkContext) = {
		val inputPredRecallItemsPaths: List[String] = toValuatedPathList
		val inputValidationSetPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/"


		LOGGER.warn(s"inputPredRecallItemsPath: ${inputPredRecallItemsPaths.mkString(", ")}, inputValidationSetPathPrefix: $inputValidationSetPathPrefix, " +
			s"validationStartTime: $validationStartTime, validationEndTime: $validationEndTime, outputPath: $valuateResultOutputPath")

		// [validationStartTime, validationEndTime]
		val validationMap: Map[String, Set[String]] = sc.union(DateUtils.getDateRange(validationStartTime, validationEndTime).map {
			date =>
				val inputValidationSetPath = inputValidationSetPathPrefix+s"date=$date"
				LOGGER.warn(s"inputValidationSetPath: $inputValidationSetPath")
				sc.thriftParquetFile(inputValidationSetPath, classOf[MCUser2SatisfiedList])
					.map(x => x.user_id -> x.real_satisfied_game_ids.split("\\|").toSet)
		}).reduceByKey(_ ++ _).collectAsMap().toMap

		val validationSize: Int = validationMap.map(item => item._2.size).sum
		val validationMapBroadcast: Broadcast[Map[String, Set[String]]] = sc.broadcast(validationMap)

		val (successGameRecalledNum, recallGameSumNum): (Long, Long) = inputPredRecallItemsPaths.map { inputPredRecallItemsPath =>
			sc.textFile(inputPredRecallItemsPath).map { line =>
				val Array(userId, gameId2ScoreListStr) = line.split("\t")
				gameId2ScoreListStr.split("\\|").map{ gameId2ScoreStr =>
					val Array(gameId, score) = gameId2ScoreStr.split(":")
					gameId -> score.toDouble
				}.sortBy(_._2).takeRight(recallNum) // 取score最大的recallNum个
        .map { case (gameId, score) =>
          val validationMap = validationMapBroadcast.value
					if (validationMap.contains(userId) && validationMap(userId).contains(gameId)) (1L, 1L)
					else (0L, 1L)
				}.reduce {
					(tup0, tup1) =>
						val successGameRecalledNum = tup0._1 + tup1._1
						val recallGameSumNum = tup0._2 + tup1._2
						(successGameRecalledNum, recallGameSumNum)
				}
			}.reduce{
				(tup0, tup1) =>
					val successGameRecalledNum = tup0._1 + tup1._1
					val recallGameSumNum = tup0._2 + tup1._2
					(successGameRecalledNum, recallGameSumNum)
			}
		}.reduce{ (tup0, tup1) =>
			val successGameRecalledNum = tup0._1 + tup1._1
			val recallGameSumNum = tup0._2 + tup1._2
			(successGameRecalledNum, recallGameSumNum)
		}

		val recalledUserId2GameCntRDD: RDD[(String, Long)] = sc.union(inputPredRecallItemsPaths.map { inputPredRecallItemsPath =>
			sc.textFile(inputPredRecallItemsPath).map { line =>
				val Array(userId, gameId2ScoreListStr) = line.split("\t")
				val gameIdsCnt = gameId2ScoreListStr.split("\\|").size
				val gameIdsCntAfterCut = if (gameIdsCnt > recallNum) recallNum else gameIdsCnt
				userId -> gameIdsCntAfterCut.toLong
			}.reduceByKey(_+_)
		}).reduceByKey(_+_)


		val minUserRecalledCntUserIdGameIdPair: (String, Long) = recalledUserId2GameCntRDD.min()(new Ordering[(String, Long)]() {
			override def compare(x: (String, Long), y: (String, Long)): Int =
				Ordering[Long].compare(x._2, y._2)
		})

		val maxUserRecalledCntUserIdGameIdPair: (String, Long) = recalledUserId2GameCntRDD.max()(new Ordering[(String, Long)]() {
			override def compare(x: (String, Long), y: (String, Long)): Int =
				Ordering[Long].compare(x._2, y._2)
		})

		val averageRecalledCnt: Double = recalledUserId2GameCntRDD.map(_._2).sum/recalledUserId2GameCntRDD.count()


		val writePath = valuateResultOutputPath
		Util.writeString(
			s"outputPath:\t$writePath\n" +
				otherPrintStr +
				s"recall radio:\t $successGameRecalledNum\t$validationSize\t${successGameRecalledNum.toDouble/validationSize}\n" +
				s"precise radio:\t $successGameRecalledNum\t$recallGameSumNum\t${successGameRecalledNum.toDouble/recallGameSumNum}\n" +
				s"minUserRecalledCntUserIdGameIdPair: ${minUserRecalledCntUserIdGameIdPair}\n" +
				s"maxUserRecalledCntUserIdGameIdPair: ${maxUserRecalledCntUserIdGameIdPair}\n" +
				s"averageRecalledCnt: $averageRecalledCnt\n"
			, writePath, true)

		LOGGER.warn("job done")
	}
	
	
	def valuateForPolicyRecalls(outputPath: String,
				toValuatedPathList: List[String],
				validationStartTime: String="20200912", validationEndTime: String="20200918",
				otherPrintStr: String="")(implicit sc: SparkContext) = {
		val inputPredRecallItemsPaths: List[String] = toValuatedPathList
		val inputValidationSetPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/"
		
		LOGGER.info(s"inputPredRecallItemsPath: ${inputPredRecallItemsPaths.mkString(", ")}, inputValidationSetPathPrefix: $inputValidationSetPathPrefix, " +
			s"validationStartTime: $validationStartTime, validationEndTime: $validationEndTime, outputPath: $outputPath")
		
		// [validationStartTime, validationEndTime]
		val validationMap: Map[String, Set[String]] = sc.union(DateUtils.getDateRange(validationStartTime, validationEndTime).map {
			date =>
				val inputValidationSetPath = inputValidationSetPathPrefix+s"date=$date"
				LOGGER.info(s"inputValidationSetPath: $inputValidationSetPath")
				val t: RDD[(String, Set[String])] = sc.thriftParquetFile(inputValidationSetPath, classOf[MCUser2SatisfiedList])
					.map(x => x.user_id -> x.real_satisfied_game_ids.split("\\|").toSet)
				t
		}).reduceByKey(_ ++ _).collectAsMap().toMap
		
		val recallItemSet: Set[String] = sc.union(inputPredRecallItemsPaths.map { inputPredRecallItemsPath =>
			sc.textFile(inputPredRecallItemsPath).filter(line => StringUtils.isNotBlank(line.trim))
		}).collect().toSet
		
		val recallInfo: List[(Int, Int)] = validationMap.toList.map { case (userId, validationGamesSet) =>
			val successfullyRecalledNum = (validationGamesSet & recallItemSet).size
			val recallSumNum = validationGamesSet.size
			(successfullyRecalledNum, recallSumNum)
		}.sortBy(pair => pair._1.toDouble/pair._2)
		
		val (minSuccessfullyRecalledNum, minRecallSumNum): (Int, Int) = recallInfo.take(1).head
		val (maxSuccessfullyRecalledNum, maxRecallSumNum): (Int, Int) = recallInfo.takeRight(1).head
		
		val averageRecalledRadio = recallInfo.map(_._1).sum.toDouble/recallInfo.map(_._2).sum
		
		val writePath = outputPath
		Util.writeString(
			s"outputPath:\t$writePath\n" +
				otherPrintStr +
				s"min radio:\t ${minSuccessfullyRecalledNum}\t${minRecallSumNum}\t${(minSuccessfullyRecalledNum.toDouble*100/minRecallSumNum).formatted("%.10f")}%\n" +
				s"max radio:\t $maxSuccessfullyRecalledNum\t$maxRecallSumNum\t${(maxSuccessfullyRecalledNum.toDouble*100/maxRecallSumNum).formatted("%.10f")}%\n" +
				s"average radio:\t ${recallInfo.map(_._1).sum}\t${recallInfo.map(_._2).sum}\t${(averageRecalledRadio*100).formatted("%.10f")}%\n"
			, writePath, true)
		
		LOGGER.info("job done")
	}
	
	
	/**
	 * 可能会少于100w个，因为没有那么多用户激活并且付款
	 * @param validationStartTime
	 * @param sc
	 * @return
	 */
	def getTop100WUsers(validationStartTime: String)(implicit sc: SparkContext): Set[String] = {
		//		val inputUserAcitivatedOrPaidGamePath = argsMap.get("inputUserAcitivatedOrPaidGamePath") // 用户激活或者付过款的游戏
		// 对应{@link ItemBasedCFPrediction.scala} 中的 outputUserItemPredictionPath
		val inputValidationSetPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/mc_user_paid_rank_for_top100w/"

		// validationTime: 20200827 => [20200827, 20200829]
		val validation: Set[String] = sc.union(DateUtils.getDateRange(0, 2, validationStartTime).map { date =>
			sc.thriftParquetFile(inputValidationSetPathPrefix+s"date=$date", classOf[MCUserPaidRankForTOP100W])
				.filter(item => (item.rank <= 100 * 10000) && (item.paid_sum > 0))
    			.map(item => item.uid -> item.paid_sum)
		}).reduceByKey(_+_)
    		.sortBy(_._2, ascending = false)
    		.zipWithIndex().filter(_._2 <= 100*10000).map(_._1._1).collect().toSet

		validation
	}


	/**
	  *
	  * @param validationStartTime
	  * @param validationEndTime
	  * @param sc
	  * @return
	  */
	def getPositiveUsersAndGameMap(validationStartTime: String="20200912",
								   validationEndTime: String="20200918")(implicit sc: SparkContext): Map[String, Set[String]] = {
		val inputValidationSetPathPrefix = "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/"

		val positiveUsersAndGamesMap: Map[String, Set[String]] = sc.union(DateUtils.getDateRange(validationStartTime, validationEndTime).map { date =>
			val inputValidationSetPath = inputValidationSetPathPrefix+s"date=$date"
			LOGGER.info(s"inputValidationSetPath: $inputValidationSetPath")

			sc.thriftParquetFile(inputValidationSetPath, classOf[MCUser2SatisfiedList])
					.map(x => x.user_id -> x.real_satisfied_game_ids.split("\\|").toSet)
		}).reduceByKey(_++_).collectAsMap().toMap


		positiveUsersAndGamesMap
	}



}
