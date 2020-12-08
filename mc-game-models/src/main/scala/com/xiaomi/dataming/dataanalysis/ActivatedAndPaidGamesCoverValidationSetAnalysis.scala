package com.xiaomi.dataming.dataanalysis

import com.xiaomi.data.aiservice.quanzhiRecommend.{MCMostPopularGames, MCUser2SatisfiedList}
import com.xiaomi.data.commons.spark.HdfsIO.SparkContextThriftFileWrapper
import com.xiaomi.dataming.cf.Constants
import com.xiaomi.dataming.util.{DateUtils, Util}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Authors: shenhao <shenhao@xiaomi.com>
  * created on 20-9-23
  */
object ActivatedAndPaidGamesCoverValidationSetAnalysis {
    implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName(this.getClass.getName)
        implicit val sc:SparkContext = new SparkContext(sparkConf)

        LOGGER.info("spark conf: {}", sparkConf)
        val argsMap = Util.getArgMap(args)

        val activatedAndPaidGamesPathsPrefixes = Constants.activatedAndPaidGamesPathsPrefixes
        
        // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/
        val validationSetPathPrefix = argsMap("validationSetPathPrefix")

        val startTime = argsMap("startTime") // ${date} <= endTime
        val endTime = "20200831"
        assert(startTime.toInt <= endTime.toInt, s"startTime: $startTime > $endTime, not allowed")

        val popularRadio = argsMap("popularRadio").toDouble // (0, 1]

        val validationStartTime = "20200901"
        val validationEndTime = "20200907"

        // hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_game_info/
        val outputPath = Constants.gameInfoPathPrefix +
          s"ActivatedAndPaidGamesCoverValidationSetAnalysis/popularRadio_${popularRadio}/startTime_${startTime}_endTime_${endTime}_result.txt"

		// 验证集
        // [validationStartTime, validationEndTime]
        val validationGamesSet: Set[String] = sc.union(DateUtils.getDateRange(validationStartTime, validationEndTime).map {
            date =>
                val inputValidationSetPath = validationSetPathPrefix+s"date=$date"
                LOGGER.info(s"inputValidationSetPath: $inputValidationSetPath")
                sc.thriftParquetFile(inputValidationSetPath, classOf[MCUser2SatisfiedList])
                    .flatMap(x => x.real_satisfied_game_ids.split("\\|"))
                    .distinct()
        }).distinct().collect().toSet

        val activatedAndPaidGamesSet: Set[String] = sc.union(DateUtils.getDateRange(startTime, endTime).flatMap {
            date =>
                val s: List[RDD[String]] = activatedAndPaidGamesPathsPrefixes.map {
                    activatedAndPaidGamesPathPrefix =>
                        val activatedAndPaidGamesPath = activatedAndPaidGamesPathPrefix + s"date=$date"
                        val sumCnt: Long = sc.thriftParquetFile(activatedAndPaidGamesPath, classOf[MCMostPopularGames]).count()
                        val headCnt: Long = (sumCnt * popularRadio).toLong // 每张表取前 headCnt 个
                        sc.thriftParquetFile(activatedAndPaidGamesPath, classOf[MCMostPopularGames])
                                .map(gameInfo => gameInfo.game_id -> gameInfo.pv)
                                .sortBy(_._2, ascending = false).zipWithIndex().filter(_._2 <= headCnt).map(_._1._1)
                }
                s
        }).distinct().collect().toSet


        val activatedAndPaidGamesCoverCnt = (validationGamesSet & activatedAndPaidGamesSet).size

        val validationGamesSetSize = validationGamesSet.size

        val activatedAndPaidGamesSetSize = activatedAndPaidGamesSet.size


        val resultString = s"outputPath: $outputPath\n" +
            s"successfully cover activatedAndPaidGamesSet size: $activatedAndPaidGamesCoverCnt, validation games size: $validationGamesSetSize, cover radio: ${activatedAndPaidGamesCoverCnt.toDouble/validationGamesSetSize}\n" +
            s"successfully cover activatedAndPaidGamesSet size: $activatedAndPaidGamesCoverCnt, activatedAndPaidGamesSet whole size: $activatedAndPaidGamesSetSize, precise radio: ${activatedAndPaidGamesCoverCnt.toDouble/activatedAndPaidGamesSetSize}\n"
        Util.writeString(resultString, outputPath, true)

    }

}
