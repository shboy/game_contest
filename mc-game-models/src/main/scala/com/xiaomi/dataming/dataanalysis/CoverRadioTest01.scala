package com.xiaomi.dataming.dataanalysis

import java.text.SimpleDateFormat

import com.xiaomi.data.aiservice.quanzhiRecommend.{MCPayInfo, MCUserDoAction}
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.cf.Constants
import com.xiaomi.dataming.util.{DateUtils, Util, Validation}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/6
 *
 * n天有行为的用户可以覆盖x%的验证集用户
 */
object CoverRadioTest01 {
	implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)
	
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName(this.getClass.getName)
		implicit val sc:SparkContext = new SparkContext(sparkConf)
		Util.setLoggerLevel("warn")
		
		LOGGER.info("spark conf: {}", sparkConf)
		val argsMap = Util.getArgMap(args)
		
		val trainStartDate = argsMap("trainStartDate") // 20200601
		val trainEndDate = argsMap.getOrElse("trainEndDate", "20200831") // 20200831
		// validation: 20200827 test: 20200901  这个为${trainEndDate}+1
		val predictDate = DateUtils.getDateFromAddNDays(trainEndDate, 1, new SimpleDateFormat("yyyyMMdd"))
		
		// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/coverRadio/CoverRadioTest01/
		val outputPath = argsMap("outputPathPrefix") + s"${trainStartDate}_${trainEndDate}"
		
		val allUidSet: RDD[String] = sc.union(
			DateUtils.getDateRange(trainStartDate, trainEndDate).reverse.map { date =>
				val userDoActionPath = Constants.userDoActionPathPrefix + s"date=$date"
				val userPaidPath = Constants.userPaidInfoPathPrefix + s"date=$date"
				val userIdSet0 = sc.thriftParquetFile(userDoActionPath, classOf[MCUserDoAction]).map(_.uid)
				val userIdSet1 = sc.thriftParquetFile(userPaidPath, classOf[MCPayInfo]).map(_.uid)
				val mergeUidSet = userIdSet0.union(userIdSet1).distinct()
				mergeUidSet
			}
		).distinct().cache()
		val allUidSetCount = allUidSet.count()
		
		val allValidSet: RDD[String] = Validation.getValidationUserSet(predictDate)
		val allValidSetCount = allValidSet.count()
		
		val validSetCount = allUidSet.intersection(allValidSet).count()
		
		val resultStr = s"path: $outputPath\n" +
			s"allUidSetCount: $allUidSetCount, allValidSetCount: $allValidSetCount, " +
			s"validSetCount: $validSetCount, cover radio: ${(validSetCount.toDouble/allValidSetCount*100).formatted("%.6f")}%\n"
		
		Util.writeString(resultStr, outputPath, true)
		
		sc.stop()
	}
	
}
