package com.xiaomi.dataming.dataanalysis

import java.text.SimpleDateFormat

import com.xiaomi.data.aiservice.quanzhiRecommend.MCUserDoAction
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.cf.Constants
import com.xiaomi.dataming.util.{DateUtils, Util, Validation}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}


/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/7
 *      user action cnt -> cover radio analysis
 *      不同行为次数的用户能覆盖多少验证集的用户
 */
object CoverRadioTest02 {
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
		val predictStartDate = DateUtils.getDateFromAddNDays(trainEndDate, 1, new SimpleDateFormat("yyyyMMdd"))
		
		// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_recall_validation/coverRadio/CoverRadioTest02/
		val outputPath = argsMap("outputPathPrefix") + s"CoverRadioTest02.${trainStartDate}_${trainEndDate}.txt"
		
		// user action 的次数
		val userActionCnt: RDD[(String, Long)] = sc.union(
			DateUtils.getDateRange(trainStartDate, trainEndDate).map {date =>
				val userDoActionPath = Constants.userDoActionPathPrefix + s"date=$date"
				sc.thriftParquetFile(userDoActionPath, classOf[MCUserDoAction]).map {userDoaction =>
					userDoaction.uid -> 1L
				}.reduceByKey(_ + _)
			}
		).reduceByKey(_ + _)
		
		val validationUid: RDD[String] = Validation.getValidationUserSet(predictStartDate)
		val validationUidCnt = validationUid.count()
		
		// uid, (actionCnt, isInValidation)
		val userActionCntWithLabel: RDD[(String, (Long, Int))] = userActionCnt.fullOuterJoin(validationUid.map(uid => uid -> 1))
    		.mapValues { case (actionCntOpt, maybeInt) =>
				if (maybeInt.isDefined) { // 是验证集中的uid
					(actionCntOpt.getOrElse(0), 1)
				} else { // 不是验证集中的uid
					(actionCntOpt.getOrElse(0), 0)
				}
			}
		
		// actionCnt, (uidCnt, isInValidationCnt)
		val actionCnt2StatInfo: Array[(Long, (Long, Long))] = userActionCntWithLabel
			.map { case (uid, (actionCnt, isInValidation)) =>
				actionCnt -> (1L, isInValidation.toLong)
			}.reduceByKey{
				case ((uidCnt1, isInvalidation1), (uidCnt2, isInvalidation2)) => (uidCnt1+uidCnt2, isInvalidation1+isInvalidation2)
			}.collect()
		
		val uidWholeSum: Long = actionCnt2StatInfo.map(_._2._1).sum// 总共有多少人
			
		var uidCusum = 0L
		var validationCusum = 0L
		var percentCusum = 0d
		
		// curActionCnt -> (多少人，累计多少人， 包含几个validation， 占百分之多少的validation， 累计多少validation， 累计百分之多少)
		val resultList: Array[String] =
			actionCnt2StatInfo.sortBy(_._1).reverse
				.map { case (curActionCnt, (uidCnt, isInValidationCnt)) =>
					val curUidCnt = uidCnt
					uidCusum = uidCusum + curUidCnt
					val insectionSize = isInValidationCnt
					val insectionSizeRadio = insectionSize.toDouble / validationUidCnt
					validationCusum = validationCusum + insectionSize
					percentCusum = percentCusum + insectionSizeRadio
					
					curActionCnt +
						Array(
							curUidCnt, uidCusum, insectionSize,
							(insectionSizeRadio*100).formatted("%.6f")+"%",
							validationCusum, (percentCusum*100).formatted("%.6f")+"%",
							((1-percentCusum)*100).formatted("%.6f")+"%", uidWholeSum-uidCusum
						).mkString(" ", " ", "")
					
				}
		
		Util.writeString(resultList.mkString(s"outputPath: ${outputPath}\n"+
			"actionCnt uidCnt uidCusum insectionSize insectionSizeRadio validationCusum percentCusum 1-percentCusum uidCusum-uidCnt\n"
			, "\n", "\n"), outputPath, true)
		
		sc.stop()
	}
	
}
