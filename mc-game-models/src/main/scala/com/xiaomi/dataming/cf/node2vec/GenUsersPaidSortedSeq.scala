package com.xiaomi.dataming.cf.node2vec

import com.xiaomi.data.aiservice.quanzhiRecommend.MCPayInfo
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.dataming.cf.Constants
import com.xiaomi.dataming.util.{DateUtils, Util}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * 产生每个用户的付钱游戏序列
 *
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/10/7
 */
object GenUsersPaidSortedSeq {
	implicit private val LOGGER: Logger = LoggerFactory.getLogger(getClass)

	def main(args: Array[String]): Unit = {
		Util.setLoggerLevel("warn")

		val sparkConf = new SparkConf().setAppName(this.getClass.getName)
		implicit val sc:SparkContext = new SparkContext(sparkConf)

		LOGGER.info("spark conf: {}", sparkConf)
		val argsMap = Util.getArgMap(args)
		val trainStartDate = argsMap("trainStartDate")
		val trainEndDate = argsMap("trainEndDate")

		// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_pay_info/
		val paidInfoPathPrefix = Constants.userPaidInfoPathPrefix

		// hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/m_c_userid_paid_info/userPaidSortedSeq/
		val userPaidSortedSeqOutputPath = argsMap("userPaidSortedSeqOutputPathPrefix") + s"${trainStartDate}_${trainEndDate}"

		val userPaidSortedSeq: RDD[String] = sc.union(DateUtils.getDateRange(trainStartDate, trainEndDate).map { date =>
			val paidInfoPath = paidInfoPathPrefix + s"date=$date"
			val curDayUserPaidRDD: RDD[(String, Array[(String, Long)])] = sc.thriftParquetFile(paidInfoPath, classOf[MCPayInfo])
				.filter(userPaid =>
						StringUtils.isNotBlank(userPaid.uid)
						&& StringUtils.isNotBlank(userPaid.game_id_s)
						&& userPaid.pay_fee_s > 0
				)
				.map{userPaid =>
					val userId: String = userPaid.uid
					val gameId: String = userPaid.game_id_s
					val timeStamp: Long = DateUtils.getTimestampByDate(userPaid.pay_time)
					userId -> Array((gameId, timeStamp))
				}
    			.reduceByKey(_ ++ _)
    			.mapValues(_.sortBy(_._2))
				.mapValues { l =>
					var preVal: (String, Long) = l.head
					val lbResult: ListBuffer[(String, Long)] = ListBuffer(preVal)
					// 去重
					// (a, 1) (b, 2) (b, 3) (c,4) => (a, 1) (b, 2) (c,4)
					l.drop(1).foreach { curVal =>
						if (curVal._1 != preVal._1) {
							lbResult += curVal
							preVal = curVal
						}
					}
					lbResult.toList.toArray
				}
			curDayUserPaidRDD
		})
		.reduceByKey(_ ++ _)
		.mapValues(_.sortBy(_._2))
		.mapValues { l =>
			var preVal: (String, Long) = l.head
			val lbResult: ListBuffer[(String, Long)] = ListBuffer(preVal)
			// 去重
			// (a, 1) (b, 2) (b, 3) (c,4) => (a, 1) (b, 2) (c,4)
			l.drop(1).foreach { curVal =>
				if (curVal._1 != preVal._1) {
					lbResult += curVal
					preVal = curVal
				}
			}
			lbResult.map(_._1).mkString(" ")
		}.map { case (userId, userPaidSortedSeqStr) => userId + "\t" + userPaidSortedSeqStr}
		
		Util.deleteFile(userPaidSortedSeqOutputPath)
		userPaidSortedSeq.repartition(500).saveAsTextFile(userPaidSortedSeqOutputPath)
		
		sc.stop()
	}
	

}
