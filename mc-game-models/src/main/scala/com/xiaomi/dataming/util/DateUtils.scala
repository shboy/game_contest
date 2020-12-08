package com.xiaomi.dataming.util

import java.text.{ParseException, SimpleDateFormat}
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * @author chengxi <zhangdebiao@xiaomi.com>
  * @date 19-5-6
  */
object DateUtils {

    private val logger = LoggerFactory.getLogger(DateUtils.getClass)
    private val dateStrRegex = Pattern.compile(".*date=(?<date>\\d{8}).*")

    val sdf = new SimpleDateFormat("yyyyMMdd")

    /**
      * 获取日期间隔。包括起止日期
      *
      * @param startDate "20190430"
      * @param endDate   "20190505"
      * @return 日期列表。日期格式"20190430"
      */
    def getDateRange(startDate: String, endDate: String): List[String] = {
        val calendar = Calendar.getInstance()
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        var startDateFormat = dateFormat.parse(startDate)
        val endDateFormat = dateFormat.parse(endDate)
        calendar.setTime(startDateFormat)
        val dateArray: ArrayBuffer[String] = ArrayBuffer()
        while (startDateFormat.compareTo(endDateFormat) <= 0) {
            dateArray += dateFormat.format(startDateFormat)
            calendar.add(Calendar.DAY_OF_MONTH, 1)
            startDateFormat = calendar.getTime
        }
        dateArray.toList
    }

    /**
     * getDateFeomAddNDays("20200901", -1, new SimpleDateFormat("yyyyMMdd"))
     * getDateFeomAddNDays("20200831", -1, new SimpleDateFormat("yyyyMMdd"))
     * getDateFeomAddNDays("20200831", 1, new SimpleDateFormat("yyyyMMdd"))
     *
     * @param baseDate 20200901
     * @param addNDays 1
     * @param dateFormat new SimpleDateFormat("yyyyMMdd")
     * @return
     */
    def getDateFromAddNDays(baseDate: String, addNDays: Int, dateFormat: SimpleDateFormat): String = {
        val calendar = Calendar.getInstance()
        val baseDateDateFrame = dateFormat.parse(baseDate)
        calendar.setTime(baseDateDateFrame)
        calendar.add(Calendar.DAY_OF_MONTH, addNDays)
        dateFormat.format(calendar.getTime)
    }

    /**
     * 得到两个时间差的天数
     * getDateDiffDay("202008-31 21:54:19", "2020-08-29 21:54:19")
     * @param endDate
     * @param startDate
     * @return
     */
    def getDateDiffDay(endDate: String, startDate: String): Int = {
        val calendar = Calendar.getInstance()
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var startDateFormat = dateFormat.parse(startDate)
        val endDateFormat = dateFormat.parse(endDate)
        assert(startDateFormat.compareTo(endDateFormat) <= 0)
        calendar.setTime(startDateFormat)
        var diff = 0
        while (startDateFormat.compareTo(endDateFormat) < 0) {
            diff += 1
            calendar.add(Calendar.DAY_OF_MONTH, 1)
            startDateFormat = calendar.getTime
        }
        diff
    }

    /**
     *
     * @param endDate 20200901
     * @param startDate 20200830
     * @param dateFormat new SimpleDateFormat("yyyyMMdd")
     * @return 若是非法情况，返回None
     */
    def getDateDiffDay(endDate: String, startDate: String, dateFormat: SimpleDateFormat): Option[Int] = {
        val calendar = Calendar.getInstance()
        var startDateFormat = dateFormat.parse(startDate)
        val endDateFormat = dateFormat.parse(endDate)
        if (startDateFormat.compareTo(endDateFormat) >= 0) return None
        calendar.setTime(startDateFormat)
        var diff = 0
        while (startDateFormat.compareTo(endDateFormat) < 0) {
            diff += 1
            calendar.add(Calendar.DAY_OF_MONTH, 1)
            startDateFormat = calendar.getTime
        }
        Some(diff)
    }

    /**
      * 获取指定时间范围内的日期,比如:-60 ~ -30, [startOffset, endOffset]
      *
      * @param startOffset
      * @param endOffset
      * @param baseDate
      * @return 日期列表。日期格式"20190430"
      */
    def getDateRange(startOffset: Int, endOffset: Int, baseDate: String): List[String] = {
        val calendar = Calendar.getInstance()
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        var baseDateTime = dateFormat.parse(baseDate)
        calendar.setTime(baseDateTime)

        (startOffset to endOffset).toList.map { offset =>
            calendar.add(Calendar.DAY_OF_MONTH, offset)
            val currentDateTime = calendar.getTime
            calendar.setTime(baseDateTime) // 重置time
            dateFormat.format(currentDateTime)
        }
    }

    /**
      * 把 20190429 转为 year=2019/month=04/day=29 的格式
      *
      * @param date yyyyMMdd
      * @return
      */
    def getYearMonthDateFormat(date: String): String = {
        if (StringUtils.length(date) != 8) {
            logger.error("illegal date format {}", date)
            return ""
        }
        val dateSb = new StringBuilder
        dateSb.append("year=").append(date.substring(0, 4))
            .append("/month=").append(date.substring(4, 6))
            .append("/day=").append(date.substring(6, 8))
        dateSb.toString
    }

    /**
      * Adds or subtracts the specified amount days from the current time
      *
      * @param amount
      * @return date with format of "yyyyMMdd"
      */
    def getDataFormatWithAmount(amount: Int): String = {
        val calendar = Calendar.getInstance
        calendar.add(Calendar.DAY_OF_MONTH, amount)
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        dateFormat.format(calendar.getTime)
    }

    /**
      * Adds or subtracts the specified amount days from the current time
      *
      * @param dateStr yyyyMMdd
      * @param amount
      * @return date with format of "yyyyMMdd"
      */
    def getDataFormatWithAmount(dateStr: String, amount: Int): String = try {
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        val calendar = Calendar.getInstance
        val date = dateFormat.parse(dateStr)
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_MONTH, amount)
        dateFormat.format(calendar.getTime)
    } catch {
        case e: ParseException =>
            logger.error("failed to parse date {}", dateStr)
            ""
    }

    /**
      * 通过时间戳获取到时间
      *
      * @param timestamp ms
      * @return 日期时间字符串 yyyy-MM-dd HH:mm:ss.SSS
      */
    def getDateTimeByTimestamp(timestamp: Long): String = {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        getDateTimeByTimestamp(timestamp, dateFormat)
    }

    /**
      * 通过时间戳获取到时间
      *
      * @param timestamp  ms(s 记得 *1000)
      * @param dateFormat 自定义格式 new SimpleDateFormat("yyyyMMdd")
      * @return 日期时间字符串
      */
    def getDateTimeByTimestamp(timestamp: Long, dateFormat: SimpleDateFormat): String = {
        val date = new Date
        date.setTime(timestamp)
        dateFormat.format(date)
    }

    /**
     * 通过date得到时间戳
     * @param date String
     * @param dateFormat 自定义格式
     * @return
     */
    def getTimestampByDate(date: String, dateFormat: SimpleDateFormat): Long = {
        val dt = dateFormat.parse(date)
        val timestamp: Long = dt.getTime
        timestamp
    }

    /**
     * 通过date得到时间戳
     * @param date String yyyy-MM-dd HH:mm:ss
     * @return
     */
    def getTimestampByDate(date: String): Long = {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dt = dateFormat.parse(date)
        val timestamp: Long = dt.getTime
        timestamp
    }

    /**
      * @param path : hdfs://zjyprc-hadoop/user/s_ai_service/quanzhi/user_data/location/date=20191002
      * @return hdfs://zjyprc-hadoop/user/s_ai_service/quanzhi/user_data/location/date={}
      */
    def replacePathDate(path: String, replacement: String): String = {
        val matcher = dateStrRegex.matcher(path)
        if (matcher.find()) {
            val date = matcher.group("date")
            path.replace(date, replacement)
        } else {
            path
        }
    }

}
