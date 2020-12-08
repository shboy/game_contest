package com.xiaomi.dataming

import java.text.SimpleDateFormat

import com.xiaomi.dataming.util.DateUtils

/**
 * @author shenhao
 * @Email shenhao@xiaomi.com
 * @date 2020/11/8
 */
object DateUtilsTest {
  def main(args: Array[String]): Unit = {
	println(DateUtils.getDateRange("20200701", "20200704"))
    println(DateUtils.getDateFromAddNDays("20200901", 6, new SimpleDateFormat("yyyyMMdd")))
	
  }
  
}
