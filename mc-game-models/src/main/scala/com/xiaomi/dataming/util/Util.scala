package com.xiaomi.dataming.util

import java.io.PrintWriter

import com.google.gson.GsonBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.slf4j.Logger

/**
 * Authors: shenhao <shenhao@xiaomi.com>
 * created on 2020/9/9
 */
object Util {
    //val gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create()
    val gson = new GsonBuilder().create()

    def getArgMap(args: Array[String])(implicit LOGGER: Logger): Map[String, String] = {
        LOGGER.info(s"input args: ${args.mkString(" ")}")
        require(args.forall(i => i.trim.startsWith("-D") && i.contains("="))) // 所有字段必须以D开头
        val argsInputMap: Map[String, String] = args.map { i =>
            val kv = i.split("=").map(_.trim).toBuffer
            val key = kv.remove(0).substring(2).trim
            val value = kv.mkString("=").trim
            (key, value)
        }.toMap
        LOGGER.info(s"valid args:${argsInputMap.map(x => x._1 + ":" + x._2).mkString(" ")}")
        argsInputMap
    }

    def writeString(str:String, outputPath:String, deleteOldFile:Boolean=false)(implicit sc: SparkContext, LOGGER: Logger):Unit = {
        //fs.delete(new Path(outputPath), true)
        if(deleteOldFile){
            deleteFile(outputPath)
        }
        val fs = FileSystem.get(new Configuration(sc.hadoopConfiguration))
        val writer1 = new PrintWriter(fs.create(new Path(outputPath)))
        writer1.write(str)
        writer1.flush()
        writer1.close()
        LOGGER.info(s"write string to output path: $outputPath")
    }

    def deleteFile(pathStr: String)(implicit sc: SparkContext, LOGGER: Logger): Boolean = {
        val fs = FileSystem.get(new Configuration(sc.hadoopConfiguration))
        val path = new Path(pathStr)
        if (fs.exists(path)) {
            LOGGER.info(s"path: $pathStr exists, delete it!")
            fs.delete(path, true)
            true
        } else {
            LOGGER.info(s"path: $pathStr is not exists, skip!")
            false
        }
    }

    def isExists(pathStr: String)(implicit sc: SparkContext, LOGGER: Logger): Boolean = {
        val fs = FileSystem.get(new Configuration(sc.hadoopConfiguration))
        val path = new Path(pathStr)
        if (fs.exists(path)) {
            LOGGER.info(s"path: $pathStr exists")
            true
        } else {
            LOGGER.info(s"path: $pathStr is not exists, skip!")
            false
        }
    }

    def isChinese(c: Char): Boolean = c >= 0x4E00 && c <= 0x9FA5 // 根据字节码判断

    /**
      * 设置spark日志级别，默认Level.ERROR
      * @param logLevel
      */
    def setLoggerLevel(logLevel: String, logName: String = "org.apache.spark"): Unit = {
        import org.apache.log4j.{Level, Logger}
        logLevel.toLowerCase() match {
            case "error" => {
                Logger.getLogger(logName).setLevel(Level.ERROR)
                Logger.getRootLogger.setLevel(Level.ERROR)
            }
            case "warn" => {
                Logger.getLogger(logName).setLevel(Level.WARN)
                Logger.getRootLogger.setLevel(Level.WARN)
            }
            case "info" => {
                Logger.getLogger(logName).setLevel(Level.INFO)
                Logger.getRootLogger.setLevel(Level.INFO)
            }
            case "debug" => {
                Logger.getLogger(logName).setLevel(Level.DEBUG)
                Logger.getRootLogger.setLevel(Level.DEBUG)
            }
            case _ => {
                Logger.getLogger(logName).setLevel(Level.ERROR)
                Logger.getRootLogger.setLevel(Level.ERROR)
            }
        }
    }

}
