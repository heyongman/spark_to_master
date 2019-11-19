package com.he

import java.net.{HttpURLConnection, URL}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.io.IOUtils


object Test4 {
  def main(args: Array[String]): Unit = {

    for (i <- 1 to 1){
      val url = new URL("https://restapi.amap.com/v3/geocode/regeo?key=72ac2468cce2672437454dd869b175f3&location=121.466600,31.220800")
      val con = url.openConnection().asInstanceOf[HttpURLConnection]
      println(con.getResponseCode)
      val is = con.getInputStream
      val str = IOUtils.toString(is,"utf-8")
      println(str)

    }
  }
}
