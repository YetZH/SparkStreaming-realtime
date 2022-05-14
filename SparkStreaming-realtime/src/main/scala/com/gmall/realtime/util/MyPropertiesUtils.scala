package com.gmall.realtime.util

import java.util.ResourceBundle

object MyPropertiesUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propertiesKey : String) ={
    bundle.getString(propertiesKey)
  }

  def main(args: Array[String]): Unit = {
    println(MyPropertiesUtils.apply("kafka.bootstrap-servers"))
  }

}
