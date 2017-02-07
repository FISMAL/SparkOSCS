package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.commons.io.IOUtils

import java.net.URL

import java.nio.charset.Charset

object MainExample {

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SparkOSCS").setMaster("local")
    val sc = new SparkContext(conf)
    val hconf = sc.hadoopConfiguration;
    
    
    hconf.set("fs.swift.SparkOSCH", "main");
    // Specify the v2.0 authentication link to your Oracle Storage Cloud Service instance
    hconf.set("fs.swift.service.main.auth.url", "https://storageid.storage.oraclecloud.com/auth/v2.0/tokens");
    

    hconf.set("fs.swift.service.main.public", "true");
    
    hconf.set("fs.swift.service.http.location-aware", "false");
    
     
    // Your storageid 
    hconf.set("fs.swift.service.main.tenant",  "Storage-storageid");
    // Specify you username and password below
    hconf.set("fs.swift.service.main.username", "Username");
    
    hconf.set("fs.swift.service.main.password", "Password");
    
     
    
    val textfile = sc.textFile("swift://firstContainer.main/testobject")
   
    textfile.take(3).foreach(println) 
    textfile.saveAsTextFile("swift://firstContainer.main/newobject")

  }
}
