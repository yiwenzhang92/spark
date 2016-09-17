//
//  PageRankTwitter.scala

package org.apache.spark.examples

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._


object SparkPageRank {
    def main(args: Array[String]): Unit = {

	val spark = SparkSession
	    .builder
   	    .appName("PageRank on Twitter")
 	    .getOrCreate()

//	val conf = new SparkConf().setAppName("PageRank on Twitter")

//	val sc = new SparkContext(conf)

        val startTime = System.currentTimeMillis;

        val graph = GraphLoader.edgeListFile(spark.sparkContext, "/home/ubuntu/edgein.txt");

        val ranks = graph.staticPageRank(args(0).toInt).vertices;

        ranks.saveAsTextFile("/home/ubuntu/out_spark");

        val elapsedTimeMillis = System.currentTimeMillis - startTime;

        println("Total latency: " + elapsedTimeMillis);

        spark.stop();
    }
}


