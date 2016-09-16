//
//  PageRankTwitter.scala

package org.apache.spark.examples

import org.apache.spark.graphx.GraphLoader


//import org.apache.spark._
//import org.apache.spark.graphx._
//import org.apache.spark.graphx.lib._


object PageRankTwitter {
    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis();

        val graph = GraphLoader.edgeListFile(sc, "/home/ubuntu/edgein.txt");

        val ranks = graph.staticPageRank(args(0)).vertices;

        ranks.saveAsTextFile("/home/ubuntu/out_spark");

        val elapsedTimeMillis = System.currentTimeMillis() - startTime;

        println("Total latency: " + elapsedTimeMillis);

        spark.stop();
    }
}


