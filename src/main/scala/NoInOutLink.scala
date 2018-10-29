import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
// Do NOT use different Spark libraries.

object NoInOutLink {
    
    def main(args: Array[String]) {
        val inputDir = "sample-input"
        val linksFile = inputDir + "/links-simple-sorted.txt"
        val titlesFile = inputDir + "/titles-sorted.txt"
        val numPartitions = 10
        
        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")
        
        val sc = new SparkContext(conf)
        val links = sc
            .textFile(inputDir+"/links-simple-sorted.txt", numPartitions)
        
        val flatten = links
            .map(reformat)
            .flatMap(flatten_page)
        
        val titles = sc
            .textFile(inputDir+"/titles-sorted.txt", numPartitions)
            .zipWithIndex()
            .map(word => (word._2+1,word._1))
        
        val no_outlinks=titles
            .leftOuterJoin(flatten)
            .map(unpackage)
            .filter(word => word._3 == None)
            .map(word => (word._1, word._2))
            .takeOrdered(10)(Ordering[Long].on(word =>word._1))
            .foreach(println)
        
        println("[ NO OUTLINKS ]")
        
        val reverseLinks=flatten
            .map( word => (word._2, word._1) )
        
        val no_inlinks=titles
            .leftOuterJoin(reverseLinks)
            .map(unpackage)
            .filter(word => word._3 == None)
            .map(word => (word._1, word._2))
            .takeOrdered(10)(Ordering[Long].on(word =>word._1))
        
        println("\n[ NO INLINKS ]")
        
        no_inlinks
            .foreach(println)
        
    }
    
    def flatten_page(line:String): Array[(Long,Long)]= {
        val pages = line.split(" ")    
        
        val result:Array[(Long,Long)]=new Array[(Long,Long)](pages.length-1)
        for(i <- 1 to (pages.length-1)  )
        {
            result(i-1)=( pages(0).toLong,pages(i).toLong)
            
        }
        result
    }
    
    def unpackage(line: (Long, (String, Option[Long]) ) ): (Long, String, Option[Long])={
        val a=line._2
        val b=a._1
        val c=a._2
        val d=line._1
        val count:(Long, String, Option[Long])=(d,b,c)
        count
    }
    
     def reformat(line: String):String={
        line.replaceAll(":", "")
    }
    
}

