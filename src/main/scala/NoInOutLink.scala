import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
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
            .map(line => (line.replaceAll(":", "")))
            .flatMap(flatten_page)
        
        val titles = sc
            .textFile(inputDir+"/titles-sorted.txt", numPartitions)
            .zipWithIndex()
            .map(word => (word._2+1,word._1))
        
        val no_outlinks = titles
            .leftOuterJoin(flatten)
            .map(distributed)
            .filter(word => word._3 == None)
            .map(word => (word._1, word._2))
            .takeOrdered(10)(Ordering[Long].on(word =>word._1))
            .foreach(println)
        
        println("[ NO OUTLINKS ]")

        val transform = flatten
            .map( word => (word._2, word._1))
        
        println("[ NO INLINKS ]")
        val no_inlinks = titles
            .leftOuterJoin(transform)
            .map(distributed)
            .filter(word => word._3 == None)
            .map(word => (word._1, word._2))
            .takeOrdered(10)(Ordering[Long].on(word =>word._1))
            .foreach(println)
    }
    
    def flatten_page(line:String): Array[(Long,Long)] = {
        val pages = line.split(" ")    
        
        val result:Array[(Long,Long)] = new Array[(Long,Long)](pages.length-1)
        var start = 1
        while (start < pages.length){
            result(start-1)=( pages(0).toLong,pages(start).toLong)
            start = start+1
        }
        result
    }
    
    def distributed(line: (Long, (String, Option[Long]))): (Long, String, Option[Long]) = {
        val first=line._2
        val second=first._1
        val third=first._2
        val forth=line._1
        val result:(Long, String, Option[Long]) = (forth,second,third)
        result
    }
    
    
    
}

