import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {

  def main(args: Array[String]) {
    val inputDir = "sample-input"
    val linksFile = inputDir + "/links-simple-sorted.txt"
    val titlesFile = inputDir + "/titles-sorted.txt"
    val numPartitions = 10
    val numIters = 10

    val conf = new SparkConf()
      .setAppName("PageRank")
      .setMaster("local[*]")
      .set("spark.driver.memory", "1g")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val links = sc
      .textFile(linksFile, numPartitions)
      .map(remove_punctuation)
      .flatMap(count_num)   
    // TODO
     /*
        create a rdd, where each element is(a,b), a 
        is page id and b is number of outlinks
        */
    val outLinks=links
        .map( word => (word._1, 1) )
        .reduceByKey( (a, b) => (a + b) )


    val titles = sc
      .textFile(titlesFile, numPartitions)
    // TODO

 //index
    val N: Long=titles.count
    val size : Double=100.0/N
    val d: Double=0.85
    val num:Double=(1-d)*100.0/N
    /* PageRank */
      /*
        each element:(index, (page name, number of outlinks, initial score) )
        */
    var table=titles   
        .zipWithIndex()
        .map(word => (word._2+1,word._1) )
        .leftOuterJoin(outLinks)
        .map(word => unpackage(word) )
        .map(word => (word._1, (word._2, word._3,size) ) )

        /*
        each element: (pageid, score sent in from a specific inlink)
        */
    var score=table
        .join(links)
        .map(word => (unpack(word)) )
        .map(word => (word._1, word._3, word._4, word._5))
        .map(word => (word._1, word._3/word._2, word._4))
        .map(word => (word._3, word._2))
        .reduceByKey((a,b) => a+b)

        
       
    for (i <- 1 to 10) {
            /*
            each element:(index, (page name, number of outlinks, initial score, score sent in) )
            */
        table=table
        .leftOuterJoin(score)
        .map(word => archive(word))
        .map(word => (word._1, (word._2, word._3, num+d*word._5) ) )  


        score=table
        .join(links)
        .map(word => (unpack(word)) )
        .map(word => (word._1, word._3, word._4, word._5))
        .map(word => (word._1, word._3/word._2, word._4))
        .map(word => (word._3, word._2))
        .reduceByKey((a,b) => a+b)
    }
        
    println("[ PageRanks ]")
        //table.foreach(println)
        //process the final result
    var table2=table.map(word => unfold(word) )
        .map(word => (word._1, word._2, word._4))
        
        //Calculate overall scores
    val sum=table2.map(word=>(1, word._3))
        .reduceByKey((a,b) => a+b)
        .first()._2

        //Normalization
        table2.map(word => (word._1, word._2, word._3*100/sum) )
        .takeOrdered(10)(Ordering[Double].reverse.on(word =>word._3))
        .foreach(println)
    }

    def remove_punctuation(line: String):String={
        line.replaceAll(":", "")    
    }

    def count_num(line:String): Array[(Long,Long)]= {
        val list= line
                  .split(" ")
        
        val num:Int=list.length-1

        val count:Array[(Long,Long)]=new Array[(Long,Long)](num)

        for(i <- 1 to (list.length-1)  )
        {
            count(i-1)=( list(0).toLong,list(i).toLong)

        }
        count
    }

    def unpackage(line: (Long,(String,Option[Int]) ) ): (Long, String,Int)={
        val a=line._2
        val b=a._1
        var c=0
        if(a._2!=None){c=a._2.get}
        else{c=0}
        val d=line._1
        val count:(Long, String,Int)=(d,b,c)
        count
    }

    def unpack(line: (Long,((String,Int,Double),Long) ) ): (Long,String,Int,Double,Long)={
        val a=line._2
        val b=a._1
        val c=b._1
        val d=b._2
        val e=b._3
        val f=a._2
        val g=line._1
        val count:(Long,String,Int,Double,Long)=(g,c,d,e,f)
        count
    }

    def archive(line: (Long,((String,Int,Double),Option[Double] ))): (Long,String,Int,Double,Double)={
        val a=line._2
        val b=a._1
        val c=b._1
        val d=b._2
        val e=b._3
        var f=0.0
        if(a._2!=None){f=a._2.get}
        else{f=0.0}
        val g=line._1
        val count:(Long,String,Int,Double,Double)=(g,c,d,e,f)
        count
    }

    def unfold(line: (Long,(String,Int,Double)) ): (Long,String,Int,Double)={
        val a=line._2
        val b=a._1
        val c=a._2
        val d=a._3
        val e=line._1
        val count:(Long,String,Int,Double)=(e,b,c,d)
        count
    }


}
