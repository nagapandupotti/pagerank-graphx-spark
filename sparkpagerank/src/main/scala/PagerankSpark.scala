import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import java.io._

object PagerankSpark {

  //Define variables for start time, wikipedia data set location, Url and number of iterations for pagerank
  val t1 = System.currentTimeMillis()
  var wiki = "freebase-wex-2009-01-12-articles.tsv"
  var remoteURL = "local"
  val iterations = 10

  //Hash function to generate hash value based on title
  def pageHash(title: String) = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }

  def main(args: Array[String]) { 

    //Check for the wikipedia customizable location and masterURL
    if (args.length >= 2) {
      remoteURL = args(1).toString.trim 
    } else if(args.length >= 1) {
      wiki = args(0).toString.trim
    }else{
      //local url and local data set
    }

    //Setting spark context
    val sparkConf = new SparkConf().setAppName("PageRankSpark").setMaster(remoteURL)
    val sc = new SparkContext(sparkConf)
    
    //Reading the wikipedia data set 
    val wikipedia = sc.textFile(wiki, 1)
    case class Article(val id: Int, val title: String, val body: String)
    case class Connection(val source: Long, val destination: Long)

    //Split the lines based on tab space
    val articles = wikipedia.map(_.split('\t')).
      filter(line => line.length > 1).
      map(line => new Article(line(0).trim.toInt, line(1).trim.toString, line(3).trim.toString)).cache()

    //Interested in internal wikipedia links, hence as per WEX documentation, those within target tags are parsed  
    val findPattern = "<target>.+?<\\/target>".r

    val originalNames = wikipedia.map { a =>
      val parts = a.split('\t')
      val sourceId = pageHash(parts(1))
      (sourceId, parts(1))
    }.cache()

    val links = wikipedia.flatMap { a =>
      val parts = a.split('\t')
      val sourceId = pageHash(parts(1))
      findPattern.findAllIn(parts(3)).map { link =>
        val destinationId = pageHash(link.replace("<target>", "").replace("</target>", ""))
        (sourceId, destinationId)
      }
    }.distinct().groupByKey().cache()

    //Set initial rank of every page to 1
    var setRanks = links.mapValues(v => 1.0)

    for (i <- 1 to iterations) {
      val contribs = links.join(setRanks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(l => (l, rank / size))
      }
      setRanks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val result = originalNames.join(setRanks)
    val output = result.collect().sortBy(x => x._2._2)
    val r = output.take(100).reverse
    val pw = new PrintWriter(new File("pageranksparkoutput.txt"))
    r.foreach(tup => (println(tup._2._1 + " has rank " + tup._2._2 + "."), pw.write(tup._2._1 + "  " + tup._2._2 + "\n")))
    pw.close
    val t2 = System.currentTimeMillis
    println("Time taken for PagerankSpark for " + iterations + " iterations is " + (t2-t1) + " msecs")
    sc.stop()
  }
}

//References: http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-2-amp-camp-2012-standalone-programs.pdf
