import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import java.io._

object GraphXPageRank {

  //Define variables for start time, wikipedia data set location, Url
  val t1 = System.currentTimeMillis()
  var wiki = "freebase-wex-2009-01-12-articles.tsv"
  var remoteUrl = "local"
  
  //Hash function to generate hash value based on title
  def pageHash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }

  def main(args: Array[String]) {

    //Check for the wikipedia customizable location and masterURL
    if (args.length >= 2) {
      remoteUrl = args(1).toString.trim 
    } else if(args.length >= 1) {
      wiki = args(0).toString.trim
    }else{
      //local url and local data set
    }
    printTop100Pages(wiki, remoteUrl)
  }

  def printTop100Pages(wikipedia: String, remoteURL: String) = {
    //Setting spark context
    val sparkConf = new SparkConf().setAppName("PageRankGraphX").setMaster(remoteURL)
    val sc = new SparkContext(sparkConf)
    val wikiData: RDD[String] = sc.textFile(wikipedia).coalesce(20)

    //Define the article class
    case class Article(val id: Int, val title: String, val body: String)

    
    //Split the lines based on tab space
    val articles = wikiData.map(_.split('\t')).
     filter(line => line.length > 1).
     map(line => new Article(line(0).trim.toInt, line(1).trim.toString, line(3).trim.toString)).cache()
     
    //Form vertices with pages 
    val vertices = articles.map(a => (pageHash(a.title), a.title))

    //Interested in internal wikipedia links, hence as per WEX documentation, those within target tags are parsed 
    val findPattern = "<target>.+?<\\/target>".r

    //Create edges for the vertices
    val edges: RDD[Edge[Double]] = articles.flatMap { a =>
      val sourceId = pageHash(a.title)
      findPattern.findAllIn(a.body).map { link =>
        val destinationId = pageHash(link.replace("<target>", "").replace("</target>", ""))
        Edge(sourceId, destinationId, 1.0)
      }
    }
   
    //Removing non existent links
    val graph = Graph(vertices, edges, "").subgraph(vpred = { (v, d) => d.nonEmpty }).cache

    //Finding pageranks for universities with 10 iterations
    val prGraph = graph.staticPageRank(10).cache

    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    val pw = new PrintWriter(new File("PagerankGraphXoutput.txt"))
    titleAndPrGraph.vertices.top(100) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => (println(t._2._2 + ": " + t._2._1), pw.write(t._2._2 + " has rank " + t._2._1 + "." + "\n")))
    pw.close
    val t2 = System.currentTimeMillis
    println("Time taken for PagerankGraphX for 10 iterations is " + (t2-t1) + " msecs")

  }
}

//References: http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html