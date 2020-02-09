import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {

  def main(args: Array[String]): Unit = {
    //1. Location of the csv ﬁle containing the ﬁelds mentioned above
    //2. Maximum number of iterations to run
    //3. Location of output ﬁle
    if (args.length != 3) {
      println("Usage: InputFile iterations OutputDir")
    }
    val sc = new SparkContext(new SparkConf().setAppName("PageRank").setMaster("local"))
    val lines = sc.textFile(args(0))
    val header = lines.first()
    val record = lines.filter(_ != header).map{ s =>
      val parts = s.split(",")
      (parts(0), parts(3))
    }.groupByKey().cache()
    var rankValue = record.mapValues(v => 10.0)
    val randomValue = 1.0/rankValue.count()
    val iter = args(1).toInt
    for (i <- 1 to iter) {
      val OutNeighbor = record.join(rankValue).values.flatMap{ case (airportID, rank) =>
        val size = airportID.size
        airportID.map(airportID => (airportID, rank / size))
      }
      rankValue = OutNeighbor.reduceByKey(_ + _).mapValues(v =>0.15*randomValue + 0.85 * v)
    }
    val result: RDD[(String, Double)] = rankValue.sortBy(-_._2)
    result.saveAsTextFile(args(2))
  }
}
