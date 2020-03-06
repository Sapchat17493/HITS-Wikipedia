import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HitsWiki {

  def normalizeEachUsingSum(inRDD: RDD[(Int, Float)], sparkSession: SparkSession): RDD[(Int, Float)] = {
    val acc = sparkSession.sparkContext.doubleAccumulator("Normalizing Accumulator")
    inRDD.foreach(f => acc.add(f._2))
    val sum = acc.value.toFloat
    val normalForm = inRDD.map(x => (x._1, x._2 / sum))
    return normalForm
  }

  def main(args: Array[String]): Unit = {
    val searchTerm = args.apply(0)

    val ss = SparkSession.builder.appName("HitsWiki").getOrCreate()
    val sortedTitles = ss.read.textFile("hdfs://juneau:31101/535PA1/titles-sorted.txt").rdd.zipWithIndex().map { case (l, i) => ((i + 1).toInt, l) }.partitionBy(new HashPartitioner(100)).persist()

    val sortedLinks = ss.read.textFile("hdfs://juneau:31101/535PA1/links-simple-sorted.txt").rdd.map(x => (x.split(":")(0).toInt, x.split(":")(1))).partitionBy(new HashPartitioner(100)).persist()

    val outlinkForEachEntry = sortedLinks.flatMapValues(y => y.trim.split(" +")).mapValues(x => x.toInt)

    val rootSet = sortedTitles.filter(f => f._2.toLowerCase.contains(searchTerm.toLowerCase))

    //Inlink
    val outlinks = outlinkForEachEntry.groupByKey()
    val trimmedOuts = outlinks.map { case (x, y) => (x, if (y.size > 30) y.slice(0, 29) else y) }
    val trimmedOut = trimmedOuts.flatMapValues(x => x)
    val baseSetOutlink = rootSet.join(trimmedOut).mapValues(x => x._2)

    //Outlink
    val inlinkForEach = outlinkForEachEntry.map(_.swap)
    val inlinkSet = rootSet.join(inlinkForEach).mapValues(x => x._2).groupByKey()
    val trimmedInlinkSet = inlinkSet.map { case (x, y) => (x, if (y.size > 30) y.slice(0, 29) else y) }
    val trimmedInlink = trimmedInlinkSet.flatMapValues(x => x)
    val baseSetInlink = trimmedInlink.map(_.swap)


    val baseSet = baseSetOutlink.union(baseSetInlink).persist()
    val allInLinks = baseSet.map(_.swap).persist()
    var hubScore = baseSet.map(x => (x._1, 1.0.toFloat)).distinct()
    var authScore = allInLinks.map(x => (x._1, 1.0.toFloat)).distinct()

    for (_ <- 0 to 50) {
      val tempAuthScore = baseSet.join(hubScore).map(x => (x._2._1, x._2._2)).reduceByKey((x, y) => x + y)
      authScore = normalizeEachUsingSum(tempAuthScore, ss)
      val tempHubScore = allInLinks.join(authScore).map(x => (x._2._1, x._2._2)).reduceByKey((x, y) => x + y)
      hubScore = normalizeEachUsingSum(tempHubScore, ss)
    }

    val bestHub = ss.sparkContext.parallelize(hubScore.takeOrdered(100)(Ordering[Double].reverse.on(_._2)))
    val hubAndTitle = bestHub.join(sortedTitles).map(x => (x._2._1, x._2._2))
    val bestAuth = ss.sparkContext.parallelize(authScore.takeOrdered(100)(Ordering[Double].reverse.on(_._2)))
    val authAndTitle = bestAuth.join(sortedTitles).map(x => (x._2._1, x._2._2))

    rootSet.coalesce(1).saveAsTextFile("hdfs://juneau:31101/535PA1/RootSet")
    baseSet.coalesce(1).saveAsTextFile("hdfs://juneau:31101/535PA1/BaseSet")
    hubAndTitle.coalesce(1).saveAsTextFile("hdfs://juneau:31101/535PA1/Top100Hub")
    authAndTitle.coalesce(1).saveAsTextFile("hdfs://juneau:31101/535PA1/Top100Auth")

    ss.stop()
  }
}