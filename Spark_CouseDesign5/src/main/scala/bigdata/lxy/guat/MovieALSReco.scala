package bigdata.lxy.guat

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object MovieALSReco {
  def main(args: Array[String]): Unit = {
    val inputPath = if (args.length > 0) args(0) else "/data/ratings.csv"
    val topK = 3
    val tableName = "movie_reco"
    val columnFamily = "info"
    val zkQuorum = "hadoop01:2181"   // 改回 hadoop01

    val spark = SparkSession.builder()
      .appName("MovieALSReco")
      .getOrCreate()
    import spark.implicits._

    val ratings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)
      .select("userId", "movieId", "rating")
      .na.drop()

    val movies = try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/movies.csv")
        .select("movieId", "title")
        .na.drop()
    } catch {
      case _: Exception => spark.emptyDataFrame
    }

    val als = new ALS()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setColdStartStrategy("drop")
    val model = als.fit(ratings)

    val allUsers = ratings.select("userId").distinct().as[Int].collect()
    val targetUsers = allUsers.take(5)

    val schema = StructType(Seq(
      StructField("userId", IntegerType, false),
      StructField("dummy", IntegerType, false)
    ))
    val rows = targetUsers.map(userId => Row(userId, 0))
    val userDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val recommendations = model.recommendForUserSubset(userDF, topK)
      .select(col("userId"), explode(col("recommendations")).as("rec"))
      .select(col("userId"), col("rec.movieId"), col("rec.rating"))

    val result = if (!movies.isEmpty) {
      recommendations.join(movies, Seq("movieId"), "left")
        .select(col("userId"), col("movieId"), col("title").as("movie_title"), col("rating").as("predict_rating"))
    } else {
      recommendations.select(col("userId"), col("movieId"), lit("unknown").as("movie_title"), col("rating").as("predict_rating"))
    }

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf(tableName))

    val puts = new ArrayBuffer[Put]()
    result.collect().foreach { row =>
      val userId = row.getAs[Int]("userId")
      val movieId = row.getAs[Int]("movieId")
      val title = row.getAs[String]("movie_title")
      val rating = row.getAs[Float]("predict_rating")

      val rowKey = s"${userId}_${movieId}"
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("user_id"), Bytes.toBytes(userId.toString))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("movie_id"), Bytes.toBytes(movieId.toString))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("movie_title"), Bytes.toBytes(title))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("predict_rating"), Bytes.toBytes(rating.toString))
      puts += put
    }

    table.put(puts.toList.asJava)
    table.close()
    connection.close()

    println(s"成功写入 ${puts.length} 条推荐记录到 HBase 表 $tableName")

    spark.stop()
  }
}