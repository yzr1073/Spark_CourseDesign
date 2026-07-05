package guat.lxy.bigdata
import org.apache.spark.sql.SparkSession

object MovieExplore {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieOfflineExplore")
      .master("local[*]")
      .getOrCreate()

    // 完整业务代码
    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://hadoop01:9000/movie_data/movies.csv")

    moviesDF.printSchema()
    moviesDF.show(10, truncate = false)
    moviesDF.createOrReplaceTempView("movies")

    println("===== 按电影分类统计数量 =====")
    spark.sql("SELECT category, COUNT(*) as movie_count FROM movies GROUP BY category").show()

    println("===== Sci-Fi分类电影列表 =====")
    spark.sql("SELECT movieId, title, category FROM movies WHERE category = 'Sci-Fi'").show(truncate = false)

    spark.stop()
  }
}




