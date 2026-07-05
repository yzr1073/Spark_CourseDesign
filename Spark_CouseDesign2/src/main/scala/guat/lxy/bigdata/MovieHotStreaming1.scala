package guat.lxy.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.sql.{Connection, DriverManager, PreparedStatement}

object MovieHotStreaming1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieHotStreaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    // ========== 1. 预加载CSV全量数据，作为实时流数据池 ==========
    val csvPath = "file:///opt/bigdata-movie-reco-platform/04-structured-streaming-hot/kafka_click_logs1.csv"
    // 读取CSV，解析电影ID和时间字符串
    val csvDF = spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .csv(csvPath)
      .select($"movieId".cast("int").as("movie_id"), $"timestamp".as("time_str"))

    // 收集到Driver端，广播到所有Executor，循环读取模拟实时流
    val csvDataList = csvDF.collect().map(row => (row.getInt(0), row.getString(1))).toList
    val dataTotal = csvDataList.size
    val csvBroadcast = spark.sparkContext.broadcast(csvDataList)

    // ========== 2. 速率控制：每5秒输出8条数据 ==========
    // rowsPerSecond = 1.6 → 5秒 × 1.6 = 8条，精准匹配每5秒8条的要求
    val rateSource = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "2")
      .option("numPartitions", "1")
      .load()

    // 将自增序号映射为CSV中的真实数据，循环读取
    val clickStream = rateSource
      .map(row => {
        // 用自增value对数据总数取模，实现循环读取
        val index = (row.getAs[Long]("value") % dataTotal).toInt
        val (movieId, timeStr) = csvBroadcast.value(index)
        (movieId, timeStr)
      })
      .toDF("movie_id", "time_str")
      // 解析CSV的斜杠时间格式为标准时间戳
      .withColumn("event_time", to_timestamp($"time_str", "yyyy/MM/dd HH:mm"))
      .select("movie_id", "event_time")

    // ========== 3. MySQL连接配置 ==========
    val mysqlUrl = "jdbc:mysql://hadoop01:3306/movie_db?useSSL=false&characterEncoding=utf8"
    val mysqlUser = "root"
    val mysqlPassword = "Root123456!"

    // ========== 4. 微批处理：窗口聚合 + 累计统计 + 写入MySQL ==========
    def processBatch(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
      var conn: Connection = null
      try {
        Class.forName("com.mysql.jdbc.Driver")
        conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword)

        // 4.1 窗口聚合：10秒窗口，5秒滑动，生成实时排行榜
        val windowResult = batchDF
          .groupBy(window($"event_time", "10 seconds", "5 seconds"), $"movie_id")
          .agg(count("*").alias("click_count"))
          .select(
            $"movie_id",
            $"click_count",
            date_format($"window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            date_format($"window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end")
          )
          .orderBy($"click_count".desc)

        // 清空窗口表，写入最新窗口数据
        val stmtTruncate = conn.createStatement()
        stmtTruncate.execute("TRUNCATE TABLE hot_movie_rank")
        stmtTruncate.close()

        val insertRankSql = "INSERT INTO hot_movie_rank (movie_id, click_count, window_start, window_end) VALUES (?,?,?,?)"
        val psRank = conn.prepareStatement(insertRankSql)
        windowResult.collect().foreach { row =>
          psRank.setInt(1, row.getInt(0))
          psRank.setLong(2, row.getLong(1))
          psRank.setString(3, row.getString(2))
          psRank.setString(4, row.getString(3))
          psRank.addBatch()
        }
        psRank.executeBatch()
        psRank.close()

        // 4.2 累计点击量统计，更新总榜
        val incrementDF = batchDF.groupBy($"movie_id").agg(count("*").alias("increment"))
        val upsertTotalSql = "INSERT INTO hot_movie_total (movie_id, total_click) VALUES (?,?) ON DUPLICATE KEY UPDATE total_click = total_click + VALUES(total_click)"
        val psTotal = conn.prepareStatement(upsertTotalSql)
        incrementDF.collect().foreach { row =>
          psTotal.setInt(1, row.getInt(0))
          psTotal.setLong(2, row.getLong(1))
          psTotal.addBatch()
        }
        psTotal.executeBatch()
        psTotal.close()

        println(s"===== 批次 $batchId 处理完成，本批次数据量：${batchDF.count()} 条 =====")
      } catch {
        case e: Exception =>
          println("MySQL写入异常：")
          e.printStackTrace()
      } finally {
        if (conn != null && !conn.isClosed) conn.close()
      }
    }

    // ========== 5. 启动流式任务 ==========
    val query = clickStream.writeStream
      .foreachBatch(processBatch _)
      .trigger(Trigger.ProcessingTime("5 seconds")) // 每5秒一个微批
      .outputMode("append")
      // 更换checkpoint目录，避免和旧任务冲突
      .option("checkpointLocation", "file:///tmp/spark-checkpoint-movie-csv")
      .start()

    query.awaitTermination()
  }
}