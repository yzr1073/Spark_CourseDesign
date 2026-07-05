package guat.lxy.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

object MovieHotStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieHotStreaming")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    // 1. 生成模拟点击流（每秒8条 → 每5秒微批40条）
    val clickStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "8")      // 整数，保证每5秒40条
      .option("numPartitions", "1")
      .load()
      .withColumn("event_time", $"timestamp".cast("timestamp"))
      .withColumn("movie_id",
        when(($"value" % 8).between(0, 3), 101)
          .when(($"value" % 8).between(4, 5), 102)
          .when(($"value" % 8).between(6, 7), 105)
      )
      .select($"movie_id", $"event_time")

    // 2. 窗口聚合（10秒窗口，5秒滑动），得到当前窗口的点击量（固定值）
    val windowedAgg = clickStream
      .withWatermark("event_time", "10 seconds")
      .groupBy(
        window($"event_time", "10 seconds", "5 seconds"),
        $"movie_id"
      )
      .agg(count("*").alias("click_count"))
      .select(
        $"movie_id",
        $"click_count",
        date_format($"window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format($"window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end")
      )

    // 3. 同时计算本次微批新增的点击量（用于累计）
    val incrementalAgg = clickStream
      .groupBy($"movie_id")
      .agg(count("*").alias("increment"))
      .select($"movie_id", $"increment")

    // MySQL 连接信息
    val mysqlUrl = "jdbc:mysql://hadoop01:3306/movie_db?useSSL=false"
    val mysqlUser = "root"
    val mysqlPassword = "Root123456!"   // 请替换

    // 4. foreachBatch 统一处理：更新两个表
    def processBatch(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
      // 4.1 计算窗口聚合结果（最新窗口的固定值）
      val windowResult = windowedAgg  // 注意：这里需要使用同一个 batchDF 计算，但由于是流式，每次微批会重新计算
      // 但由于我们在 foreachBatch 外部定义的 windowedAgg 是未执行的流式DataFrame，不能直接引用。
      // 正确做法：在 foreachBatch 内部基于 batchDF 计算窗口聚合和增量
      // 但为了清晰，我们重新从 batchDF 计算，但 batchDF 只包含原始数据。
      // 更优雅：在外部定义好流式查询，但这里为了便于理解，我们在内部重新计算。
      // 但 windowedAgg 和 incrementalAgg 是流式DataFrame，不能直接用于 foreachBatch。
      // 改为在 foreachBatch 中手动计算：
      import spark.implicits._
      val batchDFWithTime = batchDF.withColumn("event_time", $"event_time".cast("timestamp"))
      // 窗口聚合
      val windowResultBatch = batchDFWithTime
        .groupBy(window($"event_time", "10 seconds", "5 seconds"), $"movie_id")
        .agg(count("*").alias("click_count"))
        .select(
          $"movie_id",
          $"click_count",
          date_format($"window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
          date_format($"window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end")
        )
      // 增量聚合
      val incrementalBatch = batchDFWithTime
        .groupBy($"movie_id")
        .agg(count("*").alias("increment"))

      // 写入数据库
      var conn: Connection = null
      try {
        Class.forName("com.mysql.jdbc.Driver")
        conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword)

        // ---- 更新 hot_movie_rank（窗口表） ----
        // 清空旧数据，插入最新窗口聚合
        val stmt1 = conn.createStatement()
        stmt1.execute("TRUNCATE TABLE hot_movie_rank")
        stmt1.close()

        // 插入窗口结果
        val insertRankSql = "INSERT INTO hot_movie_rank (movie_id, click_count, window_start, window_end) VALUES (?, ?, ?, ?)"
        val psRank = conn.prepareStatement(insertRankSql)
        windowResultBatch.collect().foreach { row =>
          psRank.setInt(1, row.getInt(0))
          psRank.setLong(2, row.getLong(1))
          psRank.setString(3, row.getString(2))
          psRank.setString(4, row.getString(3))
          psRank.addBatch()
        }
        psRank.executeBatch()
        psRank.close()

        // ---- 更新 hot_movie_total（累计表） ----
        // 使用 ON DUPLICATE KEY UPDATE 累加
        val upsertTotalSql = "INSERT INTO hot_movie_total (movie_id, total_click) VALUES (?, ?) ON DUPLICATE KEY UPDATE total_click = total_click + VALUES(total_click)"
        val psTotal = conn.prepareStatement(upsertTotalSql)
        incrementalBatch.collect().foreach { row =>
          psTotal.setInt(1, row.getInt(0))
          psTotal.setLong(2, row.getLong(1))
          psTotal.addBatch()
        }
        psTotal.executeBatch()
        psTotal.close()

        println(s"===== 批次 $batchId 更新完成，时间：${new java.util.Date()} =====")
      } catch {
        case e: Exception =>
          println("写入失败：")
          e.printStackTrace()
      } finally {
        if (conn != null && !conn.isClosed) conn.close()
      }
    }

    // 5. 启动流式查询（使用原始流）
    val query = clickStream.writeStream
      .foreachBatch(processBatch _)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")   // 因为是原始数据，用 append
      .option("checkpointLocation", "file:///tmp/spark-checkpoint-movie-dual")
      .start()

    query.awaitTermination()
  }
}