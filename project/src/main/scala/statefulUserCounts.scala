import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder()
  .appName("StatefulUserCounts")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

val input = spark.readStream
  .format("rate")
  .option("rowsPerSecond", 5)
  .load()
  .selectExpr("CAST(value AS STRING) as user_id")

case class UserState(runningCount: Long, status: String)

def updateUserCount(userId: String, values: Iterator[String], state: GroupState[UserState]): UserState = {
  val previousCount = if (state.exists) state.get.runningCount else 0L
  val increment = values.size
  val newCount = previousCount + increment
  val newState = UserState(newCount, "active")
  state.update(newState)
  newState
}

val statefulCounts = input
  .groupByKey(_.getString(0))
  .mapGroupsWithState[UserState, UserState](GroupStateTimeout.NoTimeout())(updateUserCount)


val query = statefulCounts.writeStream
  .format("console")
  .outputMode("update")     // only print updated rows
  .option("truncate", false) // don't truncate columns
  .option("numRows", 20)    // only show 20 rows
  .start()

query.awaitTermination()
