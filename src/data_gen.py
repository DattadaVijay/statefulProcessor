from pyspark import pipelines as dp

@dp.table(
    name="events_bronze",
    comment="Raw events from simulated rate stream",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "quality": "bronze"
    }
)
def events_bronze():
    return (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .withColumn("user_id", col("value") % 3)
    )