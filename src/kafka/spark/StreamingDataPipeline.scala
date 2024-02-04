package kafka.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, explode, from_json, lit}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StringType, StructType}

object StreamingDataPipeline {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("Kafka Stream Processor")
      .master("local[3]")
      .getOrCreate()

    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart-events")
      //.option("startingOffsets", "earliest") // From starting
      .load()


    val personStringDF = kafkaStreamDF.selectExpr("CAST(value AS STRING)")

    logger.info("jhlj ======================================+>>>>>>>>>>>> " + personStringDF)

    val schema1 = new StructType()
      .add("results", StringType)

    val schema2 = new StructType()
      .add("id",StringType)
      .add("ville",StringType)
      .add("dep_name",StringType)
      .add("reg_name",StringType)
      .add("com_name",StringType)
      .add("prix_nom",StringType)
      .add("prix_maj",StringType)
      .add("prix_valeur",FloatType)



    val personDF = personStringDF.select(from_json(col("value"), schema1).as("d"))
      .select("d.*")

    val df2 = personDF.selectExpr("""explode(split(regexp_replace(regexp_replace(results,'(\\\},)','}},'),'(\\\[|\\\])',''),"},")) as results""")

    val df = df2.select(from_json(lit(col("results")), schema2).as("data"))
      .select("data.*")

    val dfAvgPerType = df
      .groupBy("prix_nom", "dep_name")
      .avg("prix_valeur").as("prix_moy")

    dfAvgPerType.printSchema()

    dfAvgPerType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

}
