package co.ogury.segmentation

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object SegmentationJob {

  def main(args: Array[String]): Unit = {

    val dataPath = args(0)
    val outputFile = args(1)
    val endDate = args(2)
    val segmentationPeriod = args(3)

    val session = createSparkSession()

    run(session, dataPath, outputFile, LocalDate.parse(endDate), segmentationPeriod.toInt)

    session.stop()
  }

  def run(session: SparkSession, dataPath: String, outputFile: String, endDate: LocalDate, segmentationPeriod: Int): Unit = {
    // load
    val transactions = loadTransactions(session, dataPath)
    val customers = loadCustomers(session, dataPath)

    // compute
    val segmentation = computeSegmentation(
      customers,
      transactions,
      startDate=java.sql.Date.valueOf(endDate.minusDays(segmentationPeriod)),
      endDate=java.sql.Date.valueOf(endDate)
    )

    // save
    saveSegmentation(segmentation, outputFile)
  }

  private def loadTransactions(session: SparkSession, dataPath: String): Dataset[Transaction] = {
    import session.implicits._

    // define fields for schema
    val fields = Array(
      StructField("productId", DataTypes.StringType),
      StructField("customerId", DataTypes.StringType),
      StructField("date", DataTypes.StringType),
      StructField("price", DataTypes.IntegerType),
      StructField("quantity", DataTypes.IntegerType),
      StructField("id", DataTypes.StringType)
    )

    // load csv
    val df = session.read.schema(StructType(fields)).option("header", "true").csv(
      dataPath + "/transactions/transactions.csv"
    )

    // fields mapping
    val transactions: Dataset[Transaction] = df.map {
      row => Transaction(
        productId=row.getAs[String](0),
        customerId=row.getAs[String](1),
        date=row.getAs[String](2),
        price=row.getAs[Int](3),
        quantity=row.getAs[Int](4),
        id=row.getAs[String](5)
      )
    }
    transactions
  }

  private def loadCustomers(session: SparkSession, dataPath: String): Dataset[String] = {
    import session.implicits._

    val fields = Array(
      StructField("customerId", DataTypes.StringType)
    )

    // load csv
    val customersDS = session.read.schema(StructType(fields)).option("header", "true").csv(
      dataPath + "/customers/customerIds.csv"
    ).toDF(
      "customerId"
    ).as[String]

    customersDS.show()
    customersDS
  }

  def computeSegmentation(customers: Dataset[String], transactions: Dataset[Transaction], startDate: Date, endDate: Date): DataFrame = {
    // transform LocalDate to string for string comparison
    val startDateStr = startDate.toString
    val endDateStr = endDate.toString

    // filter transactions earlier or equal than end date
    val period_transactions = transactions.filter(
      col("date") <= endDateStr
    )

    // right join with customers to get only specified customers' transactions
    val customersTransactions = period_transactions.joinWith(
      customers,
      transactions("customerId") === customers("customerId"),
      "right"
    ).select(
      col("_1.id").as("id"),
      col("_1.customerId").as("matched_customerId"),
      col("_1.productId").as("productId"),
      col("_1.date").as("date"),
      col("_2").as("customerId")
    )

    // compute customers' activity
    var segmentation = customersTransactions.groupBy(
      "customerId"
    ).agg(
      min(col("date")).as("first_transaction"),
      max(col("date")).as("last_transaction")
    ).withColumn(
      "activity",
      when(col("last_transaction").isNull, lit(ActivitySegment.UNDEFINED.toString)
      ).otherwise(
        when(col("last_transaction") < startDateStr, lit(ActivitySegment.INACTIVE.toString)
        ).otherwise(
          when(col("first_transaction") < startDateStr, lit(ActivitySegment.ACTIVE.toString)
          ).otherwise(lit(ActivitySegment.NEW.toString))
        )
      )
    )

    // keep relevant columns
    segmentation = segmentation.select(
      "customerId",
      "activity"
    )
    segmentation
  }

  private def saveSegmentation(segmentation: DataFrame, outputFile: String): Unit = {
    segmentation.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outputFile)
  }

  private def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder.getOrCreate()
    spark
  }

}
