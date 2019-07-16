package co.ogury.segmentation

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

object SegmentationJob {

  def main(args: Array[String]): Unit = {

    val dataPath = args(0)
    val outputFile = args(1)
    val endDate = args(2)
    val segmentationPeriod = args(3)

    val session = createSparkSession()

    run(session, dataPath, outputFile, LocalDate.parse(endDate), segmentationPeriod.toInt)

    val idUdf = udf { Random.alphanumeric.take(7).map(_.toLower).mkString }

    session.stop()
  }

  def run(session: SparkSession, dataPath: String, outputFile: String, endDate: LocalDate, segmentationPeriod: Int): Unit = {
    // TODO
    // load
    // compute
    // save
    ???
  }

  private def loadTransactions(session: SparkSession, dataPath: String): Dataset[Transaction] = ???

  private def loadCustomers(session: SparkSession, dataPath: String): Dataset[String] = ???

  def computeSegmentation(customers: Dataset[String], transactions: Dataset[Transaction], startDate: Date, endDate: Date): DataFrame = ???

  private def saveSegmentation(segmentation: DataFrame, outputFile: String): Unit = ???

  private def createSparkSession(): SparkSession = ???

}
