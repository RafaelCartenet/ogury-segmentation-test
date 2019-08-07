package co.ogury.segmentation

import java.sql.Date
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.UUID

import co.ogury.segmentation.ActivitySegment.ActivitySegment
import co.ogury.test.BaseSpec
import org.apache.spark.sql.Row

class SegmentationJobSpec extends BaseSpec {

  private val Id = "customerId"
  private val defaultCustomerId = "customerId"
  private val otherCustomerId = "otherCustomerId"
  private val otherTransactions = Seq(
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 1)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 5)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 6)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 10)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 15)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 16)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 20))
  )

  private val startDate = Date.valueOf(LocalDate.of(2018, 1, 6))
  private val endDate = Date.valueOf(LocalDate.of(2018, 1, 15))

  "ActivitySegmentation" should "be 'undefined' for customers who have never bought" in {
    withSparkSession { session =>
      Given("a customer without transactions")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = otherTransactions.toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("checking segmentation")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.UNDEFINED)
    }
  }

  it should "be 'undefined' for customers who have first purchase after P" in {
    withSparkSession { session =>
      Given("a customer with transactions only after period")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 23))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("checking segmentation")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.UNDEFINED)
    }
  }

  it should "be 'new' if the first purchase is during P" in {
    withSparkSession { session =>
      Given("a customer with one transaction during period")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 10))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("checking segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.NEW)
    }
  }

  it should "be 'new' if the first purchase is during P with multiple purchases" in {
    withSparkSession { session =>
      Given("a customer with multiple transactions during period")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 10)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 2, 15))
        )
          ++ otherTransactions
      ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("checking segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.NEW)
    }
  }

  it should "be 'new' if the first purchase is during P and with multiple purchases during and after P" in {
    withSparkSession { session =>
      Given("a customer whose first purchase is during P with multiple others during and after P")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 10)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 13)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 2, 8)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 2, 14))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("checking segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.NEW)
    }
  }

  it should "be 'active' if there is a first-purchase is before P and a non-first purchase during P" in {
    withSparkSession { session =>
      Given("a customer whose first purchase is before P and a non-first purchase during P")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 3)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 13))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("checking segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.ACTIVE)
    }
  }

  it should "be 'Inactive' if the first purchase is before P but no purchase during P" in {
    withSparkSession { session =>
      Given("a customer whose first purchase is before P but no purchase during P")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 3))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("checking segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.INACTIVE)
    }
  }

  it should "be 'Inactive' if the first purchase is before P and purchases after but no purchase during P" in {
    withSparkSession { session =>
      Given("a customer whose first purchase is before P and purchases after P but no purchase during P")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 3)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 15))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("checking segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.INACTIVE)
    }
  }

  private def genTransaction(customerId: String, date: LocalDate): Transaction =
    Transaction(
      UUID.randomUUID().toString,
      customerId,
      "2",
      date.atStartOfDay(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
      10,
      1
    )

  private def defaultCustomerShouldBeInSegment(segmentations: Seq[Row], segment: ActivitySegment): Unit = {
    Then("the segmentation processed 2 customers")
    segmentations should not be empty
    segmentations.length shouldBe 2
    And(s"the customer is classified as $segment'")
    segmentations.find(_.getString(0) == defaultCustomerId) match {
      case None      => fail("customer have not been found")
      case Some(row) => row.getString(1) shouldBe segment.toString
    }
  }




}
