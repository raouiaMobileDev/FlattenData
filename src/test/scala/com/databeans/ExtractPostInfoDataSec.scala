package com.databeans

import com.databeans.ExtractPostInfoData.extractPostInfoData
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class ResultPostData(profile_id:String, post_id:String, typename:String, comments_disabled:Boolean, edge_media_preview_like:Long, edge_media_to_comment: Long, is_video: Boolean, taken_at_timestamp:Long, username:String)

class ExtractPostDataSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("wordCountDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "ExtractCoutinhoData" should "Extract coutinho data within the DataFrame" in {
    Given("The input nasted dataframe")
    val inputData = spark.read.option("multiLine", true).json("phil.coutinho-1.json")
    When("ExtractPostData is invoked")
    val resultCommentData = extractPostInfoData(spark,inputData)
    val selectRows=resultCommentData.filter(resultCommentData("post_id") === "2556864304565671217" || resultCommentData("post_id") === "2541687857152679185")
    Then("The dataframe should be returned")
    val expectedResultPostData = Seq(
      ResultPostData("1382894360", "2556864304565671217", "GraphImage", false, 483475, 80, false, 1619021998, "phil.coutinho"),
      ResultPostData("1382894360", "2541687857152679185", "GraphImage", false, 654176, 50, false, 1617212825, "phil.coutinho")
    ).toDF()
    expectedResultPostData.collect() should contain theSameElementsAs (selectRows.collect())
  }
}
