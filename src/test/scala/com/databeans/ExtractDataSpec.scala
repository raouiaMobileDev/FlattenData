package com.databeans

import com.databeans.ExtractCommentData.extractCommentData
import com.databeans.ExtractProfileInfoData.extractProfileInfoData
import com.databeans.ExtractPostInfoData.extractPostInfoData
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

//case class Data(a_word:String, count:Int)
case class DimensionsData(height:Int, width:Int)
case class InputPostInfoData(dimensions:DimensionsData, display_url:String, gating_info:String,id:String,is_video:Boolean, media_preview:String, taken_at_timestamp:Long, tags:Array[String] )
case class InputProfileInfoData(id:String, profile_pic_url:String, username:String)
case class InputCommentData(created_at:Long, id:String, text:String, owner:InputProfileInfoData)

case class ResultPostInfoData(height:Int, width:Int, display_url:String, gating_info:String,id:String,is_video:Boolean, media_preview:String, taken_at_timestamp:Long, tags:Array[String] )
case class ResultProfileInfoData(id:String, profile_pic_url:String, username:String)
case class ResultCommentData(created_at:Long, id:String, text:String, id_profile:String, profile_pic_url:String, username:String)

class ExtractDataSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("wordCountDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "ExtractCoutinhoData" should "Extract coutinho data within the DataFrame" in {
    Given("The input nasted dataframe")
    val inputPostInfoData = Seq(
      InputPostInfoData(DimensionsData(720, 1080), "025486428978_n.jpg", null, "2556864304565671217", false, "ACocvTf68f7v9RWXrCybgST5eBgZ4z9PWtSX", 1619021998, Array("xxxx", "xxx")),
      InputPostInfoData(DimensionsData(722, 1000), "025486428978_n.jpg", null, "2556864304565671217", false, "ACocvTf68f7v9RWXrCybgST5eBgZ4z9PWtSX", 1619021998, Array("xxxx", "xxx")),
      InputPostInfoData(DimensionsData(700, 1050), "025486428978_n.jpg", null, "2556864304565671217", false, "ACocvTf68f7v9RWXrCybgST5eBgZ4z9PWtSX", 1619021998, Array("xxxx", "xxx"))
    )
    val inputProfileInfoData = Seq(
      InputProfileInfoData("20740995", "instagram.net", "Ali"),
      InputProfileInfoData("20740996", "instagram.net", "Mohamed"),
      InputProfileInfoData("20740997", "instagram.net", "Sami")
    )
    val inputCommentData = Seq(
      InputCommentData(1619023963, "18209883163069294", "💪🏼💪🏼", InputProfileInfoData("20740995", "instagram.net", "Ali")),
      InputCommentData(1619023964, "18209883163069294", "💪🏼💪🏼", InputProfileInfoData("20740996", "instagram.net", "Mohamed")),
      InputCommentData(1619023965, "18209883163069294", "💪🏼💪🏼", InputProfileInfoData("20740997", "instagram.net", "Sami"))
    )
    When("Extraction postInfo, profileInfo and commentData is invoked")
    val resultPostInfoData = extractPostInfoData(inputPostInfoData.toDF())
    val resultProfileInfoData = extractProfileInfoData(inputProfileInfoData.toDF())
    val resultCommentData = extractCommentData(inputCommentData.toDF())
    Then("the dataframe should be returned")
    val expectedResultPostInfoData = Seq(
      ResultPostInfoData(720, 1080, "025486428978_n.jpg", null, "2556864304565671217", false, "ACocvTf68f7v9RWXrCybgST5eBgZ4z9PWtSX", 1619021998, Array("xxxx", "xxx")),
      ResultPostInfoData(722, 1000, "025486428978_n.jpg", null, "2556864304565671217", false, "ACocvTf68f7v9RWXrCybgST5eBgZ4z9PWtSX", 1619021998, Array("xxxx", "xxx")),
      ResultPostInfoData(700, 1050, "025486428978_n.jpg", null, "2556864304565671217", false, "ACocvTf68f7v9RWXrCybgST5eBgZ4z9PWtSX", 1619021998, Array("xxxx", "xxx"))
    )
    val expectedResultCommentData = Seq(
      ResultCommentData(1619023963, "18209883163069294", "💪🏼💪🏼", "20740995", "instagram.net", "Ali"),
      ResultCommentData(1619023964, "18209883163069294", "💪🏼💪🏼", "20740996", "instagram.net", "Mohamed"),
      ResultCommentData(1619023965, "18209883163069294", "💪🏼💪🏼", "20740997", "instagram.net", "Sami")
    )
    val expectedResultProfileInfoData = Seq(
      InputProfileInfoData("20740995", "instagram.net", "Ali"),
      InputProfileInfoData("20740996", "instagram.net", "Mohamed"),
      InputProfileInfoData("20740997", "instagram.net", "Sami")
    )
    expectedResultPostInfoData.toDF().collect() should contain theSameElementsAs (resultPostInfoData.collect())
    expectedResultCommentData.toDF().collect() should contain theSameElementsAs (resultCommentData.collect())
    expectedResultProfileInfoData.toDF().collect() should contain theSameElementsAs (resultProfileInfoData.collect())

  }
}
