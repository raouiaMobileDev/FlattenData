package com.databeans

import com.databeans.ExtractCommentData.extractCommentData
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class ResultCommentData(profile_id:String, post_id:String, created_at:Long, id:String, commenter_id:String, username:String, text:String)

class ExtractCommentDataSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("wordCountDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "ExtractCoutinhoData" should "Extract coutinho data within the DataFrame" in {
    Given("The input nasted dataframe")
    val inputData = spark.read.option("multiLine", true).json("phil.coutinho-1.json")
    When("ExtractCommentData is invoked")
    val resultCommentData = extractCommentData(spark,inputData)
    val selectRows=resultCommentData.filter(resultCommentData("id") === "18209883163069294" || resultCommentData("id") === "18114517408211027")
    Then("The dataframe should be returned")
    val expectedResultCommentData = Seq(
      ResultCommentData("1382894360", "2556864304565671217", 1619023963, "18209883163069294", "20740995", "sergiroberto", "💪🏼💪🏼"),
      ResultCommentData("1382894360", "2556864304565671217", 1619023981, "18114517408211027", "268668518", "juliana_gilaberte", "🙏🏻 Deus não erra, não falha, Ele sabe de todas as coisas! 🙌🏻 Deus está no comando da sua vida e logo vc estará de volta aos campos com força total 🦵🏻 ⚽️ 🥅")
    ).toDF()
    expectedResultCommentData.collect() should contain theSameElementsAs (selectRows.collect())
  }
}
