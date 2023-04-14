import com.databeans.ExtractProfileInfoData.extractProfileInfoData
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class ResultProfileInfoData(id:String, followers_count:Long, following_count: Long, full_name:String, is_business_account:Boolean, is_private:Boolean, posts_count:Long, username:String)

class ExtractProfileInfoDataSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("wordCountDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "ExtractCoutinhoData" should "Extract coutinho data within the DataFrame" in {
    Given("The input nasted dataframe")
    val inputData = spark.read.option("multiLine", true).json("phil.coutinho-1.json")
    When("ExtractProfileInfoData is invoked")
    val resultCommentData = extractProfileInfoData(spark,inputData)
   val selectRows=resultCommentData.filter(resultCommentData("id") === "1382894360" )
    Then("The dataframe should be returned")
    val expectedResultCommentData = Seq(
      ResultProfileInfoData("1382894360", 23156762, 1092, "Philippe Coutinho", false, false, 618,"phil.coutinho"),
    ).toDF()
    expectedResultCommentData.collect() should contain theSameElementsAs (selectRows.collect())
  }
}
