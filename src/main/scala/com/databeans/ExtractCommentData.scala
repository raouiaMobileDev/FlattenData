package com.databeans

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

object ExtractCommentData  {
  def extractCommentData(spark:SparkSession, inputData: DataFrame): DataFrame = {
    import spark.implicits._
    val extractedData=inputData.select($"GraphProfileInfo.info.id",explode($"GraphImages").as("GraphImages"))
      .select(col("id").as("profile_id"),
              col("GraphImages.id").as("post_id"),
              col("GraphImages.comments.data"))
    val extractedCommentData=extractedData.select($"profile_id",$"post_id", explode($"data").as("data"))
      .select(col("profile_id"),
              col("post_id"),
              col("data.created_at"),
              col("data.id"),
              col("data.owner.id").as("commenter_id"),
              col("data.owner.username"),
              col("data.text"))
    extractedCommentData
  }
}
