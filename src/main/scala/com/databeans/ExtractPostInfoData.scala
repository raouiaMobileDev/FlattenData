package com.databeans

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

object ExtractPostInfoData {
  def extractPostInfoData(spark:SparkSession, inputData: DataFrame): DataFrame = {
    import spark.implicits._
    val extractedPostInfoData=inputData.select($"GraphProfileInfo.info.id",explode($"GraphImages").as("GraphImages"))
      .select(col("id").as("profile_id"),
        col("GraphImages.id").as("post_id"),
        col("GraphImages.__typename").as("typename"),
        col("GraphImages.comments_disabled"),
        col("GraphImages.edge_media_preview_like.count").as("edge_media_preview_like"),
        col("GraphImages.edge_media_to_comment.count").as("edge_media_to_comment"),
        col("GraphImages.is_video"),
        col("GraphImages.taken_at_timestamp"),
        col("GraphImages.username"))

        extractedPostInfoData
  }
}
