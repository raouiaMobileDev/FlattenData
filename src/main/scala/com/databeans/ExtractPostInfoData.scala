package com.databeans

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ExtractPostInfoData {
  def extractPostInfoData(postInfoData: DataFrame): DataFrame = {
    var flattenPostInfoData = postInfoData.select(col("dimensions.*"),col("display_url"),col("gating_info"), col("id"),col("is_video"),col("media_preview"),col("taken_at_timestamp"),col("tags"))
    flattenPostInfoData
  }
}
