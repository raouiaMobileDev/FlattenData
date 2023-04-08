package com.databeans

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, split}

object ExtractCommentData {

  def extractCommentData(commentData: DataFrame): DataFrame = {
    val flattenCommentData = commentData.select(col("created_at"),col("id"),col("text"),col("owner.id").as("id_profile"), col("owner.profile_pic_url"),col("owner.username"))
    flattenCommentData
  }

}
