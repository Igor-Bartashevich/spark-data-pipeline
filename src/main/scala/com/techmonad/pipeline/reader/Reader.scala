package com.techmonad.pipeline.reader

import com.techmonad.pipeline.logging.Logging
import com.techmonad.pipeline.util.JsonHelper
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


class Reader extends JsonHelper with Logging {

  def readCSV(etlMetadata: ETLMetadata)(implicit spark: SparkSession): DataFrame = {
    import etlMetadata._
    spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", s"${source.delimiter}")
      .schema(source.schema)
      .load(source.path)

  }


}

case class ETLMetadata(source: SourceInfo)

case class SourceInfo(path: String, schema: StructType, delimiter: Char)


