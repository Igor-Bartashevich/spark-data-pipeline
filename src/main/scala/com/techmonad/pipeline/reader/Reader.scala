package com.techmonad.pipeline.reader

import com.techmonad.pipeline.logging.Logging
import com.techmonad.pipeline.util.{JsonHelper, SchemaConverters}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Reader extends JsonHelper with Logging {

  import SchemaConverters._

  def readCSV(etlMetadata: ETLMetadata)(implicit spark: SparkSession): DataFrame = {
    println(etlMetadata)
    import etlMetadata._
    spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", source.delimiter)
      .schema(source.schema)
      .load(source.path)

  }


}

case class ETLMetadata(source: SourceInfo)

case class SourceInfo(path: String, schema: CSVSchema, delimiter: String)

case class CSVSchema(fields: List[SchemaField])

case class SchemaField(name: String, dataType: String, nullable: Boolean)


