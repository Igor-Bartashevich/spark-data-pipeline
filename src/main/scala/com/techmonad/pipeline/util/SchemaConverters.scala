package com.techmonad.pipeline.util

import com.techmonad.pipeline.reader.{CSVSchema, SchemaField}
import org.apache.spark.sql.types._


object SchemaConverters {


  implicit def convert(csvSchema: CSVSchema) =
    StructType(csvSchema.fields.map {
      case SchemaField(name, dataType, nullable) => StructField(name = name, dataType = typeConverter(dataType), nullable = nullable)
    })

  private def typeConverter(dataType: String): DataType = {
    dataType match {
      case "IntegerType" => IntegerType
      case "StringType" => StringType
      case "DoubleType" => DoubleType
      case "LongType" => LongType
      case "FloatType" => FloatType
      case "BooleanType" => BooleanType
      case unknownType => throw new IllegalArgumentException(s"Invalid type [$unknownType]")
    }
  }
}
