package com.techmonad.pipeline.reader


import com.techmonad.pipeline.SparkSupport
import com.techmonad.pipeline.util.JsonHelper
import org.apache.spark.sql.types.{StructType, _}
import org.scalatest.{Matchers, WordSpec}


class ReaderSpec extends WordSpec with Matchers with SparkSupport with JsonHelper{

  val structType = StructType(List(
      StructField(name = "id", dataType = IntegerType, nullable = false),
      StructField(name = "name", dataType = StringType, nullable = false),
      StructField(name = "age", dataType = IntegerType, nullable = false),
      StructField(name = "salary", dataType = DoubleType, nullable = false)))
val schema = """[{"name":"id","dataType":{},"nullable":false,"metadata":{"map":{}}},{"name":"name","dataType":{},"nullable":false,"metadata":{"map":{}}},{"name":"age","dataType":{},"nullable":false,"metadata":{"map":{}}},{"name":"salary","dataType":{},"nullable":false,"metadata":{"map":{}}}]"""

  val etlMetadataJson: String = """{"source":{"path":"src/test/resources/csv/emp.csv","schema":{}, "delimiter":','  } }"""
  val reader = new Reader()

  "Reader " should {
    "read csv " in withSparkSession { implicit spark =>
      println(write(structType))
      /*      val csvData: Array[Row] = reader.readCSV("""src/test/resources/csv/emp.csv""", ',').collect()
            csvData.length shouldBe 3
            csvData(0).get(0).toString.toInt shouldBe 1
            csvData(0).get(1).toString shouldBe "jon"*/

    }

  }
}
