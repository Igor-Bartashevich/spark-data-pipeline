package com.techmonad.pipeline.reader


import com.techmonad.pipeline.SparkSupport
import com.techmonad.pipeline.util.JsonHelper
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}


class ReaderSpec extends WordSpec with Matchers with SparkSupport with JsonHelper {

  val schema =
    """{"fields":[{"name":"id","dataType":"IntegerType","nullable":false},
      |{"name":"name","dataType":"StringType","nullable":false},
      |{"name":"age","dataType":"IntegerType","nullable":false},
      |{"name":"salary","dataType":"DoubleType","nullable":false}]}""".stripMargin
  val csvPath = "src/test/resources/csv/emp.csv"
  val etlMetadataJson: String = s"""{"source":{"path":"$csvPath","schema": $schema, "delimiter":","  } }"""
  val etlMetadata = parse(etlMetadataJson).extract[ETLMetadata]
  val reader = new Reader()

  "Reader " should {
    "read csv " in withSparkSession { implicit spark =>
      val csvData: Array[Row] = reader.readCSV(etlMetadata).collect()
      csvData.length shouldBe 3
      csvData(0).get(0).toString.toInt shouldBe 1
      csvData(0).get(1).toString shouldBe "jon"

    }

  }
}
