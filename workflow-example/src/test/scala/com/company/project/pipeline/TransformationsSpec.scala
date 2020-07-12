package com.company.project.pipeline

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.DataFrame
import Model._
class TransformationsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("Transformation `query 1`") {

    it("should work") {
      val matchesDF = List(
        Disaster("All natural disasters", 2000, 50000),
        Disaster("All natural disasters", 2001, 40000),
        Disaster("Flood", 2000, 20000),
        Disaster("Hurracan", 2000, 20000),
        Disaster("fire", 2000, 10000),
        Disaster("Flood", 2001, 20000),
        Disaster("Starve", 2001, 20000)
      ).toDF
      val expectedDF = List(
        Disaster("All natural disasters", 2000, 50000),
        Disaster("All natural disasters", 2001, 40000)
      ).toDF
      Transformations.query1(matchesDF).fold(
        fail(_),
        (obtainedDF: DataFrame) =>
          assertDataFrameEquals(
            expectedDF.sort($"Year"),
            obtainedDF.sort($"Year"))
      )
    }
  }

}
