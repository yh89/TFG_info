package com.company.project.pipeline

import org.apache.spark.sql.{DataFrame, functions => func}
import com.company.project._
import org.apache.spark.sql.types._, func._
import scala.util.Try

object Transformations {

  def query1(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(3).filter($"Entity" ==="All natural disasters")
    }.toEither(e => TransformationError(e.getMessage))

  def query2(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(1).filter($"Entity"==="All natural disasters")
    }.toEither(e => TransformationError(e.getMessage))

  def query3(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      input(3).join(input(1), Seq("Entity","Year"))
    }.toEither(e => TransformationError(e.getMessage))

  def query4(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      val temperatureByCountryYear = input(0).withColumn("dt", date_format(col("dt"), "yyyy").cast("Int"))
      temperatureByCountryYear
        .groupBy($"dt")
        .avg("AverageTemperature")
        .orderBy($"dt")
    }.toEither(e => TransformationError(e.getMessage))

  def query5(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      val temperatureByCountryYear = input(0).withColumn("dt", date_format(col("dt"), "yyyy").cast("Int"))
      temperatureByCountryYear
        .groupBy($"dt",$"Country")
        .avg("AverageTemperature")
        .orderBy($"dt", $"Country")
    }.toEither(e => TransformationError(e.getMessage))

  def query6(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      val temperatureByCountryYear = input(0).withColumn("dt", date_format(col("dt"), "yyyy").cast("Int"))
      val temperatureAVGTotal = temperatureByCountryYear
        .groupBy($"dt")
        .avg("AverageTemperature")
        .orderBy($"dt")
      val aux1 = input(3)
        .filter($"Entity" ==="All natural disasters")

      aux1.join(
        temperatureAVGTotal,
        aux1("Year")===temperatureAVGTotal("dt"))
    }.toEither(e => TransformationError(e.getMessage))

  def query7(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(2)
        .groupBy($"Year")
        .sum("Deaths")
        .orderBy($"Year")
    }.toEither(e => TransformationError(e.getMessage))


  def query8(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(5)
        .groupBy($"Year")
        .avg("Deaths(Percent) (%)")
        .orderBy($"Year")
    }.toEither(e => TransformationError(e.getMessage))

  def query9(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(2)
        .groupBy($"Entity")
        .sum("Deaths")
        .orderBy($"sum(Deaths)".desc)
    }.toEither(e => TransformationError(e.getMessage))

  def query10(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(3)
        .join(input(4), Seq("Entity","Year"))
        .filter($"Entity"==="All natural disasters")
    }.toEither(e => TransformationError(e.getMessage))

  def query11(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(3)
        .filter($"Entity" =!="All natural disasters")
        .groupBy($"Year", $"Entity")
        .sum("Number")
        .orderBy($"Year".desc)
    }.toEither(e => TransformationError(e.getMessage))

  def query12(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      val disasterNumberDeath = input(3)
        .join(input(4), Seq("Entity","Year"))
        .filter($"Entity"==="All natural disasters")
      disasterNumberDeath
        .groupBy($"Entity")
        .sum("Deaths")
        .filter($"Entity" =!= "All natural disasters")
        .orderBy($"sum(Deaths)".desc)
    }.toEither(e => TransformationError(e.getMessage))

  def query13(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(1)
        .groupBy($"Entity")
        .sum("Money")
        .filter($"Entity" =!= "All natural disasters")
        .orderBy($"sum(Money)".desc)
    }.toEither(e => TransformationError(e.getMessage))

  def query14(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(6).filter($"Year">1700)
    }.toEither(e => TransformationError(e.getMessage))


  def query15(input: List[DataFrame]): Either[PipelineError, DataFrame] =
    Try{
      val dataFrame = input(0)
      import dataFrame.sparkSession.implicits._
      input(7).filter($"Year">1700)
    }.toEither(e => TransformationError(e.getMessage))
}
