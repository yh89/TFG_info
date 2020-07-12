package com.company.project.pipeline

import org.apache.spark.sql.{DataFrame, functions => func}
import com.company.project._
import org.apache.spark.sql.types._, func._
import scala.util.Try

object Transformations {

  def query1(disasterNumber: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import disasterNumber.sparkSession.implicits._
      disasterNumber.filter($"Entity" === "All natural disasters")
    }.toEither(e => TransformationError(e.getMessage))

  def query2(disasterEconomic: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import disasterEconomic.sparkSession.implicits._
      disasterEconomic.filter($"Entity"==="All natural disasters")
    }.toEither(e => TransformationError(e.getMessage))

  def query3(disasterEconomic: DataFrame, disasterNumber: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      disasterNumber.join(disasterEconomic, Seq("Entity","Year"))
    }.toEither(e => TransformationError(e.getMessage))

  def query4(temperatureByCountry: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import temperatureByCountry.sparkSession.implicits._

      val temperatureByCountryYear = temperatureByCountry
          .withColumn("dt", date_format(col("dt"), "yyyy").cast("Int"))

      val aux =temperatureByCountryYear
          .groupBy($"dt")
          .avg("AverageTemperature")
          .orderBy($"dt")
      aux.withColumnRenamed("avg(AverageTemperature)", "average" )
    }.toEither(e => TransformationError(e.getMessage))

  def query5(temperatureByCountry: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import temperatureByCountry.sparkSession.implicits._
      val temperatureByCountryYear = temperatureByCountry.withColumn("dt", date_format(col("dt"), "yyyy").cast("Int"))
      val aux =temperatureByCountryYear
        .groupBy($"dt",$"Country")
        .avg("AverageTemperature")
        .orderBy($"dt", $"Country")
      aux.withColumnRenamed("avg(AverageTemperature)", "average" )
    }.toEither(e => TransformationError(e.getMessage))

  def query6(temperatureByCountry: DataFrame, disasterNumber: DataFrame ): Either[PipelineError, DataFrame] =
    Try{
      import temperatureByCountry.sparkSession.implicits._
      val temperatureByCountryYear = temperatureByCountry.withColumn("dt", date_format(col("dt"), "yyyy").cast("Int"))
      val temperatureAVGTotal = temperatureByCountryYear
        .groupBy($"dt")
        .avg("AverageTemperature")
        .orderBy($"dt")
        .withColumnRenamed("avg(AverageTemperature)", "average" )
      val aux1 = disasterNumber
        .filter($"Entity" ==="All natural disasters")

      aux1.join(
        temperatureAVGTotal,
        aux1("Year")===temperatureAVGTotal("dt"))
    }.toEither(e => TransformationError(e.getMessage))

  def query7(deathByCountry: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import deathByCountry.sparkSession.implicits._
      deathByCountry
        .groupBy($"Year")
        .sum("Deaths")
        .orderBy($"Year")
        .withColumnRenamed("sum(Deaths)", "total_death" )
    }.toEither(e => TransformationError(e.getMessage))


  def query8(deathPercentByCountry: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import deathPercentByCountry.sparkSession.implicits._
      deathPercentByCountry
        .groupBy($"Year")
        .avg("Deaths(Percent) (%)")
        .orderBy($"Year")
        .withColumnRenamed("avg(Deaths(Percent) (%))", "average")
    }.toEither(e => TransformationError(e.getMessage))

  def query9(deathByCountry: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import deathByCountry.sparkSession.implicits._
      deathByCountry
        .groupBy($"Entity")
        .sum("Deaths")
        .orderBy($"sum(Deaths)".desc)
        .withColumnRenamed("sum(Deaths)", "total_death")
    }.toEither(e => TransformationError(e.getMessage))

  def query10(disasterNumber: DataFrame, disasterDeath:DataFrame ): Either[PipelineError, DataFrame] =
    Try{
      import disasterNumber.sparkSession.implicits._
      disasterNumber
        .join(disasterDeath, Seq("Entity","Year"))
        .filter($"Entity"==="All natural disasters")
    }.toEither(e => TransformationError(e.getMessage))

  def query11(disasterNumber: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import disasterNumber.sparkSession.implicits._
      disasterNumber
        .filter($"Entity" =!="All natural disasters")
        .groupBy($"Year", $"Entity")
        .sum("Number")
        .orderBy($"Year".desc)
        .withColumnRenamed("sum(Number)", "Number")
    }.toEither(e => TransformationError(e.getMessage))

  def query12(disasterNumber : DataFrame, disasterDeath: DataFrame ): Either[PipelineError, DataFrame] =
    Try{
      import disasterNumber.sparkSession.implicits._
      val disasterNumberDeath = disasterNumber
        .join(disasterDeath, Seq("Entity","Year"))
        .filter($"Entity"==="All natural disasters")
      disasterNumberDeath
        .groupBy($"Entity")
        .sum("Deaths")
        .filter($"Entity" =!= "All natural disasters")
        .orderBy($"sum(Deaths)".desc)
        .withColumnRenamed("sum(Deaths)", "total_death")
    }.toEither(e => TransformationError(e.getMessage))

  def query13(disasterEconomic: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import disasterEconomic.sparkSession.implicits._
      disasterEconomic
        .groupBy($"Entity")
        .sum("Money")
        .filter($"Entity" =!= "All natural disasters")
        .orderBy($"sum(Money)".desc)
        .withColumnRenamed("sum(Money)", "total_money")
    }.toEither(e => TransformationError(e.getMessage))

  def query14(earthquakes: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import earthquakes.sparkSession.implicits._
      earthquakes
        .filter($"Year">1700)
        .withColumnRenamed("Number of significant earthquakes (significant earthquakes)", "Number")
    }.toEither(e => TransformationError(e.getMessage))


  def query15(volcano: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import volcano.sparkSession.implicits._
      volcano
        .filter($"Year">1700)
        .withColumnRenamed("Number of significant volcanic eruptions (NGDC-WDS) (significant eruptions)", "Number")
    }.toEither(e => TransformationError(e.getMessage))
}
