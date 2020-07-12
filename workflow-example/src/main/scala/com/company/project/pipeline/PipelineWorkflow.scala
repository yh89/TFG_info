package com.company.project.pipeline

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.company.project._
import org.apache.spark._
import org.apache.spark.sql.{functions => func, _}
import org.apache.spark.sql.types._, func._

import scala.util.Try

class PipelineWorkflow(inputPath: String, outputPath: String) {

  def run(implicit spark: SparkSession): Either[PipelineError, Unit] = {
    for {
      list <- readData().right
      res <- transformation(list).right
    } yield res
  }

  def readCsv(input: String)(implicit spark: SparkSession): Either[ReadError, DataFrame] =
    Try(spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputPath + input + ".csv"))
      .toEither(error => ReadError(error.getMessage))

  def writeData(df: DataFrame, output: String): Either[ExecutionError, Unit] =
    Try(df.write.mode(SaveMode.Overwrite)
      .parquet(outputPath + output + ".parquet"))
      .toEither(error => ExecutionError(error.getMessage))

  def writeDataPartition(df: DataFrame, output: String, partition: String): Either[ExecutionError, Unit] =
    Try(df.write.mode(SaveMode.Overwrite)
      .partitionBy(partition)
      .parquet(outputPath + output + ".parquet"))
      .toEither(error => ExecutionError(error.getMessage))


  def readData()(implicit spark: SparkSession): Either[ReadError, List[DataFrame]] = {
    import spark.implicits._
    for {
      df1 <- readCsv("GlobalLandTemperaturesByCountry").right
      res2 <- readCsv("economic-damage-from-natural-disasters").right
      df2 <- Right(res2.drop("Code")).right
      res3 <- readCsv("deaths-natural-disasters-ihme").right
      df3 <-  Right(res3.withColumn("Deaths", $"Deaths".cast("Long"))).right
      res4 <- readCsv("number-of-natural-disaster-events").right
      df4 <- Right(res4.drop("Code")).right
      res5 <- readCsv("number-of-deaths-from-natural-disasters").right
      df5 <- Right(res5.drop("Code")).right
      res6 <- readCsv("share-deaths-from-natural-disasters").right
      df6 <- Right(res6.withColumn("Deaths(Percent) (%)", format_number($"Deaths(Percent) (%)", 2).cast("Double"))).right
      res7 <- readCsv("significant-earthquakes").right
      df7 <- Right(res7.drop("Code")).right
      res8 <- readCsv("significant-volcanic-eruptions").right
      df8 <- Right(res8.drop("Code")).right
      //df9 <- readCsv("GlobalLandTemperaturesByCity").right
    } yield df1 :: df2 :: df3 :: df4 :: df5 :: df6 :: df7 :: df8 :: df9:: Nil
  }

  def transformation(list: List[DataFrame]): Either[PipelineError, Unit] = {
    for {
      input1 <- Transformations.query1(list(3)).right
      res1 <- writeData(input1, "1").right

      input <- Transformations.query2(list(1)).right
      res2 <- writeData(input, "2").right

      input <- Transformations.query3(list(1), list(3)).right
      res <- writeData(input, "3").right

      input <- Transformations.query4(list(0)).right
      res <- writeData(input, "4").right

      input <- Transformations.query5(list(0)).right
      res <- writeData(input, "5").right

      input <- Transformations.query6(list(0), list(3)).right
      res <- writeData(input, "6").right

      input <- Transformations.query7(list(2)).right
      res <- writeData(input, "7").right

      input <- Transformations.query8(list(5)).right
      res <- writeData(input, "8").right

      input <- Transformations.query9(list(2)).right
      res <- writeData(input, "9").right

      input <- Transformations.query10(list(3), list(4)).right
      res <- writeData(input, "10").right

      input <- Transformations.query11(list(3)).right
      res <- writeData(input, "11").right

      input <- Transformations.query12(list(3), list(4)).right
      res <- writeData(input, "12").right

      input <- Transformations.query13(list(1)).right
      res <- writeData(input, "13").right

      input <- Transformations.query14(list(6)).right
      res <- writeData(input, "14").right

      input <- Transformations.query15(list(7)).right
      res <- writeData(input, "15").right

      res <- writeDataPartition(list(8), "16", "Country").right
    } yield ()
  }

}

