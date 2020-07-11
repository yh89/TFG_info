package com.company.project.pipeline

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.company.project._
import org.apache.spark._
import org.apache.spark.sql.{functions => func, _}
import org.apache.spark.sql.types._, func._

import scala.util.Try

class PipelineWorkflow(inputPath: String, outputPath: String) {

  def run(implicit spark: SparkSession): Either[PipelineError, Unit] = {
    val list = readData()
    Right(transformation(list))
  }

  def readCsv(input: String )(implicit spark: SparkSession): DataFrame =
    spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputPath + input + ".csv")

  def writeData(df: DataFrame, output: String): Either[ExecutionError, Unit] =
    Try(df.write.mode(SaveMode.Overwrite)
      .parquet(outputPath+output+".parquet"))
      .toEither(error => ExecutionError(error.getMessage))

  def writeDataPartition(df: DataFrame, output: String, partition:String): Either[ExecutionError, Unit] =
    Try(df.write.mode(SaveMode.Overwrite)
      .partitionBy(partition)
      .parquet(outputPath+output+".parquet"))
      .toEither(error => ExecutionError(error.getMessage))


  def readData()(implicit spark: SparkSession): List[DataFrame] = {
    val list: List[DataFrame] =
      readCsv("GlobalLandTemperaturesByCountry") ::
      readCsv("economic-damage-from-natural-disasters").drop("Code") ::
      readCsv("deaths-natural-disasters-ihme") ::
      readCsv("number-of-natural-disaster-events").drop("Code") ::
      readCsv("number-of-deaths-from-natural-disasters").drop("Code") ::
      readCsv("share-deaths-from-natural-disasters") ::
      readCsv("significant-earthquakes").drop("Code") ::
      readCsv("significant-volcanic-eruptions").drop("Code") ::
      readCsv("GlobalLandTemperaturesByCity") ::
      Nil
    list
  }

  def transformation(list: List[DataFrame]): Unit ={
    for{
      input <- Transformations.query1(list).right
      res <- writeData(input, "1").right
    }yield res

    for{
      input <- Transformations.query2(list).right
      res <- writeData(input, "2").right
    }yield res

    for{
      input <- Transformations.query3(list).right
      res <- writeData(input, "3").right
    }yield res


    println("------------4-----------")
    for{
      input <- Transformations.query4(list).right
      res <- writeData(input, "4").right
    }yield res


    println("------------5-----------")
    for{
      input <- Transformations.query5(list).right
      res <- writeData(input, "5").right
    }yield res

    println("------------6-----------")
    for{
      input <- Transformations.query6(list).right
      res <- writeData(input, "6").right
    }yield res

    println("------------7-----------")
    for{
      input <- Transformations.query7(list).right
      res <- writeData(input, "7").right
    }yield res

    println("------------8-----------")
    for{
      input <- Transformations.query8(list).right
      res <- writeData(input, "8").right
    }yield res

    println("------------9-----------")
    for{
      input <- Transformations.query9(list).right
      res <- writeData(input, "9").right
    }yield res


    for{
      input <- Transformations.query10(list).right
      res <- writeData(input, "10").right
    }yield res

    println("------------11-----------")
    for{
      input <- Transformations.query11(list).right
      res <- writeData(input, "11").right
    }yield res


    println("------------12-----------")
    for{
      input <- Transformations.query12(list).right
      res <- writeData(input, "12").right
    }yield res

    println("------------13-----------")
    for{
      input <- Transformations.query13(list).right
      res <- writeData(input, "13").right
    }yield res

    println("------------14-----------")
    for{
      input <- Transformations.query14(list).right
      res <- writeData(input, "14").right
    }yield res

    println("------------15-----------")
    for{
      input <- Transformations.query15(list).right
      res <- writeData(input, "15").right
    }yield res

    println("------------16-----------")
    for{
      res <- writeDataPartition(list(8), "16", "Country").right
    }yield res
  }

}

