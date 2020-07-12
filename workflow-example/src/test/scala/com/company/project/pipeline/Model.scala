package com.company.project.pipeline
import java.sql.Timestamp
object Model{

  case class Disaster(Entity: String,
                   Year: Int,
                   Number: Int)

  case class Temperature(dt: Timestamp,
                         AverageTemperature: Int)
}
