package org.kliusa.otusde201911hex7.safetyboston

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SafetyBoston extends App {
  val crimeCsv = if (args.length > 0) args(0) else "crime.csv"
  val offenseCodesCsv = if (args.length > 1) args(1) else "offense_codes.csv"
  val outFolder = if (args.length > 2) args(2) else "."

  println(
    "Welcome to safety Boston! ;)\n" +
      s"Crime csv: $crimeCsv\n" +
      s"Offence codes csv: $offenseCodesCsv\n" +
      s"Output folder: $outFolder"
  )
  println("------------------------------------------------")

  val sparkSession = SparkSession.builder().master("local").getOrCreate()

  import sparkSession.implicits._

  val crimesDs = sparkSession.read.format("csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load(crimeCsv).as[CrimeObj]
  val offenseCodesDs = sparkSession.read.format("csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load(offenseCodesCsv).as[OffenseObj]

  crimesDs.createGlobalTempView("crime")
  offenseCodesDs.createGlobalTempView("codes")

  //val mixDs = crimesDs.join(offenseCodesDs, crimesDs("OFFENSE_CODE") === offenseCodesDs("CODE") )

  //val group = crimes.groupBy("NAME").count()

  //val sql1 = crimes.
  //  sqlContext.sql("select name, count(*) from defaul group by name")

  //group.show()

  val sql = sparkSession.sql("select DISTRICT, count(*) from global_temp.crime group by DISTRICT order by 1")
  sql.show()

  //broadcast(
//  val sql1 = sparkSession.sql("select DISTRICT, NAME, count(*) from global_temp.crime join global_temp.codes on crime.OFFENSE_CODE=codes.CODE group by NAME, DISTRICT order by 1, 2")
//  sql1.show(5)

  //offenseCodesDs.show(5)
  //crimesDs.show(5)

  //mixDs.show(10)

}
