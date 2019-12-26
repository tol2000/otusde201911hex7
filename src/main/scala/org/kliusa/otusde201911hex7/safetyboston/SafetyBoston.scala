package org.kliusa.otusde201911hex7.safetyboston

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SafetyBoston extends App {

  val crimeCsv = if (args.length > 0) args(0) else "crime.csv"
  val offenseCodesCsv = if (args.length > 1) args(1) else "offense_codes.csv"
  val outFolder = if (args.length > 2) args(2) else "."

  BasicConfigurator.configure()
  val logger = Logger.getRootLogger
  logger.setLevel(Level.ERROR)

  val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  println(
    "Welcome to safety Boston! ;)\n" +
      s"Crime csv: $crimeCsv\n" +
      s"Offence codes csv: $offenseCodesCsv\n" +
      s"Output folder: $outFolder"
  )
  println("------------------------------------------------")

  import sparkSession.implicits._

  val crimeDs = sparkSession.read.format("csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load(crimeCsv).as[CrimeObj]

  val offenseCodesDs = sparkSession.read.format("csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load(offenseCodesCsv).as[OffenseObj]

  // Сджойним-ка в общую вьюху, а потом будем делать над ней всякие вещи...
  val viewDs = crimeDs.join(broadcast(offenseCodesDs), crimeDs("OFFENSE_CODE") === offenseCodesDs("CODE") )
  viewDs.createOrReplaceTempView("crimeView")

  //println(s"Count of viewDs: ${viewDs.count()}")

  //val group = crimes.groupBy("NAME").count()

  //val sql1 = crimes.
  //sqlContext.sql("select name, count(*) from defaul group by name")

  //group.show()

  val sql = sparkSession.sql("select DISTRICT, NAME, count(*) from crimeView where DISTRICT is not null group by DISTRICT, NAME order by 1, 2")
  sql.show(150)

}
