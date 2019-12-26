package org.kliusa.otusde201911hex7.safetyboston

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SafetyBoston extends App {

  val crimeCsv =
    if ( args.length > 0 ) args(0)
    else "crime.csv"
  val offenseCodesCsv =
    if ( args.length > 1 ) args(1)
    else "offense_codes.csv"
  val outFolder =
    if ( args.length > 2 ) args(2)
    else "."

  BasicConfigurator.configure()

  Logger.getRootLogger
    .setLevel( Level.ERROR )

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

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

  val offenseCodesDs/*Src*/ = sparkSession.read.format("csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load(offenseCodesCsv).as[OffenseObj]
  //offenseCodesDsSrc.createOrReplaceTempView("offenseCodesDsSrc")
  // // This is because of fail in offense codes
  //val offenseCodesDs = sparkSession.sql("select CODE, NAME from offenseCodesDsSrc where CODE is not null and NAME is not NULL group by CODE, NAME")

  val tot1Ds = crimeDs.groupBy("DISTRICT")
    .agg(
      Map (
        "Lat" -> "avg",
        "Long" -> "avg"
        , "*" -> "count"
      )
    )

  tot1Ds.show(100)


  val sqlCrime = crimeDs.join( broadcast(offenseCodesDs), crimeDs("OFFENSE_CODE")===offenseCodesDs("CODE"), "inner" )
  sqlCrime.createOrReplaceTempView("sqlCrime")

  val sqlTot = sparkSession.sql("select nvl(DISTRICT,'00') as district, count(*) as crimes_total, avg(Lat) as lat, avg(Long) as lng from sqlCrime group by DISTRICT")

  //val sqlMonthly = sparkSession.sql("select nvl(DISTRICT,'00') as district, trim(split(NAME,'-')[0]) as offType, count(*), avg(Lat), avg(Long) from sqlCrime group by DISTRICT, trim(split(NAME,'-')[0]) order by substr(offDistrict,1,1), int(substr(offDistrict,2))")
  //totSql.show(100)

  val sqlFreq1 = sparkSession.sql("select nvl(DISTRICT,'00') as district1, trim(split(NAME,'-')[0]) as crime_type, count(*) as crimes_total from sqlCrime group by DISTRICT, trim(split(NAME,'-')[0])")
  sqlFreq1.createOrReplaceTempView("sqlFreq1")
  val sqlFreq = sparkSession.sql("select district1, max('wrong! '||crimes_total||' - '||crime_type) as frequent_crime_types from sqlFreq1 group by district1")

  val sqlAll = sqlTot.join(sqlFreq, sqlTot("district") === sqlFreq("district1"), "left_outer" )
    .drop("district1")

  sqlAll.show(100)

  sqlTot.show(100)

  values not equal in datasets :)

}
