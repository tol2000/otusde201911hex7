package org.kliusa.otusde201911hex7.safetyboston

import scala.io.Source
import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
object SafetyBoston extends App {

  def readSql(name:String) = {
    val src = Source.fromFile(name+".sql")
    val str = src.mkString
    src.close()
    str
  }

  val crimeCsv =
    if ( args.length > 0 ) args(0)
    else "crime.csv"
  val offenseCodesCsv =
    if ( args.length > 1 ) args(1)
    else "offense_codes.csv"
  val outFolder =
    if ( args.length > 2 ) args(2)
    else "../parquetoutdir"

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
    .load(crimeCsv).as[Crime]

  val offenseCodesDsSrc = sparkSession.read.format("csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load(offenseCodesCsv).as[Offense]

  // This is because of duplicates of codes (e.g. 311) in offense codes
  val offenseCodesDs =
    offenseCodesDsSrc
      .groupByKey( x => x.CODE )
      .reduceGroups(
        (x,y) => if(x.NAME.length >= y.NAME.length) x else y
      )
      .map( x => x._2 )


  crimeDs.createOrReplaceTempView("crime")
  offenseCodesDs.createOrReplaceTempView("codes")

  val sqlTot = sparkSession.sql( readSql("totcounts") )
  sqlTot.createOrReplaceTempView("tot")

  val sqlFreq = sparkSession.sql(readSql("sqlfreq"))
  sqlFreq.createOrReplaceTempView("freq")

  val sqlMonthly = sparkSession.sql(readSql("monthly"))
  sqlMonthly.createOrReplaceTempView("monthly")

  val sqlAll = sparkSession.sql(readSql("sqlall"))

  // Show query plan to see that broadcast works...
  sqlFreq.explain()
  sqlAll.explain()

  sqlAll.show(200, 200)

  sqlAll.coalesce(1)         // Writes to a single file
    .write
    .mode(SaveMode.Overwrite)
    .parquet(outFolder)

}
