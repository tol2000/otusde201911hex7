package org.kliusa.otusde201911hex7.safetyboston

import scala.io.Source
import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SafetyBoston extends App {

  def readSql(name:String) = {
    Source.fromFile(name+".sql").mkString
  }

  def writeToTsv(df:DataFrame) {
    val tsvWithHeaderOptions: Map[String, String] = Map(
      ("delimiter", "\t"), // Uses "\t" delimiter instead of default ","
      ("header", "true"))  // Writes a header record with column names

    df.coalesce(1)         // Writes to a single file
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv(df.getClass.getName+".tsv")
  }

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
    .load(crimeCsv) //.as[CrimeObj]
  crimeDs.createOrReplaceTempView("crime")

  val offenseCodesDsSrc = sparkSession.read.format("csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load(offenseCodesCsv) //.as[OffenseObj]
  offenseCodesDsSrc.createOrReplaceTempView("offenseCodesDsSrc")
  // This is because duplicates, etc. in offense codes
  val offenseCodesDs = sparkSession.sql("select CODE, NAME, count(*) as dup_qnty from offenseCodesDsSrc /*where CODE=3108*/ group by CODE, NAME")
  offenseCodesDs.createOrReplaceTempView("codes")

  val sqlTot = sparkSession.sql( readSql("totcounts") )
  sqlTot.createOrReplaceTempView("tot")
  sqlTot.show(100)

  val sqlFreq = sparkSession.sql(readSql("sqlfreq"))
  sqlFreq.createOrReplaceTempView("freq")
  sqlFreq.show(100)

  //val sqlAll = sparkSession.sql(readSql("sqlall"))
  //sqlAll.show(100)

}
