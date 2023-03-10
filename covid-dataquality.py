%scala
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.concat
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Compliance, Distinctness, InMemoryStateProvider, Size}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.util.Date


def writeDStoS3(resultDF: DataFrame, s3Bucket: String, s3Prefix: String, team: String, dataset: String, tabName: String, getYear: String, getMonth: String, getDay: String, getTimestamp: String) = {

      resultDF.write.mode("append").parquet(s3Bucket + "/"
        + s3Prefix + "/"
        + "team=" + team + "/"
        + "dataset=" + dataset + "/"
        + "table=" + tabName + "/"
        + "year=" + getYear + "/"
        + "month=" + getMonth + "/"
        + "day=" + getDay + "/"
        + "hour=" + getTimestamp.split("-")(0) + "/"
        + "min=" + getTimestamp.split("-")(1) + "/"
      )

    }
 def verificationRunner(glueDF: DataFrame, allConstraints: Seq[com.amazon.deequ.constraints.Constraint], team: String, dataset: String, tabName: String, getYear: String, getMonth: String, getDay: String, getTimestamp: String) = {
      val autoGeneratedChecks = Check(CheckLevel.Error, "data constraints", allConstraints)
      val verificationResult = VerificationSuite().onData(glueDF).addChecks(Seq(autoGeneratedChecks)).run()
      val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
      writeDStoS3(resultDataFrame, "s3://sdlf-poc-dataquality/", "constraints-verification-results", team, dataset, tabName, getYear, getMonth, getDay, getTimestamp)
    }

def analysisRunner(glueDF: DataFrame, allConstraints: Seq[com.amazon.deequ.constraints.Constraint], team: String, dataset: String, tabName: String, getYear: String, getMonth: String, getDay: String, getTimestamp: String) = {
      val autoGeneratedChecks = Check(CheckLevel.Error, "data constraints", allConstraints)
      val analyzersFromChecks = Seq(autoGeneratedChecks).flatMap { _.requiredAnalyzers() }
      val analysisResult: AnalyzerContext = {
                  AnalysisRunner
                  .onData(glueDF)
                  .addAnalyzers(analyzersFromChecks)
                  .run()
      }
      val resultDataFrame = successMetricsAsDataFrame(spark, analysisResult)
      writeDStoS3(resultDataFrame, "s3://sdlf-poc-dataquality/", "constraints-analysis-results", team, dataset, tabName, getYear, getMonth, getDay, getTimestamp)
    }

val team = "engineering"
val dataset = "legislators"
val dbName = "sdlf-poc-db"
val tabName = "covid"
val getYear = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy"))
val getMonth = LocalDate.now().format(DateTimeFormatter.ofPattern("MM"))
val getDay = LocalDate.now().format(DateTimeFormatter.ofPattern("dd"))
val getTimestamp = new SimpleDateFormat("HH-mm-ss").format(new Date)

val Data = spark.sql("SELECT * FROM delta.`s3://sdlf-poc-bronze/covid/`")

val suggestionResult = ConstraintSuggestionRunner()
    .onData(Data)
    .addConstraintRules(Rules.DEFAULT)
    .run()

val allConstraints: Seq[com.amazon.deequ.constraints.Constraint] = suggestionResult.constraintSuggestions.flatMap
      { case (_, suggestions) => suggestions.map {
        _.constraint
      }
      }.toSeq

val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
        case (column, suggestions) =>
          suggestions.map { constraint =>
            (column, constraint.description, constraint.codeForConstraint)
          }
      }.toSeq.toDS().withColumn("uniqueID", monotonicallyIncreasingId)

suggestionDataFrame.createOrReplaceTempView("suggestionDataFrame")

val suggestionDataFrame_UniqId = spark.sql("select row_number() over (order by uniqueID) as row_num, * from suggestionDataFrame")

val suggestionDataFrameRenamed = suggestionDataFrame_UniqId
        .withColumn("suggestion_hash_key", concat(lit("##"), lit(team), lit("##"), lit(dataset), lit("##"), lit(tabName), lit("##"), $"_1", lit("##"), $"row_num"))
        .withColumnRenamed("_1", "column")
        .withColumnRenamed("_2", "constraint")
        .withColumnRenamed("_3", "constraint_code")
        .withColumn("enable", lit("Y"))
        .withColumn("table_hash_key", concat(lit(team), lit("-"), lit(dataset), lit("-"), lit(tabName)))
        .drop("uniqueID").drop("row_num")

print(suggestionDataFrameRenamed)

writeDStoS3(suggestionDataFrameRenamed, "s3://sdlf-poc-dataquality/", "constraint-suggestion-results", team, dataset, tabName, getYear, getMonth, getDay, getTimestamp)

verificationRunner(Data, allConstraints, team, dataset, tabName, getYear, getMonth, getDay, getTimestamp)
analysisRunner(Data, allConstraints, team, dataset, tabName, getYear, getMonth, getDay, getTimestamp)
