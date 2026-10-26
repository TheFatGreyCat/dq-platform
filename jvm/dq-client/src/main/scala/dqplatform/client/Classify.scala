package dqplatform.client

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Row-level classification of a candidate: every business row kept, `_dq_errors` = sorted distinct failed QUARANTINE rule ids. */
object Classify {

  val ErrorsColumn = "_dq_errors"
  val Prefix = "_dq_"

  def business(df: DataFrame): DataFrame = df.drop(df.columns.filter(_.startsWith(Prefix)): _*)

  def clean(classified: DataFrame): DataFrame = business(classified.where(size(col(ErrorsColumn)) === 0))

  /** Tags rules with action QUARANTINE (stage CANDIDATE); batchView is the sql_metric/predicate placeholder value. */
  def tag(spark: SparkSession, rules: Seq[CompiledRule], data: DataFrame, batchView: String = ""): DataFrame = {
    val q = rules.filter(r => r.action == "QUARANTINE" && r.stage == "CANDIDATE").sortBy(_.ruleId)
    if (q.isEmpty)
      return data.withColumn(ErrorsColumn, array().cast(ArrayType(StringType, containsNull = false)))

    val verification = VerificationSuite().onData(data).addChecks(q.map(Measure.check(_, batchView))).run()
    val rowLevel = VerificationResult.rowLevelResultsAsDataFrame(spark, verification, data)
    q.foreach { r =>
      if (!rowLevel.columns.contains(Measure.columnName(r.ruleId)))
        throw new ProtocolException("CLASSIFICATION_INCOMPLETE", r.ruleId)
    }
    val errors = array_sort(array_compact(array(q.map { r =>
      when(!coalesce(col(Measure.columnName(r.ruleId)), lit(false)), lit(r.ruleId))
    }: _*)))
    val tagged = rowLevel.withColumn(ErrorsColumn, errors)
    tagged.drop(tagged.columns.filter(_.startsWith(Measure.ColumnPrefix)): _*)
  }

  final case class Counts(candidate: Long, valid: Long, quarantine: Long, perRule: Map[String, Long])

  def counts(classified: DataFrame, quarantineRuleIds: Seq[String]): Counts = {
    val bad = size(col(ErrorsColumn)) > 0
    val perRule = quarantineRuleIds.map(id => count(when(array_contains(col(ErrorsColumn), id), 1)).as(id))
    val row = classified.agg(count(lit(1)).as("_c"), count(when(bad, 1)).as("_q") +: perRule: _*).head()
    Counts(row.getLong(0), row.getLong(0) - row.getLong(1), row.getLong(1),
      quarantineRuleIds.zipWithIndex.map { case (id, i) => id -> row.getLong(i + 2) }.toMap)
  }
}
