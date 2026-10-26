package dqplatform.client

import com.amazon.deequ.analyzers.{AnalyzerOptions, FilteredRowOutcome}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{NullNode, ObjectNode}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Measures compiled rules on a DataFrame. Deequ is used only to measure (assertions always true);
 * the server alone decides pass/fail. Every Result carries all wire keys.
 */
object Measure {

  /** Prefix of temporary row-level columns (Check description) so they never collide with data columns. */
  val ColumnPrefix = "_dq_rule_"
  def columnName(ruleId: String): String = ColumnPrefix + ruleId

  private val SchemaKinds = Set("schema_columns", "schema_type")
  private val Pass: Double => Boolean = _ => true

  /** Deequ columns each rule reads, for the case-insensitive existence pre-check. */
  private def columnsOf(c: JsonNode): Seq[String] = {
    def arr(f: String) = Option(c.get(f)).filter(_.isArray).map(_.elements().asScala.map(_.asText()).toSeq).getOrElse(Nil)
    c.get("kind").asText() match {
      case "size" | "sql_metric" | "schema_columns" => Nil
      case "unique"                                 => arr("columns")
      case "sql_predicate"                          => arr("columns")
      case _                                        => Option(c.get("column")).map(_.asText()).toSeq
    }
  }

  def isDeequRule(rule: CompiledRule): Boolean = !SchemaKinds(rule.kind) && rule.kind != "size"

  private def sub(s: String, batchView: String): String = s.replace("${batch}", batchView)

  /** Deequ Check for a rule; description = columnName so row-level columns are addressable. */
  def check(rule: CompiledRule, batchView: String): Check = {
    val c = rule.check
    val base = Check(CheckLevel.Error, columnName(rule.ruleId))
    def s(f: String) = c.get(f).asText()
    def strs(f: String) = c.get(f).elements().asScala.map(_.asText()).toSeq
    c.get("kind").asText() match {
      case "complete"      => base.isComplete(s("column"))
      case "completeness"  => base.hasCompleteness(s("column"), Pass)
      case "unique"        => base.hasUniqueness(strs("columns"), Pass)
      case "pattern" =>
        base.hasPattern(s("column"), s("regex").r, Pass,
          analyzerOptions = Some(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)))
          .where(s"`${s("column")}` IS NOT NULL")
      case "range" =>
        base.isContainedIn(s("column"), new java.math.BigDecimal(s("min")).doubleValue(),
          new java.math.BigDecimal(s("max")).doubleValue(),
          includeLowerBound = Option(c.get("include_min")).forall(_.asBoolean(true)),
          includeUpperBound = Option(c.get("include_max")).forall(_.asBoolean(true)))
      case "in_set"        => base.isContainedIn(s("column"), strs("values").toArray)
      case "mean"          => base.hasMean(s("column"), Pass)
      case "sql_predicate" =>
        base.satisfies(sub(s("predicate"), batchView), rule.ruleId, Pass, columns = strs("columns").toList)
      case "sql_metric"    => base.customSql(sub(s("sql"), batchView), Pass)
      case other           => throw new ProtocolException("INVALID_PROTOCOL", s"kind.$other")
    }
  }

  def run(spark: SparkSession, rules: Seq[CompiledRule], data: DataFrame, batchView: String): Seq[ObjectNode] = {
    val lower = data.columns.map(_.toLowerCase).toSet
    def missing(r: CompiledRule): Option[String] = columnsOf(r.check).find(c => !lower(c.toLowerCase))

    val (unreadable, readable) = rules.partition(r => !SchemaKinds(r.kind) && missing(r).isDefined)
    val deequRules = readable.filter(isDeequRule)
    val checks = deequRules.map(r => r -> check(r, batchView))
    val verification: Try[VerificationResult] =
      if (checks.isEmpty) Failure(new IllegalStateException("none"))
      else Try(VerificationSuite().onData(data).addChecks(checks.map(_._2)).run())
    val checkByRule = checks.toMap

    val bySize = readable.filter(_.kind == "size")
    val sizeValue: Option[Try[Long]] = if (bySize.isEmpty) None else Some(Try(data.count()))

    val out = rules.map { r =>
      if (unreadable.contains(r))
        error(r, ClientError("COLUMN_NOT_FOUND", Map("rule_id" -> r.ruleId, "column" -> missing(r).get)))
      else r.kind match {
        case "size" => sizeValue.get match {
          case Success(n) => ok(r).put("metric_name", r.metricName.orNull).put("metric_value", n)
          case Failure(e) => error(r, classify(r, e))
        }
        case "schema_columns" => schemaColumns(r, data.schema)
        case "schema_type"    => schemaType(r, data.schema)
        case _ =>
          verification match {
            case Failure(e) => error(r, classify(r, e))
            case Success(v) =>
              val metric = v.checkResults(checkByRule(r)).constraintResults.headOption.flatMap(_.metric)
              metric.map(_.value) match {
                case Some(Success(d: Double)) if !d.isNaN && !d.isInfinity =>
                  ok(r).put("metric_name", r.metricName.orNull)
                    .put("metric_value", Canonical.decimal(java.math.BigDecimal.valueOf(d)))
                case Some(Failure(e)) => error(r, classify(r, e))
                case _ => error(r, ClientError("UNCLASSIFIED_ENGINE_ERROR",
                  Map("rule_id" -> r.ruleId, "exception_class" -> "java.lang.IllegalStateException")))
              }
          }
      }
    }
    out
  }

  private def base(r: CompiledRule, status: String): ObjectNode = {
    val n = Json.node()
    n.put("rule_id", r.ruleId)
    n.put("measurement_definition_hash", r.measurementHash)
    n.put("scope", r.scope)
    n.put("stage", r.stage)
    n.put("kind", r.kind)
    n.put("measurement_status", status)
    n.set[ObjectNode]("metric_name", NullNode.instance)
    n.set[ObjectNode]("metric_value", NullNode.instance)
    n.set[ObjectNode]("schema_evidence", NullNode.instance)
    n.set[ObjectNode]("error", NullNode.instance)
    n.set[ObjectNode]("failed_rows", NullNode.instance)
    n
  }

  private def ok(r: CompiledRule): ObjectNode = base(r, "OK")

  def error(r: CompiledRule, e: ClientError): ObjectNode = {
    val n = base(r, "ERROR")
    n.set[ObjectNode]("error", e.toJson)
    n
  }

  private def causes(t: Throwable): Iterator[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(16)

  /** Maps engine failures to stable codes; only the exception class name survives, never the message. */
  private def classify(r: CompiledRule, t: Throwable): ClientError = {
    val chain = causes(t).toSeq
    def withRule(code: String, e: Throwable) = {
      val ce = ClientError.withClass(code, e)
      ce.copy(params = ce.params + ("rule_id" -> r.ruleId))
    }
    chain.collectFirst { case e: AnalysisException => withRule("SQL_ANALYSIS_ERROR", e) }
      .orElse(chain.collectFirst {
        case e: NumberFormatException => withRule("CAST_OVERFLOW", e)
        case e: ArithmeticException   => withRule("CAST_OVERFLOW", e)
      })
      .getOrElse(ClientError.unclassified(chain.last, Some(r.ruleId)))
  }

  private def schemaColumns(r: CompiledRule, schema: StructType): ObjectNode = {
    val expected = r.check.get("columns").elements().asScala.map(_.asText().toLowerCase).toSet
    val actual = schema.fieldNames.toSeq
    val actualSet = actual.map(_.toLowerCase).toSet
    val exact = Option(r.check.get("exact")).exists(_.asBoolean(false))
    val matches = expected.subsetOf(actualSet) && (!exact || actualSet == expected)
    val n = ok(r)
    val ev = Json.node()
    val arr = ev.putArray("actual_columns")
    actual.foreach(arr.add)
    ev.put("matches", matches)
    n.set[ObjectNode]("schema_evidence", ev)
    n
  }

  private def schemaType(r: CompiledRule, schema: StructType): ObjectNode = {
    val column = r.check.get("column").asText()
    schema.find(_.name.equalsIgnoreCase(column)) match {
      case None => error(r, ClientError("COLUMN_NOT_FOUND", Map("rule_id" -> r.ruleId, "column" -> column)))
      case Some(f) =>
        val ev = Json.node()
        ev.put("column", column)
        ev.put("actual_type", f.dataType.simpleString)
        ev.put("matches", typeMatches(f.dataType, r.check.get("type").asText()))
        val n = ok(r)
        n.set[ObjectNode]("schema_evidence", ev)
        n
    }
  }

  private def typeMatches(actual: DataType, expected: String): Boolean = (actual, expected) match {
    case (StringType, "string")       => true
    case (BooleanType, "boolean")     => true
    case (IntegerType, "int")         => true
    case (LongType, "long")           => true
    case (FloatType, "float")         => true
    case (DoubleType, "double")       => true
    case (_: DecimalType, "decimal")  => true
    case (DateType, "date")           => true
    case (TimestampType, "timestamp") => true
    case _                            => false
  }
}
