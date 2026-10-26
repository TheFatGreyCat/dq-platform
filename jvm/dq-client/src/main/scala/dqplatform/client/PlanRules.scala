package dqplatform.client

import com.fasterxml.jackson.databind.JsonNode

import scala.collection.JavaConverters._

/** One server-compiled rule of an ExecutionPlan stage binding; `check` is the resolved_check object. */
final class CompiledRule(
    val ruleId: String,
    val scope: String,
    val stage: String,
    val kind: String,
    val metricName: Option[String],
    val valueType: String,
    val measurementHash: String,
    val check: JsonNode,
    val action: String,
    val required: Boolean)

object PlanRules {

  def fromPlan(plan: JsonNode): Seq[CompiledRule] = {
    val bindings = plan.get("stage_bindings")
    if (bindings == null || !bindings.isArray) Json.fail("plan.stage_bindings")
    bindings.elements().asScala.toSeq.flatMap { b =>
      val scope = Json.str(b, "scope", "stage_binding")
      val stage = Json.str(b, "stage", "stage_binding")
      val rules = b.get("rules")
      if (rules == null || !rules.isArray) Json.fail("stage_binding.rules")
      rules.elements().asScala.toSeq.map { r =>
        val check = Json.obj(r, "resolved_check", "rule")
        val required = r.get("required")
        if (required == null || !required.isBoolean) Json.fail("rule.required")
        new CompiledRule(
          Json.str(r, "rule_id", "rule"), scope, stage, Json.str(r, "kind", "rule"),
          Json.optStr(r, "metric_name", "rule"), Json.str(r, "value_type", "rule"),
          Json.str(r, "measurement_definition_hash", "rule"), check,
          Json.str(r, "action", "rule"), required.booleanValue())
      }
    }
  }
}
