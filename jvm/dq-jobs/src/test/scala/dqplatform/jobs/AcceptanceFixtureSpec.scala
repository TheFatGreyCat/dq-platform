package dqplatform.jobs

import com.fasterxml.jackson.databind.JsonNode
import dqplatform.client._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate
import scala.collection.JavaConverters._

class AcceptanceFixtureSpec extends AnyFunSuite with Matchers with JobsFixture {

  private val Dt = LocalDate.parse("2026-10-20")

  private def ids(variant: String, where: String): Seq[Long] =
    AcceptanceFixture.candidate(spark, Dt, variant, 100).where(where).orderBy("id").collect().map(_.getLong(0)).toSeq

  test("args: run needs --dt and --variant good|deny; unknown options rejected") {
    AcceptanceFixture.parseRun(Seq("--dt", "2026-10-20", "--variant", "good")).rows shouldBe 100
    an[IllegalArgumentException] should be thrownBy AcceptanceFixture.parseRun(Seq("--dt", "2026-10-20", "--variant", "x"))
    an[IllegalArgumentException] should be thrownBy
      AcceptanceFixture.parseRun(Seq("--dt", "2026-10-20", "--variant", "good", "--foo", "1"))
  }

  test("good candidate: 100 rows, exactly ids 1,2,3 violate (id1 violates three rules)") {
    val df = AcceptanceFixture.candidate(spark, Dt, "good", 100)
    df.schema shouldBe AcceptanceFixture.Silver
    df.count() shouldBe 100
    ids("good", "value IS NULL") shouldBe Seq(1L, 2L)
    ids("good", "amount < 0") shouldBe Seq(1L, 3L)
    ids("good", "event_time < TIMESTAMP '2026-10-20 00:00:00'") shouldBe Seq(1L)
    ids("good", "value IS NOT NULL AND amount >= 0 AND event_time >= TIMESTAMP '2026-10-20 00:00:00'") should have size 97
  }

  test("deny candidate: ids 1..6 have null value, nothing else violates") {
    ids("deny", "value IS NULL") shouldBe (1L to 6L)
    ids("deny", "amount < 0 OR event_time < TIMESTAMP '2026-10-20 00:00:00'") shouldBe empty
  }

  test("bootstrap seeds the other partition; verify reports facts of a pinned version") {
    val root = s"${newDir()}/tables"
    val reg = AcceptanceFixture.bootstrap(spark, root)
    reg.get("silver").get("uri").asText() shouldBe s"$root/silver"
    reg.get("business_schema").elements().asScala.map(_.get("name").asText()).toSeq shouldBe Seq("id", "value", "amount", "event_time")
    val f = AcceptanceFixture.verify(spark, root, "2026-10-20", None, None)
    f.get("silver_ids").size() shouldBe 0
    f.get("silver_other").elements().asScala.map(_.asText()).toSeq shouldBe
      Seq("9001|other|5.00|2026-10-19 08:00:00|2026-10-19", "9002|other|6.00|2026-10-19 09:00:00|2026-10-19")
    f.get("silver_total").asLong() shouldBe 2L
    f.get("quarantine").size() shouldBe 0
  }

  /** Service double whose plan carries the acceptance rules (value complete, amount range, partition timestamp, sizes). */
  private final class Svc(dir: String, silver: String, quarantine: String, decision: String)
      extends ServiceApi {
    val inner = new Service(dir, silver, quarantine, decision)
    override def request(method: String, path: Seq[String], query: Map[String, String], body: Option[JsonNode],
                         idem: Option[String]): JsonNode = {
      val res = inner.request(method, path, query, body, idem)
      if (method == "POST" && path.drop(2) == Seq("executions")) {
        val plan = res.get("plan").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
        val sb = plan.putArray("stage_bindings")
        def bind(stage: String, rules: JsonNode*): Unit = {
          val b = sb.addObject()
          b.put("scope", "PARTITION").put("stage", stage).put("reference_role", "raw")
          val a = b.putArray("rules"); rules.foreach(a.add)
        }
        bind("RAW", rule("raw_rows", """{"kind":"size"}""", "RAW", "OBSERVE", "Size", "INTEGER"))
        bind("CANDIDATE",
          rule("value_complete", """{"kind":"complete","column":"value"}""", "CANDIDATE", "QUARANTINE", "Completeness", "DECIMAL"),
          rule("amount_range", """{"kind":"range","column":"amount","min":"0","max":"100"}""", "CANDIDATE", "QUARANTINE", "Compliance", "DECIMAL"),
          rule("event_time_in_partition",
            """{"kind":"sql_predicate","columns":["event_time"],"predicate":"event_time IS NULL OR (event_time >= TIMESTAMP '2026-10-20 00:00:00' AND event_time < TIMESTAMP '2026-10-21 00:00:00')"}""",
            "CANDIDATE", "QUARANTINE", "Compliance", "DECIMAL"))
        bind("CLEAN", rule("clean_row_count_min", """{"kind":"size"}""", "CLEAN", "BLOCK", "Size", "INTEGER"))
      }
      res
    }
  }

  test("run good: exit 0, Silver ids 4..100, quarantine 1,2,3 with sorted _dq_errors, other partition untouched") {
    val dir = newDir()
    val root = s"$dir/tables"
    val reg = AcceptanceFixture.bootstrap(spark, root)
    val svc = new Svc(dir, reg.get("silver").get("uri").asText(), reg.get("quarantine").get("uri").asText(), "ALLOW")
    val dq = new DQClient(spark, DQConfig("http://unused", 5, "ds", "default", java.util.UUID.randomUUID().toString,
      "run-1", None, s"$dir/state"), svc, () => manifest)
    AcceptanceFixture.run(spark, AcceptanceFixture.RunArgs(Dt, "good", 100), dq) shouldBe 0
    val f = AcceptanceFixture.verify(spark, root, "2026-10-20", None, None)
    f.get("silver_ids").elements().asScala.map(_.asLong()).toSeq shouldBe (4L to 100L)
    f.get("quarantine").elements().asScala.map(q => q.get("id").asLong() -> q.get("errors").elements().asScala.map(_.asText()).toSeq).toSeq shouldBe
      Seq(1L -> Seq("amount_range", "event_time_in_partition", "value_complete"), 2L -> Seq("value_complete"), 3L -> Seq("amount_range"))
    f.get("silver_other").size() shouldBe 2
  }

  test("run deny: exit 3 and neither table gains a version") {
    val dir = newDir()
    val root = s"$dir/tables"
    val reg = AcceptanceFixture.bootstrap(spark, root)
    val before = AcceptanceFixture.verify(spark, root, "2026-10-20", None, None)
    val svc = new Svc(dir, reg.get("silver").get("uri").asText(), reg.get("quarantine").get("uri").asText(), "DENY")
    val dq = new DQClient(spark, DQConfig("http://unused", 5, "ds", "default", java.util.UUID.randomUUID().toString,
      "run-1", None, s"$dir/state"), svc, () => manifest)
    AcceptanceFixture.run(spark, AcceptanceFixture.RunArgs(Dt, "deny", 100), dq) shouldBe 3
    AcceptanceFixture.verify(spark, root, "2026-10-20", None, None) shouldBe before
  }
}
