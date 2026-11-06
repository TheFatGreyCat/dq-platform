package dqplatform.jobs

import dqplatform.client.{DeltaRuntime, SchemaHash}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class BootstrapFinanceTablesSpec extends AnyFunSuite with Matchers with JobsFixture {

  private def boot(dir: String, root: String = "t") =
    BootstrapFinanceTables.bootstrap(spark, s"file:$dir/$root", s"file:$dir/")

  test("schemas: enriched business columns, dt DateType partition, quarantine adds the _dq_* columns") {
    val b = BootstrapFinanceTables
    b.Business.fieldNames.toSeq shouldBe Seq("transaction_id", "customer_id", "account_id", "amount", "currency",
      "transaction_type", "status", "event_time", "merchant_category", "channel", "country", "customer_name",
      "customer_segment", "risk_level", "kyc_status", "customer_country", "amount_usd", "transaction_month",
      "customer_day_txn_count", "customer_day_total", "customer_day_avg")
    b.Business("amount").dataType.simpleString shouldBe "decimal(18,2)"
    b.Business("amount_usd").dataType.simpleString shouldBe "decimal(18,2)"
    b.Business("event_time").dataType.typeName shouldBe "timestamp"
    b.Business("customer_day_txn_count").dataType.typeName shouldBe "long"
    b.Silver.fieldNames.toSeq shouldBe b.Business.fieldNames.toSeq :+ "dt"
    b.Silver("dt").dataType.typeName shouldBe "date"
    b.Quarantine.fieldNames.toSeq shouldBe b.Silver.fieldNames.toSeq ++
      Seq("_dq_execution_id", "_dq_errors", "_dq_candidate_version", "_dq_quarantined_at")
  }

  test("creates distinct empty Delta tables partitioned by dt; registration carries actual snapshot ids, hashes, roots") {
    val dir = newDir()
    val reg = boot(dir)
    val (s, q) = (reg.get("silver"), reg.get("quarantine"))
    s.get("uri").asText() should not be q.get("uri").asText()
    s.get("table_id").asText() should not be q.get("table_id").asText()
    for ((name, extra) <- Seq("silver" -> false, "quarantine" -> true)) {
      val ref = reg.get(name)
      val st = DeltaRuntime.state(spark, ref.get("uri").asText())
      st.partitionColumns shouldBe Seq("dt")
      st.properties should not contain key(DeltaRuntime.RetentionProperty)
      st.ref.tableId shouldBe ref.get("table_id").asText()
      st.ref.schemaHash shouldBe ref.get("schema_hash").asText()
      ref.get("partition_schema_hash").asText() shouldBe SchemaHash.of(BootstrapTables.Partition)
      ref.get("set_transaction_retention").isNull shouldBe true
      val df = spark.read.format("delta").load(ref.get("uri").asText())
      df.count() shouldBe 0L
      df.schema("dt").dataType.typeName shouldBe "date"
      df.columns.contains("_dq_errors") shouldBe extra
    }
    reg.get("business_schema").elements().asScala.map(_.get("name").asText()).toSeq shouldBe
      BootstrapFinanceTables.Business.fieldNames.toSeq
    reg.get("partition_schema").get(0).get("type").asText() shouldBe "date"
    reg.get("allowed_storage_roots").elements().asScala.map(_.asText()).toSeq shouldBe Seq(s"file:$dir/")
    reg.get("candidate_root").asText() shouldBe s"file:$dir/t/candidates"
    reg.get("history_context").asText() shouldBe "default"
    reg.get("retention").get("retry_window_seconds").asLong() should be > 0L
    reg.get("retention").get("cleanup_enabled").asBoolean() shouldBe false
  }

  test("default allowed storage root is s3a://dq-lake/") {
    BootstrapFinanceTables.DefaultAllowedRoot shouldBe "s3a://dq-lake/"
  }

  test("fails and writes nothing when either target already exists; never overwrites") {
    val dir = newDir()
    boot(dir)
    an[IllegalArgumentException] should be thrownBy boot(dir)
    val before = DeltaRuntime.snapshot(spark, s"file:$dir/t/silver_finance_transactions")
    an[IllegalArgumentException] should be thrownBy boot(dir)
    DeltaRuntime.snapshot(spark, s"file:$dir/t/silver_finance_transactions") shouldBe before

    val partial = newDir()
    BootstrapTables.bootstrap(spark, s"file:$partial/x") // unrelated tables, different names
    new java.io.File(s"$partial/x/quarantine_finance_transactions").mkdirs()
    an[IllegalArgumentException] should be thrownBy boot(partial, "x")
    new java.io.File(s"$partial/x/silver_finance_transactions").exists() shouldBe false
  }

  test("root outside the allowed storage root is rejected") {
    val dir = newDir()
    an[IllegalArgumentException] should be thrownBy
      BootstrapFinanceTables.bootstrap(spark, s"file:$dir/t", "s3a://dq-lake/")
    new java.io.File(s"$dir/t").exists() shouldBe false
  }
}
