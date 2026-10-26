package dqplatform.jobs

import dqplatform.client.{Canonical, DeltaRuntime, SchemaHash}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class JobsToolsSpec extends AnyFunSuite with Matchers with JobsFixture {

  test("bootstrap precreates both tables partitioned by dt, retention property absent, refs match the persisted schema") {
    val dir = newDir()
    val reg = BootstrapTables.bootstrap(spark, s"$dir/t")
    for ((name, extra) <- Seq("silver" -> false, "quarantine" -> true)) {
      val ref = reg.get(name)
      val st = DeltaRuntime.state(spark, ref.get("uri").asText())
      st.partitionColumns shouldBe Seq("dt")
      st.properties should not contain key(DeltaRuntime.RetentionProperty)
      st.ref.tableId shouldBe ref.get("table_id").asText()
      st.ref.schemaHash shouldBe ref.get("schema_hash").asText()
      spark.read.format("delta").load(ref.get("uri").asText()).schema("dt").dataType.typeName shouldBe "date"
      st.schemaJson.contains("_dq_errors") shouldBe extra
    }
    reg.get("business_schema").size() shouldBe 7
    reg.get("partition_schema").get(0).get("type").asText() shouldBe "date"
    reg.get("retention").get("retry_window_seconds").asLong() should be > 0L
    an[Exception] should be thrownBy BootstrapTables.bootstrap(spark, s"$dir/t")
  }

  test("restore probe lists every dq token with its partitions; empty overwrite partition comes from userMetadata") {
    val dir = newDir()
    val reg = BootstrapTables.bootstrap(spark, s"$dir/t")
    val (silver, quar) = (reg.get("silver").get("uri").asText(), reg.get("quarantine").get("uri").asText())
    import spark.implicits._
    val ts = java.sql.Timestamp.valueOf("2026-10-17 12:00:00")
    val d17 = java.sql.Date.valueOf("2026-10-17")
    val one = Seq(("T1", "S", "849", BigDecimal(1), "OK", "HN", ts, d17))
      .toDF("transaction_id", "subscriber_id", "msisdn", "amount", "status", "province_code", "event_time", "dt").withColumn("amount", org.apache.spark.sql.functions.col("amount").cast("decimal(18,2)"))
    val e1 = "11111111-1111-1111-1111-111111111111"
    one.write.format("delta").mode("overwrite").option("replaceWhere", "dt = DATE '2026-10-17'")
      .option("txnAppId", s"dq:$e1:silver").option("txnVersion", 1L)
      .option("userMetadata", s"""{"partition_values":{"dt":"2026-10-17"}}""").save(silver)
    val e2 = "22222222-2222-2222-2222-222222222222"
    one.limit(0).write.format("delta").mode("overwrite").option("replaceWhere", "dt = DATE '2026-10-18'")
      .option("txnAppId", s"dq:$e2:silver").option("txnVersion", 1L)
      .option("userMetadata", s"""{"partition_values":{"dt":"2026-10-18"}}""").save(silver)
    val pages = RestoreProbe.scan(spark, Map("silver" -> silver, "quarantine" -> quar), "sched-ref")
    pages should have size 1
    val p = pages.head
    p.get("final_page").asBoolean() shouldBe true
    p.get("page").asLong() shouldBe 0L
    p.get("scans").size() shouldBe 2
    p.get("scans").elements().asScala.map(_.get("destination").asText()).toSet shouldBe Set("silver", "quarantine")
    val toks = p.get("tokens").elements().asScala.map(t =>
      (t.get("txn_app_id").asText(), t.get("partition_values").get("dt").asText())).toSet
    toks shouldBe Set((s"dq:$e1:silver", "2026-10-17"), (s"dq:$e2:silver", "2026-10-18"))
    p.get("tokens").get(0).get("destination").asText() shouldBe "silver"
    val before = DeltaRuntime.snapshot(spark, silver).version
    RestoreProbe.scan(spark, Map("silver" -> silver, "quarantine" -> quar), "sched-ref")
    DeltaRuntime.snapshot(spark, silver).version shouldBe before
  }

  test("restore probe on a missing target fails with TARGET_NOT_INITIALIZED, never an empty complete scan") {
    val dir = newDir()
    val reg = BootstrapTables.bootstrap(spark, s"$dir/t")
    val e = intercept[dqplatform.client.ProtocolException](
      RestoreProbe.scan(spark, Map("silver" -> reg.get("silver").get("uri").asText(), "quarantine" -> s"$dir/none"), "r"))
    e.code shouldBe "TARGET_NOT_INITIALIZED"
  }

  test("maintenance deletes only unpinned candidate table directories inside the candidate root") {
    val dir = newDir()
    val root = s"file:$dir/cand"
    def item(uri: String, pinned: Boolean) = Canonical.parse(
      s"""{"uri":"$uri","execution_id":"e","attempt_id":"a","retain_until":null,"pinned":$pinned,"reason":null}""")
    val free = new java.io.File(s"$dir/cand/e/a/raw"); free.mkdirs()
    val held = new java.io.File(s"$dir/cand/e/b/raw"); held.mkdirs()
    val outside = new java.io.File(s"$dir/silver/raw"); outside.mkdirs()
    val items = Seq(item(s"$root/e/a/raw", pinned = false), item(s"$root/e/b/raw", pinned = true),
      item(s"file:$dir/silver/raw", pinned = false), item(s"$root/e/a", pinned = false))
    val removed = Maintenance.run(spark, items, root)
    removed.map(_.get("uri").asText()) shouldBe Seq(s"$root/e/a/raw")
    free.exists() shouldBe false
    held.exists() shouldBe true
    outside.exists() shouldBe true
    new java.io.File(s"$dir/cand/e/a").exists() shouldBe true
  }
}
