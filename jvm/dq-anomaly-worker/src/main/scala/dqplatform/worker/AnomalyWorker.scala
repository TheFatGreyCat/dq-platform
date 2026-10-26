package dqplatform.worker

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.sun.net.httpserver.{HttpExchange, HttpServer}

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.util.concurrent.{ArrayBlockingQueue, Callable, Executors, RejectedExecutionException, ThreadPoolExecutor, TimeUnit}

object AnomalyWorker {
  val MaxBodyBytes = 1048576
  val MaxItems = 20
  private val mapper = new ObjectMapper()

  private def respond(ex: HttpExchange, status: Int, body: JsonNode): Unit = {
    val bytes = mapper.writeValueAsBytes(body)
    ex.getResponseHeaders.set("Content-Type", "application/json")
    ex.sendResponseHeaders(status, bytes.length.toLong)
    ex.getResponseBody.write(bytes)
    ex.close()
  }

  private def err(ex: HttpExchange, status: Int, code: String): Unit = {
    val n = JsonNodeFactory.instance.objectNode()
    n.putObject("error").put("code", code)
    respond(ex, status, n)
  }

  /** Reads at most MaxBodyBytes; None when the body is larger. */
  private def readBody(ex: HttpExchange): Option[Array[Byte]] = {
    val in = ex.getRequestBody
    val buf = new ByteArrayOutputStream()
    val chunk = new Array[Byte](8192)
    var n = in.read(chunk)
    while (n >= 0) {
      buf.write(chunk, 0, n)
      if (buf.size > MaxBodyBytes) return None
      n = in.read(chunk)
    }
    Some(buf.toByteArray)
  }

  /** Binds 127.0.0.1 only; port 0 = ephemeral. */
  def start(port: Int): HttpServer = {
    val compute = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue[Runnable](4))
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0)
    server.setExecutor(Executors.newFixedThreadPool(8))
    server.createContext("/health", (ex: HttpExchange) => {
      val n = JsonNodeFactory.instance.objectNode(); n.put("status", "UP"); respond(ex, 200, n)
    })
    server.createContext("/v1/evaluate", (ex: HttpExchange) => {
      try {
        if (ex.getRequestMethod != "POST") err(ex, 405, "METHOD_NOT_ALLOWED")
        else readBody(ex) match {
          case None => err(ex, 413, "PAYLOAD_TOO_LARGE")
          case Some(bytes) =>
            val items = try Option(mapper.readTree(bytes)).map(_.get("items")).orNull catch { case _: Exception => null }
            if (items == null || !items.isArray || items.size > MaxItems) err(ex, 400, "INVALID_REQUEST")
            else {
              val future = compute.submit(new Callable[JsonNode] {
                def call(): JsonNode = {
                  val r = JsonNodeFactory.instance.objectNode()
                  r.set[JsonNode]("results", AnomalyEvaluator.evaluateBatch(items))
                  r
                }
              })
              respond(ex, 200, future.get())
            }
        }
      } catch {
        case _: RejectedExecutionException => err(ex, 503, "BUSY")
        case _: Exception => err(ex, 500, "INTERNAL")
      }
    })
    server.start()
    server
  }

  private def probeItem(prev: String, cur: String): JsonNode = {
    val n = JsonNodeFactory.instance.objectNode()
    n.put("metric_definition_hash", "probe").put("scope", "PARTITION").put("stage", "CLEAN")
    n.put("lower", "0.8").put("upper", "1.2")
    val b = n.putArray("baseline")
    if (prev != null) b.addObject().put("execution_id", "p").put("event_time", "2026-01-01T00:00:00.000000Z").put("value", prev)
    n.put("current", cur)
    n
  }

  def probe(): (Boolean, JsonNode) = {
    val cases = Seq(("100", "130", "ANOMALY"), ("100", "120", "NORMAL"), ("0", "5", "ERROR"), (null, "5", "INSUFFICIENT_HISTORY"))
    val out = JsonNodeFactory.instance.objectNode()
    val checks = out.putArray("checks")
    val ok = cases.map { case (p, c, expected) =>
      val actual = AnomalyEvaluator.evaluate(probeItem(p, c)).get("status").asText
      checks.addObject().put("name", s"$p->$c").put("expected", expected).put("actual", actual)
      actual == expected
    }.forall(identity)
    out.put("status", if (ok) "PASS" else "FAIL")
    (ok, out)
  }

  def main(args: Array[String]): Unit = {
    if (args.contains("--probe")) {
      val (ok, out) = probe()
      println(mapper.writeValueAsString(out))
      sys.exit(if (ok) 0 else 1)
    }
    val port = args.sliding(2).collectFirst { case Array("--port", p) => p }
      .orElse(sys.env.get("DQ_WORKER_PORT")).map(_.toInt).getOrElse(8081)
    start(port)
  }
}
