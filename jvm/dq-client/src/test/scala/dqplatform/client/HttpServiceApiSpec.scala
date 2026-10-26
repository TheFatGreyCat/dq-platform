package dqplatform.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class HttpServiceApiSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private case class Seen(method: String, rawPath: String, rawQuery: String, headers: Map[String, String], body: Array[Byte])

  private val mapper = new ObjectMapper()
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  private val last = new AtomicReference[Seen]()

  private def reply(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(UTF_8)
    ex.sendResponseHeaders(status, bytes.length.toLong)
    ex.getResponseBody.write(bytes)
    ex.close()
  }

  override def beforeAll(): Unit = {
    server.createContext("/", (ex: HttpExchange) => {
      val h = Seq("Authorization", "Idempotency-Key", "Content-Type", "Upgrade")
        .flatMap(k => Option(ex.getRequestHeaders.getFirst(k)).map(k -> _)).toMap
      last.set(Seen(ex.getRequestMethod, ex.getRequestURI.getRawPath, ex.getRequestURI.getRawQuery, h,
        ex.getRequestBody.readAllBytes()))
      ex.getRequestURI.getPath match {
        case "/conflict" => reply(ex, 409, """{"error":{"code":"CONTENT_CONFLICT","message":"x"}}""")
        case "/bad"      => reply(ex, 502, "<html>bad gateway</html>")
        case "/garbage"  => reply(ex, 200, "not json")
        case _           => reply(ex, 200, """{"ok":true}""")
      }
    })
    server.start()
  }

  override def afterAll(): Unit = server.stop(0)

  private val url = () => s"http://127.0.0.1:${server.getAddress.getPort}/"
  private def api = new HttpServiceApi(url(), "secret-token", Duration.ofSeconds(5))

  test("path segments are encoded individually; headers and canonical body are sent") {
    val body = mapper.readTree("""{"b":1,"a":[2,{"d":1,"c":2}]}""")
    api.request("POST", Seq("api", "v2", "datasets", "a/b c", "x"), Map("k y" -> "v&1"), Some(body), Some("key-1"))
      .get("ok").asBoolean shouldBe true
    val s = last.get()
    s.method shouldBe "POST"
    s.rawPath shouldBe "/api/v2/datasets/a%2Fb%20c/x"
    s.rawQuery shouldBe "k%20y=v%261"
    s.headers("Authorization") shouldBe "Bearer secret-token"
    s.headers("Idempotency-Key") shouldBe "key-1"
    s.headers("Content-Type") shouldBe "application/json"
    s.headers.get("Upgrade") shouldBe None
    s.body shouldBe Canonical.bytes(body)
    new String(s.body, UTF_8) shouldBe """{"a":[2,{"c":2,"d":1}],"b":1}"""
  }

  test("GET without body sends no body and no idempotency key") {
    api.request("GET", Seq("health"))
    val s = last.get()
    (s.method, s.body.length) shouldBe (("GET", 0))
    s.headers.get("Idempotency-Key") shouldBe None
  }

  test("409 error body maps to code and status") {
    val e = intercept[ServiceException](api.request("GET", Seq("conflict")))
    (e.code, e.status) shouldBe (("CONTENT_CONFLICT", 409))
  }

  test("non-JSON error and non-JSON success map to SERVICE_UNAVAILABLE with HTTP status") {
    val e = intercept[ServiceException](api.request("GET", Seq("bad")))
    (e.code, e.status) shouldBe (("SERVICE_UNAVAILABLE", 502))
    val g = intercept[ServiceException](api.request("GET", Seq("garbage")))
    g.code shouldBe "SERVICE_UNAVAILABLE"
  }

  test("connection refused -> status 0; message never leaks token, URI or body") {
    val dead = new HttpServiceApi("http://127.0.0.1:1", "secret-token", Duration.ofSeconds(1))
    val e = intercept[ServiceException](dead.request("GET", Seq("hidden-path")))
    (e.code, e.status) shouldBe (("SERVICE_UNAVAILABLE", 0))
    e.getMessage should not include "secret-token"
    e.getMessage should not include "hidden-path"
    e.getMessage should not include "127.0.0.1"
    val c = intercept[ServiceException](api.request("GET", Seq("conflict")))
    c.getMessage should not include "message"
  }
}
