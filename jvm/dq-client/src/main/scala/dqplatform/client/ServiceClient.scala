package dqplatform.client

import com.fasterxml.jackson.databind.JsonNode

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{URLEncoder => Enc}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration

/** Lỗi gọi DQ service. status 0 = mạng/timeout. Message không bao giờ chứa body, URI hay token. */
final class ServiceException(val code: String, val status: Int) extends RuntimeException(s"$code ($status)")

trait ServiceApi {
  def request(method: String, path: Seq[String], query: Map[String, String] = Map.empty,
              body: Option[JsonNode] = None, idempotencyKey: Option[String] = None): JsonNode
}

final class HttpServiceApi(baseUrl: String, token: String, timeout: Duration) extends ServiceApi {
  private val base = baseUrl.stripSuffix("/")
  // HTTP/1.1: mặc định HttpClient thử nâng lên h2c (header Upgrade) với http://, uvicorn xử lý sai
  // request có body khi gặp header này -> mất body
  private val http = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .connectTimeout(timeout)
    .build()

  private def enc(s: String): String = Enc.encode(s, UTF_8).replace("+", "%20")

  override def request(method: String, path: Seq[String], query: Map[String, String],
                       body: Option[JsonNode], idempotencyKey: Option[String]): JsonNode = {
    val q = if (query.isEmpty) "" else query.toSeq.sortBy(_._1).map { case (k, v) => s"${enc(k)}=${enc(v)}" }.mkString("?", "&", "")
    val builder = HttpRequest.newBuilder(URI.create(base + path.map(p => "/" + enc(p)).mkString + q))
      .timeout(timeout)
      .header("Authorization", s"Bearer $token")
    idempotencyKey.foreach(builder.header("Idempotency-Key", _))
    val publisher = body match {
      case Some(b) =>
        builder.header("Content-Type", "application/json")
        HttpRequest.BodyPublishers.ofByteArray(Canonical.bytes(b))
      case None => HttpRequest.BodyPublishers.noBody()
    }
    val resp =
      try http.send(builder.method(method, publisher).build(), HttpResponse.BodyHandlers.ofString())
      catch { case _: Exception => throw new ServiceException("SERVICE_UNAVAILABLE", 0) }
    if (resp.statusCode / 100 != 2) throw new ServiceException(errorCode(resp.body), resp.statusCode)
    try Canonical.parse(resp.body)
    catch { case _: Exception => throw new ServiceException("SERVICE_UNAVAILABLE", resp.statusCode) }
  }

  private def errorCode(body: String): String =
    try {
      val c = Canonical.parse(body).path("error").path("code")
      if (c.isTextual) c.asText else "SERVICE_UNAVAILABLE"
    } catch { case _: Exception => "SERVICE_UNAVAILABLE" }
}
