package dqplatform.worker

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

class StandaloneWorkerSpec extends AnyFunSuite {
  private val m = new ObjectMapper()
  private def post(port: Int, body: String) = HttpClient.newHttpClient().send(
    HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port/v1/evaluate")).POST(HttpRequest.BodyPublishers.ofString(body)).build(),
    HttpResponse.BodyHandlers.ofString())

  test("batch evaluation over HTTP and oversize rejection") {
    val server = AnomalyWorker.start(0)
    val port = server.getAddress.getPort
    try {
      val item = """{"metric_definition_hash":"h","scope":"PARTITION","stage":"CLEAN","lower":"0.8","upper":"1.2","baseline":[{"execution_id":"e","event_time":"t","value":"100"}],"current":"130"}"""
      val ok = post(port, s"""{"items":[$item]}""")
      assert(ok.statusCode == 200)
      assert(m.readTree(ok.body).get("results").get(0).get("status").asText == "ANOMALY")

      val big = post(port, "x" * (AnomalyWorker.MaxBodyBytes + 1))
      assert(big.statusCode == 413)
      assert(m.readTree(big.body).get("error").get("code").asText == "PAYLOAD_TOO_LARGE")
      assert(post(port, "{}").statusCode == 400)
    } finally server.stop(0)
  }
}
