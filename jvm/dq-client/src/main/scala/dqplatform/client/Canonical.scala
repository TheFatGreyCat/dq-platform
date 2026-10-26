package dqplatform.client

import com.fasterxml.jackson.core.{JsonFactoryBuilder, JsonParser, StreamReadFeature}
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.collection.JavaConverters._

/**
 * Canonical JSON v1, byte-identical with dq-service/canonical.py (golden vectors in
 * src/test/resources/protocol-v3.json): UTF-8, object keys sorted by code point, compact
 * separators, numbers as plain normalized decimals, Python-compatible string escaping.
 */
object Canonical {

  val Version = "v1"

  /** Strict mapper: duplicate keys rejected, NaN/Infinity rejected, non-integers as BigDecimal. */
  val mapper: ObjectMapper = {
    val f = new JsonFactoryBuilder()
      .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
      .build()
    val m = new ObjectMapper(f)
    m.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    m.enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
    m.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    m.disable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)
    m
  }

  def parse(json: String): JsonNode = mapper.readTree(json)

  def bytes(node: JsonNode): Array[Byte] = {
    val sb = new java.lang.StringBuilder
    write(node, sb)
    sb.toString.getBytes(StandardCharsets.UTF_8)
  }

  def string(node: JsonNode): String = new String(bytes(node), StandardCharsets.UTF_8)

  def hash(node: JsonNode): String = sha256(bytes(node))

  def sha256(data: Array[Byte]): String =
    "sha256:" + MessageDigest.getInstance("SHA-256").digest(data).map(b => f"${b & 0xff}%02x").mkString

  /** Plain normalized decimal: no exponent, trailing zeros stripped, -0 -> 0. */
  def decimal(v: java.math.BigDecimal): String = {
    if (v.signum == 0) "0" else v.stripTrailingZeros.toPlainString
  }

  private def write(n: JsonNode, sb: java.lang.StringBuilder): Unit = n.getNodeType match {
    case JsonNodeType.OBJECT =>
      val keys = n.fieldNames().asScala.toVector.sortWith(codePointLess)
      sb.append('{')
      keys.zipWithIndex.foreach { case (k, i) =>
        if (i > 0) sb.append(',')
        str(k, sb)
        sb.append(':')
        write(n.get(k), sb)
      }
      sb.append('}')
    case JsonNodeType.ARRAY =>
      sb.append('[')
      n.elements().asScala.zipWithIndex.foreach { case (e, i) =>
        if (i > 0) sb.append(',')
        write(e, sb)
      }
      sb.append(']')
    case JsonNodeType.STRING => str(n.textValue(), sb)
    case JsonNodeType.NUMBER =>
      if (n.isIntegralNumber) sb.append(n.bigIntegerValue().toString)
      else if (n.isFloat || n.isDouble) throw new IllegalArgumentException("binary floating point not canonical")
      else sb.append(decimal(n.decimalValue()))
    case JsonNodeType.BOOLEAN => sb.append(if (n.booleanValue()) "true" else "false")
    case JsonNodeType.NULL    => sb.append("null")
    case other                => throw new IllegalArgumentException(s"unsupported JSON node $other")
  }

  private def codePointLess(a: String, b: String): Boolean = {
    var i = 0
    var j = 0
    while (i < a.length && j < b.length) {
      val ca = a.codePointAt(i)
      val cb = b.codePointAt(j)
      if (ca != cb) return ca < cb
      i += Character.charCount(ca)
      j += Character.charCount(cb)
    }
    (a.length - i) < (b.length - j)
  }

  /** Same escaping as Python json.dumps(ensure_ascii=False). */
  private def str(s: String, sb: java.lang.StringBuilder): Unit = {
    sb.append('"')
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      c match {
        case '"'  => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case _ if c < 0x20 => sb.append(f"\\u${c.toInt}%04x")
        case _ => sb.append(c)
      }
      i += 1
    }
    sb.append('"')
  }
}
