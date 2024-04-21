/*
 * Created by Sidharth Babu
 */

package org.apache.openwhisk.core.entity

import org.apache.openwhisk.core.ConfigKeys
import pureconfig._
import pureconfig.generic.auto._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import spray.json._

//Units are Mbit
case class NetworkLimitConfig(min: Int, max: Int, std: Int)

/**
 * NetworkLimit encapsulates allowed network bandwidth in a single container for an action. The limit must be within a
 * permissible range, dictated by the capability of the invoker. 
 * 
 * It is a value type (hence == is .equals, immutable and cannot be assigned null).
 * The constructor is private so that argument requirements are checked and normalized
 * before creating a new instance.
 *
 * @param bandwidth the bandwidth limit in Mbit for the action
 */
protected[entity] class NetworkLimit private (val bandwidth: Int) extends AnyVal

protected[core] object NetworkLimit extends ArgNormalizer[NetworkLimit] {
    val config = loadConfigOrThrow[NetworkLimitConfig](ConfigKeys.network)

  /** These values are set once at the beginning. Dynamic configuration updates are not supported at the moment. */
  protected[core] val MIN_NETWORK: Int = config.min
  protected[core] val MAX_NETWORK: Int = config.max
  protected[core] val STD_NETWORK: Int = config.std

  /** A singleton CpuLimit with default value */
  protected[core] val standardNetworkLimit = NetworkLimit(STD_NETWORK)

  /** Gets CpuLimit with default value */
  protected[core] def apply(): NetworkLimit = standardNetworkLimit

  /**
   * Creates CpuLimit for limit, iff limit is within permissible range.
   *
   * @param cores the limit, must be within permissible range
   * @return CpuLimit with limit set
   * @throws IllegalArgumentException if limit does not conform to requirements
   */
  @throws[IllegalArgumentException]
  protected[core] def apply(bandwidth: Int): NetworkLimit = {
    require(bandwidth >= MIN_NETWORK, s"Network Bandwidth $bandwidth Mbit below allowed threshold of $MIN_NETWORK Mbit")
    require(cores <= MAX_NETWORK, s"Network Bandwidth $bandwidth Mbit exceeds allowed threshold of $MAX_NETWORK Mbit")
    new NetworkLimit(bandwidth)
  }

  override protected[core] implicit val serdes = new RootJsonFormat[NetworkLimit] {
    def write(m: NetworkLimit) = JsNumber(m.bandwidth)

    def read(value: JsValue) = {
      Try {
        val JsNumber(c) = value
        require(c.isWhole, "Network bandwidth limit must be whole number")

        NetworkLimit(c.toInt)
      } match {
        case Success(limit)                       => limit
        case Failure(e: IllegalArgumentException) => deserializationError(e.getMessage, e)
        case Failure(e: Throwable)                => deserializationError("Network Bandwidth limit malformed", e)
      }
    }
  }
}
