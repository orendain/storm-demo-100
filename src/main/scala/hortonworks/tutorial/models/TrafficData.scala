package hortonworks.tutorial.models

/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */
case class TrafficData(eventTime: Long, routeId: Int, congestionLevel: Int)

object TrafficData {
  def apply(str: String): TrafficData = {
    val Array(eventTime, routeId, congestionLevel) = str.split("\\|")
    new TrafficData(eventTime.toLong, routeId.toInt, congestionLevel.toInt)
  }
}
