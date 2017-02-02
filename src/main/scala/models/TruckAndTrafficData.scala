package models

/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */
case class TruckAndTrafficData(eventTime: Long, truckId: Int, driverId: Int, driverName: String,
                               routeId: Int, routeName: String, latitude: Double, longitude: Double,
                               speed: Int, eventType: String, congestionLevel: Int) {

  lazy val toCSV: String = s"$eventTime|$truckId|$driverId|$driverName|$routeId|$routeName|$latitude|$longitude|$speed|$eventType|$congestionLevel"
}
