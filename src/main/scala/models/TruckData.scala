package models

case class TruckData(eventTime: Long, truckId: Int, driverId: Int, driverName: String,
                     routeId: Int, routeName: String, latitude: Double, longitude: Double,
                     speed: Int, eventType: String)

object TruckData {
  def apply(str: String): TruckData = {
    val Array(eventTime, truckId, driverId, driverName, routeId, routeName, latitude, longitude, speed, eventType) = str.split("\\|")
    new TruckData(eventTime.toLong, truckId.toInt, driverId.toInt, driverName, routeId.toInt, routeName, latitude.toDouble, longitude.toDouble, speed.toInt, eventType)
  }
}