package hortonworks.tutorial

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.windowing.TupleWindow

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}

class MergeBolt extends BaseWindowedBolt {

  private var outputCollector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    outputCollector = collector
  }

  override def execute(inputWindow: TupleWindow) = {
    // Collections to collect data into
    val truckDataPerRoute = mutable.HashMap.empty[Int, ListBuffer[TruckData]]
    val trafficDataPerRoute = mutable.HashMap.empty[Int, ListBuffer[TrafficData]]

    // Process each one of the tuples captured in the input window, separating data into bins according to routeId
    inputWindow.get().foreach { tuple =>
      // Deserialize each tuple and convert it into its proper case class (e.g. hortonworks.tutorial.models.TruckData or hortonworks.tutorial.models.TrafficData
      tuple.getSourceComponent match {
        case "truckDataFromFile" =>
          val data = TruckData(tuple.getString(0))
          truckDataPerRoute += (data.routeId -> (truckDataPerRoute.getOrElse(data.routeId, ListBuffer.empty[TruckData]) += data))
        case "trafficDataFromFile" =>
          val data = TrafficData(tuple.getString(0))
          trafficDataPerRoute += (data.routeId -> (trafficDataPerRoute.getOrElse(data.routeId, ListBuffer.empty[TrafficData]) += data))
      }
      outputCollector.ack(tuple)
    }

    processAndEmitData(truckDataPerRoute, trafficDataPerRoute)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("mergedData"))
  }

  private def processAndEmitData(truckDataPerRoute: Map[Int, ListBuffer[TruckData]],
                                 trafficDataPerRoute: Map[Int, ListBuffer[TrafficData]]) {
    // For each hortonworks.tutorial.models.TruckData object, find the hortonworks.tutorial.models.TrafficData object with the closest timestamp
    truckDataPerRoute.foreach { case (routeId, truckDataList) =>
      trafficDataPerRoute.get(routeId) match {
        case None => // No traffic data for this routeId, so drop/ignore truck data
        case Some(trafficDataList) =>
          truckDataList foreach { truckData =>
            trafficDataList.sortBy(data => math.abs(data.eventTime - truckData.eventTime)).headOption match {
              case None => // Window didn't capture any traffic data for this truck's route
              case Some(trafficData) =>
                val mergedData = TruckAndTrafficData(truckData.eventTime, truckData.truckId, truckData.driverId,
                  truckData.driverName, truckData.routeId, truckData.routeName, truckData.latitude, truckData.longitude,
                  truckData.speed, truckData.eventType, trafficData.congestionLevel)
                outputCollector.emit(new Values(mergedData))
            }
          }
      }
    }
  }
}
