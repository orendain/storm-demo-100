package hortonworks.tutorial

import java.util

import com.typesafe.scalalogging.Logger
import hortonworks.tutorial.models.{TrafficData, TruckAndTrafficData, TruckData}
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.windowing.TupleWindow

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}

class MergeBolt extends BaseWindowedBolt {

  private lazy val log = Logger(this.getClass)

  // The OutputCollector allows this bolt to emit Tuples at anytime.  Once the instance of this collector is saved
  // inside the prepare() method, we can emit Tuples from within execute(), cleanup(), or even asynchronous threads.
  private var outputCollector: OutputCollector = _

  /**
    * This method is meant to prepare the bolt. It is called once when a task prepares to set up this bolt.
    * It provides the bolt with the environment in which the bolt executes.
    *
    * @param stormConf The Storm configuration for this spout.
    * @param context This object can be used to get information about this task.
    * @param collector The collector is thread-safe and is used to emit tuples from this spout. Tuples can be emitted at any time in any method.
    */
  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    outputCollector = collector
  }

  /**
    * Storm will call this method once for every tuple that flows in from the input streams this bolt is connected to.
    *
    * @param inputWindow A window (much like a list) of tuples for this bolt to process.
    */
  override def execute(inputWindow: TupleWindow) = {
    // Collections to collect data into
    val truckDataPerRoute = mutable.HashMap.empty[Int, ListBuffer[TruckData]]
    val trafficDataPerRoute = mutable.HashMap.empty[Int, ListBuffer[TrafficData]]

    // Process each one of the tuples captured in the input window, separating data into bins according to routeId
    inputWindow.get().asScala.foreach { tuple =>

      log.info(s"Exec: ${tuple.getSourceComponent} ${tuple.getStringByField("data")}")

      // Deserialize each tuple and convert it into its proper case class
      // (e.g. hortonworks.tutorial.models.TruckData or hortonworks.tutorial.models.TrafficData)
      // and add that data to a list of either truck data or traffic data.
      tuple.getSourceComponent match {

        case "truckDataFromFile" =>
          val data = TruckData(tuple.getStringByField("data"))
          truckDataPerRoute += (data.routeId -> (truckDataPerRoute.getOrElse(data.routeId, ListBuffer.empty[TruckData]) += data))

        case "trafficDataFromFile" =>
          val data = TrafficData(tuple.getStringByField("data"))
          trafficDataPerRoute += (data.routeId -> (trafficDataPerRoute.getOrElse(data.routeId, ListBuffer.empty[TrafficData]) += data))
      }

      // We let the system know that we've processed each tuple.  This allows Storm to guarantee each message has been
      // processed from beginning to end.
      outputCollector.ack(tuple)
    }

    // Now that we've collected all of the tuples, pass the work off to another method.
    // This process could be run asynchronously on another thread, Storm allows that.
    processAndEmitData(truckDataPerRoute, trafficDataPerRoute)
  }

  /**
    * Tell storm which fields are emitted by the spout.
    *
    * Here, we are emitting tuples of a single field.  We're nicknaming this field "mergedData".
    */
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("data"))
  }

  /**
    * Correlate the two sets of data so that traffic data is merged with truck data.
    * After correlation, emit the data into an output stream.
    *
    * Note: the specific inner-workings of this method aren't important, except for how we emit the resulting
    * tuple using outputCollector.emit()
    */
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

                // Now that we've merged data, push the combined tuple of data out onto the output stream.
                outputCollector.emit(new Values(mergedData))
            }
          }
      }
    }
  }
}
