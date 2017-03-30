package hortonworks.tutorial

import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.{Config, StormSubmitter}

import scala.concurrent.duration.SECONDS

/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */
object TruckingTopology100 {

  def main(args: Array[String]): Unit = {

    // Create a TopologyBuilder, which allows us to build our topology one component at a time.
    val builder = new TopologyBuilder()

    // Create a FileReaderSpout and add it to the builder.
    builder.setSpout("truckDataFromFile", new FileReaderSpout("/tmp/tutorial/storm/truck-input.txt"))

    // Create a second FileReaderSpout and add it to the builder.
    builder.setSpout("trafficDataFromFile", new FileReaderSpout("/tmp/tutorial/storm/traffic-input.txt"))

    // Create a MergeBolt, which reads data from two sources (truckDataFromFile and trafficDataFromFile)
    // and add it to the builder.
    builder.setBolt("windowedMergeBolt", new MergeBolt().withTumblingWindow(new BaseWindowedBolt.Duration(5, SECONDS)))
      .shuffleGrouping("truckDataFromFile")
      .shuffleGrouping("trafficDataFromFile")

    // Finally, create a FileWriterBolt, which reads data from the MergeBolt above, and add it to the builder.
    builder.setBolt("mergedDataToFile", new FileWriterBolt("/tmp/tutorial/storm/merged-output.txt")).shuffleGrouping("windowedMergeBolt")

    // Storm provides a Config class which makes it easy to set configuration options for a topology.
    // To keep things simple, we create one worker and set parallelism to one
    // (see following tutorials for a deeper explanation on these).
    val stormConfig = new Config()
    stormConfig.setDebug(false)
    stormConfig.setNumWorkers(1)
    stormConfig.setMaxTaskParallelism(1)

    // Let's submit this sucker to our cluster!
    // We give the topology the name of "truckingTopology100"
    // stormConfig tells Storm the configurations we want to use
    // builder.createTopology() creates a blueprint of our topology to feed to Storm
    StormSubmitter.submitTopologyWithProgressBar("TruckingTopology100", stormConfig, builder.createTopology())


    // The following is used for local mode. Can ignore.
    //val localCluster = new LocalCluster()
    //localCluster.submitTopology("truckingTopology100", stormConfig, builder.createTopology())
  }

}
