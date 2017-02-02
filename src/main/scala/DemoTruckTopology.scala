import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.{Config, LocalCluster}

import scala.concurrent.duration.SECONDS

object DemoTruckTopology {

  def main(args: Array[String]): Unit = {

    val builder = new TopologyBuilder()
    builder.setSpout("truckDataFromFile", new FileReaderSpout("truck-input.txt"))
    builder.setSpout("trafficDataFromFile", new FileReaderSpout("traffic-input.txt"))
    builder.setBolt("windowedMergeBolt", new MergeBolt().withTumblingWindow(new BaseWindowedBolt.Duration(5, SECONDS)))
      .shuffleGrouping("truckDataFromFile")
      .shuffleGrouping("trafficDataFromFile")
    builder.setBolt("mergedDataToFile", new FileWriterBolt()).shuffleGrouping("windowedMergeBolt")

    val stormConfig = new Config()
    stormConfig.setDebug(true)
    stormConfig.setNumWorkers(1)
    stormConfig.setMaxTaskParallelism(1)

    val cluster = new LocalCluster()

    cluster.submitTopology("truckingTopology", stormConfig, builder.createTopology())
    //StormSubmitter.submitTopologyWithProgressBar("truckingTopology", stormConfig, builder.createTopology())
  }

}
