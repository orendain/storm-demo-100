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

    //val localCluster = new LocalCluster()
    //localCluster.submitTopology("truckingTopology100", stormConfig, builder.createTopology())
    StormSubmitter.submitTopologyWithProgressBar("truckingTopology100", stormConfig, builder.createTopology())
  }

}
