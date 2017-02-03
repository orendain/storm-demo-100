package hortonworks.tutorial

import java.io.PrintWriter
import java.nio.file.StandardOpenOption
import java.util

import better.files.File
import hortonworks.tutorial.models.TruckAndTrafficData
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple

class FileWriterBolt extends BaseRichBolt {

  // An instance of a writer so that we can write data to a file.
  private var fileWriter: PrintWriter = _

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
  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    fileWriter = File("/tmp/tutorial/storm/merged-output.txt").newPrintWriter(autoFlush = true)(Seq(StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE))
    outputCollector = collector
  }

  /**
    * Storm will call this method once for every tuple that flows in from the input streams this bolt is connected to.
    *
    * @param input The tuple for this bolt to process.
    */
  override def execute(input: Tuple) {
    fileWriter.println(input.getValue(0).asInstanceOf[TruckAndTrafficData].toCSV)
    outputCollector.ack(input)
  }

  /**
    * Tell storm which fields are emitted by the spout.
    * We are not emitting any fields back into Storm, so we leave this empty.
    */
  override def declareOutputFields(declarer: OutputFieldsDeclarer) {}

  /**
    * Called when this bolt is being shut down.
    * Note: This method is not guaranteed to be called.
    */
  override def cleanup() {
    fileWriter.close()
  }
}
