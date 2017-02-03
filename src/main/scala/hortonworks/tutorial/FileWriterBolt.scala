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

  private var fileWriter: PrintWriter = _
  private var outputCollector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    fileWriter = File("/tmp/tutorial/storm/merged-output.txt").newPrintWriter(autoFlush = true)(Seq(StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE))
    outputCollector = collector
  }

  override def execute(input: Tuple) {
    fileWriter.println(input.getValue(0).asInstanceOf[TruckAndTrafficData].toCSV)
    outputCollector.ack(input)
  }

  // Not outputting fields, so we can leave this empty
  override def declareOutputFields(declarer: OutputFieldsDeclarer) {}

  /**
    * Called when this spout is being shut down.
    * Note: This method is not guaranteed to be called.
    */
  override def cleanup() {
    fileWriter.close()
  }
}
