package hortonworks.tutorial

import java.io.BufferedReader
import java.util

import better.files._
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}

class FileReaderSpout(filePath: String) extends BaseRichSpout {

  // An instance of a reader so that we can read pull data in from a file.
  private var fileReader: BufferedReader = _

  // The OutputCollector allows this bolt to emit Tuples at anytime.  Once the instance of this collector is saved
  // inside the open() method, we can emit Tuples from within nextTuple(), close(), or even asynchronous threads.
  private var outputCollector: SpoutOutputCollector = _

  /**
    * This method is meant to prepare the spout. It is called once when a task prepares to set up this spout.
    * It provides the spout with the environment in which the spout executes.
    *
    * @param conf The Storm configuration for this spout.
    * @param context This object can be used to get information about this task.
    * @param collector The collector is thread-safe and is used to emit tuples from this spout. Tuples can be emitted at any time in any method.
    */
  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector) {
    fileReader = File(filePath).newBufferedReader
    outputCollector = collector
  }

  /**
    * Storm will call this method repeatedly to pull tuples from the spout
    */
  override def nextTuple() {
    val line = fileReader.readLine()
    outputCollector.emit(new Values(line))
    //Thread sleep 1
  }

  /**
    * Tell storm which fields are emitted by the spout
    */
  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("data"))
  }

  /**
    * Called when this spout is being shut down.
    * Note: This method is not guaranteed to be called.
    */
  override def close() {
    fileReader.close()
  }
}
