package lg.scala.contrastMethod

import java.io.PrintWriter

import lg.scala.utils.ExperimentTools
import org.apache.spark.graphx.{EdgeContext, Graph, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by lg on 2018/5/9.
  */
object OneStepTransfer {

  def run(initialGraph: Graph[(Double, Int), Double], maxflowSubGraph: RDD[(Long, (Double, Int))], b: Double, threashold: Double) = {

    def sendMsg(edge: EdgeContext[(Double, Int), Double, (Double, Double, Int, Int)]) = { //发送的消息为 影响值，边权重，有问题，总个数
      if (edge.attr > threashold)
        edge.sendToDst((edge.srcAttr._1 * edge.attr, edge.attr,
          if (edge.srcAttr._2 == 1) edge.srcAttr._2
          else if (edge.srcAttr._2 == 2) 0
          else {
            if (edge.srcAttr._1 > 0.5) 1
            else 0
          }, 1))
    }

    def mergeMsg(m: (Double, Double, Int, Int), n: (Double, Double, Int, Int)) = {
      (m._1 + n._1, m._2 + n._2, m._3 + n._3, m._4 + n._4)
    }

    val perprocessGraph = initialGraph.cache()
    val message = perprocessGraph.aggregateMessages[(Double, Double, Int, Int)](sendMsg, mergeMsg).mapValues(x => (x._1 / x._2, x._3, x._4 - x._3)) //影响值，周边有问题个数，无问题个数
    message.join(maxflowSubGraph).map(x => (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._2._2)).repartition(1).saveAsTextFile("/user/lg/maxflowCredit/message")

    val GLscoreAndLabel = maxflowSubGraph.join(message).map(x => (x._2._2._1, x._2._1._2.toDouble))
    val RHscoreAndLabel = maxflowSubGraph.leftOuterJoin(message).map(x => (b * x._2._1._1 + (1 - b) * (x._2._2.getOrElse((0D, 0, 0)))._1, x._2._1._2.toDouble))


    (GLscoreAndLabel, RHscoreAndLabel)

  }

}