package lg.scala.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by lg on 2017/7/17.
  */
object ExperimentTools {
  def verify(sc: SparkContext, maxflowCredit: Array[(VertexId, Double)], selectGraph: Graph[(Double, Boolean), Double]) = {

    val B = sc.parallelize(maxflowCredit).join(selectGraph.vertices).map(x => (x._2._1, (x._1, x._2._2._2))).repartition(1).sortByKey(false).map(x => (x._2._1, x._1, x._2._2))
    //前2000名波动性
    var i = 100
    var result = HashMap[VertexId, Double]()
    while (i <= 7000) {
      val P = sc.parallelize(B.top(i)).filter(_._3 == true).count() / i.toDouble
      result.put(i, P.%(3))
      i += 100
    }
    (B.repartition(1), sc.parallelize(result.toSeq))
  }
}
