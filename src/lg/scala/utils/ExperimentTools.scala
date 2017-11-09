package lg.scala.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by lg on 2017/7/17.
  */
object ExperimentTools {

  def verify3(sc: SparkContext, maxflowCredit: RDD[(VertexId, Double)], selectGraph: RDD[(Long, (Double, Boolean))]) = {
    /**
      * B为最大流计算排名，A为原始纳税信用等级排名;前100中、前200、前300、…中标志为true的个数占所有标志为true的个数的比值
      */
    val A = selectGraph.map(v => (v._2._1, (v._2._2, v._1))).repartition(1).sortByKey(true).map(v => (v._2._2, v._1, v._2._1))
    val B = maxflowCredit.join(selectGraph).map(x => (x._2._1, (x._1, x._2._2._2))).repartition(1).sortByKey(true).map(x => (x._2._1, x._1, x._2._2))
    //按100名波动清况
    var i = 100
    var result = HashMap[VertexId, (Double, Double)]() //节点id,最大流命中率，原始命中率
    var number = A.count()
    if (number > 10000) {
      number = 10000
    }
    while (i <= number) {
      //  val P = B.repartition(1).top(i)//sc.parallelize(B.top(i)).filter(_._3 == true).count() / i.toDouble
      val PB = B.take(i).filter(_._3 == true).size / B.filter(_._3 == true).count().toDouble
      val PA = A.take(i).filter(_._3 == true).size / A.filter(_._3 == true).count().toDouble
      result.put(i, (PB.%(3), PA.%(3)))
      i += 100
    }

    (B.repartition(1), sc.parallelize(result.toSeq))
  }

  def verify2(sc: SparkContext, maxflowCredit: RDD[(VertexId, Double)], selectGraph: RDD[(Long, (Double, Boolean))]) = {
    /**
      * B为最大流计算排名，A为原始纳税信用等级排名;前100、前200、前300、…中标志为true
      */
    val A = selectGraph.map(v => (v._2._1, (v._2._2, v._1))).repartition(1).sortByKey(true).map(v => (v._2._2, v._1, v._2._1))
    val B = maxflowCredit.join(selectGraph).map(x => (x._2._1, (x._1, x._2._2._2))).repartition(1).sortByKey(true).map(x => (x._2._1, x._1, x._2._2))
    //按100名波动清况
    var i = 100
    var result = HashMap[VertexId, (Double, Double)]() //节点id,最大流命中率，原始命中率
    var number = A.count()
    if (number > 10000) {
      number = 10000
    }
    while (i <= number) {
      //  val P = B.repartition(1).top(i)//sc.parallelize(B.top(i)).filter(_._3 == true).count() / i.toDouble
      val PB = B.take(i).filter(_._3 == true).size / i.toDouble
      val PA = A.take(i).filter(_._3 == true).size / i.toDouble
      result.put(i, (PB.%(3), PA.%(3)))
      i += 100
    }

    (B.repartition(1), sc.parallelize(result.toSeq))
  }

  def verify1(sc: SparkContext, maxflowCredit: RDD[(VertexId, Double)], selectGraph: RDD[(Long, (Double, Boolean))]) = {
    /**
      * B为最大流计算排名，A为原始纳税信用等级排名;0-100、100-200、200-300、…中标志为true依次计算命中率
      */
    val A = selectGraph.map(v => (v._2._1, (v._2._2, v._1))).repartition(1).sortByKey(true).map(v => (v._2._2, v._1, v._2._1)).collect().toList
    val Boutput = maxflowCredit.join(selectGraph).map(x => (x._2._1, (x._1, x._2._2._2))).repartition(1).sortByKey(true).map(x => (x._2._1, x._1, x._2._2))
    val B = Boutput.collect.toList

    var Atemp = A
    var Btemp = B
    //按100名波动清况
    var i = 100
    var result = HashMap[VertexId, (Double, Double)]() //节点id,最大流命中率，原始命中率
    var number = A.size
    if (number > 10000) {
      number = 10000
    }
    while (i <= number) {
      //   val PB = B.take(i).filter(_._3 == true).size / i.toDouble
      //    val PA = A.take(i).filter(_._3 == true).size / i.toDouble

      val PB = Btemp.take(100).filter(_._3 == true).size / 100D
      val PA = Atemp.take(100).filter(_._3 == true).size / 100D
      result.put(i, (PB.%(3), PA.%(3)))
      Btemp = Btemp.drop(100)
      Atemp = Atemp.drop(100)
      i += 100
    }

    (Boutput.repartition(1), sc.parallelize(result.toSeq))
  }
}
