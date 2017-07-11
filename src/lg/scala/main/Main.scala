package lg.scala.main

//  选用ojdbc8.jar 同时 --jars 后面的参数用，隔开
//  spark-submit --master spark://cloud-03:7077 --executor-memory 6G --total-executor-cores 10 --executor-cores 2  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar --class lg.scala.main.Main /opt/lg/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --executor-memory 8G --total-executor-cores 10 --executor-cores 2  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/lg/maxflowCredit.jar

import java.beans.Transient

import lg.scala.entity.{EdgeAttr, InitEdgeAttr, InitVertexAttr, VertexAttr}
import lg.scala.utils.{CreditGraphTools, InputOutputTools, MaxflowCreditTools}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}

/**
  * Created by lg on 2017/6/19.
  */
object Main {
  def main(args: Array[String]): Unit = {
    @Transient
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)

    @transient
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    /*

        //如果不存在最大流信用网络，则从初始化tpin开始
        if (!InputOutputTools.Exist(sc, "/lg/maxflowCredit/vertices")) {
          //  val tpin = InputOutputTools.getFromOracle(hiveContext).persist()
          val tpin = InputOutputTools.getFromOracle2(hiveContext).persist()
          println("\nafter construct:  \n节点数：" + tpin.vertices.count)
          println("边数：" + tpin.edges.count)
          // 节点数：2882824      边数：2396513
          InputOutputTools.saveAsObjectFile(tpin, sc, "/lg/maxflowCredit/initVertices", "/lg/maxflowCredit/initEdges")
        }
        val tpinFromObject = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/lg/maxflowCredit/initVertices", "/lg/maxflowCredit/initEdges")

        //添加控制人亲密度边
        val tpinWithCohesion = CreditGraphTools.addCohesion(tpinFromObject, weight = 0.0, degree = 1).persist()
        //抽取所有纳税人
        val tpin_NSR0 = CreditGraphTools.extractNSR(tpinWithCohesion)
        println("\nafter construct企业:  \n节点数：" + tpin_NSR0.vertices.count)
        println("边数：" + tpin_NSR0.edges.count)
        //节点数：1495357    边数：2075879
        InputOutputTools.saveAsObjectFile(tpin_NSR0, sc, "/lg/maxflowCredit/vertices", "/lg/maxflowCredit/edges")

      */
    /*
        val tpin = InputOutputTools.getFromObjectFile[VertexAttr, EdgeAttr](sc, "/lg/maxflowCredit/vertices", "/lg/maxflowCredit/edges").persist()

        //修正图上的边权值,并提取点度>0的节点（信息融合等原理）
        val fixEdgeWeightGraph = MaxflowCreditTools.fixEdgeWeight(tpin).persist()
        println("\nfixEdgeWeightGraph:  \n节点数：" + fixEdgeWeightGraph.vertices.count)
        println("边数：" + fixEdgeWeightGraph.edges.count)

        //各节点向外扩展3步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）
        val extendPair = MaxflowCreditTools.extendSubgraph(fixEdgeWeightGraph, 3)
        println("\nextendPair:  \n社团最大个数：" + extendPair.groupByKey().map(_._2.size).max)
        println("社团数：" + extendPair.map(_._1).distinct.count)

         extendPair.groupByKey().map(x => (x._2.size, x._1)).sortByKey(false).collect
    */
    val fixEdgeWeightGraph = InputOutputTools.getFromObjectFile[Double, Double](sc, "/lg/maxflowCredit/fixVertices", "/lg/maxflowCredit/fixEdges").persist()
    val extendPair = InputOutputTools.getRDDAsObjectFile[VertexId, VertexId](sc, "/lg/maxflowCredit/extendSubgraph").persist()

    //运行最大流算法
    val maxflowCredit = MaxflowCreditTools.run(extendPair, fixEdgeWeightGraph, sc)
    InputOutputTools.saveRDDAsObjectFile(extendPair, sc, "/lg/maxflowCredit/extendSubgraph")
    //   InputOutputTools.saveAsObjectFile(fixEdgeWeightGraph, sc, "/lg/maxflowCredit/fixVertices", "/lg/maxflowCredit/fixEdges")

    sc.parallelize(maxflowCredit.toSeq).saveAsObjectFile("/lg/maxflowCredit/maxflowScore_o20")
    sc.parallelize(maxflowCredit.toSeq).map(x => (x._2, x._1)).repartition(1).sortByKey(false).map(x => (x._2, x._1)).repartition(1).saveAsTextFile("/lg/maxflowCredit/maxflowScore_t20")

  }

}

/*
val v = sc.parallelize (Array ((1L, 1.0), (2L, 0.6), (3L, 0D), (4L, 0.95D), (5L, 0.5D), (6L, 0.6D) ,(7L,0.5D),(8L,0.9D)) )
val e = sc.parallelize (Array (Edge (1L, 2L, 0.5),Edge (1L, 7L, 0.5), Edge (1L, 3L, 0.6), Edge (3L, 2L, 0.8), Edge (3L, 4L, 0.7), Edge (7L, 4L, 0.2), Edge (2L, 4L, 0.3), Edge (2L, 5L, 0.7), Edge (5L, 4L, 0.9), Edge (4L, 6L, 0.8) ) )
val graph = Graph (v, e)
val fixEdgeWeightGraph = graph
val iteration = 3
val maxflow = MaxflowCreditTools.extendSubgraph (graph, 2)
*/

