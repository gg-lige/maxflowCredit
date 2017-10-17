package lg.scala.main

//  选用ojdbc8.jar 同时 --jars 后面的参数用，隔开
//  spark-submit --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar --class lg.scala.main.Main /opt/lg/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/lg/maxflowCredit.jar
//16 spark-shell --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 4G --total-executor-cores 2 --driver-memory 2G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 64G --total-executor-cores 8 --executor-cores 8 --driver-memory 8G --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/maxflowCredit.jar
//用的jar2


import java.beans.Transient

import lg.scala.entity._
import lg.scala.utils.{CreditGraphTools, ExperimentTools, InputOutputTools, MaxflowCreditTools}
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

    if (!InputOutputTools.Exist(sc, "/lg/maxflowCredit/startVertices")) {
      val edge_temp = InputOutputTools.saveE2Oracle_V2HDFS(hiveContext)
    } //结束后在数据库进行操作

    if (!InputOutputTools.Exist(sc, "/lg/maxflowCredit/initVertices")) {
      val tpin = InputOutputTools.getFromOracle2(hiveContext, sc)
      println("\n初始TPIN网络 :after construct:  \n节点数：" + tpin.vertices.count)
      println("边数：" + tpin.edges.count)
      // 节点数：2792215     边数：3944847
      InputOutputTools.saveAsObjectFile(tpin, sc, "/lg/maxflowCredit/initVertices", "/lg/maxflowCredit/initEdges")
    }

    if (!InputOutputTools.Exist(sc, "/lg/maxflowCredit/vertices")) {
      val tpinFromObject = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/lg/maxflowCredit/initVertices", "/lg/maxflowCredit/initEdges")
      //添加控制人亲密度边
      //    val tpinWithCohesion = CreditGraphTools.addCohesion(tpinFromObject, weight = 0.0, degree = 1).persist()
      //抽取所有纳税人
      //     val tpin_NSR0 = CreditGraphTools.extractNSR(tpinWithCohesion)
      val tpin_NSR0 = CreditGraphTools.extractNSR2(tpinFromObject)
      println("\n纳税人网络: after construct企业:  \n节点数：" + tpin_NSR0.vertices.count)
      println("边数：" + tpin_NSR0.edges.count)
      //节点数：1483300    边数：706787
      InputOutputTools.saveAsObjectFile(tpin_NSR0, sc, "/lg/maxflowCredit/vertices", "/lg/maxflowCredit/edges")
    }


    /*
        hadoop fs -rm -r /lg/maxflowCredit/fixVertices
        hadoop fs -rm -r /lg/maxflowCredit/fixEdges
    */
    if (!InputOutputTools.Exist(sc, "/lg/maxflowCredit/fixVertices")) {
      val tpin = InputOutputTools.getFromObjectFile[VertexAttr, EdgeAttr](sc, "/lg/maxflowCredit/vertices", "/lg/maxflowCredit/edges").persist()
      //修正图上的边权值,并提取点度>0的节点（信息融合等原理）,
      val fixEdgeWeightGraph = MaxflowCreditTools.fixEdgeWeight(tpin).persist()
      println("\n修正边权值fixEdgeWeightGraph:  \n节点数：" + fixEdgeWeightGraph.vertices.count)
      println("边数：" + fixEdgeWeightGraph.edges.count)
      //节点数：124782   边数：373051
      InputOutputTools.saveAsObjectFile(fixEdgeWeightGraph, sc, "/lg/maxflowCredit/fixVertices", "/lg/maxflowCredit/fixEdges")
    }
    /*
        val fixEdgeWeightGraph = InputOutputTools.getFromObjectFile[(Double, Boolean), Double](sc, "/lg/maxflowCredit/fixVertices", "/lg/maxflowCredit/fixEdges").persist()
        //取子图，只选择节点有纳税信用评分的节点

        val selectGraph0 = fixEdgeWeightGraph.subgraph(vpred = (vid, vattr) => vattr._1 > 0D)
        val degreeGra = selectGraph0.degrees
        val selectGraph: Graph[(Double, Boolean), Double] = Graph(selectGraph0.vertices.join(degreeGra).map(v => (v._1, v._2._1)), selectGraph0.edges)
     //   val selectGraph=fixEdgeWeightGraph
        println("\n抽取信用节点selectGraph:  \n节点数：" + selectGraph.vertices.count)
        println("边数：" + selectGraph.edges.count)
        //节点数：5771    边数：7451
        //验证
        println("\n节点中有问题的：" + selectGraph.vertices.filter(_._2._2 == true).count)
        //各节点向外扩展3步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）
        val extendPair = MaxflowCreditTools.extendSubgraph(selectGraph.mapVertices((vid, vattr) => (vattr._1)), 6)
      */
    val selectGraph = InputOutputTools.getFromObjectFile[(Double, Boolean), Double](sc, "/lg/maxflowCredit/selectVertices", "/lg/maxflowCredit/selectEdges").persist()
    val extendPair = sc.objectFile[(VertexId, MaxflowGraph)]("/lg/maxflowCredit/extendSubgraph").repartition(128)
    /*
        InputOutputTools.save3RDDAsObjectFile(extendPair, sc, "/lg/maxflowCredit/extendSubgraph7_9")
        extendPair.saveAsObjectFile("/lg/maxflowCredit/extendSubgraph")
        val extendPair = InputOutputTools.get3RDDAsObjectFile[VertexId, Seq[(VertexId, Double)], Seq[Edge[Double]]](sc, "/lg/maxflowCredit/extendSubgraph").persist()
        val vGraph = InputOutputTools.getFromCsv(sc, "/lg/maxflowCredit/vertices.csv", "/lg/maxflowCredit/edges.csv")
        val extendPair = sc.objectFile[(VertexId, MaxflowGraph)]("/lg/maxflowCredit/extendSubgraph310").repartition(128)
        extendPair.map(_._2.getAllEdge().size).max
        extendPair.map(_._2.getGraph().keySet.size).max
    */

    //运行最大流算法
    println("最大流Start!")

    var j = 0.1
    for (i <- 86 to 90) {

        val maxflowCredit = MaxflowCreditTools.run3(extendPair, j)

        //  验证
        val experimentResult = ExperimentTools.verify(sc, maxflowCredit.collect, selectGraph)
        println("验证Done!"+i)
     //   InputOutputTools.saveRDDAsFile(sc, maxflowCredit, "/lg/maxflowCredit/o"+i, experimentResult._1, "/lg/maxflowCredit/t"+i)
        experimentResult._2.repartition(1).sortByKey(true).repartition(1).saveAsTextFile("/lg/maxflowCredit/s"+i)
        j = j + 0.2
      }
    }
}

/*
val v = sc.parallelize (Array ((1L, 1.0), (2L, 0.6), (3L, 0D), (4L, 0.95D), (5L, 0.5D), (6L, 0.6D)) )
val e = sc.parallelize (Array (Edge (1L, 2L, 0.5), Edge (1L, 3L, 0.6), Edge (3L, 2L, 0.8), Edge (3L, 4L, 0.7), Edge (2L, 4L, 0.3), Edge (2L, 5L, 0.7), Edge (5L, 4L, 0.9), Edge (4L, 6L, 0.8) ) )
val graph = Graph (v, e)
val fixEdgeWeightGraph = graph
val selectTopN=6
val iteration = 3
val extendPair = MaxflowCreditTools.extendSubgraph (graph, 6)

//使数据可以按多分片数进行读入
    val sqlContext = new SQLContext(sparkContext)
    val properties = new java.util.Properties()
    properties.put("driver", "oracle.jdbc.OracleDriver")
    properties.put("user", "tax")
    properties.put("password", "taxgm2016")
    val dataFrame = sqlContext.read.jdbc("jdbc:oracle:thin:@oracle:1521/tax", "(SELECT ROWNUM ROW_ID, SOURCE_ID, TARGET_ID, CATEGORY FROM JJJ_FA1_TPIN_EDGE)", "ROW_ID", 1, 469385, 40, properties)

*/

