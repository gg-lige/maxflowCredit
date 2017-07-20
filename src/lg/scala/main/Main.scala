package lg.scala.main

//  选用ojdbc8.jar 同时 --jars 后面的参数用，隔开
//  spark-submit --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar --class lg.scala.main.Main /opt/lg/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/lg/maxflowCredit.jar

import java.beans.Transient

import lg.scala.entity.{EdgeAttr, InitEdgeAttr, InitVertexAttr, VertexAttr}
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

    //如果不存在最大流信用网络，则从初始化tpin开始
    if (!InputOutputTools.Exist(sc, "/lg/maxflowCredit/vertices")) {
      //  val tpin = InputOutputTools.getFromOracle(hiveContext).persist()
      val tpin = InputOutputTools.getFromOracle2(hiveContext).persist()
      println("\nafter construct:  \n节点数：" + tpin.vertices.count)
      println("边数：" + tpin.edges.count)
      // 节点数：2873109      边数：2411724
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

    val tpin = InputOutputTools.getFromObjectFile[VertexAttr, EdgeAttr](sc, "/lg/maxflowCredit/vertices", "/lg/maxflowCredit/edges").persist()

    //修正图上的边权值,并提取点度>0的节点（信息融合等原理）,取子图，只选择节点有纳税信用评分的节点
    val fixEdgeWeightGraph = MaxflowCreditTools.fixEdgeWeight(tpin).persist()
    println("\nfixEdgeWeightGraph:  \n节点数：" + fixEdgeWeightGraph.vertices.count)
    println("边数：" + fixEdgeWeightGraph.edges.count)
    InputOutputTools.saveAsObjectFile(fixEdgeWeightGraph, sc, "/lg/maxflowCredit/fixVertices", "/lg/maxflowCredit/fixEdges")
  //  val fixEdgeWeightGraph = InputOutputTools.getFromObjectFile[(Double, Boolean), Double](sc, "/lg/maxflowCredit/fixVertices", "/lg/maxflowCredit/fixEdges").persist()

    //各节点向外扩展3步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）
    val selectGraph = fixEdgeWeightGraph.subgraph(vpred = (vid, vattr) => vattr._1 > 0D && vattr._1 <= 0.4)

    println("\nselectGraph:  \n节点数：" + selectGraph.vertices.count)
    println("边数：" + selectGraph.edges.count)
    val extendPair = MaxflowCreditTools.extendSubgraph(selectGraph.mapVertices((vid, vattr) => (vattr._1)))
    println("extendPair运行Done!")
    InputOutputTools.save3RDDAsObjectFile(extendPair, sc, "/lg/maxflowCredit/extendSubgraph")

  //  val extendPair = InputOutputTools.get3RDDAsObjectFile[VertexId, Seq[(VertexId, Double)], Seq[Edge[Double]]](sc, "/lg/maxflowCredit/extendSubgraph").persist()

    //运行最大流算法
    val maxflowCredit = MaxflowCreditTools.run(extendPair, sc)
    println("最大流运行Done!")
    //验证
    val experimentResult = ExperimentTools.verify(sc, maxflowCredit, selectGraph)
    println("验证Done!")
    InputOutputTools.saveRDDAsFile(sc, sc.parallelize(maxflowCredit), "/lg/maxflowCredit/maxflowScore_oD", experimentResult._1, "/lg/maxflowCredit/maxflowScore_tD")
    sc.parallelize(experimentResult._2).repartition(1).saveAsTextFile("/lg/maxflowCredit/TopSortD")


  }
}

/*
val v = sc.parallelize (Array ((1L, 1.0), (2L, 0.6), (3L, 0D), (4L, 0.95D), (5L, 0.5D), (6L, 0.6D) ,(7L,0.5D),(8L,0.9D)) )
val e = sc.parallelize (Array (Edge (1L, 2L, 0.5),Edge (1L, 7L, 0.5), Edge (1L, 3L, 0.6), Edge (3L, 2L, 0.8), Edge (3L, 4L, 0.7), Edge (7L, 4L, 0.2), Edge (2L, 4L, 0.3), Edge (2L, 5L, 0.7), Edge (5L, 4L, 0.9), Edge (4L, 6L, 0.8) ) )
val fixEdgeWeightGraph = graph
val graph = Graph (v, e)
val selectTopN=2
val iteration = 3
val maxflow = MaxflowCreditTools.extendSubgraph (graph, 2)

//使数据可以按多分片数进行读入
    val sqlContext = new SQLContext(sparkContext)
    val properties = new java.util.Properties()
    properties.put("driver", "oracle.jdbc.OracleDriver")
    properties.put("user", "tax")
    properties.put("password", "taxgm2016")
    val dataFrame = sqlContext.read.jdbc("jdbc:oracle:thin:@oracle:1521/tax", "(SELECT ROWNUM ROW_ID, SOURCE_ID, TARGET_ID, CATEGORY FROM JJJ_FA1_TPIN_EDGE)", "ROW_ID", 1, 469385, 40, properties)


*/

