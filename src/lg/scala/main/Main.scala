package lg.scala.main

//  选用ojdbc8.jar 同时 --jars 后面的参数用，隔开
//  spark-submit --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar --class lg.scala.main.Main /opt/lg/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/lg/maxflowCredit.jar
//16 spark-shell --master spark://cloud-03:7077 --executor-memory 4G --total-executor-cores 2 --driver-memory 2G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --executor-memory 64G --total-executor-cores 8 --executor-cores 8 --driver-memory 8G --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/maxflowCredit.jar
// ./spark-submit --class lg.scala.main.Main --master yarn --executor-memory 6G  --executor-cores 4 --driver-memory 8G /opt/lg/maxflowCredit.jar
// ./spark-shell --master yarn --conf spark.driver.maxResultSize=6G --executor-memory 6G  --executor-cores 4 --driver-memory 8G --jars /opt/lg/maxflowCredit.jar

//用的jar3


import java.beans.Transient

import lg.scala.entity._
import lg.scala.utils.{CreditGraphTools, ExperimentTools, InputOutputTools, MaxflowCreditTools}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession


/**
  * Created by lg on 2017/6/19.
  */
object Main {
  def main(args: Array[String]): Unit = {
    @transient
    val session = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    val sc = session.sparkContext
    session.conf.set("spark.driver.maxResultSize", "6g")

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/startVertices")) {
      val edge_temp = InputOutputTools.saveE2Oracle_V2HDFS(session)
    } //结束后在数据库进行操作

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/initVertices")) {
      val tpin0 = InputOutputTools.getFromOracle2(session, sc)
      println("\n初始TPIN网络 :after construct:  \n节点数：" + tpin0.vertices.count)
      println("边数：" + tpin0.edges.count)
      // 节点数：2063483     边数：3563796
      InputOutputTools.saveAsObjectFile(tpin0, sc, "/user/lg/maxflowCredit/initVertices", "/user/lg/maxflowCredit/initEdges")
    }

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/cohesionVertices")) {
      val tpinFromObject = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/user/lg/maxflowCredit/initVertices", "/user/lg/maxflowCredit/initEdges")
      //添加控制人亲密度边
      val tpinWithCohesion = CreditGraphTools.addCohesion(tpinFromObject, weight = 0.0, degree = 2).persist()
      println("\n添加亲密度网络: after construct企业:  \n节点数：" + tpinWithCohesion.vertices.count)
      println("边数：" + tpinWithCohesion.edges.count)
      // 节点数：2063483     边数：6302952
      InputOutputTools.saveAsObjectFile(tpinWithCohesion, sc, "/user/lg/maxflowCredit/cohesionVertices", "/user/lg/maxflowCredit/cohesionEdges")
    }

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/vertices")) {
      // val tpinFromObject = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/user/lg/maxflowCredit/initVertices", "/user/lg/maxflowCredit/initEdges")
      val tpinWithCohesion = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/user/lg/maxflowCredit/cohesionVertices", "/user/lg/maxflowCredit/cohesionEdges")
      //抽取所有纳税人子图
      //val tpin_NSR = CreditGraphTools.extractNSR2(tpinFromObject) //不含亲密度边
      val tpin_NSR = CreditGraphTools.extractNSR(tpinWithCohesion) //含亲密度边
      println("\n纳税人网络: after construct企业:  \n节点数：" + tpin_NSR.vertices.count)
      println("边数：" + tpin_NSR.edges.count)
      //节点数：475686    边数：3433856
      InputOutputTools.saveAsObjectFile(tpin_NSR, sc, "/user/lg/maxflowCredit/vertices", "/user/lg/maxflowCredit/edges")
    }

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/fixVertices")) {
      val tpin = InputOutputTools.getFromObjectFile[VertexAttr, EdgeAttr](sc, "/user/lg/maxflowCredit/vertices", "/user/lg/maxflowCredit/edges").persist()
      //修正图上的边权值,并提取点度>0的节点（信息融合等原理）,
      val fixEdgeWeightGraph = MaxflowCreditTools.fixEdgeWeight(tpin).persist()
      println("\n修正边权值fixEdgeWeightGraph:  \n节点数：" + fixEdgeWeightGraph.vertices.count)
      println("边数：" + fixEdgeWeightGraph.edges.count)
      println("有问题：" + fixEdgeWeightGraph.vertices.filter(_._2._2 == true).count)
      //节点数：475686   边数：3433856
      //有问题：4273
      InputOutputTools.saveAsObjectFile(fixEdgeWeightGraph, sc, "/user/lg/maxflowCredit/fixVertices", "/user/lg/maxflowCredit/fixEdges")
    }

    //----------------------------------------------------
    val selectHaveInitCreditScore = false
    val selectProblemOrNotRatio = false
    val runMaxflowAlgorithm = true
    val outputVerifyMode = 2
    //----------------------------------------------------

    val fixEdgeWeightGraph = InputOutputTools.getFromObjectFile[(Double, Boolean), Double](sc, "/user/lg/maxflowCredit/fixVertices", "/user/lg/maxflowCredit/fixEdges").persist()
    var selectGraph = fixEdgeWeightGraph
    /*
    //取子图，只选择节点有纳税信用评分的节点
    if (selectHaveInitCreditScore) {
      val selectGraph0 = selectGraph.subgraph(vpred = (vid, vattr) => vattr._1 > 0D)
      val degreeGra = selectGraph0.degrees
      selectGraph = Graph(selectGraph0.vertices.join(degreeGra).map(v => (v._1, v._2._1)), selectGraph0.edges)
    }

    //按有问题与无问题相应比例选择
    if (selectProblemOrNotRatio) {
      val Tcompany = selectGraph.vertices.filter(_._2._2 == true).map(_._1).take(4500)
      val Fcompany = selectGraph.vertices.filter(_._2._2 == false).map(_._1).take(500)
      val testCompany = Tcompany.++:(Fcompany)
      val g = selectGraph.subgraph(vpred = (vid, vattr) => testCompany.contains(vid))
      selectGraph = g
    }

    println("\n为得到最大流子图的大图selectGraph:  \n节点数：" + selectGraph.vertices.count)
    println("边数：" + selectGraph.edges.count)
    println("有问题：" + selectGraph.vertices.filter(_._2._2 == true).count)
    //节点数：475686    边数：3433856
    //有问题：4273

    //求最大流子图：各节点向外扩展3步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）,同时选择中心节点至少含有一个入度
    val extendPair = MaxflowCreditTools.extendSubgraph(selectGraph.mapVertices((vid, vattr) => (vattr._1)), 6)
    val oneIndegreeExtendPair = extendPair.filter(x => x._2.getAllEdge().map(_.dst.id).contains(x._1))
    val maxflowSubExtendPair = oneIndegreeExtendPair //.filter(x=>x._2.getAllEdge().size>=10)
    val maxflowSubGraph = selectGraph.vertices.join(maxflowSubExtendPair).map(x => (x._1, x._2._1))

    println("\n最大流子图maxflowSubExtendPair:  \n节点数：" + maxflowSubExtendPair.count)
    println("最大子图规模：" + maxflowSubExtendPair.map(_._2.getAllEdge().size).max)
    println("节点中有问题的：" + maxflowSubGraph.filter(_._2._2 == true).count)
    //节点数：474440
    //最大子图规模：243
    //节点中有问题的：4112


    maxflowSubExtendPair.map(x => (x._2.getAllEdge().size, x._1)).repartition(1).sortByKey(false).repartition(1).saveAsTextFile("/user/lg/maxflowCredit/maxflowSubExtendPairScale")
    maxflowSubExtendPair.saveAsObjectFile("/user/lg/maxflowCredit/maxflowSubExtendPair")
    maxflowSubExtendPair.map(x => (x._1, x._2.getAllEdge())).repartition(1).saveAsTextFile("/user/lg/maxflowCredit/maxflowSubExtendPair_T")
    */

    val maxflowSubExtendPair = sc.objectFile[(VertexId, MaxflowGraph)]("/user/lg/maxflowCredit/maxflowSubExtendPair").repartition(128).filter(_._1 == 61100)
    val maxflowSubGraph = selectGraph.vertices.join(maxflowSubExtendPair).map(x => (x._1, x._2._1))

    //运行最大流算法
    if (runMaxflowAlgorithm) {
      println("\n最大流Start!")
      var j = 0.1
      for (i <- 1 to 5) {
        val maxflowCredit = MaxflowCreditTools.run3(maxflowSubExtendPair, j)
        //验证方式一:输出（节点编号、节点原始纳税信用评分、周边节点传递得分、最大流得分，是否为问题企业）
        if (outputVerifyMode == 1) {
          val verify = maxflowCredit.map(x => (x._1, (x._2, x._3))).join(maxflowSubGraph).map(x => (x._2._1._2, (x._1, x._2._2._1 * 100, x._2._1._1, x._2._2._2)))
            .filter(x => (!(x._2._2 <= 40D && x._2._4 == false))).repartition(1).sortByKey(true).map(x => (x._2._1, x._2._2, x._2._3, x._1, x._2._4)).repartition(1)
          verify.saveAsTextFile("/user/lg/maxflowCredit/verify")
        }
        //验证方式二：输出准确率验证
        else if (outputVerifyMode == 2) {
          val experimentResult = ExperimentTools.verify2(sc, maxflowCredit.map(x => (x._1, x._3)), maxflowSubGraph)
          println("验证Done!" + i)

          //InputOutputTools.saveRDDAsFile(sc, maxflowCredit, "/lg/maxflowCredit/o" + i, experimentResult._1, "/lg/maxflowCredit/t" + i)
          experimentResult._2.repartition(1).sortByKey(true).map(line => {
            val id = line._1
            val maxflowScore = line._2._1
            val originalScore = line._2._2
            id + "," + maxflowScore + "," + originalScore
          }
          ).repartition(1).saveAsTextFile("/user/lg/maxflowCredit/s" + i)
        }
        //验证方式三：输出至数据库，由税务系统可视化验证
        else if (outputVerifyMode == 3) {
          val outputV = maxflowCredit.flatMap(v1 => v1._4.map(v2 => (v2._1, (v1._1, v2._2, v2._3)))).leftOuterJoin(maxflowCredit.map(x => (x._1, x._3))).map(x => (x._2._1._1.toString, x._1.toString, x._2._1._2, x._2._2.getOrElse(0D), x._2._1._3))
          val outputE = maxflowSubExtendPair.flatMap(e1 => e1._2.getAllEdge().map(e2 => ((e2.src.id, e2.dst.id), e1._1))).leftOuterJoin(fixEdgeWeightGraph.edges.map(e => ((e.srcId, e.dstId), e.attr))).map(e => (e._2._1.toString, e._1._1.toString, e._1._2.toString, e._2._2.getOrElse(0D).toString))
          InputOutputTools.saveMaxflowResultToOracle(outputV, outputE, session)
        }

        j = j + 0.2
      }
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


    val v1= (1L,new InitVertexAttr("v1","1",false))
    val v2= (2L,new InitVertexAttr("v2","2",true))
    val v3= (3L,new InitVertexAttr("v3","3",true))
    val e1 =Edge(v1._1,v2._1,new InitEdgeAttr(1.0,0.0,0.0,0.0))
    val e2 =Edge(v2._1,v1._1,new InitEdgeAttr(1.0,0.0,0.0,0.0))
    val e3 =Edge(v2._1,v3._1,new InitEdgeAttr(0.0,1.0,0.0,0.0))
    val e4 =Edge(v3._1,v2._1,new InitEdgeAttr(1.0,0.0,0.8,0.0))
    val v=sc.parallelize(Array(v1,v2,v3))
    val e=sc.parallelize(Array(e1,e2,e3,e4))
    val tpinFromObject=Graph(v,e)




*/

