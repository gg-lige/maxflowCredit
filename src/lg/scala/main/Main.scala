package lg.scala.main

//  选用ojdbc8.jar 同时 --jars 后面的参数用，隔开
//  spark-submit --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar --class lg.scala.main.Main /opt/lg/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/lg/maxflowCredit.jar
//16 spark-shell --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 4G --total-executor-cores 2 --driver-memory 2G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --master spark://cloud-03:7077 --executor-memory 64G --total-executor-cores 8 --executor-cores 8 --driver-memory 8G --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/maxflowCredit.jar
//用的jar3


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
      val tpin0 = InputOutputTools.getFromOracle2(hiveContext, sc)
      println("\n初始TPIN网络 :after construct:  \n节点数：" + tpin0.vertices.count)
      println("边数：" + tpin0.edges.count)
      // 节点数：2792215     边数：3944847
      InputOutputTools.saveAsObjectFile(tpin0, sc, "/lg/maxflowCredit/initVertices", "/lg/maxflowCredit/initEdges")
    }

    if (!InputOutputTools.Exist(sc, "/lg/maxflowCredit/vertices2")) {
      val tpinFromObject = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/lg/maxflowCredit/initVertices", "/lg/maxflowCredit/initEdges")
      //添加控制人亲密度边
      val tpinWithCohesionWithoutFusion = CreditGraphTools.addCohesion(tpinFromObject, weight = 0.0, degree = 1).persist()
      val tpinWithoutFusionEDGE = tpinWithCohesionWithoutFusion.edges.map(e => ((e.srcId, e.dstId), e.attr))
        .reduceByKey(InitEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2).map(e => Edge(e._1._1, e._1._2, e._2))
      val tpinWithCohesion = Graph(tpinWithCohesionWithoutFusion.vertices, tpinWithoutFusionEDGE)

      //抽取所有纳税人
      val tpin_NSR0 = CreditGraphTools.extractNSR(tpinWithCohesion)
      //    val tpin_NSR0 = CreditGraphTools.extractNSR2(tpinFromObject)
      println("\n纳税人网络: after construct企业:  \n节点数：" + tpin_NSR0.vertices.count)
      println("边数：" + tpin_NSR0.edges.count)
      //节点数：1483300    边数：706787
      InputOutputTools.saveAsObjectFile(tpin_NSR0, sc, "/lg/maxflowCredit/vertices3", "/lg/maxflowCredit/edges3")
    }
    /*
        hadoop fs -rm -r /lg/maxflowCredit/fixVertices
        hadoop fs -rm -r /lg/maxflowCredit/fixEdges
    */
    if (!InputOutputTools.Exist(sc, "/lg/maxflowCredit/fixVertices3")) {
      //    val tpin = InputOutputTools.getFromObjectFile[VertexAttr, EdgeAttr](sc, "/lg/maxflowCredit/vertices", "/lg/maxflowCredit/edges").persist()
      val tpin0 = InputOutputTools.getFromObjectFile[VertexAttr, EdgeAttr](sc, "/lg/maxflowCredit/vertices2", "/lg/maxflowCredit/edges2").persist()
      val tpin0E = tpin0.edges.map(x => ((x.srcId, x.dstId), x.attr)).reduceByKey { (a, b) =>
        val toReturn = new EdgeAttr()
        toReturn.w_invest = a.w_invest + b.w_invest
        toReturn.w_stockholder = a.w_stockholder + b.w_stockholder
        toReturn.w_trade = a.w_trade + b.w_trade
        toReturn.w_cohesion = a.w_cohesion + b.w_cohesion
        toReturn
      }.filter(e => e._1._1 != e._1._2).map(e => Edge(e._1._1, e._1._2, e._2))
      val tpin = Graph(tpin0.vertices, tpin0E)
      //修正图上的边权值,并提取点度>0的节点（信息融合等原理）,
      val fixEdgeWeightGraph = MaxflowCreditTools.fixEdgeWeight(tpin).persist()
      println("\n修正边权值fixEdgeWeightGraph:  \n节点数：" + fixEdgeWeightGraph.vertices.count)
      println("边数：" + fixEdgeWeightGraph.edges.count)
      println("有问题：" + fixEdgeWeightGraph.vertices.filter(_._2._2 == true).count)
      //节点数：124782   边数：373051
      InputOutputTools.saveAsObjectFile(fixEdgeWeightGraph, sc, "/lg/maxflowCredit/fixVertices3", "/lg/maxflowCredit/fixEdges3")
    }

    //----------------------------------------------------
    val selectHaveInitCreditScore = false
    val selectProblemOrNotRatio = false
    val runMaxflowAlgorithm = true
    val outputVerifyMode = 3
    //----------------------------------------------------

    val fixEdgeWeightGraph = InputOutputTools.getFromObjectFile[(Double, Boolean), Double](sc, "/lg/maxflowCredit/fixVertices3", "/lg/maxflowCredit/fixEdges3").persist()
    var selectGraph = fixEdgeWeightGraph

    //取子图，只选择节点有纳税信用评分的节点
    if (selectHaveInitCreditScore) {
      val selectGraph0 = fixEdgeWeightGraph.subgraph(vpred = (vid, vattr) => vattr._1 > 0D)
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
    println("边数：" + selectGraph.edges.count) //节点数：5771    边数：7451
    println("有问题：" + selectGraph.vertices.filter(_._2._2 == true).count)

    //求最大流子图：各节点向外扩展3步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）,同时选择中心节点至少含有一个入度
    val extendPair = MaxflowCreditTools.extendSubgraph(selectGraph.mapVertices((vid, vattr) => (vattr._1)), 6)
    val oneIndegreeExtendPair = extendPair.filter(x => x._2.getAllEdge().map(_.dst.id).contains(x._1))
    val maxflowSubExtendPair = oneIndegreeExtendPair //.filter(x=>x._2.getAllEdge().size>=10)
    val maxflowSubGraph = selectGraph.vertices.join(maxflowSubExtendPair).map(x => (x._1, x._2._1))

    println("\n最大流子图maxflowSubExtendPair:  \n节点数：" + maxflowSubExtendPair.count)
    println("最大子图规模：" + maxflowSubExtendPair.map(_._2.getAllEdge().size).max)
    println("节点中有问题的：" + maxflowSubGraph.filter(_._2._2 == true).count)


    //    maxflowSubExtendPair.map(x=>(x._2.getAllEdge().size,x._1)).repartition(1).sortByKey(false).repartition(1).saveAsTextFile("/lg/maxflowCredit/maxflowSubExtendPairScale")
    //    maxflowSubExtendPair.saveAsObjectFile("/lg/maxflowCredit/maxflowSubExtendPair")
    //  maxflowSubExtendPair.map(x => (x._1, x._2.getAllEdge())).repartition(1).saveAsTextFile("/lg/maxflowCredit/maxflowSubExtendPair")

    //运行最大流算法
    if (runMaxflowAlgorithm) {
      println("\n最大流Start!")
      var j = 0.5
      for (i <- 197 to 197) {
        val maxflowCredit = MaxflowCreditTools.run3(maxflowSubExtendPair, j)
        //验证方式一:输出（节点编号、节点原始纳税信用评分、周边节点传递得分、最大流得分，是否为问题企业）
        if (outputVerifyMode == 1) {
          val verify = maxflowCredit.map(x => (x._1, (x._2, x._3))).join(maxflowSubGraph).map(x => (x._2._1._2, (x._1, x._2._2._1 * 100, x._2._1._1, x._2._2._2)))
            .filter(x => (!(x._2._2 <= 40D && x._2._4 == false))).repartition(1).sortByKey(true).map(x => (x._2._1, x._2._2, x._2._3, x._1, x._2._4)).repartition(1)
          verify.saveAsTextFile("/lg/maxflowCredit/verify11")
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
          ).repartition(1).saveAsTextFile("/lg/maxflowCredit/s" + i)
        }
        //验证方式三：输出至数据库，由税务系统可视化验证
        else if (outputVerifyMode == 3) {
          val outputV = maxflowCredit.flatMap(v1=>v1._4.map(v2=>(v2._1,(v1._1,v2._2,v2._3)))).join(maxflowCredit.map(x=>(x._1,x._3))).map(x=>(x._2._1._1.toString,x._1.toString,x._2._1._2,x._2._2,x._2._1._3))
          //sc.parallelize(maxflowCredit._2.toList).map(x => (x._2, (x._1, x._3, x._4))).join(maxflowCredit._1.map(x => (x._1, x._3))).map(x => (x._2._1._1.toString, x._1.toString, x._2._1._2, x._2._2, x._2._1._3))
          //     val v=maxflowSubExtendPair.flatMap(v1=>v1._2.getAllEdge().map(v2=>(v1._1,List(v2.src.id,v2.dst.id)))).reduceByKey(_.++(_)).map(x=>(x._1,x._2.distinct))
          val outputE = maxflowSubExtendPair.flatMap(e1 => e1._2.getAllEdge().map(e2 => (e1._1.toString, e2.src.id.toString, e2.dst.id.toString, e2.weight.toString)))
          InputOutputTools.saveMaxflowResultToOracle(outputV, outputE, hiveContext)
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

*/

