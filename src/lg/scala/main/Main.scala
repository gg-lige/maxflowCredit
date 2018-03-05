package lg.scala.main

//  选用ojdbc8.jar 同时 --jars 后面的参数用，隔开
//  ./spark-submit --master spark://cluster1:7077 --executor-memory 10G --total-executor-cores 40 --driver-memory 10G --class lg.scala.main.Main /opt/lg/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --executor-memory 32G --total-executor-cores 20 --driver-memory 16G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/lg/maxflowCredit.jar
//16 spark-shell --master spark://cloud-03:7077 --executor-memory 4G --total-executor-cores 2 --driver-memory 2G  --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/maxflowCredit.jar
//  spark-shell --master spark://cloud-03:7077 --executor-memory 64G --total-executor-cores 8 --executor-cores 8 --driver-memory 8G --jars /opt/hive/lib/ojdbc8.jar,/opt/hive/lib/mysql-connector-java-5.1.35-bin.jar,/opt/maxflowCredit.jar
// ./spark-submit --class lg.scala.main.Main --master yarn --executor-memory 10G --driver-memory 8G --num-executors 10 --executor-cores 4 /opt/lg/maxflowCredit.jar
// ./spark-shell --master yarn --conf spark.driver.maxResultSize=6G --executor-memory 10G --driver-memory 8G --num-executors 10 --executor-cores 4 --jars /opt/lg/maxflowCredit.jar

// /opt/wwd/spark/bin/spark-shell --master spark://cluster1:7077 --executor-memory 5G --total-executor-cores 20 --driver-memory 6G --jars /opt/lg/maxflowCredit.jar
//  /opt/wwd/spark/bin/spark-submit --master spark://cluster1:7077 --executor-memory 5G --total-executor-cores 20 --driver-memory 6G --class lg.scala.main.Main /opt/lg/maxflowCredit.jar
//用的jar3


import java.beans.Transient
import java.io.{File, PrintWriter}
import java.math.BigDecimal

import lg.scala.contrastMethod.{MeachineLearning, TidalTrust}
import lg.scala.entity._
import lg.scala.utils.{CreditGraphTools, ExperimentTools, InputOutputTools, MaxflowCreditTools}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession

/*rm -rf /opt/wwd/hadoop/tmp
rm -rf /opt/wwd/hadoop/hdfs/name
rm -rf /opt/wwd/hadoop/hdfs/data*/

/**
  * Created by lg on 2017/6/19.
  */
object Main {
  def main(args: Array[String]): Unit = {
    @transient
    val session = SparkSession.builder.appName(this.getClass.getSimpleName).getOrCreate()
    val sc = session.sparkContext
    session.conf.set("spark.driver.maxResultSize", "6g")

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/startVertices")) {
      val edge_temp = InputOutputTools.saveE2Oracle_V2HDFS(session)
    } //结束后在数据库进行操作

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/initVertices")) {
      val tpin0 = InputOutputTools.getFromOracle2(session, sc)
      println("\n初始TPIN网络 :after construct:  \n节点数：" + tpin0.vertices.count)
      println("边数：" + tpin0.edges.count)
      // 节点数：2063478     边数：3563778
      InputOutputTools.saveAsObjectFile(tpin0, sc, "/user/lg/maxflowCredit/initVertices", "/user/lg/maxflowCredit/initEdges")
    }

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/cohesionVertices2")) {
      val tpinFromObject = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/user/lg/maxflowCredit/initVertices", "/user/lg/maxflowCredit/initEdges")
      //添加控制人亲密度边
      val tpinWithCohesion = CreditGraphTools.addCohesion(tpinFromObject, weight = 0.0, degree = 2).persist()
      println("\n添加亲密度网络: after construct企业:  \n节点数：" + tpinWithCohesion.vertices.count)
      println("边数：" + tpinWithCohesion.edges.count)
      // 节点数：2063478     边数：6302955
      InputOutputTools.saveAsObjectFile(tpinWithCohesion, sc, "/user/lg/maxflowCredit/cohesionVertices2", "/user/lg/maxflowCredit/cohesionEdges2")
    }

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/vertices2")) {
      // val tpinFromObject = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/user/lg/maxflowCredit/initVertices", "/user/lg/maxflowCredit/initEdges")
      val tpinWithCohesion = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/user/lg/maxflowCredit/cohesionVertices2", "/user/lg/maxflowCredit/cohesionEdges2")
      //抽取所有纳税人子图
      //val tpin_NSR = CreditGraphTools.extractNSR2(tpinFromObject) //不含亲密度边
      val tpin_NSR = CreditGraphTools.extractNSR(tpinWithCohesion) //含亲密度边
      println("\n纳税人网络: after construct企业:  \n节点数：" + tpin_NSR.vertices.count)
      println("边数：" + tpin_NSR.edges.count)
      //节点数：475681    边数：3433844
      InputOutputTools.saveAsObjectFile(tpin_NSR, sc, "/user/lg/maxflowCredit/vertices2", "/user/lg/maxflowCredit/edges2")
    }

    if (!InputOutputTools.Exist(sc, "/user/lg/maxflowCredit/fixVertices2")) {
      val tpin = InputOutputTools.getFromObjectFile[VertexAttr, EdgeAttr](sc, "/user/lg/maxflowCredit/vertices2", "/user/lg/maxflowCredit/edges2").persist()
      //修正图上的边权值,并提取点度>0的节点（信息融合等原理）,
      val fixEdgeWeightGraph = MaxflowCreditTools.fixEdgeWeight(tpin).persist()
      println("\n修正边权值fixEdgeWeightGraph:  \n节点数：" + fixEdgeWeightGraph.vertices.count)
      println("边数：" + fixEdgeWeightGraph.edges.count)
      println("有问题：" + fixEdgeWeightGraph.vertices.filter(_._2._2 == true).count)
      //节点数：475681   边数：3433844
      //有问题：4273
      InputOutputTools.saveAsObjectFile(fixEdgeWeightGraph, sc, "/user/lg/maxflowCredit/fixVertices2", "/user/lg/maxflowCredit/fixEdges2")
    }

    //----------------------------------------------------
    val selectHaveInitCreditScore = false
    val beforeSelectProblemOrNotRatio = false
    val afterSelectProblemOrNotRatio = false
    val runMaxflowAlgorithm = true
    val outputVerifyMode = 4

    val runContrastMethod = false
    //----------------------------------------------------
    val writer = new PrintWriter(new File("/opt/lg/randomFroest7.csv"))
    writer.write("β,threashold,P_test,N_test,TP,TN,FP,FN,auc,precision,recall,f1,accuracy")
    for (m <- List(5)) {
      print("Method " + m + " start------------------------------------------------------------------------------")
      val fixEdgeWeightGraph = InputOutputTools.getFromObjectFile[(Double, Boolean), Double](sc, "/user/lg/maxflowCredit/fixVertices", "/user/lg/maxflowCredit/fixEdges").persist()
      val complianceScore = sc.textFile("/user/lg/maxflowCredit/compliance_score" + m).filter(!_.contains("VERTEXID")).map(_.split(",")).filter(_.length == 3).map(row => (row(0).toLong, (row(1).toDouble, row(2).toInt)))
      //数据库中有标签的
      val test_2015 = InputOutputTools.getFeatures(session)._2.select("vertexid").rdd.map(row => (row.getAs[java.math.BigDecimal]("vertexid").longValue()))
 /*     val complianceScore = complianceScore_temp.map(_._1).subtract(test_2015).map((_, 1)).join(complianceScore_temp).map(x => (x._1, x._2._2)).map(x => {
        if ((x._2._2 == 0 && x._2._1 < 0.5)||(x._2._2 == 1 && x._2._1 > 0.5))
          (x._1, (1-x._2._1, x._2._2))
        else
          x
      }).union(complianceScore_temp.join(test_2015.map((_, 1))).map(x => (x._1, x._2._1)))*/


      var selectGraph = Graph(fixEdgeWeightGraph.vertices.leftOuterJoin(complianceScore).map(x => (x._1, (x._2._2.map(_._1).getOrElse(0.0), x._2._2.map(_._2).getOrElse(0)))), fixEdgeWeightGraph.edges)
      /*
          //取子图，只选择节点有纳税信用评分的节点
          if (selectHaveInitCreditScore) {
            val selectGraph0 = selectGraph.subgraph(vpred = (vid, vattr) => vattr._1 > 0D)
            val degreeGra = selectGraph0.degrees
            selectGraph = Graph(selectGraph0.vertices.join(degreeGra).map(v => (v._1, v._2._1)), selectGraph0.edges)
          }

          //未扩展子图前：按有问题与无问题相应比例选择
          if (beforeSelectProblemOrNotRatio) {
            val Tcompany = selectGraph.vertices.filter(_._2._2 == 1).map(_._1).take(4273)
            val Fcompany = selectGraph.vertices.filter(_._2._2 == 0).map(_._1).take(4273)
            val testCompany = Tcompany.++:(Fcompany)
            val g = selectGraph.subgraph(vpred = (vid, vattr) => testCompany.contains(vid))
            selectGraph = g
          }

          println("\n为得到最大流子图的大图selectGraph:  \n节点数：" + selectGraph.vertices.count)
          println("边数：" + selectGraph.edges.count)
          println("有问题：" + selectGraph.vertices.filter(_._2._2 == 1).count)
          //节点数：475681    边数：3433844
          //有问题：4273
      */
      //求最大流子图：各节点向外扩展3步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）,同时选择中心节点至少含有一个入度
      val extendPair = MaxflowCreditTools.extendSubgraph(selectGraph.mapVertices((vid, vattr) => (vattr._1)), 6)
      var oneIndegreeExtendPair = extendPair.filter(x => x._2.getAllEdge().map(_.dst.id).contains(x._1))
      /*

          //扩展子图后：按有问题与无问题相应比例选择
          if (afterSelectProblemOrNotRatio) {
            val Tcompany = oneIndegreeExtendPair.join(selectGraph.vertices).filter(_._2._2._2 == 1).map(_._1).take(4273)
            val Fcompany = oneIndegreeExtendPair.join(selectGraph.vertices).filter(_._2._2._2 == 0).map(_._1).take(4273)
            val testCompany = Tcompany.++:(Fcompany)
            val g = oneIndegreeExtendPair.join(sc.parallelize(testCompany).map(x => (x, 1))).map(x => (x._1, x._2._1))
            oneIndegreeExtendPair = g
          }
      */


      //      val maxflowSubExtendPair = test_2015.map(x => (x, 1)).join(complianceScore).map(x => (x._1, x._2._2._1)).join(oneIndegreeExtendPair).map(x => (x._1, x._2._2))
      val maxflowSubExtendPair = test_2015.map(x => (x, 1)).join(complianceScore).map(x => (x._1, x._2._2._1)).leftOuterJoin(oneIndegreeExtendPair).map(x => (x._1, x._2._2.getOrElse {
        val a = new MaxflowGraph()
        a.getGraph().put(MaxflowVertexAttr(x._1, x._2._1), List[MaxflowEdgeAttr]())
        a
      }))
      //  maxflowSubExtendPair.join(selectGraph.vertices).map(x=>(x._1,x._2._2._2)).filter(_._2==1).count
      //  maxflowSubExtendPair.flatMap(_._2.getAllEdge().map(_.src.id)).union(maxflowSubExtendPair.flatMap(_._2.getAllEdge().map(_.dst.id))).union(test_2015).distinct().repartition(1).saveAsTextFile("/user/lg/maxflowCredit/v")
      //val maxflowSubGraph = selectGraph.vertices.join(maxflowSubExtendPair).map(x => (x._1, x._2._1))
      val maxflowSubGraph = complianceScore.join(maxflowSubExtendPair).map(x => (x._1, x._2._1))


      /*
          println("\n最大流子图maxflowSubExtendPair:  \n节点数：" + maxflowSubExtendPair.count)
          println("最大子图规模：" + maxflowSubExtendPair.map(_._2.getAllEdge().size).max)
          println("节点中有问题的：" + maxflowSubGraph.filter(_._2._2 == 1).count)
        */
      //节点数：475653
      //最大子图规模：258
      //节点中有问题的：4269

      //运行最大流算法
      if (runMaxflowAlgorithm) {
        println("\n最大流Start!")
        val threasholds = List(0D, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8,0.9)
      //  val threasholds = List(0.5, 0.6, 0.7, 0.8, 0.9)
        val Bs = List(0.1,0.3,0.5,0.7,0.9)
        //        val threasholds = List(0D)
        //        val Bs = List(0.1)
        for (b <- Bs) {
          for (threashold <- threasholds) {
            val maxflowCredit = MaxflowCreditTools.run3(maxflowSubExtendPair, b, threashold)

            //     val company = maxflowCredit.map(x => (x._1, x._3)).join(maxflowSubGraph).map(x => (x._1, x._2._2._2, x._2._1, x._2._2._1))
            //没有关联企业的企业的最大流分=个体评分
            val company = complianceScore.join(test_2015.map((_, 1))).map(x => (x._1, x._2._1)).leftOuterJoin(maxflowCredit.map(x => (x._1, x._3))).map(x => (x._1, x._2._1._2, x._2._2.getOrElse(x._2._1._1), x._2._1._1))
            //   company.filter(x=>(x._2==0&&x._3<0.5)).map(_._1).

            //直接算（1-β）后面的值的
            //     val company = complianceScore.join(test_2015.map((_,1))).map(x=>(x._1,x._2._1)).join(maxflowCredit.map(x => (x._1, x._2))).map(x => (x._1,x._2._1._2,x._2._2,x._2._1._1))
            if (outputVerifyMode == 4) {
              val originalScoreAndLabels = company.map(x => (1 - x._4, x._2.toDouble))
              val scoreAndLabels = company.map(x => (1 - x._3, x._2.toDouble))
              ExperimentTools.computeIndex2(b, threashold, scoreAndLabels, writer)
          //    ExperimentTools.computeIndex3(b, threashold, originalScoreAndLabels)
            }
          }
        }
      }


    }
    writer.close()

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




./hadoop fs -rm -r /user/lg/maxflowCredit/initVertices
./hadoop fs -rm -r /user/lg/maxflowCredit/initEdges
./hadoop fs -rm -r /user/lg/maxflowCredit/cohesionVertices
./hadoop fs -rm -r /user/lg/maxflowCredit/cohesionEdges
./hadoop fs -rm -r /user/lg/maxflowCredit/vertices
./hadoop fs -rm -r /user/lg/maxflowCredit/edges
./hadoop fs -rm -r /user/lg/maxflowCredit/fixEdges
./hadoop fs -rm -r /user/lg/maxflowCredit/fixVertices


*/

