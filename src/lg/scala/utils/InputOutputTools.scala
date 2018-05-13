package lg.scala.utils

import java.math.BigDecimal
import java.net.URI
import java.util.Properties

import lg.java.{DataBaseManager, Parameters}
import lg.scala.entity.{InitEdgeAttr, InitVertexAttr, MaxflowGraph, MaxflowVertexAttr}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.{LabeledPoint, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag


/**
  * Created by lg on 2017/6/19.
  */
object InputOutputTools {
  val url = Parameters.DataBaseURL
  val user = Parameters.DataBaseUserName
  val password = Parameters.DataBaseUserPassword
  val driver = Parameters.JDBCDriverString

  val properties = new Properties()
  properties.put("user", user)
  properties.put("password", password)
  properties.put("driver", driver)

  val db = Map(
    "url" -> Parameters.DataBaseURL,
    "user" -> Parameters.DataBaseUserName,
    "password" -> Parameters.DataBaseUserPassword,
    "driver" -> Parameters.JDBCDriverString
  )


  def saveRDDAsFile(sc: SparkContext, objectRdd: RDD[(VertexId, Double)], objectPath: String, textRdd: RDD[(VertexId, Double, Boolean)], textPath: String) = {
    //检查hdfs中是否已经存在
    val hdfs = FileSystem.get(new URI("hdfs://cluster1:8020"), sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(objectPath), true)
      hdfs.delete(new Path(textPath), true)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    objectRdd.saveAsObjectFile(objectPath)
    textRdd.saveAsTextFile(textPath)
  }

  def get3RDDAsObjectFile[AD: ClassTag, BD: ClassTag, CD: ClassTag](sc: SparkContext, path: String) = {
    val RDDPair = sc.objectFile[(AD, BD, CD)](path).repartition(128)
    RDDPair
  }

  def save3RDDAsObjectFile[AD: ClassTag, BD: ClassTag, CD: ClassTag](RDDPair: RDD[(AD, BD, CD)], sc: SparkContext, path: String) = {
    //检查hdfs中是否已经存在
    val hdfs = FileSystem.get(new URI("hdfs://cluster1:8020"), sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(path), true)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    RDDPair.saveAsObjectFile(path)
  }

  /**
    * 从hdfs中读取初始图
    */
  def getFromObjectFile[VD: ClassTag, ED: ClassTag](sc: SparkContext, pathV: String, pathE: String) = {
    val vertices = sc.objectFile[(VertexId, VD)](pathV).repartition(128)
    val edges = sc.objectFile[Edge[ED]](pathE).repartition(128)
    Graph[VD, ED](vertices, edges)
  }

  /**
    * 将图先存入hdfs中
    */
  def saveAsObjectFile[VD, ED](graph: Graph[VD, ED], sc: SparkContext, pathV: String, pathE: String): Unit = {
    //检查hdfs中是否已经存在
    val hdfs = FileSystem.get(new URI("hdfs://cluster1:8020"), sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(pathV), true)
      hdfs.delete(new Path(pathE), true)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    graph.vertices.saveAsObjectFile(pathV)
    graph.edges.saveAsObjectFile(pathE)
  }

  /**
    * 从数据库中读取构建初始图的点和边(即将用j够的新数据)
    */
  def getFromOracle(hiveContext: HiveContext): Graph[InitVertexAttr, InitEdgeAttr] = {
    //注册成表
    // import hiveContext.implicits._
    val V_DF = hiveContext.read.format("jdbc").options(db + (("dbtable", "jjj_vertex"))).load()
    val E_DF = hiveContext.read.format("jdbc").options(db + (("dbtable", "jjj_edge"))).load()
    //点表
    val xy = V_DF.selectExpr("id", "xydj", "xyfz").rdd.map(v => (v.getAs[BigDecimal]("id").longValue(), (v.getAs[String]("xydj"), v.getAs[Int]("xyfz"))))
    val vertex = V_DF.selectExpr("id", "name", "sbh", "isNSR").rdd.map(v => (v.getAs[BigDecimal]("id").longValue(), InitVertexAttr(v.getAs[String]("name"), v.getAs[String]("sbh"), v.getAs[Boolean]("isNSR"))))
      .leftOuterJoin(xy).map { case (vid, (vattr, djfz)) =>
      vattr.xydj = djfz.get._1
      vattr.xyfz = djfz.get._2
      (vid, vattr)
    }.persist(StorageLevel.MEMORY_AND_DISK)
    //边表
    val edge = E_DF.selectExpr("source_id", "target_id", "w_control", "w_invest", "w_stockholder", "w_trade").rdd.map(e => (e.getAs[BigDecimal]("source_id").longValue(), e.getAs[BigDecimal]("target_id").longValue(), InitEdgeAttr(e.getAs[Double]("w_legal"), e.getAs[Double]("w_invest"), e.getAs[Double]("w_stockholder"), e.getAs[Double]("w_trade"))))
      .map(e => Edge(e._1, e._2, e._3))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val degrees = Graph(vertex, edge).degrees.persist()
    //使用度大于0的顶点边构图
    Graph(vertex.join(degrees).map(v => (v._1, v._2._1)), edge).persist()
  }

  /**
    * 从数据库中读取构建初始图的点和边,重新编完号后投资、法人、股东边存入数据库添加反向影响，点存入HDFS
    */
  def saveE2Oracle_V2HDFS(sqlContext: SparkSession) = {

    import sqlContext.implicits._
    val FR_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_NSR_FDDBR"))).load()
    val TZ_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_NSR_TZF"))).load()
    val GD_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_NSR_GD"))).load()
    //  val JY_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_XFNSR_GFNSR"))).load()
    val XYJB_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_GROUNDTRUTH"))).load()
    val xyjb = XYJB_DF.select("VERTEXID", "XYGL_XYJB_DM", "FZ", "WTBZ").rdd
      .map(row => (row.getAs[BigDecimal]("VERTEXID").longValue(), (row.getAs[BigDecimal]("FZ").intValue(), row.getAs[String]("XYGL_XYJB_DM"), row.getAs[String]("WTBZ"))))


    //计算点表(先计算出所有纳税人节点，在计算所有非纳税人节点)
    //抽出投资方为纳税人的数据行
    val TZ_NSR_DF = TZ_DF.filter($"TZFXZ".startsWith("1") || $"TZFXZ".startsWith("2") || $"TZFXZ".startsWith("3"))
      .selectExpr("ZJHM as TZ_ZJHM", "VERTEXID as BTZ_VERTEXID", "TZBL", "TZFMC AS NAME")
    //抽出股东为纳税人的数据行
    val GD_NSR_DF = GD_DF.filter($"JJXZ".startsWith("1") || $"JJXZ".startsWith("2") || $"JJXZ".startsWith("3"))
      .selectExpr("ZJHM as TZ_ZJHM", "VERTEXID as BTZ_VERTEXID", "TZBL", "GDMC AS NAME")
    val ZJHM_NSR_DF = TZ_NSR_DF.unionAll(GD_NSR_DF)
    val NSR_VERTEX = ZJHM_NSR_DF.selectExpr("TZ_ZJHM AS ZJHM").except(FR_DF.selectExpr("ZJHM")) //投资方与股东表中投资方的证件号码除去法人表中的法人的证件号码
      .join(ZJHM_NSR_DF, $"ZJHM" === $"TZ_ZJHM").select("TZ_ZJHM", "NAME") //再join原dataframe是为了得到名称
      .rdd.map(row => (row.getAs[String]("NAME"), row.getAs[String]("TZ_ZJHM"), true))

    //抽出投资方为非纳税人的数据行
    val TZ_FNSR_DF = TZ_DF.filter($"TZFXZ".startsWith("4") || $"TZFXZ".startsWith("5"))
    //抽出股东为非纳税人的数据行
    val GD_FNSR_DF = GD_DF.filter($"JJXZ".startsWith("4") || $"JJXZ".startsWith("5"))
    val FNSR_VERTEX = FR_DF.selectExpr("ZJHM", "FDDBRMC AS NAME")
      .unionAll(TZ_FNSR_DF.selectExpr("ZJHM", "TZFMC AS NAME"))
      .unionAll(GD_FNSR_DF.selectExpr("ZJHM", "GDMC AS NAME"))
      .rdd.map(row => (row.getAs[String]("NAME"), row.getAs[String]("ZJHM"), false))

    val maxNsrID = FR_DF.agg(max("VERTEXID")).head().getDecimal(0).longValue()
    val NSR_FNSR_VERTEX = FNSR_VERTEX.union(NSR_VERTEX).map { case (name, sbh, isNSR) => (sbh, InitVertexAttr(name, sbh, isNSR)) }
      .reduceByKey(InitVertexAttr.combine).zipWithIndex().map { case ((nsrsbh, attr), index) => (index + maxNsrID, attr) }

    val ALL_VERTEX = NSR_FNSR_VERTEX.union(FR_DF.select("VERTEXID", "NSRDZDAH", "NSRMC").rdd.map(row =>
      (row.getAs[BigDecimal]("VERTEXID").longValue(), InitVertexAttr(row.getAs[String]("NSRMC"), row.getAs[BigDecimal]("NSRDZDAH").toString, true))))
      .leftOuterJoin(xyjb)
      .map { case (vid, (vattr, opt_fz_dm)) =>
        if (!opt_fz_dm.isEmpty) {
          vattr.xyfz = opt_fz_dm.get._1
          vattr.xydj = opt_fz_dm.get._2
          if (opt_fz_dm.get._3.equals("Y"))
            vattr.wtbz = true
        }
        (vid, vattr)
      }.persist(StorageLevel.MEMORY_AND_DISK)

    //计算边表
    //投资方为纳税人（表示为投资方证件号码对应法人表证件号码所对应的公司）的投资关系
    val tz_cc = TZ_NSR_DF.
      join(FR_DF, $"TZ_ZJHM" === $"ZJHM").
      select("VERTEXID", "BTZ_VERTEXID", "TZBL").
      rdd.map { case row =>
      val eattr = InitEdgeAttr(0.0, row.getAs[BigDecimal](2).doubleValue(), 0.0, 0.0)
      ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
    }

    val gd_cc = GD_NSR_DF.
      join(FR_DF, $"TZ_ZJHM" === $"ZJHM").
      select("VERTEXID", "BTZ_VERTEXID", "TZBL").
      rdd.map { case row =>
      val eattr = InitEdgeAttr(0.0, 0.0, row.getAs[BigDecimal](2).doubleValue(), 0.0)
      ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
    }

    //投资方为非纳税人的投资关系
    val tz_pc_cc = TZ_DF.
      selectExpr("ZJHM", "VERTEXID", "TZBL").
      except(TZ_NSR_DF.join(FR_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
      rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
      join(NSR_FNSR_VERTEX.keyBy(_._2.sbh)).
      map { case (sbh1, ((dstid, tzbl), (srcid, attr))) =>
        val eattr = InitEdgeAttr(0.0, tzbl, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }

    val gd_pc_cc = GD_DF.
      selectExpr("ZJHM", "VERTEXID", "TZBL").
      except(GD_NSR_DF.join(FR_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
      rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
      join(NSR_FNSR_VERTEX.keyBy(_._2.sbh)).
      map { case (sbh1, ((dstid, gdbl), (srcid, attr))) =>
        val eattr = InitEdgeAttr(0.0, 0.0, gdbl, 0.0)
        ((srcid, dstid), eattr)
      }

    /*   val trade_cc = JY_DF.
         select("xf_VERTEXID", "gf_VERTEXID", "jybl", "je", "se", "sl").
         rdd.map { case row =>
         val eattr = InitEdgeAttr(0.0, 0.0, 0.0, row.getAs[BigDecimal]("jybl").doubleValue())
         eattr.trade_je = row.getAs[BigDecimal]("je").doubleValue()
         eattr.tax_rate = row.getAs[BigDecimal]("sl").doubleValue()
         ((row.getAs[BigDecimal]("xf_VERTEXID").longValue(), row.getAs[BigDecimal]("gf_VERTEXID").longValue()), eattr)
       }*/

    val fddb_pc = FR_DF.select("VERTEXID", "ZJHM").
      rdd.map(row => (row.getAs[String](1), row.getAs[BigDecimal](0).longValue())).
      join(NSR_FNSR_VERTEX.keyBy(_._2.sbh)).
      map { case (sbh1, (dstid, (srcid, attr))) =>
        val eattr = InitEdgeAttr(1.0, 0.0, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }
    // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环
    val ALL_EDGE = tz_cc.union(gd_cc).union(tz_pc_cc).union(gd_pc_cc).union(fddb_pc).
      reduceByKey(InitEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2).
      map(edge => Edge(edge._1._1, edge._1._2, edge._2)).
      persist(StorageLevel.MEMORY_AND_DISK)

    //将所有点存入hdfs
    ALL_VERTEX.saveAsObjectFile("/user/lg/maxflowCredit/startVertices")


    DataBaseManager.execute("truncate table " + "lg_startEdge") //注意词表不是很准确，因为集群崩时重跑，hdfs 上的是准确的
    val schema = StructType(
      List(
        StructField("SRC", LongType, true),
        StructField("DST", LongType, true),
        StructField("W_LEGAL", DoubleType, true),
        StructField("W_INVEST", DoubleType, true),
        StructField("W_STOCKHOLDER", DoubleType, true)
      )
    )

    val rowRDD = ALL_EDGE.filter(e => e.attr.w_invest != 0.0 || e.attr.w_legal != 0.0 || e.attr.w_stockholder != 0.0).map(p => Row(p.srcId, p.dstId, p.attr.w_legal, p.attr.w_invest, p.attr.w_stockholder)).distinct()
    val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    val options = new JDBCOptions(db + ("dbtable" -> "lg_startEdge"))
    JdbcUtils.saveTable(edgeDataFrame, Option(schema), false, options)
    // JdbcUtils.saveTable(edgeDataFrame, url, "lg_startEdge", properties)
  }

  /**
    * 从数据库中读取构建初始图的点和边
    */
  def getFromOracle2(sqlContext: SparkSession, sc: SparkContext): Graph[InitVertexAttr, InitEdgeAttr] = {
    val FR_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_FR2"))).load()
    val TZ_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_TZ2"))).load()
    val GD_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_GD2"))).load()
    val JY_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_XFNSR_GFNSR"))).load()

    //计算边表
    //法人边正反影响融合
    val fr = FR_DF.select("SRC", "DST", "W_LEGAL").
      rdd.map(row => ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), row.getAs[BigDecimal](2).doubleValue())).
      reduceByKey((a, b) => {
        val f_positive = a * b //正向融合因子
        val f_inverse = (1 - a) * (1 - b)
        f_positive / (f_positive + f_inverse)
      }).map { case row =>
      val eattr = InitEdgeAttr(row._2, 0.0, 0.0, 0.0)
      (row._1, eattr)
    }

    //投资边正反影响融合
    val tz = TZ_DF.select("SRC", "DST", "W_INVEST").
      rdd.map(row => ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), row.getAs[BigDecimal](2).doubleValue())).
      reduceByKey((a, b) => {
        val f_positive = a * b
        val f_inverse = (1 - a) * (1 - b)
        f_positive / (f_positive + f_inverse)
      }).map { case row =>
      val eattr = InitEdgeAttr(0.0, row._2, 0.0, 0.0)
      (row._1, eattr)
    }

    //股东边正反影响融合
    val gd = GD_DF.select("SRC", "DST", "W_STOCKHOLDER").
      rdd.map(row => ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), row.getAs[BigDecimal](2).doubleValue())).
      reduceByKey((a, b) => {
        val f_positive = a * b
        val f_inverse = (1 - a) * (1 - b)
        f_positive / (f_positive + f_inverse)
      }).map { case row =>
      val eattr = InitEdgeAttr(0.0, 0.0, row._2, 0.0)
      (row._1, eattr)
    }


    //========================================
    /*
    val tz_forward = tz_cc.union(tz_pc_cc).reduceByKey(InitEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2)
    val tz_backward = tz_forward.collect.map {
      case (v, e) =>
        val srcOutSum = tz_forward.filter(_._1._1 == v._1).map(_._2.w_invest).reduce(_ + _)
        ((v._2, v._1), InitEdgeAttr(0.0, e.w_invest / srcOutSum, 0.0, 0.0))
    }
    val tz_fusion= tz_forward.union(tz_backward).reduceByKey((a,b)=> {
      val f_invest_positive = a.w_invest * b.w_invest
      val f_invest_inverse = (1 - a.w_invest) * (1 - b.w_invest)
      InitEdgeAttr(a.w_legal,f_invest_positive / (f_invest_positive + f_invest_inverse),a.w_stockholder,a.w_trade)
    })
  */
    //========================================

    val jy = JY_DF.
      select("xf_VERTEXID", "gf_VERTEXID", "jybl", "je", "se", "sl").
      rdd.map { case row =>
      val eattr = InitEdgeAttr(0.0, 0.0, 0.0, row.getAs[BigDecimal]("jybl").doubleValue())
      eattr.trade_je = row.getAs[BigDecimal]("je").doubleValue()
      eattr.tax_rate = row.getAs[BigDecimal]("sl").doubleValue()
      ((row.getAs[BigDecimal]("xf_VERTEXID").longValue(), row.getAs[BigDecimal]("gf_VERTEXID").longValue()), eattr)
    }

    // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环,交易边选择此为避免出现<0的边，可能存在 交易边不是双向边
    val ALL_EDGE = fr.union(tz).union(gd).union(jy.filter(x => x._2.w_trade > 0.01)).
      reduceByKey(InitEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2).
      map(edge => Edge(edge._1._1, edge._1._2, edge._2)).
      persist(StorageLevel.MEMORY_AND_DISK)

    val ALL_VERTEX = sc.objectFile[(Long, InitVertexAttr)]("/user/lg/maxflowCredit/startVertices").repartition(128)
    val degrees = Graph(ALL_VERTEX, ALL_EDGE).degrees.persist
    // 使用度大于0的顶点和边构建图
    Graph(ALL_VERTEX.join(degrees).map(vertex => (vertex._1, vertex._2._1)), ALL_EDGE).subgraph(vpred = (vid, vattr) => vattr.xyfz >= 0).persist()

  }

  /**
    * 判断hdfs中是否存在
    */
  def Exist(sc: SparkContext, path: String) = {
    val hdfs = FileSystem.get(new URI("hdfs://cluster1:8020"), sc.hadoopConfiguration)
    hdfs.exists(new Path(path))
  }

  def getFromCsv(sc: SparkContext, vertexPath: String, edgePath: String): MaxflowGraph = {
    val edgesTxt = sc.textFile(edgePath)
    val vertexTxt = sc.textFile(vertexPath)
    val vertices = vertexTxt.filter(!_.startsWith("id")).map(_.split(",")).map {
      case node => val vtemp = MaxflowVertexAttr(node(0).toLong, node(1).toDouble)
        vtemp
    }.collect()
    var G = new MaxflowGraph()
    val edges = edgesTxt.filter(!_.startsWith("src")).map(_.split(",")).map {
      case e => {
        val a = vertices.filter(_.id == e(0).toLong).toList.head
        val b = vertices.filter(_.id == e(1).toLong).toList.head
        G.addEdge(a, b, e(2).toDouble)
      }
    }
    return G
  }


  def saveMaxflowResultToOracle(//outputV: RDD[(String, String, Long, Double, Double, Double)],
                                outputV: RDD[(String, String, Long, Double, Double)],
                                outputE: RDD[(String, String, String, String)],
                                sqlContext: SparkSession,
                                vertex_dst: String = "LG_MAXFLOW_VERTEX", edge_dst: String = "LG_MAXFLOW_EDGE"): Unit = {
    DataBaseManager.execute("truncate table " + vertex_dst)
    val schemaV = StructType(
      List(
        StructField("COMPANY", StringType, true),
        StructField("VERTICE", StringType, true),
        StructField("V", LongType, true),
        StructField("INITSCORE", LongType, true),
        StructField("FINALSCORE", DoubleType, true),
        StructField("INFLUENCE", DoubleType, true)
     //   StructField("RATIO", DoubleType, true)
      )
    )
  //  val rowRDD1 = outputV.map(p => Row(p._1, p._2, p._2.toLong, p._3, p._4, p._5, p._6)).distinct()
    val rowRDD1 = outputV.map(p => Row(p._1, p._2, p._2.toLong, p._3, p._4, p._5)).distinct()
    val vertexDataFrame = sqlContext.createDataFrame(rowRDD1, schemaV).repartition(3)
    //  JdbcUtils.saveTable(vertexDataFrame, url, vertex_dst, properties)
    val optionsV = new JDBCOptions(db + ("dbtable" -> vertex_dst))
    JdbcUtils.saveTable(vertexDataFrame, Option(schemaV), false, optionsV)
    DataBaseManager.execute("truncate table " + edge_dst)
    val schemaE = StructType(
      List(
        StructField("COMPANY", StringType, true),
        StructField("SOURCE", StringType, true),
        StructField("TARGET", StringType, true),
        StructField("DIRECTINFLUENCE", StringType, true)
      )
    )
    val rowRDD = outputE.map(e => Row(e._1, e._2, e._3, e._4)).distinct()

    val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schemaE).repartition(3)
    //   JdbcUtils.saveTable(edgeDataFrame, url, edge_dst, properties)
    val optionsE = new JDBCOptions(db + ("dbtable" -> edge_dst))
    JdbcUtils.saveTable(edgeDataFrame, Option(schemaE), false, optionsE)
  }

  def getFeatures(spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._
    val FEATURES_DF_2014 = spark.read.format("jdbc").options(db + (("dbtable" -> "tax.lg_features_2014_final"))).load()
    val FEATURES_DF_2015 = spark.read.format("jdbc").options(db + (("dbtable" -> "tax.lg_features_2015_final"))).load()
    (FEATURES_DF_2014,FEATURES_DF_2015)
  }





  /*
  //三大报表数据处理转轴处理
  def handleThreeReports(sqlContext: SparkSession) = {
    import sqlContext.implicits._
    val ZCFZB_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.lg_zcfzb"))).load()
    val LRB_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.lg_lrb"))).load()
    val XJLLB_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.lg_xjllb"))).load()

    val threeBB_2014 = tranferFeatures(ZCFZB_DF, 66, 2014).union(tranferFeatures(LRB_DF, 20, 2014)).reduceByKey((a, b) => if (a.length > b.length) a ++ b else b ++ a).union(tranferFeatures(XJLLB_DF, 20, 2014)).reduceByKey((a, b) => if (a.length > b.length) a ++ b else b ++ a)
    val threeBB_2015 = tranferFeatures(ZCFZB_DF, 66, 2015).union(tranferFeatures(LRB_DF, 20, 2015)).reduceByKey((a, b) => if (a.length > b.length) a ++ b else b ++ a).union(tranferFeatures(XJLLB_DF, 20, 2015)).reduceByKey((a, b) => if (a.length > b.length) a ++ b else b ++ a)
  }
  def tranferFeatures(df: DataFrame, num: Int, year: Int) = {
    df.rdd.map(row => (row.getAs[BigDecimal](0).longValue(), row.getAs[String](1).toInt, row.getAs[BigDecimal](2).doubleValue(), row.getAs[BigDecimal](3).doubleValue(), row.getAs[BigDecimal](4).intValue())).filter(_._5 == year).
      map(x => (x._1, (x._2, x._3, x._4))).groupByKey().map { x =>
      val bn = new HashMap[Int, Double] //本年金额
    val sn = new HashMap[Int, Double] //上年金额
      for (je <- x._2) {
        bn.put(je._1, je._2)
        sn.put(je._1, je._3)
      }
      for (i <- 1 to num) {
        if (!bn.contains(i)) {
          bn.put(i, 0)
        }
        if (!sn.contains(i)) {
          sn.put(i, 0)
        }
      }
      (x._1, (bn.toSeq.sorted ++ sn.toSeq.sorted).map(_._2))
    }
  }
*/

}
