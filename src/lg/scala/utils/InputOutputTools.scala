package lg.scala.utils

import java.math.BigDecimal
import java.net.URI

import lg.java.Parameters
import lg.scala.entity.{InitEdgeAttr, InitVertexAttr}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag


/**
  * Created by lg on 2017/6/19.
  */
object InputOutputTools {
  def getRDDAsObjectFile[AD: ClassTag, BD: ClassTag](sc: SparkContext, path: String) = {
    val RDDPair = sc.objectFile[(AD, BD)](path).repartition(128)
    RDDPair
  }


  def saveRDDAsObjectFile[AD: ClassTag, BD: ClassTag](RDDPair: RDD[(AD, BD)], sc: SparkContext, path: String) = {
    //检查hdfs中是否已经存在
    val hdfs = FileSystem.get(new URI("hdfs://cloud-03:9000"), sc.hadoopConfiguration)
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
    val hdfs = FileSystem.get(new URI("hdfs://cloud-03:9000"), sc.hadoopConfiguration)
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
    val db = Map(
      "url" -> Parameters.DataBaseURL,
      "user" -> Parameters.DataBaseUserName,
      "password" -> Parameters.DataBaseUserPassword,
      "driver" -> Parameters.JDBCDriverString
    )
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
    * 从数据库中读取构建初始图的点和边(达的数据)
    */
  def getFromOracle2(sqlContext: HiveContext): Graph[InitVertexAttr, InitEdgeAttr] = {
    val db = Map(
      "url" -> Parameters.DataBaseURL,
      "user" -> Parameters.DataBaseUserName,
      "password" -> Parameters.DataBaseUserPassword,
      "driver" -> Parameters.JDBCDriverString
    )

    import sqlContext.implicits._
    val FR_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.WWD_NSR_FDDBR"))).load()
    val TZ_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.WWD_NSR_TZF"))).load()
    val GD_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_NSR_GD"))).load()
    val JY_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.WWD_XFNSR_GFNSR"))).load()
    val XYJB_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.WWD_GROUNDTRUTH"))).load()
    val xyjb = XYJB_DF.select("VERTEXID", "XYGL_XYJB_DM", "FZ", "WTBZ").rdd
      .map(row => (row.getAs[BigDecimal]("VERTEXID").longValue(), (row.getAs[BigDecimal]("FZ").intValue(), row.getAs[String]("XYGL_XYJB_DM"))))


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
        }
        (vid, vattr)
      }.persist(StorageLevel.MEMORY_AND_DISK)

    //计算边表
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

    val trade_cc = JY_DF.
      select("xf_VERTEXID", "gf_VERTEXID", "jybl", "je", "se", "sl").
      rdd.map { case row =>
      val eattr = InitEdgeAttr(0.0, 0.0, 0.0, row.getAs[BigDecimal]("jybl").doubleValue())
      eattr.trade_je = row.getAs[BigDecimal]("je").doubleValue()
      eattr.tax_rate = row.getAs[BigDecimal]("sl").doubleValue()
      ((row.getAs[BigDecimal]("xf_VERTEXID").longValue(), row.getAs[BigDecimal]("gf_VERTEXID").longValue()), eattr)
    }

    val fddb_pc = FR_DF.select("VERTEXID", "ZJHM").
      rdd.map(row => (row.getAs[String](1), row.getAs[BigDecimal](0).longValue())).
      join(NSR_FNSR_VERTEX.keyBy(_._2.sbh)).
      map { case (sbh1, (dstid, (srcid, attr))) =>
        val eattr = InitEdgeAttr(1.0, 0.0, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }
    // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环
    val ALL_EDGE = tz_cc.union(gd_cc).union(tz_pc_cc).union(gd_pc_cc).union(trade_cc).union(fddb_pc).
      reduceByKey(InitEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2).
      map(edge => Edge(edge._1._1, edge._1._2, edge._2)).
      persist(StorageLevel.MEMORY_AND_DISK)

    val degrees = Graph(ALL_VERTEX, ALL_EDGE).degrees.persist
    // 使用度大于0的顶点和边构建图

    Graph(ALL_VERTEX.join(degrees).map(vertex => (vertex._1, vertex._2._1)), ALL_EDGE).persist()
  }

  /**
    * 判断hdfs中是否存在
    */
  def Exist(sc: SparkContext, path: String) = {
    val hdfs = FileSystem.get(new URI("hdfs://cloud-03:9000"), sc.hadoopConfiguration)
    hdfs.exists(new Path(path))
  }

  def getFromCsv(sc: SparkContext, vertexPath: String, edgePath: String) = {
    val edgesTxt = sc.textFile(edgePath)
    val vertexTxt = sc.textFile(vertexPath)
    val vertices = vertexTxt.filter(!_.startsWith("id")).map(_.split(",")).map {
      case node => (node(0).toLong, node(1).toDouble)
    }
    val edges = edgesTxt.filter(!_.startsWith("src")).map(_.split(",")).map {
      case e => Edge(e(0).toLong, e(1).toLong, e(2).toDouble)
    }
    Graph(vertices, edges)
  }

}
