package lg.scala.utils


import lg.scala.entity.{EdgeAttr, InitEdgeAttr, InitVertexAttr, VertexAttr}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by lg on 2017/6/22.
  */
object CreditGraphTools {
  type Path = Seq[(VertexId, Double)]
  type Paths = Seq[Seq[(VertexId, Double)]]

  def extractNSR2(graph: Graph[InitVertexAttr, InitEdgeAttr]): Graph[VertexAttr, EdgeAttr] = {
    graph.subgraph(vpred = (vid, vattr) => vattr.isNSR == true)
      .mapVertices { (vid, vattr) =>
        val newVattr = VertexAttr(vattr.sbh, vattr.name)
        newVattr.xydj = vattr.xydj
        newVattr.xyfz = vattr.xyfz
        newVattr.wtbz = vattr.wtbz
        newVattr
      }
      .mapEdges { edge =>
        val newEattr = EdgeAttr()
        if (edge.attr.w_invest > 1D)
          newEattr.w_invest = 1.0
        else
          newEattr.w_invest = edge.attr.w_invest
        if (edge.attr.w_stockholder > 1D)
          newEattr.w_stockholder = 1.0
        else
          newEattr.w_stockholder = edge.attr.w_stockholder
        if (edge.attr.w_trade > 1D)
          newEattr.w_trade = 1.0
        else
          newEattr.w_trade = edge.attr.w_trade
        if (edge.attr.w_legal > 1D)
          newEattr.w_cohesion = 1.0
        else
          newEattr.w_cohesion = edge.attr.w_legal
        newEattr
      }
  }


  /**
    * 抽取出仅含公司的tpin大图
    */
  def extractNSR(graph: Graph[InitVertexAttr, InitEdgeAttr]): Graph[VertexAttr, EdgeAttr] = {
    //   val cohesionDifference = graph.edges.map(x => (x.attr.w_cohesion)).max - graph.edges.map(x => (x.attr.w_cohesion)).min
    val g = graph.subgraph(vpred = (vid, vattr) => vattr.isNSR == true)
      .mapVertices { (vid, vattr) =>
        val newVattr = VertexAttr(vattr.sbh, vattr.name)
        newVattr.xydj = vattr.xydj
        newVattr.xyfz = vattr.xyfz
        newVattr.wtbz = vattr.wtbz
        newVattr
      }
      .mapEdges { edge =>
        val newEattr = EdgeAttr()
        if (edge.attr.w_invest > 1D)
          newEattr.w_invest = 1.0
        else
          newEattr.w_invest = edge.attr.w_invest
        if (edge.attr.w_stockholder > 1D)
          newEattr.w_stockholder = 1.0
        else
          newEattr.w_stockholder = edge.attr.w_stockholder
        if (edge.attr.w_trade > 1D)
          newEattr.w_trade = 1.0
        else
          newEattr.w_trade = edge.attr.w_trade
        newEattr.w_cohesion = edge.attr.w_cohesion //  /cohesionDifference
        newEattr
      }
    //过滤掉边上无权值的
    val filter_E = g.edges.filter(edge => edge.attr.w_cohesion != 0.0 || edge.attr.w_invest != 0.0 || edge.attr.w_stockholder != 0.0 || edge.attr.w_trade != 0.0)
    val filterGraph = Graph(g.vertices, filter_E)
    val vertexDegree = filterGraph.degrees.persist()
    Graph(filterGraph.vertices.join(vertexDegree).map(v => (v._1, v._2._1)), filterGraph.edges)

  }

  def sendPaths(edge: EdgeContext[Paths, Double, Paths], length: Int) = { //EdgeContext传递的为[VD, ED, A]
    //得到非纳税人控制的关系链，所以以非纳税人为起点，初始长度中非纳税人为1，纳税人为0
    //过滤掉非起点 (得到与所需length一样的路径,因为起点必须是非纳税人) 与 非环
    val filterEdge = edge.srcAttr.filter(_.size == length).filter(!_.map(_._1).contains(edge.dstId))

    if (filterEdge.size > 0)
    //向终点发送控制人关系路径集合
      edge.sendToDst(filterEdge.map(_ ++ Seq((edge.dstId, edge.attr))))
  }

  def reducePaths(a: Paths, b: Paths): Paths = a ++ b

  def sendPathsReverse(edge: EdgeContext[Paths, Double, Paths], length: Int) = {
    val filterEdge = edge.dstAttr.filter(_.size == length).filter(!_.contains(edge.srcId))
    if (filterEdge.size > 0)
      edge.sendToSrc(filterEdge.map(Seq((edge.srcId, edge.attr)) ++ _))
  }


  /**
    * 得到路径
    * direction=1 表示从源向终传递消息 ； -1从终向源
    *
    */
  def getPath[VD: ClassTag, ED: ClassTag](initialGraph: Graph[VD, ED],
                                          sendPaths: (EdgeContext[VD, ED, VD], Int) => Unit,
                                          reducePaths: (VD, VD) => VD,
                                          maxIteration: Int = Int.MaxValue,
                                          direction: Int = 1,
                                          initLength: Int = 1) = {
    /*
     preprocessGraph.vertices.filter(_._1==1).first()._2.size
     preprocessGraph.vertices.filter(_._1==2).first()._2.size
     preprocessGraph.vertices.filter(_._1==1).filter(!_._2.contains(2)).collect
     preprocessGraph.vertices.filter(_._1==2).filter(!_._2.contains(1)).collect
 */
    //使用度大于0的顶点和边构建前件网络图（得到点前件路径【空】，边重新计算权值）
    var preprocessGraph = initialGraph.cache()

    //路径长度（当迭代路径为2时，可能出现人-公司-人的情况）
    var i = initLength;
    var messages: VertexRDD[VD] = null
    //message的长度为i+1
    if (direction == 1)
      messages = preprocessGraph.aggregateMessages[VD](sendPaths(_, i), reducePaths)
    //    else
    //      messages = preprocessGraph.aggregateMessages[VD](sendPathsReverse(_, i), reducePaths)

    var activeMessages = messages.count()
    var preG: Graph[VD, ED] = null
    while (activeMessages > 0 && i <= maxIteration) {
      preG = preprocessGraph
      //长度=maxIteration+1
      preprocessGraph = preprocessGraph.joinVertices[VD](messages)((id, oldVD, newPath) => reducePaths(oldVD, newPath)).cache() //迭代给点上记录路径信息
      println(i + " 次迭代完成！")
      i += 1

      val oldMessages = messages
      if (direction == 1)
        messages = preprocessGraph.aggregateMessages[VD](sendPaths(_, i), reducePaths).cache()
      //      else
      //        messages = preprocessGraph.aggregateMessages[VD](sendPathsReverse(_, i), reducePaths).cache()
      activeMessages = messages.count()
      oldMessages.unpersist(blocking = false)
      preG.unpersistVertices(blocking = false)
      preG.edges.unpersist(blocking = false)
    }
    preprocessGraph.vertices
  }

  //要求亲密度关联节点的重叠度为2（严格版）  两层for 循环
  def reComputeWeight(d: Seq[(VertexId, mutable.Map[Long, Double])], degree: Int = 1) = {
    val overlap = d
    //until 返回所有小于但不包括上限的数字。而to是返回包括上线的数字
    val result =
      for (i <- 0 until overlap.length) yield //非纳税人控制的每条链的上的节点的两两遍历
        for (j <- i + 1 until overlap.length) yield {
          val (vid1, list1) = overlap(i)
          val (vid2, list2) = overlap(j)
          val intersect = list1.keySet.intersect(list2.keySet) //所有重叠部分的董事会
          var weight = 0D
          if (intersect.size >= degree) {
            for (key <- intersect)
            //   weight += map1.get(key).get.min(map2.get(key).get) //同一人控制两公司时权值较小的公司
            //    weight = map1.get(key).get.max(map2.get(key).get) //同一人控制两公司时权值较大的公司
              weight += list1.getOrElse(key, 0.0).min(list2.getOrElse(key, 0.0)) //4. 相加
          }
          if (weight > 0)
            Option(Iterable(((vid1, vid2), weight), ((vid2, vid1), weight))) //Option是一个数据。Option[A] 是一个类型为 A 的可选值的容器： 如果值存在， Option[A] 就是一个 Some[A] ，如果不存在， Option[A] 就是对象 None
          else
            Option.empty
        }
    result.flatten.filter(!_.isEmpty).map(_.get).flatten
  }

  def removeDegreeZero[VD: ClassTag, ED: ClassTag](tpin: Graph[VD, ED]): Graph[VD, ED] = {
    val degreesRDD = tpin.degrees.cache()
    var preproccessedGraph = tpin.
      outerJoinVertices(degreesRDD)((vid, vattr, degreesVar) => (vattr, degreesVar.getOrElse(0))).
      subgraph(vpred = {
        case (vid, (vattr, degreesVar)) =>
          degreesVar > 0
      }).
      mapVertices {
        case (vid, (attr, degree)) => attr
      }
    preproccessedGraph
  }


  /**
    * 添加企业之间的亲密度关系
    */
  def addCohesion(tpinFromObject: Graph[InitVertexAttr, InitEdgeAttr], weight: Double) = {
    //每个非纳税人直接或间接控制的企业列表
    val initialGraph = tpinFromObject.mapVertices { case (id, vattr) =>
      if (!vattr.isNSR) //非纳税人
        Seq(Seq((id, 1D))) //1D表示1的Double类型
      else //纳税人
        Seq[Seq[(VertexId, Double)]]()
    }.subgraph(epred = triplet =>
      triplet.attr.isAntecedent(weight)).mapEdges(edge => Seq(edge.attr.w_legal, edge.attr.w_invest, edge.attr.w_stockholder).max).cache() //1.

    //信息（公司id,Map(自然人id,权重)）：此处无法使用反向获取路径，,即使用正向获取路径，要求源点为人 ，maxIteration表示前件路径最长长度为3
    val messageOfControls = CreditGraphTools.getPath(removeDegreeZero(initialGraph), sendPaths, reducePaths, maxIteration = 3).mapValues { lists =>
      val result = mutable.HashMap[Long, Double]()
      lists.filter(_.size > 1).foreach { case list =>
        val influ = list.map(_._2).min //2.
        result.update(list.head._1, result.getOrElse(list.head._1, influ).max(influ)) //3.
      }
      result
    }.filter(_._2.size > 0). //留下有链的
      join(tpinFromObject.vertices.filter(_._2.isNSR == true)).map(x => (x._1, x._2._1)) //留下节点为公司的，去除（人-公司-人） 类型

    //信息(自然人id,List((公司id1,Map(自然人id,权重)),(公司id2,Map(自然人id,权重)),...))：
    // 重要，转移消息 首先人A控制B、C公司，将B、C得到的控制链消息转移到A上（即在非纳税人上存储其控制的公司链表及其权值）
    val moveMessageOfControls = messageOfControls.flatMap {
      case (cid, controllist) => controllist.map(pcontrol => (pcontrol._1, (cid, controllist)))
    }.groupByKey().map { case (vid, iterab) => (vid, iterab.toSeq) }


    //添加新的亲密度关系边（list 为）
    val newCohesionE = moveMessageOfControls.flatMap { case (vid, list) => CreditGraphTools.reComputeWeight(list) }.
      distinct.reduceByKey(_.min(_)) //再次筛选,因为会出现两次的结果小数位数不同的情况
    val cohe_max = newCohesionE.map(_._2).max
    val cohe_min = newCohesionE.map(_._2).min

    val c=newCohesionE.count
    val bcnewCohesionE= newCohesionE.collect

     val newCohesionEdges= newCohesionE.map { case ((src, dst), weight) =>
      val edgeAttr = InitEdgeAttr()
      edgeAttr.is_Cohesion = true
      //edgeAttr.w_cohesion = (weight/(cohe_max-cohe_min)).formatted("%.3f").toDouble
      edgeAttr.w_cohesion = (bcnewCohesionE.filter(x=>(x._2<weight)).length/c).toDouble.formatted("%.3f").toDouble

      Edge(src, dst, edgeAttr)
    }

    val newEdge = tpinFromObject.edges.union(newCohesionEdges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(InitEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2)
      .map(e => Edge(e._1._1, e._1._2, e._2))
    Graph(tpinFromObject.vertices, newEdge)
  }


}
