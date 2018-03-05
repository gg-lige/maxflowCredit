package lg.scala.utils


import lg.scala.entity.{EdgeAttr, InitEdgeAttr, InitVertexAttr, VertexAttr}
import org.apache.spark.graphx._

import scala.collection.mutable

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

  /**
    * 得到路径
    * direction=1 表示从源向终传递消息 ； -1从终向源
    *
    */
  def getPath(initialGraph: Graph[Paths, InitEdgeAttr], weight: Double, maxIteration: Int = Int.MaxValue, initLength: Int = 1, direction: Int = 1, justGD: Boolean = false) = {
    def sendPaths(edge: EdgeContext[Paths, Double, Paths], length: Int) = { //EdgeContext传递的为[VD, ED, A]
      //得到非纳税人控制的关系链，所以以非纳税人为起点，初始长度中非纳税人为1，纳税人为0
      //过滤掉非起点 (得到与所需length一样的路径,因为起点必须是非纳税人) 与 非环
      val filterEdge = edge.srcAttr.filter(_.size == length).filter(!_.map(_._1).contains(edge.dstId))

      if (filterEdge.size > 0)
      //向终点发送控制人关系路径集合
        edge.sendToDst(filterEdge.map(_ ++ Seq((edge.dstId, edge.attr))))
    }

    def sendPathsReverse(edge: EdgeContext[Paths, Double, Paths], length: Int) = {
      val filterEdge = edge.dstAttr.filter(_.size == length).filter(!_.contains(edge.srcId))
      if (filterEdge.size > 0)
        edge.sendToSrc(filterEdge.map(Seq((edge.srcId, edge.attr)) ++ _))
    }

    //节点度
    val degreesRDD = initialGraph.degrees.cache()
    //使用度大于0的顶点和边构建前件网络图（得到点前件路径【空】，边重新计算权值）
    var preprocessGraph = initialGraph.subgraph(epred = triplet => triplet.attr.isAntecedent(weight, justGD)) //抽取含前件关系（即除交易关系)网络图
      .outerJoinVertices(degreesRDD)((vid, vattr, newattr) => (vattr, newattr.getOrElse(0))) //添一列属性为度值
      .subgraph(vpred = {
      case (vid, (vattr, newattr)) => newattr > 0
    }) //选择度大于0的构图
      .mapVertices { case (vid, (list, degree)) => list } // 点属性均为 Paths,此时还没有路径，均为空
 //1.     .mapEdges(edge => Seq(edge.attr.w_legal, edge.attr.w_invest, edge.attr.w_stockholder).max) //边上属性为 （法人、投资、股东） 关系最大值
      .mapEdges(edge => Seq(edge.attr.w_legal, edge.attr.w_invest, edge.attr.w_stockholder).min) //边上属性为 （法人、投资、股东） 关系最大值
      .cache()
    /*
        preprocessGraph.vertices.filter(_._1==1).first()._2.size
        preprocessGraph.vertices.filter(_._1==2).first()._2.size
        preprocessGraph.vertices.filter(_._1==1).filter(!_._2.contains(2)).collect
        preprocessGraph.vertices.filter(_._1==2).filter(!_._2.contains(1)).collect
    */
    //路径长度（当迭代路径为2时，可能出现人-公司-人的情况）
    var i = initLength;
    var messages: VertexRDD[Paths] = null
    //message的长度为i+1
    if (direction == 1)
      messages = preprocessGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _)
    else
      messages = preprocessGraph.aggregateMessages[Paths](sendPathsReverse(_, i), _ ++ _)

    var activeMessages = messages.count()
    var preG: Graph[Paths, Double] = null
    while (activeMessages > 0 && i <= maxIteration) {
      preG = preprocessGraph
      //长度=maxIteration+1
      preprocessGraph = preprocessGraph.joinVertices[Paths](messages)((id, oldVD, newPath) => oldVD ++ newPath).cache() //迭代给点上记录路径信息
      println(i + " 次迭代完成！")
      i += 1

      val oldMessages = messages
      if (direction == 1)
        messages = preprocessGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _).cache()
      else
        messages = preprocessGraph.aggregateMessages[Paths](sendPathsReverse(_, i), _ ++ _).cache()
      activeMessages = messages.count()
      oldMessages.unpersist(blocking = false)
      preG.unpersistVertices(blocking = false)
      preG.edges.unpersist(blocking = false)
    }
    preprocessGraph.vertices
  }

  //要求亲密度关联节点的重叠度为2（严格版）  两层for 循环
  def reComputeWeight(d: Seq[(VertexId, Seq[(Long, Double)])], degree: Int = 2) = {
    val overlap = d
    //until 返回所有小于但不包括上限的数字。而to是返回包括上线的数字
    val result =
      for (i <- 0 until overlap.length) yield //非纳税人控制的每条链的上的节点的两两遍历
        for (j <- i + 1 until overlap.length) yield {
          val (vid1, list1) = overlap(i)
          val (vid2, list2) = overlap(j)
          val map1 = list1.toMap //变成map方便查询重叠部分
          val map2 = list2.toMap
          val intersect = map1.keySet.intersect(map2.keySet) //所有重叠部分的董事会
          var weight = 0D
          if (intersect.size >= degree) {
            for (key <- intersect)
            //   weight += map1.get(key).get.min(map2.get(key).get) //当A到B之间有多条路径时，选择权值为较少者         ？？
        //2.      weight = map1.get(key).get.max(map2.get(key).get) //当A到B之间有多条路径时，选择权值为较大者
              weight = map1.get(key).get.min(map2.get(key).get) //当A到B之间有多条路径时，选择权值为较大者
          }
          if (weight > 0)
            Option(Iterable(((vid1, vid2), weight), ((vid2, vid1), weight))) //Option是一个数据。Option[A] 是一个类型为 A 的可选值的容器： 如果值存在， Option[A] 就是一个 Some[A] ，如果不存在， Option[A] 就是对象 None
          else
            Option.empty
        }
    result.flatten.filter(!_.isEmpty).map(_.get).flatten
  }

  /**
    * 添加企业之间的亲密度关系
    */
  def addCohesion(tpinFromObject: Graph[InitVertexAttr, InitEdgeAttr], weight: Double, degree: Int = 2) = {
    //每个非纳税人直接或间接控制的企业列表
    val initialGraph = tpinFromObject.mapVertices { case (id, vattr) =>
      if (!vattr.isNSR) //非纳税人
        Seq(Seq((id, 1D))) //1D表示1的Double类型
      else //纳税人
        Seq[Seq[(VertexId, Double)]]()
    }

    //此处无法使用反向获取路径，,即使用正向获取路径，要求源点为人 ，maxIteration表示前件路径最长长度为3
    val messageOfControls = CreditGraphTools.getPath(initialGraph, weight, maxIteration = 3).mapValues(lists =>
      lists.filter(_.size > 1).map {
        case list => val influ = list.map(_._2).min //一条控制链上有多条边，选择该链上边的最小权值
          (list.head._1, influ) //对于目标结点，仅留下链的头ID，与该链的权值
      }).map(x1 => (x1._1, x1._2.filter(_._1 != x1._1))).map {
      case (vid, repeat_list) =>
        val listMap = mutable.HashMap[VertexId, Double]()
        for (l <- repeat_list) {
          if (!listMap.contains(l._1))
            listMap.put(l._1, l._2)
          else if (listMap(l._1) > l._2)
            listMap.update(l._1, l._2)
        }
        (vid, listMap.toSeq) //处理平行路径，留下权值较小的那条
    }.filter(_._2.size > 0). //留下有链的
      join(tpinFromObject.vertices.filter(_._2.isNSR == true)).map(x => (x._1, x._2._1)) //留下节点为公司的，去除（人-公司-人） 类型

    //重要，转移消息 首先人A控制B、C公司，将B、C得到的控制链消息转移到A上（即在非纳税人上存储其控制的公司链表及其权值）
    val moveMessageOfControls = messageOfControls.flatMap {
      case (vid, controllist) => controllist.map(control => (control._1, (vid, controllist)))
    }.groupByKey().map { case (vid, iterab) => (vid, iterab.toSeq) }

    //添加新的亲密度关系边（list 为）
    val newCohesionEdges = moveMessageOfControls.flatMap { case (vid, list) => CreditGraphTools.reComputeWeight(list, degree) }.distinct.
//3.      reduceByKey(_.max(_)).//再次筛选为较大亲密度关系的
      reduceByKey(_.min(_)).//再次筛选为较大亲密度关系的
      map { case ((src, dst), weight) =>
      val edgeAttr = InitEdgeAttr()
      edgeAttr.is_Cohesion = true
      edgeAttr.w_cohesion = weight
      Edge(src, dst, edgeAttr)
    }

    val newEdge = tpinFromObject.edges.union(newCohesionEdges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(InitEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2)
      .map(e => Edge(e._1._1, e._1._2, e._2))
    Graph(tpinFromObject.vertices, newEdge)
  }


}
