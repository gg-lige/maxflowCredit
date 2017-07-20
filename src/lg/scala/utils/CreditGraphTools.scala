package lg.scala.utils


import lg.scala.entity.{EdgeAttr, InitEdgeAttr, InitVertexAttr, VertexAttr}
import org.apache.spark.graphx._

/**
  * Created by lg on 2017/6/22.
  */
object CreditGraphTools {
  type Path = Seq[(VertexId, Double)]
  type Paths = Seq[Seq[(VertexId, Double)]]

  /**
    * 抽取出仅含公司的tpin大图
    */
  def extractNSR(graph: Graph[InitVertexAttr, InitEdgeAttr]): Graph[VertexAttr, EdgeAttr] = {
    val cohesionDifference = graph.edges.map(x => (x.attr.w_cohesion)).max - graph.edges.map(x => (x.attr.w_cohesion)).min
    graph.subgraph(vpred = (vid, vattr) => vattr.isNSR == true)
      .mapVertices { (vid, vattr) =>
        val newVattr = VertexAttr(vattr.sbh, vattr.name)
        newVattr.xydj = vattr.xydj
        newVattr.xyfz = vattr.xyfz
        newVattr.wtbz= vattr.wtbz
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
        newEattr.w_cohesion = edge.attr.w_cohesion / cohesionDifference
        newEattr
      }
  }

  /**
    * 得到路径
    * direction=1 表示从源向终传递消息 ； -1从终向源
    *
    */
  def getPath(graph: Graph[Paths, InitEdgeAttr], weight: Double, maxIteration: Int = Int.MaxValue, initLength: Int = 1, direction: Int = 1, justGD: Boolean = false) = {
    def sendPaths(edge: EdgeContext[Paths, Double, Paths], length: Int) = {
      //过滤掉仅含交易关系的边与非起点      ??
      val filterEdge = edge.srcAttr.filter(_.size == length).filter(!_.contains(edge.dstId))
      if (filterEdge.size > 0)
      //向终点发送路径集合
        edge.sendToDst(filterEdge.map(_ ++ Seq((edge.dstId, edge.attr))))
    }

    def sendPathsReverse(edge: EdgeContext[Paths, Double, Paths], length: Int) = {
      //过滤掉仅含交易关系的边与非终点      ??
      val filterEdge = edge.dstAttr.filter(_.size == length).filter(!_.contains(edge.srcId))
      if (filterEdge.size > 0)
      //向终点发送路径集合
        edge.sendToSrc(filterEdge.map(Seq((edge.srcId, edge.attr)) ++ _))
    }

    //节点度
    val degreesRDD = graph.degrees.cache()
    //使用度大于0的顶点和边构建图
    var preprocessGraph = graph.subgraph(epred = triplet => triplet.attr.isAntecedent(weight, justGD)) //处理为前件网络图   ？？
      .outerJoinVertices(degreesRDD)((vid, vattr, newattr) => (vattr, newattr.getOrElse(0))) //添一列属性为度值
      .subgraph(vpred = {
      case (vid, (vattr, newattr)) => newattr > 0
    }) //选择度大于0的构图
      .mapVertices { case (vid, (list, degree)) => list } // 去掉度属性
      .mapEdges(edge => Seq(edge.attr.w_legal, edge.attr.w_invest, edge.attr.w_stockholder).max) //边上属性为 法人、投资、股东 关系最大值
      .cache()
    //路径长度
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

  //要求亲密度关联节点的重叠度为2（严格版）   ??
  def reComputeWeight(d: Seq[(VertexId, Seq[(Long, Double)])], degree: Int = 2) = {
    val overlap = d
    //until 返回所有小于但不包括上限的数字。而to是返回包括上线的数字
    val result =
      for (i <- 0 until overlap.length) yield
        for (j <- i + 1 until overlap.length) yield {
          val (vid1, list1) = overlap(i)
          val (vid2, list2) = overlap(j)
          val map1 = list1.toMap
          val map2 = list2.toMap
          val intersect = map1.keySet.intersect(map2.keySet)
          var weight = 0D
          for (key <- intersect)
            weight += map1.get(key).get.min(map2.get(key).get) //当A到B之间有多条路径时，选择权值为较少者         ？？
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
  def addCohesion(graph: Graph[InitVertexAttr, InitEdgeAttr], weight: Double, degree: Int = 2) = {
    //每个非纳税人直接或间接控制的企业列表
    val initialGraph = graph.mapVertices { case (id, vattr) =>
      if (!vattr.isNSR) //非纳税人
        Seq(Seq((id, 1D))) //1D表示1的Double类型
      else //纳税人
        Seq[Seq[(VertexId, Double)]]()
    }

    //此处无法使用反向获取路径，,即使用正向获取路径，要求源点为人
    val messageOfControls = getPath(initialGraph, weight, maxIteration = 3).mapValues(lists => lists.filter(_.size > 1)
      .map {
        case list => val influ = list.map(_._2).min //一条控制链上有多条边，选择该链上权值最小的
          (list.head._1, influ) //对于目标结点，仅留下链的头ID，与该链的权值
      }).filter(_._2.size > 0) //???

    //重要，转移消息 首先A控制B、C，将B、C得到的控制链消息转移到A上
    val moveMessageOfControls = messageOfControls.flatMap {
      case (vid, controllist) => controllist.map(control => (control._1, (vid, controllist)))
    }.groupByKey().map { case (vid, iterab) => (vid, iterab.toSeq) }

    //添加新的亲密度关系边
    val newCohesionEdges = moveMessageOfControls.flatMap { case (vid, list) => reComputeWeight(list, degree) }.distinct.map { case ((src, dst), weight) =>
      val edgeAttr = InitEdgeAttr()
      edgeAttr.is_Cohesion = true
      edgeAttr.w_cohesion = weight
      Edge(src, dst, edgeAttr)
    }
    Graph(graph.vertices, graph.edges.union(newCohesionEdges))
  }
}
