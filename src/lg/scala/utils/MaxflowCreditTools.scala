package lg.scala.utils


import java.util.{Comparator, PriorityQueue}

import lg.scala.entity._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{HashMap, LinkedHashMap, Set}

/**
  * Created by lg on 2017/6/27.
  */
object MaxflowCreditTools {
  /**
    * 修正图上的边权值（DS信息融合等原理）
    */
  def fixEdgeWeight(tpin1: Graph[VertexAttr, EdgeAttr]) = {
    //  val edgetemp = tpin1.mapEdges(e => (e.attr.w_cohesion * e.attr.w_invest * e.attr.w_stockholder * e.attr.w_trade) / (e.attr.w_cohesion * e.attr.w_invest * e.attr.w_stockholder * e.attr.w_trade + (1 - e.attr.w_cohesion) * (1 - e.attr.w_invest) * (1 - e.attr.w_stockholder) * (1 - e.attr.w_trade))).edges
    val edgetemp = tpin1.mapEdges(e => EdgeAttr.fusion(e)).edges
    val vertextemp = tpin1.vertices.map(v => (v._1, (v._2.xyfz / 100.0, v._2.wtbz)))
    Graph(vertextemp, edgetemp)
    /*
        val totalGraph = Graph(vertextemp, edgetemp).subgraph(epred = edgeTriplet => edgeTriplet.attr > 0.01)
        val vertexDegree = totalGraph.degrees.persist()
        Graph(totalGraph.vertices.join(vertexDegree).map(v => (v._1, v._2._1)), totalGraph.edges)

    val vertexDegree = tpin1.degrees.persist()
    Graph(vertextemp.join(vertexDegree).map(v => (v._1, v._2._1)), edgetemp)
 */
  }

  /**
    * 节点上存储子图【选用这个】
    */
  def extendSubgraph(fixEdgeWeightGraph: Graph[Double, Double], selectTopN: Int): RDD[(VertexId, MaxflowGraph)] = {

    val neighbor = fixEdgeWeightGraph.aggregateMessages[Set[(VertexId, VertexId, Double, Double, Boolean)]](triple => {
      //发送的消息为：节点编号，(源终节点编号),边权重，节点属性，向源|终点发
      if (triple.srcId != triple.dstId) { //去环
        //   triple.sendToSrc(Set((triple.dstId, triple.srcId, triple.attr, triple.dstAttr, false)))
        triple.sendToDst(Set((triple.srcId, triple.dstId, triple.attr, triple.srcAttr, true))) //只向终点发
      }
    }, _ ++ _)

    val neighborVertex1 = neighbor.map { case (vid, vattr) =>
      val newattr = vattr.toSeq.filter(_._5 == true)
      if (newattr.size > selectTopN)
        (vid, newattr.sortBy(_._3)(Ordering[Double].reverse).slice(0, selectTopN).toSet)
      else
        (vid, newattr)
    }.cache()

    val neighborVertex = neighbor.map { case (vid, vattr) =>
      if (vattr.size > selectTopN)
        (vid, vattr.toSeq.sortBy(_._3)(Ordering[Double].reverse).slice(0, selectTopN).toSet)
      else
        (vid, vattr)
    }.cache()

    //一层邻居 （社团，不包含节点自身）
    // val neighborPair1 = neighborVertex.map(x => (x._1, x._2)).flatMap(v1 => v1._2.map(v2 => (v1._1, v2))).distinct //指向及指出中心节点
    val neighborPair1 = neighborVertex1.map(x => (x._1, x._2)).flatMap(v1 => v1._2.map(v2 => (v1._1, v2))).distinct //只指向中心节点
    //二层邻居
    val neighborPair2 = neighborPair1.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //三层邻居
    val neighborPair3 = neighborPair2.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //四层邻居
    //   val neighborPair4 = neighborPair3.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //五层邻居
    //   val neighborPair5 = neighborPair4.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //六层邻居
    //  val neighborPair6 = neighborPair5.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //后面构子图时为中心节点添加边权重，因为前面只选择了前6名的较高权重，有可能把中心节点删除掉
    val neighborPairgroup = neighborPair1.union(neighborPair2).union(neighborPair3).distinct
    val neighborPair = neighborPairgroup.join(fixEdgeWeightGraph.vertices).map(v => (v._1, (v._2._1, v._2._2)))

    val returnNeighbor = neighborPair.aggregateByKey(Set[((VertexId, VertexId, Double, Double, Boolean), Double)]())(_ += _, _ ++ _).map { case (vid, vattr) =>
      var e = HashMap[(VertexId, VertexId), Double]()
      var v = HashMap[VertexId, Double]()

      vattr.foreach { case ((vSubId, vRelate, eWeight, vWeight, srcOrdst), vSubWeight) =>
        v.put(vSubId, vWeight)
        if (srcOrdst == true)
          e.put((vSubId, vRelate), eWeight)
        else
          e.put((vRelate, vSubId), eWeight)
      }
      if (!v.contains(vid)) {
        //  RDD里面不能套RDD，所以collect不到信息，会报空指针异常
        //  v.put(vid, fixEdgeWeightGraph.vertices.filter(_._1 == vid).map(_._2).head)
        v.put(vid, vattr.map(_._2).head)
      }
      /* val subVertex: List[MaxflowVertexAttr] = v.map(v => MaxflowVertexAttr(v._1, v._2)).toList
       val subEdge: List[MaxflowEdgeAttr] = e.map(e => MaxflowEdgeAttr(e._1._1, e._1._2, e._2)).toList
       (vid, subVertex, subEdge)*/

      var G = new MaxflowGraph()
      e.map { case ((src, dst), eweight) =>
        var a = v.filter(_._1 == src).toList.head
        var b = v.filter(_._1 == dst).toList.head
        G.addEdge(MaxflowVertexAttr(a._1, a._2), MaxflowVertexAttr(b._1, b._2), eweight)
      }
      (vid, G)
    }
    returnNeighbor
  }

  /**
    * 数据量过大，无法跑动选择Top all的
    *
    */
  def extendSubgraph2(fixEdgeWeightGraph: Graph[Double, Double], maxIteration: Int = 3) = {
    def sendMsg(edge: EdgeContext[Set[VertexId], Double, Set[VertexId]]) = {
      edge.sendToDst(edge.srcAttr -- edge.dstAttr)
      //   edge.sendToSrc(edge.dstAttr -- edge.srcAttr)
    }

    var graph = fixEdgeWeightGraph.mapVertices { case (vid, _) => Set(vid) }.cache()
    val initLength = 1
    var length = initLength
    var messages = graph.aggregateMessages[Set[VertexId]](sendMsg(_), (a, b) =>
      if (a == b)
        a
      else
        a ++ b)
    var activeMessages = messages.count()
    var prevG: Graph[Set[VertexId], Double] = null
    while (activeMessages > 0 && length <= maxIteration) {
      prevG = graph
      graph = graph.joinVertices[Set[VertexId]](messages)((id, dist, newDist) => {
        if (newDist != Set())
          newDist ++ dist
        else
          dist
      }).cache()
      print("iterator " + length + " finished! ")
      length += 1
      val oldMessages = messages
      messages = graph.aggregateMessages[Set[VertexId]](sendMsg(_), (a, b) =>
        if (a == b)
          a
        else
          a ++ b).cache()
      activeMessages = messages.filter(_._2.size > 0).count()
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
    }
    /*
    val subgraphVertexPair = graph.vertices.flatMap(e1 => e1._2.map(e2 => (e1._1, e2)))
    val constructGraph = subgraphVertexPair.map(x => (x._2, x._1)).groupByKey().collect.map {
      case (vid, vattr) =>
        val subgraph = fixEdgeWeightGraph.subgraph(vpred = (i, d) => vattr.toSeq.contains(i))
        (vid, subgraph.vertices, subgraph.edges)
      //   (vid,subgraph)
    }
    val returnNeighbor = constructGraph.map { case (vid, vertices, edges) =>
      var G = new MaxflowGraph()
      edges.collect.map { case e =>
        var a = vertices.filter(_._1 == e.srcId).collect().head
        var b = vertices.filter(_._1 == e.dstId).collect().head
        G.addEdge(new MaxflowVertexAttr(a._1, a._2), new MaxflowVertexAttr(b._1, b._2), e.attr)
      }
      (vid, G)
    }
    returnNeighbor*/

  }


  /**
    * 每个节点的泄露函数
    **/
  def gain(x: Int): Double = {
    var toReturn: Double = 0D
    if (x == 0)
      toReturn = 1
    else
      toReturn = 0.9
    // toReturn = math.cos(0.13x)
    //  toReturn = 1-math.pow(15,-x)
    //   toReturn = 1 - math.pow(x + 1, -5)
    toReturn
  }


  /**
    * 计算最大流分数
    */

  def run3(maxflowSubExtendPair: RDD[(VertexId, MaxflowGraph)], lambda: Double,threashold:Double) = {
    maxflowSubExtendPair.map { case (vid, vGraph) =>
      var vAndInflu: Set[(VertexId, Long, Double)] = Set() //(周边节点ID,周边节点初始纳税评分，单条传递分)
    var pairflow = 0D
      var allpairflow = 0D
      var fusionflow = 0D
      var returnflow = 0D
      //    val vid = 596267
      //   val vGraph=maxflowSubExtendPair.filter(_._1==vid).map(_._2).first()
      //    val src = vGraph.getGraph().keySet.filter(_.id == 936680).head
      //周边节点及其传递的最大流值
      var dst = vGraph.getGraph().keySet.filter(_.id == vid).head
      if (vGraph.getGraph().size>1){
        for (src <- vGraph.getGraph().keySet.-(dst).filter(_.initScore != 0)) {
          //调用最大流算法计算pair节点对内所有节点对其传递值
          val flowtemp = MaxflowCreditTools.maxflowNotGraphX(vGraph, src, dst,threashold)
          vAndInflu.add((src.id, (src.initScore * 100).toLong, flowtemp._3))
          //避免周边节点为100分时，公式中省略了这部分影响
          if (src.initScore == 1) {
            pairflow += flowtemp._3// * (1 - src.initScore + 0.01)
            allpairflow += flowtemp._3 / src.initScore //* (1 - src.initScore + 0.01)
          } else {
            pairflow += flowtemp._3// * (1 - src.initScore)
            allpairflow += flowtemp._3 / src.initScore //* (1 - src.initScore)
          }
        }
        //加入周边没有初始纳税信用评分的节点
        for (src <- vGraph.getGraph().keySet.-(dst).filter(_.initScore == 0)) {
          vAndInflu.add((src.id, 0, 0D))
        }
        //加入自身
        vAndInflu.add((vid, (dst.initScore * 100).toLong, 0D))
        if (allpairflow != 0) {
          fusionflow = lambda * dst.initScore + (1 - lambda) * pairflow / allpairflow
          returnflow = pairflow / allpairflow
        } else {
          fusionflow = dst.initScore
          returnflow = 0D
        }
        val vAndInfluAndRatio = vAndInflu.map { x =>
          var ratio = 0D
          if (allpairflow != 0) {
            if (x._2 == 100)
              ratio = x._3 * (1 - x._2 / 100D + 0.01) / allpairflow
            else
              ratio = x._3 * (1 - x._2 / 100D) / allpairflow
          }
          (x._1, x._2, x._3, ratio)
        }

        (vid, returnflow, fusionflow, vAndInfluAndRatio.toList)   //vAndInfluAndRatio(周边节点ID,周边节点初始纳税评分，单条传递分，单条传递分占总传递分的比值)
        //（中心节点ID，(1-β)后面，最终最大流得分，周边各节点流向中间的流量列表）
       //    (vid, returnflow, fusionflow, vAndInflu.toList)

      }
        else{
        (vid, 0D, lambda * dst.initScore, List[(VertexId, Long, Double, Double)]())
     //   (vid, 0D, lambda * dst.initScore, List[(VertexId, Long, Double)]())
      }
    }
  }


  /**
    * 计算最大流分数
    */

  def run2(maxflowSubExtendPair: RDD[(VertexId, MaxflowGraph)], lambda: Double) = {
    maxflowSubExtendPair.map { case (vid, vGraph) =>
      var pairflow = 0D
      var dst = vGraph.getGraph().keySet.filter(_.id == vid).head
      for (src <- vGraph.getGraph().keySet.-(dst)) {
        //调用最大流算法计算pair节点n步之内所有节点对其传递值
        val flowtemp = maxflowNotGraphX(vGraph, src, dst,1)
        pairflow += flowtemp._3 * (1 - src.initScore)
      }
      val fusionflow = lambda * dst.initScore + (1 - lambda) * pairflow
      (vid, fusionflow)
    }
  }


  /**
    * 计算最大流分数
    */

  def run1(maxflowSubExtendPair: RDD[(VertexId, MaxflowGraph)], lambda: Double) = {
    maxflowSubExtendPair.map { case (vid, vGraph) =>
      var allflow = 0D
      var dst = vGraph.getGraph().keySet.filter(_.id == vid).head
      for (src <- vGraph.getGraph().keySet.-(dst)) {
        //调用最大流算法计算pair节点n步之内所有节点对其传递值
        val flowtemp = maxflowNotGraphX(vGraph, src, dst,1)
        allflow += flowtemp._3
      }
      (vid, allflow)
    }
  }


  /*
  测试用例
   val vGraph =extendPair.filter(_._1==6L).map(_._2).collect.head
   var src = vGraph.getGraph().keySet.filter(_.id == 1L).head
   var dst = vGraph.getGraph().keySet.filter(_.id == 6L).head
   */

  def maxflowNotGraphX(vGraph0: MaxflowGraph, src: MaxflowVertexAttr, dst: MaxflowVertexAttr,threashold:Double): (MaxflowVertexAttr, MaxflowVertexAttr, Double) = {
    var fs = src.initScore
    //重新构图，注意引用问题，否则会导致在相同的vGraph上进行修改
    var vGraph = new MaxflowGraph
    vGraph0.getAllEdge().foreach { case e =>
      val s = new MaxflowVertexAttr(e.src.id, e.src.initScore)
      val d = new MaxflowVertexAttr(e.dst.id, e.dst.initScore)
      vGraph.addEdge(s, d, e.weight)
    }
    var maxflows = 0D
    var i = 1
    val empty = List[MaxflowVertexAttr]()
    while (fs > 0) {
      println(src.id)
      println("---->" + fs)
      val shortest = bfs4(src, dst, vGraph, fs,threashold)
      println(shortest)
      println()
      if (shortest != empty) {
        var path = Set[MaxflowEdgeAttr]()
        for (i <- 0 until shortest.size - 1) {
          path = path.+(MaxflowEdgeAttr(shortest(i), shortest(i + 1), shortest(i + 1).capacity))
        }
        val edgeTemp = vGraph.getAllEdge()

        for (a <- path) {
          for (b <- edgeTemp) {
            if (a == b) {
              b.weight = b.weight - a.weight //修正正向弧
              vGraph.addEdge(a.dst, a.src, a.weight) //添加反向弧
            }
          }
        }
        fs -= shortest(1).capacity
      } else
        return (src, dst, maxflows)
      maxflows += shortest.last.capacity
    }
    return (src, dst, maxflows)
  }

  //===========================================================================================================================================
  //第四种最大流方法【最终正确版本】
  def bfs4(src: MaxflowVertexAttr, dst: MaxflowVertexAttr, vGraph: MaxflowGraph, fs: Double,threashold:Double): List[MaxflowVertexAttr] = {
    //  重新构图
    var residual = new MaxflowGraph
    vGraph.getAllEdge().foreach { case e =>
      val s = new MaxflowVertexAttr(e.src.id, e.src.initScore)
      val d = new MaxflowVertexAttr(e.dst.id, e.dst.initScore)
      residual.addEdge(s, d, e.weight)
    }

    var queue = List[MaxflowVertexAttr]()
    //初始节点更新距离为0，容量为fs.
    residual.getGraph().keySet.find(_ == src).get.update(0, fs)
    queue = residual.getGraph().keySet.find(_ == src).get +: queue

    while (!queue.isEmpty) {
      //更新图
      for (a <- queue) {
        residual.getGraph().keySet.filter(x => (x == a)).head.update(a.distance, a.capacity)
        val edgetemp = residual.getAllEdge()
        for (b <- edgetemp) {
          if (b.src == a)
            b.src.update(a.distance, a.capacity)
          if (b.dst == a)
            b.dst.update(a.distance, a.capacity)
        }
      }

      queue = queue.sortWith { (p1: MaxflowVertexAttr, p2: MaxflowVertexAttr) =>
        p1.distance == p2.distance match {
          case false => -p1.distance.compareTo(p2.distance) > 0
          case _ => p1.capacity - p2.capacity > 0
        }
      }

      var top = queue.head
      queue = queue.drop(1)

      for (edge <- residual.getAdj(top)) {
        //top的邻居结点，（仅以top为源节点的）
        if (edge.src.distance + 1 < edge.dst.distance && edge.weight > threashold) { //??标记更新
          val candi = residual.getGraph().keySet.find(_ == edge.dst).get
          candi.distance = edge.src.distance + 1
          if (candi == dst) {
            candi.capacity = Math.min(Math.min(edge.src.capacity * gain(edge.src.distance), edge.weight), fs)
          } else {
            candi.capacity = Math.min(Math.min(Math.min(edge.src.capacity, edge.weight) * gain(edge.src.distance), edge.weight), fs)
          }
          candi.edgeTo = top.id
          queue = candi +: queue
        }
      }
    }

    // 检查是否有路径
    if (residual.getGraph().keySet.find(_ == dst).get.capacity != (Double.MaxValue - 1)) {
      var links = new LinkedHashMap[MaxflowVertexAttr, MaxflowVertexAttr]
      links += residual.getGraph().keySet.find(_ == dst).get -> null
      while (links.keySet.last.id != src.id) {
        links += residual.getGraph().keySet.find(_.id == residual.getGraph().keySet.find(_ == links.keySet.last).get.edgeTo).get -> links.keySet.last
      }
      parse(links, residual.getGraph().keySet.find(_.id == src.id).get)
    }
    else
      return List()

  }

  //===========================================================================================================================================
  //第二种最大流方法：采用自己构造的图结构MaxflowGraph、串行算法
  //解析路径
  def parse(path: LinkedHashMap[MaxflowVertexAttr, MaxflowVertexAttr], key: MaxflowVertexAttr): List[MaxflowVertexAttr] = {
    key match {
      case null => Nil
      case _ => val value = path.get(key).get
        key :: parse(path -= key, value)
    }
  }

  def bfs(src: MaxflowVertexAttr, dst: MaxflowVertexAttr, vGraph: MaxflowGraph, fs: Double): List[MaxflowVertexAttr] = {
    //  重新构图
    var residual = new MaxflowGraph
    vGraph.getAllEdge().foreach { case e =>
      val s = new MaxflowVertexAttr(e.src.id, e.src.initScore)
      val d = new MaxflowVertexAttr(e.dst.id, e.dst.initScore)
      residual.addEdge(s, d, e.weight)
    }
    //需要更新的图上相关节点的属性，当前准备更新的节点
    var queue: Set[MaxflowVertexAttr] = Set()
    //返回的路径
    var paths = LinkedHashMap[MaxflowVertexAttr, MaxflowVertexAttr]()
    //初始节点更新距离为0，容量为fs.
    residual.getGraph().keySet.find(_ == src).get.update(0, fs)

    var top = src
    queue = queue + top
    //用于找最短距离最大容量的，每次循环删除已选择的节点
    var bigQueue = residual.getGraph().keySet.filter(_ != src)
    paths += (top -> null)
    while (bigQueue.nonEmpty) {
      //更新图
      for (a <- queue) {
        residual.getGraph().keySet.filter(x => (x == a)).head.update(a.distance, a.capacity)
        val edgetemp = residual.getAllEdge()
        for (b <- edgetemp) {
          if (b.src == a)
            b.src.update(a.distance, a.capacity)
          if (b.dst == a)
            b.dst.update(a.distance, a.capacity)
        }
      }
      queue.clear()
      val adj = residual.getAdj(top)
      for (edge <- adj) {
        //top的邻居结点，（仅以top为源节点的）
        if (edge.src.distance + 1 < edge.dst.distance && edge.weight > 0D && bigQueue.contains(edge.dst)) {
          val candi = bigQueue.find(_ == edge.dst).get
          candi.distance = edge.src.distance + 1
          candi.capacity = Math.min(Math.min(edge.src.capacity * gain(edge.src.distance), edge.weight), fs)
          queue = queue + candi
        }
      }
      //最小距离，最大容量
      var minDistance = bigQueue.minBy(_.distance).distance
      top = bigQueue.filter(_.distance == minDistance).maxBy(_.capacity)
      //加入路径中
      paths += (top -> paths.keySet.last)
      if (top == dst) {
        if (top.distance == Int.MaxValue - 1)
          return List[MaxflowVertexAttr]()
        else {
          return parse(paths, paths.keySet.last).reverse
          /*     var pathReverse = parse(paths, paths.keySet.last).reverse
               val shortestDis=pathReverse.last.distance
               var returnPath =List[MaxflowVertexAttr]()
               for(i<-0 to shortestDis){
                 val p=pathReverse.filter(_.distance==i).head
                 returnPath=returnPath:+ p
               }
               return returnPath
               */
        }
      }
      bigQueue = bigQueue.diff(queue)
      //bigQueue = bigQueue.diff(Set(top))
    }
    null
  }

  //==============================================================================
  //第三种最大流方法：采用自己构造的图结构MaxflowGraph、串行算法，采用 优先队列存储
  class NodeCompator extends Comparator[lg.scala.entity.MaxflowVertexAttr] {
    override def compare(n1: lg.scala.entity.MaxflowVertexAttr, n2: lg.scala.entity.MaxflowVertexAttr): Int = {
      //最小距离，最大容量
      if (n1.distance > n2.distance) {
        1
      }
      /*    else if (n1.distance == n2.distance) {
            if (n1.capacity < n2.capacity) {
              3
            }
            else {
              2
            }
          }*/
      else {
        -1
      }
    }
  }

  /*
    val a=MaxflowVertexAttr(1,12,5,7)
    val b=MaxflowVertexAttr(2,12,1,5)
    val c=MaxflowVertexAttr(3,12,1,9)
    val d=MaxflowVertexAttr(4,12,4,8)
    queue.add(a)
    queue.add(b)
    queue.add(c)
    queue.add(d)
    */

  def bfs2(src: MaxflowVertexAttr, dst: MaxflowVertexAttr, vGraph: MaxflowGraph, fs: Double): List[MaxflowVertexAttr] = {
    val queue = new PriorityQueue[MaxflowVertexAttr](10, new NodeCompator())
    /*  queue.sortBy(_.distance)
      queue= a+:queue
      queue= b+:queue
      queue= c+:queue
      queue= d+:queue*/
    /*   for ((key, value) <- residual.getGraph) {
         val currNode = key
         if (currNode == src) {
           currNode.distance = 0
           currNode.capacity = fs
           queue = currNode +: queue
         }
       }*/
    //  重新构图
    var residual = new MaxflowGraph
    vGraph.getAllEdge().foreach { case e =>
      val s = new MaxflowVertexAttr(e.src.id, e.src.initScore)
      val d = new MaxflowVertexAttr(e.dst.id, e.dst.initScore)
      residual.addEdge(s, d, e.weight)
    }
    //存储该节点是否有下一访问点
    //   var queue = List[MaxflowVertexAttr]()
    val currNode = residual.getGraph().keySet.find(_ == src).get
    currNode.update(0, fs)
    //   queue = currNode +: queue
    queue.add(currNode)
    //已经访问过的节点
    var doneSet = mutable.LinkedHashSet[MaxflowVertexAttr]()
    //返回的路径
    var paths = LinkedHashMap[MaxflowVertexAttr, MaxflowVertexAttr]()
    //  paths += (currNode -> null)

    while (!queue.isEmpty) {
      //更新图属性
      //   for (a <- queue) {
      val it = queue.iterator()
      while (it.hasNext) {
        val a = it.next()
        residual.getGraph().keySet.find(_ == a).get.update(a.distance, a.capacity)
        val edgetemp = residual.getAllEdge()
        for (b <- edgetemp) {
          if (b.src == a)
            b.src.update(a.distance, a.capacity)
          if (b.dst == a)
            b.dst.update(a.distance, a.capacity)
        }
      }
      //  }
      //用于检索并移除此队列的头,此时queue已为空
      /*  val minDistance = queue.minBy(_.distance).distance
        val src = queue.filter(_.distance == minDistance).maxBy(_.capacity)
        queue = queue.filter(_ != src)*/
      val src = queue.poll()
      /*  if (doneSet.map(_.distance).contains(src.distance)) {
          val addQueue = doneSet.filter(_.distance >= minDistance)
          addQueue.foreach { case v =>
            queue = v +: queue
          }
          doneSet = doneSet.filter(_.distance < minDistance)
          doneSet.add(src)

        } else
  */
      doneSet.add(src)
      for (edge <- residual.getAdj(src)) {
        val currentNode = edge.getAdjacentNode(src)
        if (!doneSet.contains(currentNode) && currentNode != null) {
          val newDistance = src.distance + 1
          if (newDistance < currentNode.distance && edge.weight > 0D) {
            currentNode.distance = newDistance
            currentNode.capacity = Math.min(Math.min(src.capacity * gain(src.distance), edge.weight), fs)
            //   queue = currentNode +: queue
            queue.add(currentNode)
          }
        }
      }
      doneSet
    }
    null
  }

  //===========================================================================================
  //第一种最大流方法：采用GraphX,并行求解最短路径[结果正确，但在大型数据集上×]
  /**
    * 最短增广路径
    */
  def shortestPath(src: VertexId, dst: VertexId, graph: Graph[Double, Double], fs: Double, sc: SparkContext) = {
    //  val graph= residual
    val test: VertexId = src // 缺省节点
    // 初始化除源节点外其他节点的距离为无穷大
    // 各节点属性为 (distance,capacity,id)
    val initialGraph = graph.mapVertices((id, _) => if (id == src) (0, fs, id)
    else (Int.MaxValue - 1, Double.MaxValue - 1, id))

    val sssp = initialGraph.pregel((Int.MaxValue - 1, Double.MaxValue - 1, test))(
      (id, dist, newDist) => {
        if (dist._1 < newDist._1) dist
        else newDist
      }, // Vertex program  ，选择距离较小的
      triplet => {
        // 某边源点的距离+1小于终点，且边容量>0，更新
        if (triplet.srcAttr._1 + 1 < triplet.dstAttr._1 && triplet.attr > 0) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, Math.min(Math.min(triplet.srcAttr._2 * gain(triplet.srcAttr._1), triplet.attr), fs), triplet.srcId))) //选择边源节点容量和边容量较小者
        }
        else {
          Iterator.empty
        }
      }, // send message
      (a, b) => {
        if (a._1 < b._1) a //如果从a出发的距离小于从b出发的距离
        else if ((a._1 == b._1) && (a._2 > b._2)) a // 如果距离相等，但a有更高的最小容量
        else b
      } // merge multiple messages
    )

    val vNum = sssp.vertices.count.toInt // 是最短路径中的节点的个数 < 初始图总个数
    val v = sssp.vertices.collect // 转 rdd 为 Array
    val dstCap = sssp.vertices.filter(v => v._1 == dst).first._2._2 //该条最短路径通过的流量
    var paths = Set[(VertexId, VertexId)]()

    // 检查是否有路径
    if (dstCap != (Double.MaxValue - 1)) {
      val links = new HashMap[VertexId, VertexId]
      for (i <- 0 to vNum - 1) {
        links += v(i)._1 -> v(i)._2._3
      }

      // 构建最短路径中边集
      var id = dst
      for (i <- 0 to vNum - 1; if id != src) {
        paths += ((links(id), id))
        id = links(id)
      }
    }
    val returnPath = sc.parallelize(paths.map(x => ((x._1, x._2), 1)).toSeq).join(sc.parallelize(v.map(x => ((x._2._3, x._1), x._2._2)))).map(x => (x._1, x._2._2))
    (returnPath, dstCap)
    // val shortest=(returnPath, dstCap)
  }

  /**
    * 任意两节点间的最大流量
    */
  def maxflow(sc: SparkContext, graph: Graph[Double, Double], src: VertexId, dst: VertexId): (VertexId, VertexId, Double) = {
    //定义初始流为各节点的信用分归一化
    var fs = graph.vertices.filter(_._1 == src).map(_._2).collect.head
    val vertices = graph.vertices
    var residual: Graph[Double, Double] = graph // 初始0流=> residual = graph
    var maxflows = 0D

    var i = 1
    val empty = Set[((VertexId, VertexId), Double)]()
    while (fs > 0) {
      var shortest = shortestPath(src, dst, residual, fs, sc)
      var path = shortest._1.collect().toSet
      if (path != empty) {

        var bcPath = sc.broadcast(path)
        // 更新剩余网络
        val flow = residual.edges.map(e => ((e.srcId, e.dstId), e.attr)).
          flatMap(e => {
            if (bcPath.value.map(_._1) contains e._1) {
              var pathweight = bcPath.value.filter(_._1 == e._1).map(_._2).head
              Seq((e._1, e._2 - pathweight), ((e._1._2, e._1._1), pathweight)) //在residual网络中添加反向弧，修正正向弧与反向弧上边权值
              //        Seq((e._1, e._2 - pathweight)) //在residual网络中不添加反向弧，仅修改正向弧的权值
            }
            else Seq(e)
          }
          ).reduceByKey(_ + _)

        val fsUsed = bcPath.value.filter(_._1._1 == src).map(_._2).head
        fs -= fsUsed
        residual = Graph(residual.vertices.map(v => if (v._1 == src) (src, fs) else (v)), flow.map(e => Edge(e._1._1, e._1._2, e._2)))

      }
      else
        return (src, dst, maxflows)
      //     println("迭代次数：" + i)
      i = i + 1
      maxflows += shortest._2
    }
    return (src, dst, maxflows)
  }


}


/*

val v = sc.parallelize(Array((1L, 1.0), (2L, 0.6), (3L, 0D), (4L, 0.95D)))
val e = sc.parallelize(Array(Edge(1L, 2L, 0.5), Edge(1L, 3L, 0.6), Edge(3L, 2L, 0.8), Edge(3L, 4L, 0.7), Edge(2L, 4L, 0.3)))

val src = 1L
val dst = 6L

/**
  * 由aggregateMessages()方法来发送消息
  */
def extendSubgraph(fixEdgeWeightGraph: Graph[Double, Double], maxIteration: Int) = {
  def sendMsg(edge: EdgeContext[mutable.Set[VertexId], Double, mutable.Set[VertexId]]) = {
  edge.sendToDst(edge.srcAttr -- edge.dstAttr)
  edge.sendToSrc(edge.dstAttr -- edge.srcAttr)
}
  var graph = fixEdgeWeightGraph.mapVertices { case (vid, _) => Set(vid) }.cache()
  val initLength = 1
  var length = initLength
  var messages = graph.aggregateMessages[Set[VertexId]](sendMsg(_), (a, b) =>
  if (a == b)
  a
  else
  a ++ b)
  var activeMessages = messages.count()
  var prevG: Graph[Set[VertexId], Double] = null
  while (activeMessages > 0 && length <= maxIteration) {
  prevG = graph
  graph = graph.joinVertices[Set[VertexId]](messages)((id, dist, newDist) => {
  if (newDist != Set())
  newDist ++ dist
  else
  dist
}).cache()
  print("iterator " + length + " finished! ")
  length += 1
  val oldMessages = messages
  messages = graph.aggregateMessages[Set[VertexId]](sendMsg(_), (a, b) =>
  if (a == b)
  a
  else
  a ++ b).cache()
  activeMessages = messages.count()
  oldMessages.unpersist(blocking = false)
  prevG.unpersistVertices(blocking = false)
  prevG.edges.unpersist(blocking = false)
}
  val subgraphVertexPair = graph.vertices.flatMap(e1 => e1._2.map(e2 => (e1._1, e2)))
  val allPair = subgraphVertexPair.leftOuterJoin(fixEdgeWeightGraph.vertices).map(v => (v._1, v._2._1))
  allPair
}

  def extendSubgraph(fixEdgeWeightGraph: Graph[Double, Double], iteration: Int, selectTopN: Int = 3) = {
    val graph = fixEdgeWeightGraph.mapVertices { case (vid, _) => Set((vid, 0D)) }
    //节点属性为其所属子图节点集合
    val subgraph = graph.pregel(Set[(VertexId, Double)](), iteration)(
      (id, dist, newDist) => {
        newDist ++ dist
      }, // Vertex program
      triplet => {
        if (triplet.srcAttr.size > selectTopN && triplet.dstAttr.size > selectTopN)
          Iterator((triplet.dstId, triplet.srcAttr.toSeq.sortBy(_._2)(Ordering[Double].reverse).slice(0, selectTopN).toSet), (triplet.srcId, triplet.dstAttr.toSeq.sortBy(_._2)(Ordering[Double].reverse).slice(0, selectTopN).toSet))
        else if (triplet.srcAttr.size < selectTopN && triplet.dstAttr.size < selectTopN)
          Iterator((triplet.dstId, triplet.srcAttr), (triplet.srcId, triplet.dstAttr))
        else if (triplet.srcAttr.size < selectTopN && triplet.dstAttr.size > selectTopN)
          Iterator((triplet.dstId, triplet.srcAttr), (triplet.srcId, triplet.dstAttr.toSeq.sortBy(_._2)(Ordering[Double].reverse).slice(0, selectTopN).toSet))
        else
          Iterator((triplet.dstId, triplet.srcAttr.toSeq.sortBy(_._2)(Ordering[Double].reverse).slice(0, selectTopN).toSet), (triplet.srcId, triplet.dstAttr))
      }, // send message
      (a, b) =>
        if (a == b)
          a
        else
          a ++ b // merge multiple messages
    ).cache()
    val subgraphVertexPair = subgraph.vertices.filter(_._2.size != 1).flatMap(v1 => v1._2.map(v2 => (v1._1, v2._1))) //注意过滤掉没有接收到消息的节点
//    subgraphVertexPair.map(x=>(x._2,x._1)).groupByKey().map(x=>(x._2.size,x._1)).sortByKey().collect
    subgraphVertexPair
  }

/**
    * 各节点向外扩展步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）
    * 点上存的是该点所包含的子图相关节点，串行逐步构子图
    */
  def extendSubgraph(fixEdgeWeightGraph: Graph[Double, Double], iteration: Int, selectTopN: Int = 6) = {
    val neighbor = fixEdgeWeightGraph.aggregateMessages[Set[(VertexId, Double)]](edge => {
      edge.sendToSrc(Set((edge.dstId, edge.attr)))
      edge.sendToDst(Set((edge.srcId, edge.attr)))
    }, _ ++ _)

    val neighborVertex = neighbor.map { case (vid, vattr) =>
      if (vattr.size > selectTopN)
        (vid, vattr.toSeq.sortBy(_._2)(Ordering[Double].reverse).slice(0, selectTopN).map(_._1).toSet)
      else
        (vid, vattr.map(_._1))
    }.cache()

    //一层邻居 （社团，包含节点）
    val neighborPair1 = neighborVertex.map(x => (x._1, x._2 + x._1)).flatMap(v1 => v1._2.map(v2 => (v1._1, v2))).distinct
    //二层邻居
    val neighborPair2 = neighborPair1.map(x => (x._2, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //三层邻居
    val neighborPair3 = neighborPair2.map(x => (x._2, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    val neighborPair = neighborPair1.union(neighborPair2).union(neighborPair3).distinct
    neighborPair
  }

  /**
    * 修正图上的边权值（信息融合等原理）
    */
  def fixEdgeWeight(tpin1: Graph[VertexAttr, EdgeAttr]): Graph[Double, Double] = {
    val edgetemp = tpin1.mapEdges(e => (e.attr.w_cohesion + e.attr.w_invest + e.attr.w_stockholder + e.attr.w_trade) / 4).edges
    val vertextemp = tpin1.vertices.map(v => (v._1, v._2.xyfz / 100.0))
    val totalGraph = Graph(vertextemp, edgetemp).subgraph(epred = edgeTriplet => edgeTriplet.attr > 0.01)
    val vertexDegree = totalGraph.degrees.persist()
    Graph(totalGraph.vertices.join(vertexDegree).map(v => (v._1, v._2._1)), totalGraph.edges)
  }
}
    /*

     run方法
     var allflow = 0D

      var maxflowScore = HashMap[VertexId, Double]()
      //选择社团编号为1-100的进行计算
      extendPair.map(_._1).filter(_ <= 20).distinct.collect.foreach { x =>
        val subV = extendPair.filter(_._1 == x).map(_._2).collect
        var bcsubV = sc.broadcast(subV)
        val sgraph = totalGraph.subgraph(vpred = (vid, vattr) => bcsubV.value.contains(vid)) //n步之内的子图
        bcsubV.value.filter(_ != x).distinct.foreach { y =>
          //调用最大流算法计算pair节点n步之内所有节点对其传递值
          val flowtemp = maxflow(sc, sgraph, y, x)
          allflow += flowtemp._3
        }
        maxflowScore.put(x, allflow)
      }
      maxflowScore*/


*/
