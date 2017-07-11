package lg.scala.utils

import lg.scala.entity.{EdgeAttr, VertexAttr}
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap

/**
  * Created by lg on 2017/6/27.
  */
object MaxflowCreditTools {


  /**
    * 每个节点的泄露函数
    **/
  def gain(x: Int): Double = {
    var toReturn: Double = 0D
    if (x == 0)
      toReturn = 1
    else
      toReturn = 0.9
    toReturn
  }


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

  /**
    * 计算最大流分数
    */
  def run(extendPair: RDD[(VertexId, VertexId)], totalGraph: Graph[Double, Double], sc: SparkContext) = {
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
    /*   val graph = extendPair.aggregateByKey(collection.mutable.Set[VertexId]())(
         (set, vertex) => {
           set += vertex
         },
         (set1, set2) => {
           set1 ++= set2
         }
       )
       val graphBroadcast = sc.broadcast(graph)
       extendPair.filter(_._1 <= 100).distinct.map({ case (x, y) =>
         val subV = graphBroadcast.value.filter(_._1 == x)
         val flowtemp = maxflow(sc, sgraph, y, x)
       })
   */
    /*

    for (centerPair <- extendPair.map(_._2).distinct.collect) {
         val subV = extendPair.filter(_._2 == centerPair).map(_._1).collect()
         var bcsubV = sc.broadcast(subV)
         //存储
         val sgraph = totalGraph.subgraph(vpred = (vid, vattr) => bcsubV.value.contains(vid)) //3步之内的子图
         //调用最大流算法计算pair节点n步之内所有节点对其传递值
         for (src <- bcsubV.value.filter(_ != centerPair)) {
           val flowtemp = maxflow(sc, sgraph, src, centerPair)
           allflow += flowtemp._3
         }
         maxflowScore.put(centerPair, allflow)
       }*/
    maxflowScore
  }

  /**
    * 各节点向外扩展步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）
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

*/
