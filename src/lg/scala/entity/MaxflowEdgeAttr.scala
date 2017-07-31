package lg.scala.entity

import org.apache.spark.graphx.VertexId

/**
  * Created by lg on 2017/6/27.
  */
class MaxflowEdgeAttr(val src: MaxflowVertexAttr, val dst: MaxflowVertexAttr, var weight: Double) extends Serializable {
  var edgeWeight = weight


  def getAdjacentNode(node: MaxflowVertexAttr): MaxflowVertexAttr = {
    //即为无向图：为源给终 ，为终给源
 //   if (node.getValue != src.getValue) src else dst
    //有向图,只有源发终
    if (node.getValue == src.getValue) dst else null
  }

  def getWeight(): Double = weight

  def setWeight(edgeWei: Double) = {
    edgeWeight = edgeWei
  }

  override def toString = s"E($src, $dst, $weight)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[MaxflowEdgeAttr]


  override def equals(other: Any): Boolean = other match {
    case that: MaxflowEdgeAttr =>
      (that canEqual this) &&
        src == that.src &&
        dst == that.dst
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(src, dst)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object MaxflowEdgeAttr {
  def apply(src: MaxflowVertexAttr, dst: MaxflowVertexAttr, weight: Double): MaxflowEdgeAttr = new MaxflowEdgeAttr(src, dst, weight)
}