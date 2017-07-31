package lg.scala.entity

import org.apache.spark.graphx.VertexId

/**
  * Created by lg on 2017/6/22.
  */
class MaxflowVertexAttr(var id: VertexId, var initScore: Double, var distance: Int = Int.MaxValue - 1, var capacity: Double = Double.MaxValue - 1) extends Serializable {
  /* var initScore = 0D
   var distance = Int.MaxValue - 1
   var capacity = Double.MaxValue - 1
 */
  var visited:Boolean =false
  def getValue: VertexId = id

  def getDistance(): Int = distance

  def setDistance(dist: Int) = {
    distance = dist
  }

  def getCapacity(): Double = capacity

  def setCapacity(cap: Double) = {
    capacity = cap
  }

  def update(dis: Int, cap: Double) = {
    distance = dis
    capacity = cap
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[MaxflowVertexAttr]

  override def equals(other: Any): Boolean = other match {
    case that: MaxflowVertexAttr =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"V($id,$initScore, $distance, $capacity)"
}

object MaxflowVertexAttr {
  def apply(id: VertexId, initScore: Double, distance: Int = Int.MaxValue - 1, capacity: Double = Double.MaxValue - 1): MaxflowVertexAttr = new MaxflowVertexAttr(id, initScore, distance, capacity)

}
