package lg.scala.entity

import org.apache.spark.graphx.VertexId

/**
  * Created by lg on 2017/6/22.
  */
class MaxflowVertexAttr(var id: VertexId, var initScore: Double) extends Serializable {
  override def toString = s"MaxflowVertexAttr($id, $initScore)"
}

object MaxflowVertexAttr {
  def apply(id: VertexId, initScore: Double): MaxflowVertexAttr = new MaxflowVertexAttr(id, initScore)
}
