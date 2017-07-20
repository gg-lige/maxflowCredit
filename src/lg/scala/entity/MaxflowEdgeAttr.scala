package lg.scala.entity

import org.apache.spark.graphx.VertexId

/**
  * Created by lg on 2017/6/27.
  */
class MaxflowEdgeAttr(val src: VertexId, val dst: VertexId, val weight: Double) extends Serializable {

  override def toString = s"MaxflowEdgeAttr($src, $dst, $weight)"
}

object MaxflowEdgeAttr {
  def apply(src: VertexId, dst: VertexId, weight: Double ): MaxflowEdgeAttr = new MaxflowEdgeAttr(src, dst, weight)
}