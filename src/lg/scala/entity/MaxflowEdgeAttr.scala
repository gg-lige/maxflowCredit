package lg.scala.entity

import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.graphx.VertexId

/**
  * Created by lg on 2017/6/27.
  */
class MaxflowEdgeAttr(mutable: Set[(VertexId,VertexId,Double)]) extends Serializable {
  // val src: String, val dst: String, val weight: Double, val subgraphID:VertexId
  //override def toString = s"MaxflowEdgeAttr($src, $dst, $weight, $subgraphID)"
}

object MaxflowEdgeAttr {
  def apply(src: String, dst: String, weight: Double, subgraphID: VertexId): MaxflowEdgeAttr = new MaxflowEdgeAttr(src, dst, weight, subgraphID)
}