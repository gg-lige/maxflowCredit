package lg.scala.entity


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.HashMap


/**
  * Created by lg on 2017/7/24.
  */
class MaxflowGraph() extends Serializable {
  private var map = new HashMap[MaxflowVertexAttr, List[MaxflowEdgeAttr]]()

  def addEdge(node1: MaxflowVertexAttr, node2: MaxflowVertexAttr, distance: Double) {
    if (node1 == null || node2 == null) {
      throw new NullPointerException("Either of the 2 nodes is null.")
    }

    val edge = new MaxflowEdgeAttr(node1, node2, distance)
    addToMap(node1, edge)
    addToMap(node2, edge)
  }

  def getAllEdge(): List[MaxflowEdgeAttr] = {
    map.flatMap(_._2).toList.distinct
  }


  private def addToMap(node: MaxflowVertexAttr, edge: MaxflowEdgeAttr) {

    if (map.contains(node)) {
      var l = map.get(node).get
      //l中已有该边，但权重不同，相加
      if (l.contains(edge)) {
        val etemp = l.filter(_ == edge).head
        etemp.weight=(etemp.weight + edge.weight)
      } else
        l = edge +: l
      map.update(node, l)
    } else {
      var l = List[MaxflowEdgeAttr]()
      l = edge +: l
      map.put(node, l)
    }
  }

  //找寻以node为源节点的边
  def getAdj(node: MaxflowVertexAttr): List[MaxflowEdgeAttr] = {
    var returnAdj=List[MaxflowEdgeAttr]()
    for(a<-map.get(node).get){
      if(a.src==node)
        returnAdj= a +: returnAdj
    }
    returnAdj
  }

  def getGraph(): HashMap[MaxflowVertexAttr, List[MaxflowEdgeAttr]] = map


  override def toString = s"G($map)"


  def deepCloneAnyRef(src: Object): Object = {
    var o: Object = null
    try {
      if (src != null) {
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(src)
        oos.close()
        val bais = new ByteArrayInputStream(baos.toByteArray())
        val ois = new ObjectInputStream(bais)
        o = ois.readObject()
        ois.close()
      }
    } catch {
      case e: Exception => throw new IllegalStateException("Failed to deepClone object.")
    }
    return o
  }


  /*
    override def clone(): MaxflowGraph = {
      var copy = new MaxflowGraph()
      this.map.foreach {
        case (v, e) =>
          copy = copy + (deepCloneAnyRef(v).asInstanceOf[MaxflowVertexAttr] -> deepCloneAnyRef(e).asInstanceOf[List[MaxflowEdgeAttr]])
      }

    }
  */


}
