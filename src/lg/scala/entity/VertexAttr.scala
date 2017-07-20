package lg.scala.entity

/**
  * Created by lg on 2017/6/22.
  */
class VertexAttr(var sbh: String, var name: String) extends Serializable {
  var gd_list: Seq[(String, Double)] = Seq[(String, Double)]()
  var tzNotNSR_list: Seq[(String, Double)] = Seq[(String, Double)]()
  var xydj: String = ""
  var xyfz: Int = 0
  var wtbz: Boolean = false

  override def toString = s" $sbh, $name，$xyfz"
}

object VertexAttr {
  def apply(sbh: String, name: String): VertexAttr = new VertexAttr(sbh, name)
}
