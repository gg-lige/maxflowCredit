package lg.scala.entity

/**
  * Created by lg on 2017/6/20.
  */
//类上肯定具有的三列属性，名称，纳税人识别号|非纳税人证件号码，是否为纳税人,
//一定要注意在创建类时要序列化，
class InitVertexAttr(var name: String, var sbh: String, var isNSR: Boolean) extends Serializable {
  var xydj: String = ""
  var xyfz: Int = 0

  //字符串插值函数，表示可以使用$
  override def toString = s"InitVertexAttr($name,$sbh,$isNSR)"
}

object InitVertexAttr {
  //初始化类时调用此方法
  def apply(name: String, sbh: String, isNSR: Boolean): InitVertexAttr = new InitVertexAttr(name, sbh.replace(".0", ""), isNSR)
  def combineNSRSBH(name1: String, name2: String): String = {
    var name = ""
    if (name1 != null) {
      // 拆分
      val name1s = name1.split(";")
      for (name1 <- name1s) {
        if (!name.contains(name1)) {
          if (name != "") {
            // 合并
            name = name + ";" + name1
          } else {
            name = name1
          }
        }
      }
    }
    if (name2 != null) {
      // 拆分
      val name2s = name2.split(";")
      for (name2 <- name2s) {
        if (!name.contains(name2)) {
          if (name != "") {
            // 合并
            name = name + ";" + name2
          } else {
            name = name2
          }
        }
      }
    }
    name
  }

  def combine(a: InitVertexAttr, b: InitVertexAttr): InitVertexAttr = {
    InitVertexAttr(combineNSRSBH(a.name,b.name), combineNSRSBH(a.sbh, b.sbh), a.isNSR && b.isNSR)
  }
}
