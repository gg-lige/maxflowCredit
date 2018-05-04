package lg.scala.entity

import org.apache.spark.graphx.Edge

/**
  * Created by lg on 2017/6/22.
  */
class EdgeAttr() extends Serializable {
  var w_invest: Double = 0.0
  var w_stockholder: Double = 0.0
  var w_trade: Double = 0.0
  var w_cohesion: Double = 0.0

  override def toString = s"$w_invest, $w_stockholder, $w_trade, $w_cohesion"
}

object EdgeAttr {
  def apply(): EdgeAttr = new EdgeAttr()

  def fusion(e: Edge[EdgeAttr]) = {
    var toReturn = 0D
    var describe = ""
    val allW = List(e.attr.w_invest, e.attr.w_stockholder, e.attr.w_trade, e.attr.w_cohesion).filter(_ > 0D)
    if (allW.size == 1)
      toReturn = allW(0)
    if (allW.size == 2)
      toReturn = allW(0) * allW(1) / (allW(0) * allW(1) + (1 - allW(0)) * (1 - allW(1)))
    if (allW.size == 3)
      toReturn = allW(0) * allW(1) * allW(2) / (allW(0) * allW(1) * allW(2) + (1 - allW(0)) * (1 - allW(1)) * (1 - allW(2)))
    if (allW.size == 4)
      toReturn = allW(0) * allW(1) * allW(2) * allW(3) / (allW(0) * allW(1) * allW(2) * allW(3) + (1 - allW(0)) * (1 - allW(1)) * (1 - allW(2)) * (1 - allW(3)))

    if (e.attr.w_invest > 0D)
      describe += "ir:" + e.attr.w_invest.formatted("%.3f") + " "
    if (e.attr.w_stockholder > 0D)
      describe += "sr:" + e.attr.w_stockholder.formatted("%.3f") + " "
    if (e.attr.w_trade > 0D)
      describe += "tr:" + e.attr.w_trade.formatted("%.3f") + " "
    if (e.attr.w_cohesion > 0D)
      describe += "cr:" + e.attr.w_cohesion.formatted("%.3f") + " "

    describe += "A:" + toReturn.formatted("%.3f")
    (toReturn, describe)
    //  toReturn
  }



}

