package lg.scala.entity

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
}
