package lg.scala.main

import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SparkSession

/**
  * Created by lg on 2018/5/7.
  */
object Main2 {
  def main(args: Array[String]): Unit = {
    type Path = Seq[(VertexId, Double)]
    type Paths = Seq[Seq[(VertexId, Double)]]

    val spark = SparkSession.builder.appName(this.getClass.getSimpleName).getOrCreate()
    val sc = spark.sparkContext


    //    val messageOfControls: org.apache.spark.graphx.VertexRDD[Paths] = null
    //
    //    messageOfControls.collect().foreach(println)
    //    reverseMessageOfControls.collect().foreach(println)
    //
    //    messageOfControls.flatMap(_._2).groupBy(_.head._1).collect().foreach(println)
    //    reverseMessageOfControls.flatMap(_._2).groupBy(_.last._1).collect().foreach(println)
    //
    //    messageOfControls.flatMap(_._2).filter(_.last._2 != 10D).groupBy(_.head._1).collect().foreach(println)
    //
    //    val src = List(List((1, 10.0), (3, 0.5)), List((2, 10.0), (5, 1.0), (3, 0.1)))
    //    val dst = List(List((1, 10.0)))
    //    val filterEdge = src.filter(_.size == 3)
    //    //.filter(!_.map(_._1).contains(edge.dstId))
    //    val filterEdge2 = dst.filter(_.last._2 != 10D)
    //    val b = (2 != 1 && filterEdge.size > 0 && filterEdge2.size > 0)
    //
    //    println(a.filter(_.last._2 != 10D))
    //    println(b)

    val list = (1, List(//List((1,10.0)),
      List((1, 10.0), (3, 0.5)),
      List((1, 10.0), (6, 0.6), (4, 0.25)),
      List((1, 10.0), (3, 0.5), (5, 0.3)),
      List((1, 10.0), (6, 0.6)),
      List((1, 10.0), (3, 0.5), (7, 0.25)),
      //List((1,10.0)),
      List((3, 0.25), (1, 10.0)),
      List((4, 1.0), (6, 0.8), (1, 10.0)),
      List((5, 0.1), (3, 0.25), (1, 10.0)),
      List((6, 0.8), (1, 10.0)),
      List((7, 0.5), (3, 0.25), (1, 10.0))))

    list._2.map { l =>
      l.size > 0

    }


  }


}
