package lg.scala.utils

import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.binary.BinaryConfusionMatrixImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by lg on 2017/7/17.
  */
object ExperimentTools {

  def verify3(sc: SparkContext, maxflowCredit: RDD[(VertexId, Double)], selectGraph: RDD[(Long, (Double, Boolean))]) = {
    /**
      * B为最大流计算排名，A为原始纳税信用等级排名;前100中、前200、前300、…中标志为true的个数占所有标志为true的个数的比值
      */
    val A = selectGraph.map(v => (v._2._1, (v._2._2, v._1))).repartition(1).sortByKey(true).map(v => (v._2._2, v._1, v._2._1))
    val B = maxflowCredit.join(selectGraph).map(x => (x._2._1, (x._1, x._2._2._2))).repartition(1).sortByKey(true).map(x => (x._2._1, x._1, x._2._2))
    //按100名波动清况
    var i = 100
    var result = HashMap[VertexId, (Double, Double)]() //节点id,最大流命中率，原始命中率
    var number = A.count()
    if (number > 10000) {
      number = 10000
    }
    while (i <= number) {
      //  val P = B.repartition(1).top(i)//sc.parallelize(B.top(i)).filter(_._3 == true).count() / i.toDouble
      val PB = B.take(i).filter(_._3 == true).size / B.filter(_._3 == true).count().toDouble
      val PA = A.take(i).filter(_._3 == true).size / A.filter(_._3 == true).count().toDouble
      result.put(i, (PB.%(3), PA.%(3)))
      i += 100
    }

    (B.repartition(1), sc.parallelize(result.toSeq))
  }

  def verify2(sc: SparkContext, maxflowCredit: RDD[(VertexId, Double)], maxflowSubGraph: RDD[(Long, (Double, Boolean))],glCredit:RDD[(VertexId, Double)]) = {
    /**
      * B为最大流计算排名，A为原始纳税信用等级排名,C是关联评价排名;前100、前200、前300、…中标志为true
      */
    val A = maxflowSubGraph.map(v => (v._2._1, (v._2._2, v._1))).repartition(1).sortByKey(false).map(v => (v._2._2, v._1, v._2._1))
    val B = maxflowCredit.join(maxflowSubGraph).map(x => (x._2._1, (x._1, x._2._2._2))).repartition(1).sortByKey(false).map(x => (x._2._1, x._1, x._2._2))
    val C = glCredit.join(maxflowSubGraph).map(x => (x._2._1, (x._1, x._2._2._2))).repartition(1).sortByKey(false).map(x => (x._2._1, x._1, x._2._2))
    //按100名波动清况
    var i = 10
    var result = HashMap[VertexId, (Double, Double,Double)]() //节点id,原始命中率,关联命中率,最大流命中率，
    var number = A.count()
    if (number > 300) {
      number = 300
    }
    while (i <= number) {
      //  val P = B.repartition(1).top(i)//sc.parallelize(B.top(i)).filter(_._3 == true).count() / i.toDouble
      val PA = A.take(i).filter(_._3 == true).size / i.toDouble
      val PC = C.take(i).filter(_._3 == true).size / i.toDouble
      val PB = B.take(i).filter(_._3 == true).size / i.toDouble
      result.put(i, (PA.%(3), PC.%(3), PB.%(3)))
      i += 10
    }

    (B.repartition(1), sc.parallelize(result.toSeq))
  }

  def verify1(sc: SparkContext, maxflowCredit: RDD[(VertexId, Double)], selectGraph: RDD[(Long, (Double, Boolean))]) = {
    /**
      * B为最大流计算排名，A为原始纳税信用等级排名;0-100、100-200、200-300、…中标志为true依次计算命中率
      */
    val A = selectGraph.map(v => (v._2._1, (v._2._2, v._1))).repartition(1).sortByKey(true).map(v => (v._2._2, v._1, v._2._1)).collect().toList
    val Boutput = maxflowCredit.join(selectGraph).map(x => (x._2._1, (x._1, x._2._2._2))).repartition(1).sortByKey(true).map(x => (x._2._1, x._1, x._2._2))
    val B = Boutput.collect.toList

    var Atemp = A
    var Btemp = B
    //按100名波动清况
    var i = 100
    var result = HashMap[VertexId, (Double, Double)]() //节点id,最大流命中率，原始命中率
    var number = A.size
    if (number > 10000) {
      number = 10000
    }
    while (i <= number) {
      //   val PB = B.take(i).filter(_._3 == true).size / i.toDouble
      //    val PA = A.take(i).filter(_._3 == true).size / i.toDouble

      val PB = Btemp.take(100).filter(_._3 == true).size / 100D
      val PA = Atemp.take(100).filter(_._3 == true).size / 100D
      result.put(i, (PB.%(3), PA.%(3)))
      Btemp = Btemp.drop(100)
      Atemp = Atemp.drop(100)
      i += 100
    }

    (Boutput.repartition(1), sc.parallelize(result.toSeq))
  }

  def computeIndex(scoreAndLabels: RDD[(Double, Double)]) = {
    val multimetrics = new MulticlassMetrics(scoreAndLabels)
    // val multimetrics = new MulticlassMetrics(originalScoreAndLabels)
    System.out.println("混淆矩阵" + multimetrics.confusionMatrix)

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    //  val metrics=new BinaryClassificationMetrics(originalScoreAndLabels)
    // Precision by threshold
    val precision = metrics.precisionByThreshold
    val precisionMax = precision.map(x => (x._2, x._1)).max
    println("最大准确率与阈值" + precisionMax)
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    println("该阈值下的召回率" + recall.filter(_._1 == precisionMax._1).max())
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    // Precision-Recall Curve
    val PRC = metrics.pr

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    println("最大f1Score" + f1Score.map(x => (x._2, x._1)).max)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    fScore.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)


  }

  //设为>0.5为有问题    <=0.5 为 无问题
  def computeIndex2(b: Double, threashold: Double, scoreAndLabels: RDD[(Double, Double)], writer: PrintWriter) = {
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val t = 0.5
    //  for(t<-List(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9)) {
    val P_test = scoreAndLabels.filter(_._1 > t).count()
    val N_test = scoreAndLabels.filter(_._1 <= t).count()
    val TP = scoreAndLabels.filter(x => (x._1 > t && x._2 == 1)).count()
    val TN = scoreAndLabels.filter(x => (x._1 <= t && x._2 == 0)).count()
    val FP = scoreAndLabels.filter(x => (x._1 > t && x._2 == 0)).count()
    val FN = scoreAndLabels.filter(x => (x._1 <= t && x._2 == 1)).count()
    // AUC
    val auc = metrics.areaUnderROC
    val precision = TP.toDouble / (TP + FP)
    val recall = TP.toDouble / (TP + FN)
    val accuracy = (TP + TN).toDouble / (TP + TN + FN + FP)
    val f1 = 2 * precision * recall / (precision + recall)
    //val results=new HashMap[(Double,Double),(Long,Long,Long,Long,Long,Long,Double,Double,Double,Double,Double)]
    writer.write("\n" + b + "," + threashold + "," + P_test + "," + N_test + "," + TP + "," + TN + "," + FP + "," + FN + "," + auc + "," + precision + "," + recall + "," + f1 + "," + accuracy)
    println(" P(test):" + P_test + " N(test):" + N_test + " TP:" + TP + " TN:" + TN + " FP:" + FP + " FN:" + FN + " AUC:" + auc + " precision:" + precision + " recall:" + recall + " f1:" + f1 + " accuracy:" + accuracy)
    //    println("B为" + b + " 阈值为" + threashold +" accuracy:" + accuracy)

    //   }
  }

  def computeIndex3(b: Double, threashold: Double, originalscoreAndLabels: RDD[(Double, Double)]) = {
    val metrics = new BinaryClassificationMetrics(originalscoreAndLabels)
    val t = 0.5
    //  for(t<-List(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9)) {
    val P_test = originalscoreAndLabels.filter(_._1 > t).count()
    val N_test = originalscoreAndLabels.filter(_._1 <= t).count()
    val TP = originalscoreAndLabels.filter(x => (x._1 > t && x._2 == 1)).count()
    val TN = originalscoreAndLabels.filter(x => (x._1 <= t && x._2 == 0)).count()
    val FP = originalscoreAndLabels.filter(x => (x._1 > t && x._2 == 0)).count()
    val FN = originalscoreAndLabels.filter(x => (x._1 <= t && x._2 == 1)).count()
    // AUC
    val auc = metrics.areaUnderROC
    val precision = TP.toDouble / (TP + FP)
    val recall = TP.toDouble / (TP + FN)
    val accuracy = (TP + TN).toDouble / (TP + TN + FN + FP)
    val f1 = 2 * precision * recall / (precision + recall)
    println("OriginalScore: B为" + b + " 阈值为" + threashold + " P(test):" + P_test + " N(test):" + N_test + " TP:" + TP + " TN:" + TN + " FP:" + FP + " FN:" + FN + " AUC:" + auc + " precision:" + precision + " recall:" + recall + " f1:" + f1 + " accuracy:" + accuracy)
    println("========================")
  }


}
