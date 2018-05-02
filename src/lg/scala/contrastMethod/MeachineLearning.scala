package lg.scala.contrastMethod

import org.apache.spark.ml.classification.{LinearSVC, RandomForestClassifier}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import lg.scala.utils.InputOutputTools
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}

/**
  * Created by lg on 2018/1/18.
  *
  *
  */
object MeachineLearning {
  def run(spark:SparkSession): Unit ={
    //训练
    val FEATURES_DF_2014=InputOutputTools.getFeatures(spark)._1
    val assembler_2014 = new VectorAssembler().setInputCols(Array("TZZE","BSRAGE","SWHZDJRQ_MONTH","ZCDZ_NUM","KYSLRQ_MONTH","CWFZR_AREA","FP_ZZSZYFP_SYQYGS_SUM","CWFZRAGE","BSR_AREA","ZCZB","CYRS","FDDBRAGE","FDDBR_AREA","JXSE","XXSE","SRJE","FPSL","SF","SF_ROC","FP_NOC","FP_ROC","JXSE_ROC","XXSE_ROC",
      "JXBD_ZZSE","XSJE_ROC","YNSE_ROC","JESE_TXXS","ZS1","ZS2","ZS3","ZS4","ZS5","ZS6","ZS7","ZS8","ZS9","ZS10","ZS11","ZS12","ZS13","ZS14","ZS15","ZS16","ZS17","ZS18","ZS19","ZS20","ZS21","ZS22","ZS23","ZS24","ZS25","ZS26","ZS27","ZS28","ZS29",
      "ZS30","ZS31","ZS32","ZS33","ZS34","ZS35","ZS36","ZS37","ZS38","ZS39","ZS40","ZS41","ZS42","ZS43","ZS44","ZS45","ZS46","ZS47","ZS48","ZS49","ZS50","ZS51","ZS52","ZS53","ZS54","ZS55","ZS56","ZS57","ZS58","ZS59","ZS60","ZS61","ZS62","ZS63",
      "ZS64","ZS65","ZS66","ZB1","ZB2","ZB3","ZB4","ZB5","ZB6","ZB7","ZB8","ZB9","ZB10","ZB11","ZB12","ZB13","ZB14","ZB15","ZB16","ZB17","ZB18","ZB19","ZB20","ZB21","ZB22","ZB23","ZB24","ZB25","ZB26","ZB27","ZB28","ZB29","ZB30","ZB31","ZB32","ZB33",
      "ZB34","ZB35","ZB36","ZB37","ZB38","ZB39","ZB40","ZB41","ZB42","ZB43","ZB44","ZB45","ZB46","ZB47","ZB48","ZB49","ZB50","ZB51","ZB52","ZB53","ZB54","ZB55","ZB56","ZB57","ZB58","ZB59","ZB60","ZB61","ZB62","ZB63","ZB64","ZB65","ZB66","LS1","LS2",
      "LS3","LS4","LS5","LS6","LS7","LS8","LS9","LS10","LS11","LS12","LS13","LS14","LS15","LS16","LS17","LS18","LS19","LS20","LB1","LB2","LB3","LB4","LB5","LB6","LB7","LB8","LB9","LB10","LB11","LB12","LB13","LB14","LB15","LB16","LB17","LB18","LB19",
      "LB20","XS1","XS2","XS3","XS4","XS5","XS6","XS7","XS8","XS9","XS10","XS11","XS12","XS13","XS14","XS15","XS16","XS17","XS18","XS19","XS20","XS21","XS22","XS23","XS24","XS25","XS26","XS27","XS28","XS29","XS30","XS31","XS32","XS33","XS34","XS35",
      "XS36","XS37","XS38","XS39","XS40","XS41","XS42","XS43","XS44","XS45","XS46","XS47","XS48","XS49","XS50","XS51","XS52","XS53","XS54","XS55","XS56","XS57","XS58","XS59","XS60","XS61","XS62","XS63","XS64","XS65","XS66","XB1","XB2","XB3","XB4",
      "XB5","XB6","XB7","XB8","XB9","XB10","XB11","XB12","XB13","XB14","XB15","XB16","XB17","XB18","XB19","XB20","XB21","XB22","XB23","XB24","XB25","XB26","XB27","XB28","XB29","XB30","XB31","XB32","XB33","XB34","XB35","XB36","XB37","XB38","XB39",
      "XB40","XB41","XB42","XB43","XB44","XB45","XB46","XB47","XB48","XB49","XB50","XB51","XB52","XB53","XB54","XB55","XB56","XB57","XB58","XB59","XB60","XB61","XB62","XB63","XB64","XB65","XB66")).setOutputCol("features")
    val labelAndFeature_2014 = assembler_2014.transform(FEATURES_DF_2014).select("WTBZ","features").withColumnRenamed( "WTBZ" , "label")

    val lsvc = new LinearSVC()//.setMaxIter(100).setRegParam(0.1)
    // Fit the model
    val lsvcModel = lsvc.fit(labelAndFeature_2014)
    //测试
    val FEATURES_DF_2015=InputOutputTools.getFeatures(spark)._2
    val assembler_2015 = new VectorAssembler().setInputCols(Array("TZZE","BSRAGE","SWHZDJRQ_MONTH","ZCDZ_NUM","KYSLRQ_MONTH","CWFZR_AREA","FP_ZZSZYFP_SYQYGS_SUM","CWFZRAGE","BSR_AREA","ZCZB","CYRS","FDDBRAGE","FDDBR_AREA","JXSE","XXSE","SRJE","FPSL","SF","SF_ROC","FP_NOC","FP_ROC","JXSE_ROC","XXSE_ROC",
      "JXBD_ZZSE","XSJE_ROC","YNSE_ROC","JESE_TXXS","ZS1","ZS2","ZS3","ZS4","ZS5","ZS6","ZS7","ZS8","ZS9","ZS10","ZS11","ZS12","ZS13","ZS14","ZS15","ZS16","ZS17","ZS18","ZS19","ZS20","ZS21","ZS22","ZS23","ZS24","ZS25","ZS26","ZS27","ZS28","ZS29",
      "ZS30","ZS31","ZS32","ZS33","ZS34","ZS35","ZS36","ZS37","ZS38","ZS39","ZS40","ZS41","ZS42","ZS43","ZS44","ZS45","ZS46","ZS47","ZS48","ZS49","ZS50","ZS51","ZS52","ZS53","ZS54","ZS55","ZS56","ZS57","ZS58","ZS59","ZS60","ZS61","ZS62","ZS63",
      "ZS64","ZS65","ZS66","ZB1","ZB2","ZB3","ZB4","ZB5","ZB6","ZB7","ZB8","ZB9","ZB10","ZB11","ZB12","ZB13","ZB14","ZB15","ZB16","ZB17","ZB18","ZB19","ZB20","ZB21","ZB22","ZB23","ZB24","ZB25","ZB26","ZB27","ZB28","ZB29","ZB30","ZB31","ZB32","ZB33",
      "ZB34","ZB35","ZB36","ZB37","ZB38","ZB39","ZB40","ZB41","ZB42","ZB43","ZB44","ZB45","ZB46","ZB47","ZB48","ZB49","ZB50","ZB51","ZB52","ZB53","ZB54","ZB55","ZB56","ZB57","ZB58","ZB59","ZB60","ZB61","ZB62","ZB63","ZB64","ZB65","ZB66","LS1","LS2",
      "LS3","LS4","LS5","LS6","LS7","LS8","LS9","LS10","LS11","LS12","LS13","LS14","LS15","LS16","LS17","LS18","LS19","LS20","LB1","LB2","LB3","LB4","LB5","LB6","LB7","LB8","LB9","LB10","LB11","LB12","LB13","LB14","LB15","LB16","LB17","LB18","LB19",
      "LB20","XS1","XS2","XS3","XS4","XS5","XS6","XS7","XS8","XS9","XS10","XS11","XS12","XS13","XS14","XS15","XS16","XS17","XS18","XS19","XS20","XS21","XS22","XS23","XS24","XS25","XS26","XS27","XS28","XS29","XS30","XS31","XS32","XS33","XS34","XS35",
      "XS36","XS37","XS38","XS39","XS40","XS41","XS42","XS43","XS44","XS45","XS46","XS47","XS48","XS49","XS50","XS51","XS52","XS53","XS54","XS55","XS56","XS57","XS58","XS59","XS60","XS61","XS62","XS63","XS64","XS65","XS66","XB1","XB2","XB3","XB4",
      "XB5","XB6","XB7","XB8","XB9","XB10","XB11","XB12","XB13","XB14","XB15","XB16","XB17","XB18","XB19","XB20","XB21","XB22","XB23","XB24","XB25","XB26","XB27","XB28","XB29","XB30","XB31","XB32","XB33","XB34","XB35","XB36","XB37","XB38","XB39",
      "XB40","XB41","XB42","XB43","XB44","XB45","XB46","XB47","XB48","XB49","XB50","XB51","XB52","XB53","XB54","XB55","XB56","XB57","XB58","XB59","XB60","XB61","XB62","XB63","XB64","XB65","XB66")).setOutputCol("features")
    val labelAndFeature_2015 = assembler_2015.transform(FEATURES_DF_2015).select("features")//.withColumnRenamed( "WTBZ" , "label")

    val randomfroest = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setNumTrees(10)
  //  val labelConvertered =

/*
    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(labelAndFeature_2015)
    */
    val predictions = lsvcModel.transform(labelAndFeature_2015)

    // obtain evaluator.
  //  val evaluator = new Multiclass ClassificationEvaluator().setMetricName("f1")
    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1 - accuracy}")

  }

}
