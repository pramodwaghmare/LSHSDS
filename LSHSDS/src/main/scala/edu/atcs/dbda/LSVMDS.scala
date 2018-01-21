package edu.atcs.dbda

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors

object LSVMDS {
   def main(args: Array[String]) {

  val conf = new SparkConf().setAppName("LinearSVM").setMaster("local[*]")
  val sc = new SparkContext(conf)
    // Load and parse the data file.
    val trainingData = sc.textFile("DATA/ECG200/ECG200_TRAIN")
    val parsedtrainingData = trainingData.map { line =>
    val parts = line.split(',').map(_.toDouble)
        LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }    
    val parsedtrainingDataf= parsedtrainingData.map(f=> if (f.label==(-1.0))LabeledPoint(0,f.features)else LabeledPoint(f.label,f.features))
    
  //  parsedtrainingDataf.foreach(f=> println(f.label))
    val testingData = sc.textFile("DATA/ECG200/ECG200_TEST")
    val parsedtestingData = testingData.map { line =>
    val parts = line.split(',').map(_.toDouble)
        LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }    
    val parsedtestingDataf= parsedtestingData.map(f=> if (f.label==(-1.0))LabeledPoint(0,f.features)else LabeledPoint(f.label,f.features))

// Run training algorithm to build the model
val numIterations = 210
val model = SVMWithSGD.train(parsedtrainingDataf, numIterations)

// Clear the default threshold.
model.clearThreshold()

// Compute raw scores on the test set.
val scoreAndLabels = parsedtestingDataf.map { point =>
  val score = model.predict(point.features)
  (score, point.label)
}

val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

println("Area under ROC = " + auROC)
val predictionAndLabels = scoreAndLabels.map { piont =>
  val prediction = if (piont._1<=0)0 else 1
  (prediction.toDouble,piont._2)
}

val metrics1 = new MulticlassMetrics(predictionAndLabels)
println("Confusion matrix:")
println(metrics1.confusionMatrix)
 val svmTotalCorrect = predictionAndLabels.map { point =>
      if(point._1 == point._2) 1 else 0
    }.sum()
     val svmAccuracy = svmTotalCorrect / parsedtestingDataf.count()
    println(s"Accuracy= $svmAccuracy")
   }
}