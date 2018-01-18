package edu.atcs.dbda

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors

object LogisticRegressionDS {
    def main(args: Array[String]) {

  val conf = new SparkConf().setAppName("LogisticRegressionWithLBFGS").setMaster("local[*]")
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
val model = new LogisticRegressionWithLBFGS()
  .setNumClasses(2)
  .run(parsedtrainingDataf)

// Compute raw scores on the test set
val predictionAndLabels = parsedtestingDataf.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Instantiate metrics object
val metrics = new MulticlassMetrics(predictionAndLabels)

// Confusion matrix
println("Confusion matrix:")
println(metrics.confusionMatrix)

 val accuracy = predictionAndLabels.filter(r => r._1 == r._2).count.toDouble / parsedtestingData.count()
    println(s"Accuracy= $accuracy")


// Overall Statistics
//val accuracy = metrics.accuracy
//println("Summary Statistics")
//println(s"Accuracy = $accuracy")

// Precision by label
//val labels = metrics.labels
//labels.foreach { l =>
//  println(s"Precision($l) = " + metrics.precision(l))
//}
//
//// Recall by label
//labels.foreach { l =>
//  println(s"Recall($l) = " + metrics.recall(l))
//}
//
//// False positive rate by label
//labels.foreach { l =>
//  println(s"FPR($l) = " + metrics.falsePositiveRate(l))
//}
//
//// F-measure by label
//labels.foreach { l =>
//  println(s"F1-Score($l) = " + metrics.fMeasure(l))
//}
//
//// Weighted stats
//println(s"Weighted precision: ${metrics.weightedPrecision}")
//println(s"Weighted recall: ${metrics.weightedRecall}")
//println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
//println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")

}
}