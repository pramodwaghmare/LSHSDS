package edu.atcs.dbda


import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.util.MLUtils

object GBMDS {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("GradientBoostedTrees").setMaster("local[*]")
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

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 20// Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 10
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(parsedtrainingDataf, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = parsedtestingDataf.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //labelAndPreds.foreach(f=>println(f._1+" "+f._2))
    val metrics = new MulticlassMetrics(labelAndPreds)

// Confusion matrix
println("Confusion matrix:")
println(metrics.confusionMatrix)
    val accuracy = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / parsedtestingDataf.count()
    println(s"Accuracy= $accuracy")
   // println(s"Learned classification GBT model:\n ${model.toDebugString}")

    // Save and load model
//    model.save(sc, "target/tmp/myGradientBoostingClassificationModel")
//    val sameModel = GradientBoostedTreesModel.load(sc,
//      "target/tmp/myGradientBoostingClassificationModel")
//
    sc.stop()
 
  }
}