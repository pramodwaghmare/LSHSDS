package edu.atcs.dbda
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object RFDSTR {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RandomForest").setMaster("local[*]")
   val sc = new SparkContext(conf)
    // Load and parse the data file.
    val trainingData = sc.textFile("DATA/ECG200/ECG200_TRAIN")
    val parsedtrainingData = trainingData.map { line =>
    val parts = line.split(',').map(_.toDouble)
        LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }    
    val parsedtrainingDataf= parsedtrainingData.map(f=> if (f.label==(-1.0))LabeledPoint(0,f.features)else LabeledPoint(f.label,f.features))
    
  //  parsedtrainingDataf.foreach(f=> println(f.label))
    val testingData = sc.textFile("/DATA/ECG200/ECG200_TEST")
    val parsedtestingData = testingData.map { line =>
    val parts = line.split(',').map(_.toDouble)
        LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }    
    val parsedtestingDataf= parsedtestingData.map(f=> if (f.label==(-1.0))LabeledPoint(0,f.features)else LabeledPoint(f.label,f.features))

// Train a RandomForest model.
// Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 3 // Use more in practice.
val featureSubsetStrategy = "all" // Let the algorithm choose.
val impurity = "gini"
val maxDepth = 4
val maxBins = 32
val seed =99

val model = RandomForest.trainClassifier(parsedtrainingDataf, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,seed)

// Save and load model
model.save(sc, "/Model/myRandomForestRegressionModel")
println("Model Trained N stored Succesfully at /Model/myRandomForestRegressionModel")
  }
  }
  
  