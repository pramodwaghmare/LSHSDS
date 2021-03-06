package edu.atcs.dbda
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.streaming._
object DTDS2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DecisionTree").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    // Load and parse the data file.
    val trainingData = sc.textFile("DATA/ECG200/ECG200_TRAIN")
    val parsedtrainingData = trainingData.map { line =>
    val parts = line.split(',').map(_.toDouble)
        LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }    
    val parsedtrainingDataf= parsedtrainingData.map(f=> if (f.label==(-1.0))LabeledPoint(0,f.features)else LabeledPoint(f.label,f.features))
    

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainClassifier(parsedtrainingDataf, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)




    

//val metrics = new MulticlassMetrics(labelAndPreds)
//
//println("Confusion matrix:")
//println(metrics.confusionMatrix)
//val accuracy = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / parsedtestingDataf.count()
//    println(s"Accuracy= $accuracy")
//predictionAndLabels.foreach(f=>println(f._1+" "+f._2))

// Save and load model
model.save(sc, "DATA/myDecisionTreeModel")
//val sameModel = DecisionTreeModel.load(sc, "target/tmp/myRandomForestRegressionModel")
  }
  }
  
  