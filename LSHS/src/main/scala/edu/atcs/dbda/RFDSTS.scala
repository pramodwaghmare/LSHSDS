package edu.atcs.dbda
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object RFDSTS {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RandomForest").setMaster("local[*]")
   val sc = new SparkContext(conf)
    // Load and parse the data file.
    val trainingData = sc.textFile("/DATA/ECG200/ECG200_TRAIN")
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

// Train a RandomForest model.
// Empty categoricalFeaturesInfo indicates all features are continuous.


val model = RandomForestModel.load(sc, "/Model/myRandomForestRegressionModel")

// Evaluate model on test instances and compute test error
val labelAndPreds = parsedtestingDataf.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedtestingDataf.count()
println("Test Error = " + testErr)
println("Learned classification forest model:\n" + model.toDebugString)

    
//    
//    val predictionAndLabels = labelsAndPredictions.map { piont =>
//  val prediction = if (piont._2<=0.5)0 else 1
//  (piont._1,prediction.toDouble)
//}
val metrics = new MulticlassMetrics(labelAndPreds)

println("Confusion matrix:")
println(metrics.confusionMatrix)
val accuracy = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / parsedtestingDataf.count()
    println(s"Accuracy= $accuracy")
//predictionAndLabels.foreach(f=>println(f._1+" "+f._2))

// Save and load model
//model.save(sc, "target/tmp/myRandomForestRegressionModel")
//val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
  }
  }
  
  