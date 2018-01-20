package edu.atcs.dbda
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.streaming._
object DTDS {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DecisionTree").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true");
        val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    // Load and parse the data file.
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




    val testingData = ssc.socketTextStream("localhost", 9999)
    testingData.print()
    val parsedtestingData = testingData.map { line =>
    val parts = line.split(',').map(_.toDouble)
        LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }    
    val parsedtestingDataf= parsedtestingData.map(f=> if (f.label==(-1.0))LabeledPoint(0,f.features)else LabeledPoint(f.label,f.features))

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
//val numClasses = 2
//val categoricalFeaturesInfo = Map[Int, Int]()
//val impurity = "gini"
//val maxDepth = 5
//val maxBins = 32
//
//val model =DecisionTreeModel.load(sc, "DATA/myDecisionTreeModel")

// Evaluate model on test instances and compute test error
val labelAndPreds = parsedtestingDataf.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testErr = labelAndPreds.filter(r => r._1 != r._2).count()// / parsedtestingDataf.count()
println("-------------------------------------\n")

labelAndPreds.count().print()
println("Learned classification forest model:\n" + model.toDebugString)

ssc.start()             // Start the computation
ssc.awaitTermination()  // Wait for the computation to terminate

//val metrics = new MulticlassMetrics(labelAndPreds)
//
//println("Confusion matrix:")
//println(metrics.confusionMatrix)
//val accuracy = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / parsedtestingDataf.count()
//    println(s"Accuracy= $accuracy")
//predictionAndLabels.foreach(f=>println(f._1+" "+f._2))

// Save and load model
//model.save(sc, "target/tmp/myRandomForestRegressionModel")
//val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
  }
  }
  
  