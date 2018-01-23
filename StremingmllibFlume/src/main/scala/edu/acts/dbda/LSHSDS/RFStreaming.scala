package edu.acts.dbda.LSHSDS


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
object RFStreaming {
	def main(args : Array[String]) {

	val conf = new SparkConf().setAppName("RandomForestClassifier").setMaster("local[*]")
	val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
	val sc: SparkContext = ss.sparkContext
	val sqlContext: SQLContext = ss.sqlContext
	// Load the data stored in LIBSVM format as a DataFrame.
	//val peopleRDD = sc.textFile("DATA/ECG200/ECG200_TRAIN")
	val trainingData1 = ss.read.format("csv").load("DATA/ECG200/ECG200_TRAIN")
		val colNames = trainingData1.schema.fieldNames
		
		
		
//		val fields = colNames
//  .map(fieldName => StructField(fieldName, StringType, nullable = true))
//val schema = StructType( Array[StructField](StructField("_c0", StringType, nullable = true),StructField("_c1", StringType, nullable = true),StructField("_c1", StringType, nullable = true)))

//val rowRDD = peopleRDD
//  .map(_.split(","))
//  .map(attributes => Row(attributes(0),attributes(1),attributes(2),attributes(3)))
//
//	val peopleDF = ss.createDataFrame(rowRDD, schema)
		
//	peopleDF.printSchema()
		
		
		
		
	val trainingDataDouble=trainingData1.columns.
	  foldLeft(trainingData1)((current, c) => current.
	        withColumn(c, trainingData1(c).cast("double")))
	        	        
//	trainingDataDouble.show()
//	
////println(colNames.length)
  val feturesDouble =colNames.filter(!_.contains(colNames.head))
  
  //println(colNames.head)
 // feturesDouble.foreach(f=>println(f))
//  
  val featuresAssembler = new VectorAssembler()
	  .setInputCols(feturesDouble)
	  .setOutputCol("features")
//	  
	val trainingDatafeatures = featuresAssembler.transform(trainingDataDouble)
	
	//	trainingDatafeatures.show()

//	
	
	val indexer = new StringIndexer()
  .setInputCol("_c0")
  .setOutputCol("label")

  
val data = indexer.fit(trainingDatafeatures).transform(trainingDatafeatures)

//trainingDatafeaturesAndlabel.show()

	  


	//				val df2 = data.select(
	//   data.columns.map {
//     case _ => df(year).cast(IntegerType).as(year)
//     case make @ "make" => functions.upper(df(make)).as(make)
//     case other         => df(other)
//   }: _*
//)
//


	// Split the data into train and test
	val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")
  .fit(data)
// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4)
  .fit(data)

// Split the data into training and test sets (30% held out for testing).
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Train a RandomForest model.
val rf = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setNumTrees(10)

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)

// Chain indexers and forest in a Pipeline.
val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

// Train model. This also runs the indexers.
val model = pipeline.fit(trainingData)

// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

model.write.overwrite().save("Model/RF21")
//val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
//println("Learned classification forest model:\n" + rfModel.toDebugString)

	}
}