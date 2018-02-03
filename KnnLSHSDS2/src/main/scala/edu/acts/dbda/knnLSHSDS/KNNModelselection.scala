package edu.acts.dbda.knnLSHSDS

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.KNNClassifier
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object KNNModelselection {

  val logger = log4j.Logger.getLogger(getClass)

  def main(args: Array[String]) {
      

	      val conf= new SparkConf().setAppName("KNNecg").setMaster("local[*]")
			  val spark = SparkSession.builder().config(conf).getOrCreate()
			  val sc = spark.sparkContext



			  //read in raw label and features
			  val rawDatasetTr = spark.read.format("csv").load("DATA/ECG200/ECG200_TRAIN")
			  val rawDatasetTs = spark.read.format("csv").load("DATA/ECG200/ECG200_TEST")



			  val colNames = rawDatasetTr.schema.fieldNames



			  val rawDatasetDoubleTr=rawDatasetTr.columns.
			  foldLeft(rawDatasetTr)((current, c) => current.
			  withColumn(c, rawDatasetTr(c).cast("double")))

			  val rawDatasetDoubleTs=rawDatasetTs.columns.
			  foldLeft(rawDatasetTs)((current, c) => current.
			  withColumn(c, rawDatasetTs(c).cast("double")))



			  val feturesDouble =colNames.filter(!_.contains(colNames.head))



			  val featuresAssembler = new VectorAssembler()
			  .setInputCols(feturesDouble)
			  .setOutputCol("features")



			  val trainingDatafeatures = featuresAssembler.transform(rawDatasetDoubleTr)

			  val testingDatafeatures = featuresAssembler.transform(rawDatasetDoubleTs)



			  val indexer = new StringIndexer()
			  .setInputCol("_c0")
			  .setOutputCol("label")



			  val trainingDatafeatureslables = indexer.fit(trainingDatafeatures).transform(trainingDatafeatures)

			  val testingDatafeatureslables = indexer.fit(testingDatafeatures).transform(testingDatafeatures)



			  // convert "features" from mllib.linalg.Vector to ml.linalg.Vector
			  val trainingdataset = MLUtils.convertVectorColumnsToML(trainingDatafeatureslables)

			  val testingdataset = MLUtils.convertVectorColumnsToML(testingDatafeatureslables)

			


			  //	
			  //create PCA matrix to reduce feature dimensions
			  val pca = new PCA()
			  .setInputCol("features")
			  .setK(1)
			  .setOutputCol("pcaFeatures")



			  //    //split training and testing
			  //    val Array(train, test) = dataset
			  //      .randomSplit(Array(0.7, 0.3), seed = 1234L)
			  //      .map(_.cache())
			  //

			  val train=trainingdataset
			  val test=testingdataset




			  val knn = new KNNClassifier()
			  .setTopTreeSize(1)
			  .setFeaturesCol("features")
			  .setPredictionCol("prediction")
			  .setK(3)
			  //.setBalanceThreshold(0)

			  val pipeline = new Pipeline()
			  .setStages(Array(pca,knn))
			  //     .fit(train)

			  val paramGrid = new ParamGridBuilder()
			  .addGrid(knn.k, 3 to 3)//if 50 then setBalanceThreshold(0)
			  .addGrid(pca.k, 1 to 1 by 1)
			  .build()

			  val cvModel = new CrossValidator()
			  .setEstimator(pipeline)
			  .setEvaluator(new MulticlassClassificationEvaluator)
			  .setEstimatorParamMaps(paramGrid)
			  .setNumFolds(5)


			  val model = cvModel.fit(train)
			  

			  val insample = validate(model.transform(train))

			  val outofsample = validate(model.transform(test))

			  val predictionResults=model.transform(test)
			  .select("label", "prediction")

//			  predictionResults.printSchema()
//			  predictionResults.show(predictionResults.count().toInt,false)
			  predictionResults.show(false)

			  val evaluator = new MulticlassClassificationEvaluator()
			  .setLabelCol("label")
			  .setPredictionCol("prediction")
			  .setMetricName("accuracy")


			  val accuracy = evaluator.evaluate(predictionResults)
			  val class1= predictionResults.selectExpr("SUM(CASE WHEN label = 1 THEN 1.0  END)")
			  .collect()
			  .head.getDecimal(0)
			  .doubleValue()
			  
			  val class0= predictionResults.count()-class1

			  println(s"In-sample: $insample, Out-of-sample: $outofsample")
			  println(s"Cross-validated: ${model.avgMetrics.toSeq}")
			  println("Accuracy :"+accuracy*100)
			  println("class1 :"+class1)
			  println("class0 :"+class0)


          //reference accuracy: in-sample 95% out-of-sample 94%
//			  logger.info(s"In-sample: $insample, Out-of-sample: $outofsample")
//			  logger.info(s"In-sample: $insample, Out-of-sample: $outofsample")
//			  logger.info(s"Cross-validated: ${cv.avgMetrics.toSeq}")
  }

  private[this] def validate(results: DataFrame): Double = {
    results
      .selectExpr("SUM(CASE WHEN label = prediction THEN 1.0 ELSE 0.0 END) / COUNT(1)")
      .collect()
      .head
      .getDecimal(0)
      .doubleValue()
  }
}
