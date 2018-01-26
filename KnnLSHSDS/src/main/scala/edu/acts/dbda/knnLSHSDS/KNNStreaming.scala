package edu.acts.dbda.knnLSHSDS

import org.apache.spark.streaming.flume._
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
import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object KNNStreming {

  val logger = log4j.Logger.getLogger(getClass)

  def main(args: Array[String]) {


	  val conf= new SparkConf().setAppName("KNNecgStreming").setMaster("local[*]");
	  val spark = SparkSession.builder().config(conf).getOrCreate()
			  val sc = spark.sparkContext
			  val ssc = new StreamingContext(sc, Seconds(5))
			  val sqlContext1 = spark.sqlContext 
			  import sqlContext1.implicits._


			  val trainingData = sc.textFile("DATA/ECG200/ECG200_TRAIN")

			  val parsedtrainingData = trainingData.map { line =>
			    val parts = line.split(',').map(_.toDouble)
			    LabeledPoint(parts(0), Vectors.dense(parts.tail))
			    }.toDF 

			    
			  val train= MLUtils.convertVectorColumnsToML(parsedtrainingData)


			  val knn = new KNNClassifier()
			    .setTopTreeSize(1)
			    .setFeaturesCol("features")
			    .setPredictionCol("prediction")
			    .setK(3)
			  //.setBalanceThreshold(0)
    
			    
			    val model = knn.fit(train)
			   // println("OKAY till Training")
			    
			    val flumeStream = FlumeUtils.createStream(ssc, "127.0.0.1", 9999)
			    
			    val flumeEvents = flumeStream.map(e=>new String(e.event.getBody.array()))
			    
			    val ClassifyEvent=flumeEvents.flatMap(_.split(" "))
			    
			    ClassifyEvent.foreachRDD{  
			      (rdd:RDD[String],time: Time)=>
			    	//  println("     rrrr"+rdd.count())
			    	val requestArray=rdd.map(r=>r.asInstanceOf[String]).collect()
			    	///////////////////IMP 0 to 96
			    	if(requestArray.size>0){
			    		val requestRDD=spark.sparkContext.parallelize(requestArray)
			    		val parsedtestingData = requestRDD.map { line =>
			    		  val parts = line.split(',').map(_.toDouble)
			    		  LabeledPoint(parts(0), Vectors.dense(parts.tail))
			    		  }



			        val requestedDF=parsedtestingData.toDF().cache()

			    		val testingdataset = MLUtils.convertVectorColumnsToML(requestedDF)

//              testingdataset.printSchema()
//			    		testingdataset.show(testingdataset.count().toInt,false)

			    		val prediction = model.transform(testingdataset)//(testingdataset)


			    		val predictionResults=prediction.select("label","prediction")//.show(predictionResults.count().toInt,false)
			    		
			    		val class1= predictionResults.selectExpr("SUM(CASE WHEN label = 1 THEN 1.0  END)")
			    		.collect()
			    		.head.getDecimal(0)
			    		.doubleValue()

			    		val class0= predictionResults.count()-class1			    		

			    		val evaluator = new MulticlassClassificationEvaluator()
			        .setLabelCol("label")
			        .setPredictionCol("prediction")
			    		.setMetricName("accuracy")
			    		
			    		val accuracy = evaluator.evaluate(predictionResults)
			    		
			    		println("================================================\nAccuracy :"+accuracy*100)
			    		println("Total Instance Classified       :"+prediction.count())
			    		println("Instance Classifiled to class:1 :"+class1)
			    		println("Instance Classifiled to class:0 :"+class0)


			    	}

			    	println(s"========================$time====================")

			    }



			    ssc.start()             // Start the computation
			    ssc.awaitTermination()  // Wait for the computation to terminate



  }

}
