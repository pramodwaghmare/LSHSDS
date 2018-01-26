package edu.acts.dbda.LSHSDS


import org.apache.spark.streaming.flume._
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
object RFStreamingTest {
	def main(args : Array[String]) {
	  
 val conf = new SparkConf().setAppName("MultilayerPerceptronClassifier").setMaster("local[*]")
	//val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
	//val sc: SparkContext = ss.sparkContext

	val ssc = new StreamingContext(conf, Seconds(1))

 val model=MultilayerPerceptronClassificationModel.load("Model/RF")

	 val flumeStream = FlumeUtils.createStream(ssc, "local", 9999)
	 
	 val flumeEvents = flumeStream.map(e=>new String(e.event.getBody.array()))
	 
	 val ecgClassifyEvent=flumeEvents.flatMap(_.split(" "))
	 
	 ecgClassifyEvent.foreachRDD{
	    (rdd:RDD[String],time: Time)=>
	
    val spark  = SparkSession.builder().config(conf).getOrCreate()//(rdd.sparkContext.getConf)
    import spark.implicits._
    val requestArray=rdd.map(r=>r.asInstanceOf[String]).collect()
    if(requestArray.size>0){
      val requestRDD=spark.sparkContext.parallelize(requestArray)
      val requestArrayD=requestRDD.map(f=>f.toDouble)
      val sqlContext1 = spark.sqlContext 
      import sqlContext1.implicits._
        val requestedDF=requestArrayD.toDF()
        val colNames = requestedDF.schema.fieldNames
        val feturesDouble =colNames.filter(!_.contains(colNames.head))
        val featuresAssembler = new VectorAssembler()
        .setInputCols(feturesDouble)
        .setOutputCol("features")
        val requestedDFfeatures = featuresAssembler.transform(requestedDF)
        val indexer = new StringIndexer()
        .setInputCol("_c0")
        .setOutputCol("label")
        val requestedDFfeaturesAndlabel = indexer.fit(requestedDFfeatures).transform(requestedDFfeatures)
        
        val result = model.transform(requestedDFfeaturesAndlabel)
	      val predictionAndLabels = result.select("prediction", "label","features")  
        
      
    }
    
    println(s"========================$time====================")
    }
	 
	 
    ssc.start()
    ssc.awaitTermination()

	}
}