package edu.acts.dbda.LSHSDS

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.VectorAssembler
import  java.lang.Double
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
object SLR {
	def main(args : Array[String]) {
		val conf = new SparkConf().setAppName("Streaming linear regression").setMaster("local[*]")
				val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
				val sc: SparkContext = ss.sparkContext
				val sqlContext: SQLContext = ss.sqlContext
				val ssc = new StreamingContext(sc, Seconds(5))
				// Load the data stored in LIBSVM format as a DataFrame.


				val trainingData = ssc.textFileStream("Train").map(LabeledPoint.parse).cache()
				val testData = ssc.textFileStream("Test").map(LabeledPoint.parse)

				val numFeatures = 96
				val model = new StreamingLinearRegressionWithSGD()
				.setInitialWeights(Vectors.zeros(numFeatures))
				model.trainOn(trainingData)
				val pl=model.predictOnValues(testData.map(lp => (lp.label, lp.features)))//.print()
				ssc.start()
				ssc.awaitTermination()


				//		val fr=colNames.
				//	colNames.foreach(f=>println(f))
				//				val df_new = trainingData.map{r => 
				//				val OO =(r.get(_) match {case null => Double.NaN 
				//                                             case d: Double => d 
				//                                             case l: Long => l}).toArrayS
				//				} 
				//				df_new.printSchema()
				//				//trainingData.  /////////////////////////row.toSeq.tail
				//				val ccc= trainingData.map{row=>
				//				val doubleArray = for (i <- 1 to 96){
				//					row.getDouble(i)
				//				}
				//				LabeledPoint(row.getDouble(0),Vectors.dense(row.getDouble(2).to)
				//		}
				//				val parsedtrainingData = trainingData.map{ row =>
				//				
				//				LabeledPoint(row.get(0), Vectors.dense(row.))
				//				}    
				///////df.map(lambda line:LabeledPoint(line[0],[line[1:]]))

				//				// Split the data into train and test
				//				val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
				//				val train = splits(0)
				//				val test = splits(1)
				//
				//				// specify layers for the neural network:
				//				// input layer of size 4 (features), two intermediate of size 5 and 4
				//				// and output of size 3 (classes)
				//				val layers = Array[Int](4, 5, 4, 3)
				//
				//				// create the trainer and set its parameters
				//				val trainer = new MultilayerPerceptronClassifier()
				//				.setLayers(layers)
				//				.setBlockSize(128)
				//				.setSeed(1234L)
				//				.setMaxIter(100)
				//
				//				// train the model
				//				val model = trainer.fit(train)
				//
				//				// compute accuracy on the test set
				//				val result = model.transform(test)
				//				val predictionAndLabels = result.select("prediction", "label")
				//				val evaluator1 = new MulticlassClassificationEvaluator()
				//				.setMetricName("f1")
				//
				//				val evaluator2 = new MulticlassClassificationEvaluator()
				//				.setMetricName("weightedPrecision");
				//		
				//				val evaluator3 = new MulticlassClassificationEvaluator()
				//				.setMetricName("weightedRecall");
				//				
				//				val evaluator4 = new MulticlassClassificationEvaluator()
				//				.setMetricName("accuracy");
				//
				//				println("Test set f1 = " + evaluator1.evaluate(predictionAndLabels))
				//				println("Test set weightedPrecision = " + evaluator2.evaluate(predictionAndLabels))
				//				println("Test set weightedRecall = " + evaluator3.evaluate(predictionAndLabels))
				//				println("Test set accuracy = " + evaluator4.evaluate(predictionAndLabels))



	}
}