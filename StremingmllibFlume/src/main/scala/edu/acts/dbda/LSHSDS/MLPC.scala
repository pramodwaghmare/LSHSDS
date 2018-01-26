package edu.acts.dbda.LSHSDS

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
object MLPC {
	def main(args : Array[String]) {
	  val conf = new SparkConf().setAppName("MultilayerPerceptronClassifier").setMaster("local[*]")
				val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
				val sc: SparkContext = ss.sparkContext
				val sqlContext: SQLContext = ss.sqlContext
				// Load the data stored in LIBSVM format as a DataFrame.
				val data = ss.read.format("libsvm")
				.load("data/mllib/sample_multiclass_classification_data.txt")
				//				val f=data.select("features")
				//				data.printSchema()
				//				option("header","true").csv("DATA/job4")
				//
				// Split the data into train and test
				val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
				val train = splits(0)
				val test = splits(1)

				// specify layers for the neural network:
				// input layer of size 4 (features), two intermediate of size 5 and 4
				// and output of size 3 (classes)
				val layers = Array[Int](4, 5, 4, 3)

				// create the trainer and set its parameters
				val trainer = new MultilayerPerceptronClassifier()
				.setLayers(layers)
				.setBlockSize(128)
				.setSeed(1234L)
				.setMaxIter(100)

				// train the model
				val model = trainer.fit(train)

				// compute accuracy on the test set
				val result = model.transform(test)
				val predictionAndLabels = result.select("prediction", "label")
				val evaluator1 = new MulticlassClassificationEvaluator()
				.setMetricName("f1")

				val evaluator2 = new MulticlassClassificationEvaluator()
				.setMetricName("weightedPrecision");

				val evaluator3 = new MulticlassClassificationEvaluator()
						.setMetricName("weightedRecall");

				val evaluator4 = new MulticlassClassificationEvaluator()
						.setMetricName("accuracy");

				println("Test set f1 = " + evaluator1.evaluate(predictionAndLabels))
				println("Test set weightedPrecision = " + evaluator2.evaluate(predictionAndLabels))
				println("Test set weightedRecall = " + evaluator3.evaluate(predictionAndLabels))
				println("Test set accuracy = " + evaluator4.evaluate(predictionAndLabels))



	}
}