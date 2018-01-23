package edu.acts.dbda.LSHSDS

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
object Kmean {
	def main(args : Array[String]) {
		val conf = new SparkConf().setAppName("StreamingKMeansExample").setMaster("local[*]")
				val ssc = new StreamingContext(conf, Seconds(2))

				val trainingData = ssc.textFileStream("Trainc").map(Vectors.parse)
				val testData = ssc.textFileStream("Test").map(LabeledPoint.parse)

				val model = new StreamingKMeans()
				.setK(2)
				.setDecayFactor(1.0)
				.setRandomCenters(2, 0.0)

				model.trainOn(trainingData)
				model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

				ssc.start()
				ssc.awaitTermination()


	}
}