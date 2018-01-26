package edu.acts.dbda.LSHSDS

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.VectorAssembler
import  java.lang.Double
/**
 * @author ${user.name}
 */
object App {
  
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("MultilayerPerceptronClassifier").setMaster("local[*]")
				val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
				val sc: SparkContext = ss.sparkContext
				val sqlContext: SQLContext = ss.sqlContext
val dataset = ss.createDataFrame(
  Seq((0, 18, 1.0, 1.0))
).toDF("id", "hour", "mobile", "userFeatures")
		val colNames = dataset.schema.fieldNames

val assembler = new VectorAssembler()
  .setInputCols(Array("hour","mobile", "userFeatures"))
  .setOutputCol("features")
  

 val output = assembler.transform(dataset)
println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
output.select("features", "id").show(false)

val assembler1 = new VectorAssembler()
  .setInputCols(Array("id"))
  .setOutputCol("label")
  
   val output1 = assembler1.transform(output)
   output1.select("features", "label").show(false)

  }

}
