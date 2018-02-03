package com.spoddutur.util

import org.viz.lightning.{Lightning, Visualization}

import scala.util.Try

/**
  * Created by sruthi on 02/07/17.
  */
object LightningUtil {

  // init lightning-viz server
  val lgn = Lightning(host="http://localhost:3000")
lgn.createSession()
lgn.createSession("KnnStreaming")

  // initialize visualization with 0 data
  var viz:Visualization = lgn.lineStreaming(
    series = Array.fill(3)(Array(0.0)),
    size = Array(1.0, 10.0))

  // posts new data to lightning-viz graph in a streaming way & plots the graph..
  def update(class1: Double, class2: Double, numRecords: Double) {
    val arr1 = Array(class1)
    val arr2 = Array(class2)
    val arr3 = Array(numRecords)
    println(numRecords)
    Try(lgn.lineStreaming(series = Array(arr1, arr2,arr3), viz = viz, yaxis = "NumRecordsPerBatch and BatchProcessingTime", xaxis = "Batch Number"))
  }
}
