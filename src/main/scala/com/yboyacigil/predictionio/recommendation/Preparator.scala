package com.yboyacigil.predictionio.recommendation

import org.apache.predictionio.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(playEvents = trainingData.playEvents)
  }
}

class PreparedData(
  val playEvents: RDD[PlayEvent]
) extends Serializable