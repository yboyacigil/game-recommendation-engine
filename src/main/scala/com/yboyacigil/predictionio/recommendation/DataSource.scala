package com.yboyacigil.predictionio.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{EmptyActualResult, EmptyEvaluationInfo, PDataSource, Params}
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class DataSourceEvalParams(kFold: Int, queryNum: Int)

case class DataSourceParams(appName: String, evalParams: Option[DataSourceEvalParams]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override def readTraining(sc: SparkContext): TrainingData = {
    // get all "player" "play" "game" events
    val playEventsRDD: RDD[PlayEvent] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("player"),
      eventNames = Some(List("play")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("game")))(sc)
      // eventsDb.find() returns RDD[Event]
      .map { event =>
        val playEvent = try {
          event.event match {
            case "play" => PlayEvent(
              player = event.entityId.toInt,
              game = event.targetEntityId.get.toInt,
              t = event.eventTime.getMillis)
            case _ => throw new Exception(s"Unexpected event ${event} is read.")
          }
        } catch {
          case e: Exception => {
            logger.error(s"Cannot convert ${event} to PlayEvent. Exception: ${e}.")
            throw e
          }
        }
        playEvent
      }.cache()

    new TrainingData(
      playEvents = playEventsRDD
    )
  }
}

case class PlayEvent(player: Int, game: Int, t: Long)

class TrainingData(val playEvents: RDD[PlayEvent]) extends Serializable {
  override def toString = {
    s"playEvents: [${playEvents.count()}] (${playEvents.take(2).toList}...)"
  }
}