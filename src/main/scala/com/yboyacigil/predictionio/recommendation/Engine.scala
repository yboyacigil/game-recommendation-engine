package com.yboyacigil.predictionio.recommendation

import org.apache.predictionio.controller.{Engine, EngineFactory}

case class Query(
  player: Int,
  games: List[Int]
)

case class PredictedResult(
  gameScores: Array[GameScore],
  isOriginal: Boolean // set to true if the items are not ranked at all.
)

case class GameScore (
  game: Int,
  score: Double
)

object GameRecommendationEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("als" -> classOf[ALSAlgorithm]),
      classOf[Serving])
  }
}