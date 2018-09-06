package com.yboyacigil.predictionio.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{P2LAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating => MLlibRating}

case class ALSAlgorithmParams(
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]) extends Params

class ALSModel(
  val rank: Int,
  val playerFeatures: Map[Int, Array[Double]],
  val gameFeatures: Map[Int, Array[Double]]
) extends Serializable {

  override def toString = {
    s" rank: ${rank}" +
    s" playerFeatures: [${playerFeatures.size}]" +
    s"(${playerFeatures.take(2).toList}...)" +
    s" gameFeatures: [${gameFeatures.size}]" +
    s"(${gameFeatures.take(2).toList}...)"
  }
}

class ALSAlgorithm(val ap: ALSAlgorithmParams)
  extends P2LAlgorithm[PreparedData, ALSModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): ALSModel = {
    require(!data.playEvents.take(1).isEmpty,
      s"playEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")

    val mllibRatings = data.playEvents
      .map { r =>
        val player = r.player
        val game = r.game

        if (player == -1)
          logger.info(s"Nonexistent player ID ${r.player}")
        if (game == -1)
          logger.info(s"Nonexistent game ID ${r.game}")

        ((player, game), 1)
      }.filter { case ((p, g), v) =>
        // keep events with valid player and game ids
        (p != -1) && (g != -1)
      }.reduceByKey(_ + _) // aggregate all play events of same player-game pair
      .map { case ((p, g), v) =>
        // MLlibRating requires player and game ids
        MLlibRating(p, g, v)
      }

    // MLLib ALS cannot handle empty training data.
    require(!mllibRatings.take(1).isEmpty,
      s"mllibRatings cannot be empty." +
      " Please check if your events contain valid player and game ID.")

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = 1.0,
      seed = seed)

    new ALSModel(
      rank = m.rank,
      playerFeatures = m.userFeatures.collectAsMap.toMap,
      gameFeatures = m.productFeatures.collectAsMap.toMap
    )
  }

  def predict(model: ALSModel, query: Query): PredictedResult = {

    val gameFeatures = model.gameFeatures

    // default itemScores array if items are not ranked at all
    lazy val notRankedItemScores = query.games.map(i => GameScore(i, 0)).toArray

    model.playerFeatures.get(query.player).map { playerFeature =>
      val scores: Vector[Option[Double]] = query.games.toVector
        .par // convert to parallel collection for parallel lookup
        .map(game => gameFeatures.get(game).map(gameFeature => dotProduct(gameFeature, playerFeature)))
        .seq // convert back to sequential collection

      // check if all scores is None (get rid of all None and see if empty)
      val isAllNone = scores.flatten.isEmpty

      if (isAllNone) {
        logger.info(s"No productFeature for all games ${query.games}.")
        PredictedResult(
          gameScores = notRankedItemScores,
          isOriginal = true
        )
      } else {
        // sort the score
        val ord = Ordering.by[GameScore, Double](_.score).reverse
        val sorted = query.games.zip(scores).map{ case (game, scoreOpt) =>
          GameScore(
            game = game,
            score = scoreOpt.getOrElse[Double](0)
          )
        }.sorted(ord).toArray

        PredictedResult(
          gameScores = sorted,
          isOriginal = false
        )
      }
    }.getOrElse {
      logger.info(s"No playerFeature found for player ${query.player}.")
      PredictedResult(
        gameScores = notRankedItemScores,
        isOriginal = true
      )
    }

  }

  private def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1
    }
    d
  }

}