package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math.{abs, sin, cos, sqrt, pow, toRadians, asin}

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  val pValue = 4
  val width = 360
  val height = 180
  val earthRadius = 6371

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val distances = temperatures.map {
      case (location2, temperature) => (greatCircleDistance(location, location2), temperature)
    }

    val (minDistance, minDistanceTemp) = distances.reduce((a, b) => if (a._1 < b._1) a else b)
    if (minDistance < 1) {
      // a close enough station (< 1km), take its temperature
      minDistanceTemp
    } else {
      // interpolate
      val weights = distances.map { case (distance, temperature) => (1 / pow(distance, pValue), temperature) }
      val denominator = weights.map(_._1).sum
      val numerator = weights.map { case (distance, temperature) => distance * temperature }.sum

      numerator / denominator
    }
  }

  /**
    * @param a First location
    * @param b Second location
    * @return Great-circle distance between a and b
    */
  def greatCircleDistance(a: Location, b: Location): Double = {
    def areAntipodes(a: Location, b: Location): Boolean = {
      a.lat == -b.lat && abs(a.lon - b.lon) == 180
    }

    val deltaSigma =
      if (a == b) {
        0
      } else if (areAntipodes(a, b)) {
        math.Pi
      } else {
        val deltaLon = toRadians(abs(a.lon - b.lon))
        val aLat = toRadians(a.lat)
        val bLat = toRadians(b.lat)
        val deltaLat = abs(aLat - bLat)
        val deltaSigma = 2 * asin(sqrt(pow(sin(deltaLat / 2), 2) + cos(aLat) * cos(bLat) * pow(sin(deltaLon / 2), 2)))
        deltaSigma
      }
    earthRadius * deltaSigma
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {
    val sortedPoints = points.toList.sortWith(_._1 < _._1).toArray

    def next(i: Int) : Int = (i + 1) % points.size
    val pairsOfPoints = for (i <- 0 until points.size) yield (sortedPoints(i), sortedPoints(next(i)))

    if (pairsOfPoints.head._1._1 >= value) {
      pairsOfPoints.head._1._2
    } else if (pairsOfPoints.last._1._1 <= value) {
      pairsOfPoints.last._1._2
    } else {
      val midPoints = pairsOfPoints.find{
        case ((temp1, _), (temp2, _)) => temp1 <= value && temp2 >= value
      }.get

      val r1 = midPoints._1._2.red
      val g1 = midPoints._1._2.green
      val b1 = midPoints._1._2.blue

      val r2 = midPoints._2._2.red
      val g2 = midPoints._2._2.green
      val b2 = midPoints._2._2.blue

      val temp1 = midPoints._1._1
      val temp2 = midPoints._2._1

      val ratio = (value - temp1) / (temp2 - temp1)
      Color(
        math.round(r1 + (r2 - r1) * ratio).toInt,
        math.round(g1 + (g2 - g1) * ratio).toInt,
        math.round(b1 + (b2 - b1) * ratio).toInt
      )
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val coords = for {
      i <- 0 until height
      j <- 0 until width
    } yield (i, j)
    val pixels = coords.par
      .map(transformCoord)
      .map(predictTemperature(temperatures, _))
      .map(interpolateColor(colors, _))
      .map(col => Pixel(col.red, col.green, col.blue, 255))
      .toArray
    Image(width, height, pixels)
  }

  /**
    * @param coord Pixel coordinates
    * @return Latitude and longitude
    */
  def transformCoord(coord: (Int, Int)): Location = {
    val lon = coord._2 - width / 2
    val lat = -(coord._1 - height / 2)
    Location(lat, lon)
  }
}