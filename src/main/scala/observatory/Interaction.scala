package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import math.{Pi, atan, pow, sinh, toDegrees}
import Visualization.{interpolateColor, predictTemperature, transformCoord}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends InteractionInterface {
  val power: Int = 7
  val width: Int = 1 << power
  val height: Int = 1 << power
  val alpha: Int = 127

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val lat = toDegrees(atan(sinh(Pi * (1.0 - 2.0 * tile.y.toDouble / (1 << tile.zoom)))))
    val lon = tile.x.toDouble / (1 << tile.zoom) * 360.0 - 180.0
    Location(lat, lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param tile         Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val xOffset = tile.x * (1 << power)
    val yOffset = tile.y * (1 << power)
    val zOffset = tile.zoom

    val coords = for {
      i <- 0 until height
      j <- 0 until width
    } yield (i, j)

    val pixels = coords.par
      .map({ case (y, x) => Tile(x + xOffset, y + yOffset, power + zOffset) })
      .map(tileLocation)
      .map(predictTemperature(temperatures, _))
      .map(interpolateColor(colors, _))
      .map(col => Pixel(col.red, col.green, col.blue, alpha))
      .toArray

    Image(width, height, pixels)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](yearlyData: Iterable[(Year, Data)], generateImage: (Year, Tile, Data) => Unit): Unit = {
    val tiles = for {
      zoom <- 0 until 4
      x <- 0 until 1 << zoom
      y <- 0 until 1 << zoom
      yearData <- yearlyData
    } yield generateImage(yearData._1, Tile(x, y, zoom), yearData._2)
  }

}
