package observatory

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}

object Main extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val tempToCol = List[(Temperature, Color)]((60, Color(255, 255, 255)), (32, Color(255, 0, 0)),
    (12, Color(255, 255, 0)), (0, Color(0, 255, 255)), (-15, Color(0, 0, 255)), (-27, Color(255, 0, 255)),
    (-50, Color(33, 0, 107)), (-60, Color(0, 0, 0)))

  val devToCol = List[(Temperature, Color)]((7, Color(0, 0, 0)), (4, Color(255, 0, 0)),
    (2, Color(255, 255, 0)), (0, Color(255, 255, 255)), (-2, Color(0, 255, 255)), (-7, Color(0, 0, 255)))

  val yearsTemperature = Range(1975, 2015 + 1) by 3
  val yearsDeviations = Range(1990, 2015 + 1) by 3

  def generateAndSaveTile(outputDirectory: String)(year: Year, tile: Tile, data: Iterable[(Location, Temperature)]): Unit = {
    val zoom = tile.zoom
    val x = tile.x
    val y = tile.y
    val zoomDirectory = f"$outputDirectory%s/$year%d/$zoom%d"
    val fileName = f"$zoomDirectory%s/$x%d-$y%d.png"
    Files.createDirectories(Paths.get(zoomDirectory))

    val image = Interaction.tile(data, tempToCol, tile)
    image.output(new File(fileName))
  }

  yearsTemperature.foreach(year => {
    Range(year, scala.math.min(year + 3, 2015)).par.foreach(year => {
      val temps = Extraction.locateTemperatures(year, "/stations.csv", s"/${year}.csv")
      val tempsAvg = Extraction.locationYearlyAverageRecords(temps)

      val data = List[(Year, Iterable[(Location, Temperature)])]((year, tempsAvg))
      Interaction.generateTiles[Iterable[(Location, Temperature)]](data, generateAndSaveTile("target/temperatures"))

      if (yearsDeviations.contains(year)) {
        val data = List[(Year, Iterable[(Location, Temperature)])]((year, tempsAvg))
        Interaction.generateTiles[Iterable[(Location, Temperature)]](data, generateAndSaveTile("target/deviations"))
      }
    })
  })
}
