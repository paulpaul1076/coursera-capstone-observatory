package observatory

import java.time.LocalDate

import org.junit.Assert._
import org.junit.Test

trait ExtractionTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _



}

class ExtractionSuite extends ExtractionTest {
  // Implement tests for the methods of the `Extraction` object
  @Test def `first method`: Unit = {

  }

  @Test def `second method`: Unit = {

    val input = Seq(
      (LocalDate.of(1990, 2, 1), Location(123.321, 32.123), 33.5),
      (LocalDate.of(1990, 2, 1), Location(123.321, 32.123), 33.5),
    )

    Extraction.locationYearlyAverageRecords(
      input
    )

    assert(true)
  }
}
