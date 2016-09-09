package converters.netflix

import java.io.{File, PrintWriter}
import java.util.regex.Pattern

import sys.process._
import scala.io.Source

/** Processed the Netflix training data and creates a sorted CSV file of form USER-ID,DATE,MOVIE-ID,RATING */
object training_set_to_csv extends App {
  // training set must be untared in the following directory
  val inputDirectory = new File("data-local/netflix/raw/training_set")

  // and result goes here
  val outputFile = new File("data-local/netflix/training_set.csv")

  // first create a temporary file
  val tempFile = File.createTempFile("training_set_unsorted", "csv")
  val tempFileWriter = new PrintWriter(tempFile);
  println(s"Using temporary file $tempFile")

  // parse the data and store in temp file
  val files = inputDirectory.listFiles
  val movieIdPattern = Pattern.compile("(\\d+)\\:");
  val entryPattern = Pattern.compile("^(\\d+),(\\d+),(.+)$")
  for (file <- files) {
    println(s"Processing $file")

    // get movie Id
    val lines = Source.fromFile(file.getPath).getLines()
    // get movie id
    val matcher = movieIdPattern.matcher(lines.next)
    matcher.find
    val movieId = matcher.group(1)

    // iterate over the remaining lines
    for (line <- lines) {
      val matcher = entryPattern.matcher(line)
      matcher.find
      val userId = matcher.group(1)
      val rating = matcher.group(2)
      val date = matcher.group(3)
      tempFileWriter.println(s"$userId,$date,$movieId,$rating")
    }
  }

  // now close the temp file and sort it
  tempFileWriter.close
  println("Sorting...")
  if (System.getProperty("os.name").contains("Windows")) {
    s"c:/cygwin64/bin/sort.exe -n $tempFile -o $outputFile" !
  } else {
    s"sort.exe -n $tempFile -o $outputFile" !
  }
  tempFile.delete
}
