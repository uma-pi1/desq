package de.uni_mannheim.desq.converters.netflix

import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}
import java.util.regex.Pattern

import de.uni_mannheim.desq.dictionary.{Dictionary, DictionaryIO, Item}
import de.uni_mannheim.desq.io.{DelSequenceWriter, SequenceReader}
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}

import scala.collection.mutable
import scala.io.Source
import scala.sys.process._

/** Process the Netflix training data and creates a sorted CSV file of form USER-ID,DATE,MOVIE-ID,RATING */
object TrainingSetToCsv extends App {
  // training set must be untared in the following directory
  val inputDirectory = new File("data-local/netflix/raw/training_set")

  // and result goes here
  val outputFile = new File("data-local/netflix/training_set.csv")

  // first create a temporary file
  val tempFile = File.createTempFile("training_set_unsorted", ".csv")
  println(s"Using temporary file $tempFile")

  // parse the data and store as CSV in temp file
  val tempFileWriter = new PrintWriter(tempFile)
  val files = inputDirectory.listFiles
  val movieIdPattern = Pattern.compile("^(\\d+)\\:$")
  val entryPattern = Pattern.compile("^(\\d+),(\\d+),(.+)$")
  for (file <- files) {
    println(s"Processing $file")

    // get movie Id
    val lines = Source.fromFile(file.getPath).getLines
    // get movie id
    val matcher = movieIdPattern.matcher(lines.next)
    matcher.find
    val movieId = matcher.group(1)

    // iterate over the remaining lines (the ratings)
    for (line <- lines) {
      val matcher = entryPattern.matcher(line)
      matcher.find
      val userId = matcher.group(1)
      val rating = matcher.group(2)
      val date = matcher.group(3)
      tempFileWriter.println(s"$userId,$date,$movieId,$rating")
    }
  }
  tempFileWriter.close

  // now sort the temporary file using GNU sort
  println("Sorting...")
  var sortCmd = s"sort -t , -k 1n,1 -k 2,2 -k 3n,3 $tempFile -o $outputFile"
  if (System.getProperty("os.name").contains("Windows")) {
    sortCmd = "c:/cygwin64/bin/" + sortCmd
  }
  sortCmd.!
  tempFile.delete
}

/** Information about a movie */
class Movie(_id : Int, _year : Option[Int], _title : String) {
  val id = _id
  val year = _year
  val title = _title

  override def toString = s"Movie($id,$year,$title)"
  def yearString = year.getOrElse("UnknownYear").toString
  def unratedSid = title + "#" + yearString + "#" + id
  def ratedSid(rating : Int) = unratedSid + "@" + rating
}

/** Utility methods to read movie data */
object Movie {
  def readMovies : Array[Movie] = {
    // training set must be untared in the following directory
    val inputFile = "data-local/netflix/raw/movie_titles.txt"
    val pattern = Pattern.compile("^(\\d+),(\\d+|NULL),(.+)$")
    val movies = new mutable.ArrayBuffer[Movie]
    for (line <- Source.fromFile(inputFile, "Cp850").getLines) {
      val matcher = pattern.matcher(line)
      matcher.find
      val id = matcher.group(1).toInt
      val yearString = matcher.group(2)
      var year : Option[Int] = None
      if (yearString != "NULL") year = Some(yearString.toInt)
      val title = matcher.group(3)
      val movie = new Movie(id, year, title)
      movies.append(movie)
    }

    movies.toArray
  }

  def idIndex(movies : Seq[Movie]): Map[Int,Movie] = {
    Map(movies.map(m => (m.id, m)): _*)
  }

  def titleIndex(movies : Seq[Movie]): Map[String,Movie] = {
    Map(movies.map(m => (m.title, m)): _*)
  }
}

/** A rating of a user for an item at a specific date */
class MovieRating(_userId : Int, _date : String, _movieId : Int, _rating : Int) {
  val userId = _userId
  val date = _date
  val movieId = _movieId
  val rating = _rating

  override def toString = s"Rating($userId,$date,$movieId,$rating)"
}

/** Utility methods for reading training data (once converted to CSV) */
object MovieRating {
  /** One sequence per user, each with that user's ratings sorted by time */
  def getMovieRatingsByUserIterator : Iterator[ Seq[MovieRating] ]= {
    val inputFile = "data-local/netflix/training_set.csv"

    new Iterator[ Seq[MovieRating] ] {
      val sequence = new mutable.ArrayBuffer[MovieRating]
      val lines = Source.fromFile(inputFile).getLines
      var nextMovieRating = readNextMovieRating(lines.next)
      var noSequences = 0

      def readNextMovieRating(line: String): MovieRating = {
        val entries = line.split(',')
        val userId = entries(0).toInt
        val date = entries(1)
        val movieId = entries(2).toInt
        val rating = entries(3).toInt
        val movieRating = new MovieRating(userId, date, movieId, rating)
        movieRating
      }

      override def hasNext: Boolean = nextMovieRating != null

      override def next(): Seq[MovieRating] = {
        sequence.clear
        sequence.append(nextMovieRating)
        var userId = nextMovieRating.userId
        var done = false
        while (lines.hasNext && !done) {
          nextMovieRating = readNextMovieRating(lines.next)
          if (nextMovieRating.userId == userId) {
            sequence.append(nextMovieRating)
          } else {
            done = true
          }
        }
        noSequences = noSequences + 1
        if (!lines.hasNext) { //} || noSequences == 10) {
          nextMovieRating = null
        }
        sequence
      }
    }
  }

  /** Once sequence per user consisting of the original movie ids */
  def getMovieIdSequenceReader : SequenceReader = {
    val sequenceIt = MovieRating.getMovieRatingsByUserIterator
    val sequenceReader = new SequenceReader {
      override def usesFids(): Boolean = false

      override def close(): Unit = {}

      override def read(items: IntList): Boolean = {
        if (!sequenceIt.hasNext)
          return false

        items.clear
        for (movieRating <- sequenceIt.next) {
          items.add(movieRating.movieId) // gids correspond to movie id's here
        }
        true
      }
    }
    sequenceReader
  }

  /** Once sequence per user consisting of the id for the rated movie (need dictionary set to deep dictionary */
  def getRatedMovieSequenceReader : SequenceReader = {
    val sequenceIt = MovieRating.getMovieRatingsByUserIterator
    val sequenceReader = new SequenceReader {
      override def usesFids(): Boolean = false

      override def close(): Unit = {}

      override def read(items: IntList): Boolean = {
        if (!sequenceIt.hasNext)
          return false

        items.clear
        for (movieRating <- sequenceIt.next) {
          val movieSid = dict.getItemById(movieRating.movieId).sid
          val movieRatingSid = movieSid + "@" + movieRating.rating
          items.add(dict.getItemBySid(movieRatingSid).gid)
        }
        true
      }
    }
    sequenceReader
  }
}

/** Creates a flat dictionary where item.gid = original movie id */
object CreateFlatDictionary extends App {
  val outputFileGid = "data-local/netflix/flat-dict-gid.del"

  // read all movies and build a dictionary
  println("Reading movies...")
  val dict = new Dictionary
  for (movie <- Movie.readMovies) {
    dict.addItem(new Item(movie.id, movie.unratedSid))
  }

  // now scan the data and count
  println("Reading training set...")
  val sequenceReader = MovieRating.getMovieIdSequenceReader
  dict.clearCounts
  dict.incCounts(sequenceReader)

  // write the dictionary
  println("Writing dictionary...")
  DictionaryIO.saveToDel(new FileOutputStream(outputFileGid), dict, false, true)
}

/** Reencodes the training set using a flat dictionary (see above) and ignoring time stamps */
object CreateFlatData extends App {
  val dictFile = "data-local/netflix/flat-dict-gid.del"
  val outputFile = "data-local/netflix/flat-data-gid.del"

  val dict = DictionaryIO.loadFromDel(new FileInputStream(dictFile), true)
  val sequenceReader = MovieRating.getMovieIdSequenceReader
  val sequenceWriter = new DelSequenceWriter(new FileOutputStream(outputFile), false) // we read gids and write them as is
  sequenceWriter.setDictionary(dict)
  val items = new IntArrayList()
  while (sequenceReader.read(items)) {
    sequenceWriter.write(items)
  }
  sequenceReader.close
  sequenceWriter.close
}

/** Creates a deep dictionary where item.gid = original movie id plus items for rated movies, ratings, and years
  * are added */
object CreateDeepDictionary extends App {
  val outputFileGid = "data-local/netflix/deep-dict-gid.del"

  // read all movies and build a dictionary
  println("Reading movies...")
  var nextId = 0
  val dict = new Dictionary
  val yearStrings = new mutable.TreeSet[String].empty
  for (movie <- Movie.readMovies) {
    yearStrings.add(movie.yearString)
    dict.addItem(new Item(movie.id, movie.unratedSid))
    nextId = Math.max(nextId, movie.id)
  }
  nextId = nextId + 1

  // create items for ratings and years
  for (yearString <- yearStrings) {
    dict.addItem(new Item(nextId, yearString))
    nextId = nextId + 1
  }
  for (rating <- Range.inclusive(1,5)) {
    dict.addItem(new Item(nextId, rating + "stars"))
    nextId = nextId + 1
  }

  // read them again and add hierarchy items
  for (movie <- Movie.readMovies) {
    val movieItem = dict.getItemBySid(movie.unratedSid)
    val yearItem = dict.getItemBySid(movie.yearString)
    Item.addParent(movieItem, yearItem)

    for (rating <- Range.inclusive(1,5)) {
      val newItem = new Item(nextId, movie.ratedSid(rating))
      nextId = nextId + 1
      dict.addItem(newItem)
      Item.addParent(newItem, movieItem)
      Item.addParent(newItem, dict.getItemBySid(rating+"stars"))
    }
  }

  // now scan the data and count
  println("Reading training set...")
  val sequenceReader = MovieRating.getRatedMovieSequenceReader
  sequenceReader.setDictionary(dict)
  dict.clearCounts
  dict.incCounts(sequenceReader)

  // write the dictionary
  println("Writing dictionary...")
  DictionaryIO.saveToDel(new FileOutputStream(outputFileGid), dict, false, true)
}

/** Reencodes the training set using the deep dictionary (see above) and ignoring time stamps */
object CreateDeepData extends App {
  val dictFile = "data-local/netflix/deep-dict-gid.del"
  val outputFile = "data-local/netflix/deep-data-gid.del"

  val dict = DictionaryIO.loadFromDel(new FileInputStream(dictFile), true)
  val sequenceReader = MovieRating.getRatedMovieSequenceReader
  sequenceReader.setDictionary(dict)
  val sequenceWriter = new DelSequenceWriter(new FileOutputStream(outputFile), false) // we read gids and write them as is
  sequenceWriter.setDictionary(dict)
  val items = new IntArrayList()
  while (sequenceReader.read(items)) {
    sequenceWriter.write(items)
  }
  sequenceReader.close
  sequenceWriter.close
}

object RunAllConverters extends App {
  TrainingSetToCsv.main(Array.empty[String])
  CreateFlatDictionary.main(Array.empty[String])
  CreateFlatData.main(Array.empty[String])
  CreateDeepDictionary.main(Array.empty[String])
  CreateDeepData.main(Array.empty[String])
}