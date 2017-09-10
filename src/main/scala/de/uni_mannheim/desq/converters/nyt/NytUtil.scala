package de.uni_mannheim.desq.converters.nyt

import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.avro.{AvroArticle, Sentence, Token}
import de.uni_mannheim.desq.dictionary.DictionaryBuilder
import de.uni_mannheim.desq.io.spark.AvroIO
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._
import scala.collection.immutable.HashSet

/**
  * Created by ivo on 03.05.17.
  *
  * Utility Class to parse AvroArcticles containing NYT articles
  */
object NytUtil {

  def loadArticlesFromFile(rootDir: String)(implicit sc: SparkContext): RDD[AvroArticle] = {
    val articles = AvroIO.read[AvroArticle](rootDir, AvroArticle.SCHEMA$)
    articles
  }

  def parse = (sentence: Sentence, seqBuilder: DictionaryBuilder) => {
    val ENTITY = "ENTITY"
    val POS_VALUES = Array[String]("CC", "CD", "DT", "EX", "FW", "IN", "JJ", "JJR", "JJS", "LS", "MD", "NN", "NNS", "NNP", "NNPS",
      "PDT", "POS", "PRP", "PRP$", "RB", "RBR", "RBS", "RP", "SYM", "TO", "UH", "VB", "VBD", "VBG", "VBN", "VBP", "VBZ", "WDT", "WP", "WP$", "WRB")
    val POS_SET = HashSet(POS_VALUES.toList: _*)
    var token: Token = null
    var word = ""
    var ner = ""
    var lemma = ""
    var pos = ""
    seqBuilder.newSequence(1)
    val tokens = sentence.getTokens
    var i = 0
    while (i < tokens.size()) {
      breakable {
        token = tokens.get(i)
        word = token.getWord.toLowerCase
        ner = token.getNer
        lemma = token.getLemma
        pos = token.getPos
        if (ner.equals("PERSON") || ner.equals("LOCATION") || ner.equals("ORGANIZATION")) {
          var nerPlus = ner
          var wordPlus = word
          var j = i + 1
          breakable {
            while (j < tokens.size()) {
              token = tokens.get(j)
              ner = token.getNer
              word = token.getWord.toLowerCase
              if (!nerPlus.equals(ner)) {
                j += 1
                break
              }
              else {
                wordPlus = wordPlus + "_" + word
              }
              i = j
              j += 1
            }
          }
          // add wordPlus -> nePlus -> entity to hierarchy
          // 1) Add item to sequence
          wordPlus = wordPlus + "@" + nerPlus + "@" + ENTITY
          var apiResult = seqBuilder.appendItem(wordPlus)
          var itemFid = apiResult.getLeft
          var newItem = apiResult.getRight
          // 2) If its a new item, we add parents
          if (newItem) {
            nerPlus = nerPlus + "@" + ENTITY
            apiResult = seqBuilder.addParent(itemFid, nerPlus)
            itemFid = apiResult.getLeft
            newItem = apiResult.getRight
            // If we have not yet added this ner
            if (newItem) {
              seqBuilder.addParent(itemFid, ENTITY)
            }
          }
          i += 1
          break
        }

        // If the word is not a named entity (additionally ignore punctuation)
        if (POS_SET.contains(pos)) {
          pos = shortenPos(pos)
          // add word -> lemma -> pos to hierarchy
          // 1) Add item to sequence
          word = word + "@" + lemma + "@" + pos
          var apiResult = seqBuilder.appendItem(word)
          var itemFid = apiResult.getLeft
          var newItem = apiResult.getRight
          // 2) If its a new item, add parents
          if (newItem) {
            lemma = lemma + "@" + pos
            apiResult = seqBuilder.addParent(itemFid, lemma)
            itemFid = apiResult.getLeft
            newItem = apiResult.getRight
            if (newItem) {
              seqBuilder.addParent(itemFid, pos)
            }
          }
        }
        i += 1
      }

      def shortenPos(pos: String): String = {
        var result = pos;
        if (pos.length() > 2) {
          result = pos.substring(0, 2);
        }
        return result;
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local").remove("spark.serializer")
    initDesq(conf)
    implicit val sc = new SparkContext(conf)
    val dir = "data-local/NYTimesProcessed/results/2007/01/"
    loadArticlesFromFile(dir)
  }
}
