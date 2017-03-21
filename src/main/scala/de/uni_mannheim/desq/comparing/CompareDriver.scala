package de.uni_mannheim.desq.comparing

/**
  * Created by ivo on 16.03.17.
  */
object CompareDriver extends App{
//  val LeftCount = 1199126 // 2007 NYT Corpus
  val LeftCount = 2523804// 2005 NYT Corpus
  val RightCount = 2591030 // 2006 NYT Corpus
  val left = "data-local/processed/run2005/DesqDfs-tt--ENTITY-(VB+-NN+-_QIN_Q)-ENTITY--5"
  val right = "data-local/processed/run2006/DesqDfs-tt--ENTITY-(VB+-NN+-_QIN_Q)-ENTITY--5"
//  val right = "data-local/processed/run2007/DesqDfs-tt--ENTITY-(VB+-NN+-_QIN_Q)-ENTITY--5"
  val patternCompare = new PatternCompare(left, LeftCount, right, RightCount)
  patternCompare.compareBySupport(0.00001f, 0.00001f, true)

}
