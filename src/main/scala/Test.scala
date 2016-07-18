import de.uni_mannheim.desq.dictionary.DictionaryIO

object Test {
  def main(args: Array[String]): Unit = {
    val dictFile = getClass.getResource("/icdm16-example/dict.del")
    val dict = DictionaryIO.loadFromDel(dictFile.openStream, false)
    println("All items: " + dict.allItems)
  }
}
