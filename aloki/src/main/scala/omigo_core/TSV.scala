package omigo_core

class TSV(val header: String, val data: List[String]) {
  def getHeader(): String = {
    header
  }

  def getData(): List[String] = {
    data
  }
}
