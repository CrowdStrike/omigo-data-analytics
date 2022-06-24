package omigo_core

class TSV(val header: String, val data: List[String]) {
  def get_header(): String = {
    header
  }

  def get_data(): List[String] = {
    data
  }

  def num_rows(): Int = {
    throw new Exception("TBD")
  } 
}

object TSV {
  def merge(tsvList: List[TSV], defValMap: Map[String, String]): TSV = {
    throw new Exception("TBD")
  }
}
