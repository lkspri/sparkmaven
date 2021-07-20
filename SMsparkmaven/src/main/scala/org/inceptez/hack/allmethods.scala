package org.inceptez.hack
import scala.util.matching.Regex

class allmethods extends Serializable {
  
  def remspecialchar(dfim:String):String={
    
    val dfstring = dfim.replaceAll("[^a-zA-Z_]+", " ")
    return dfstring
    //registerudf.select(regexp_replace(
      //    registerudf(NetworkName),"[0-9]|\\[|\\]|\\-|\\?|\\(|\\)|\\,|_|/","").as(NetworkName))
     
  }
  
}
