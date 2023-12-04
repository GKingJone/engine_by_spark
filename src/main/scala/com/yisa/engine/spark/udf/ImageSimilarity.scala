package com.yisa.engine.spark.udf

import java.util.Base64
 
class ImageSimilarity {
   def getSimilarity(text: String, test2: String): Long = {
    val byteData = Base64.getDecoder.decode(text)
    val oldData = Base64.getDecoder.decode(test2)
    var num: Long = 0
    for (i <- 0 until byteData.length if test2.length > 30) {
      var n = (byteData(i) & 0xff) - (oldData(i) & 0xff)
      num += n * n;
      if (num > 20000l) {
            return num
      }

    }
    return num
  }
} 