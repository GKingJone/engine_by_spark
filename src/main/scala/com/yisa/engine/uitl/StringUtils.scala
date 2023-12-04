package com.yisa.engine.uitl

object StringUtils {
  /**
    * 从拼接的字符串中提取字段
    *
    * @param str       字符串
    * @param delimiter 分隔符
    * @param field     字段
    * @return 字段值
    */
  def getFieldFromConcatString(str: String, delimiter: String, field: String): String = {
    val fields = str.split(delimiter)
    var result = "0"
    for (concatField <- fields) {
      if (concatField.split("=").length == 2) {
        val fieldName = concatField.split("=")(0)
        val fieldValue = concatField.split("=")(1)
        if (fieldName == field) {
          result = fieldValue
        }
      }
    }
    result
  }

    /**
      * 从拼接的字符串中给字段设置值
      *
      * @param str           字符串
      * @param delimiter     分隔符
      * @param field         字段名
      * @param newFieldValue 新的field值
      * @return 字段值
      */
    def setFieldInConcatString(str: String, delimiter: String, field: String, newFieldValue: String): String = {
      val fields = str.split(delimiter)

      val buffer = new StringBuffer("")
      for (item <- fields) {
        val fieldName = item.split("=")(0)
        if (fieldName == field) {
          val concatField = fieldName + "=" + newFieldValue
          buffer.append(concatField).append("|")
        } else {
          buffer.append(item).append("|")
        }
      }
      buffer.substring(0, buffer.length() - 1)
    }

 

}