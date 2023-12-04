package com.yisa.engine.common

case class InputBean(
//    jobId: String,
    startTime: String,
    endTime: String,
    locationId: Array[String],
    carModel: Array[String],
    carBrand: String,
    carYear: Array[String],
    carColor: String,
    plateNumber: String, 
    count: Int,
    feature:String,
    differ:Int,   // taskid
    isRepair:Int, // 否定条件
    carLevel:Array[String],
    direction:String
    
    )     