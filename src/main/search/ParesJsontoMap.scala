package search

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * @author zhaoming on 2018-06-06 15:17
  **/


object ParesJsontoMap {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)



    val str1 ="""{\"result_ids\":[{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":37.12917},\"category_id\":1028,\"objid\":11014616074,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":37.176617},\"category_id\":1028,\"objid\":11014342756,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":40.01795},\"category_id\":1028,\"objid\":11014141809,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":33.59832},\"category_id\":1028,\"objid\":11015238048,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.66695},\"category_id\":1028,\"objid\":11014445495,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.712013},\"category_id\":1028,\"objid\":11012834417,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.712013},\"category_id\":1028,\"objid\":11014340840,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":35.941055},\"category_id\":1028,\"objid\":11014422053,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":37.176617},\"category_id\":1028,\"objid\":11012376022,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":35.941055},\"category_id\":1028,\"objid\":11014549926,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":37.12917},\"category_id\":1028,\"objid\":11014320153,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.712013},\"category_id\":1028,\"objid\":11015282009,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":37.217037},\"category_id\":1028,\"objid\":11012614035,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":33.59832},\"category_id\":1028,\"objid\":11014299255,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":37.217037},\"category_id\":1028,\"objid\":11012233083,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":37.12917},\"category_id\":1028,\"objid\":11014388051,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.712013},\"category_id\":1028,\"objid\":11015226591,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":33.55437},\"category_id\":1028,\"objid\":11012712413,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.74966},\"category_id\":1028,\"objid\":11014195883,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":37.12917},\"category_id\":1028,\"objid\":11012563317,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":31.606539},\"category_id\":1028,\"objid\":11014572965,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":33.634724},\"category_id\":1028,\"objid\":11014191848,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.712013},\"category_id\":1028,\"objid\":11014130855,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.66695},\"category_id\":1028,\"objid\":11014596369,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":30.681486},\"category_id\":1028,\"objid\":11014489389,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.66695},\"category_id\":1028,\"objid\":11014212359,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":35.902065},\"category_id\":1028,\"objid\":11014210096,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":35.902065},\"category_id\":1028,\"objid\":11013533086,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":33.59832},\"category_id\":1028,\"objid\":11014256341,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":30.648378},\"category_id\":1028,\"objid\":11012863439,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":30.648378},\"category_id\":1028,\"objid\":11014224888,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.510986},\"category_id\":1028,\"objid\":11014499495,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.66695},\"category_id\":1028,\"objid\":11012454075,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.74966},\"category_id\":1028,\"objid\":11012364043,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.589104},\"category_id\":1028,\"objid\":11015298973,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.55387},\"category_id\":1028,\"objid\":11014512061,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.66695},\"category_id\":1028,\"objid\":11014078714,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.589104},\"category_id\":1028,\"objid\":11013962160,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":30.607483},\"category_id\":1028,\"objid\":11014342683,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":31.530529},\"category_id\":1028,\"objid\":11014350915,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.66695},\"category_id\":1028,\"objid\":11013776958,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.589104},\"category_id\":1028,\"objid\":11014245055,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":31.572401},\"category_id\":1028,\"objid\":11012995474,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":29.776901},\"category_id\":1028,\"objid\":11012230501,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":30.607483},\"category_id\":1028,\"objid\":11012642670,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":29.73694},\"category_id\":1028,\"objid\":11014618669,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.55387},\"category_id\":1028,\"objid\":11014543221,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":30.648378},\"category_id\":1028,\"objid\":11014626531,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":33.55437},\"category_id\":1028,\"objid\":11013488210,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":31.530529},\"category_id\":1028,\"objid\":11014445749,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":30.607483},\"category_id\":1028,\"objid\":11012243181,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.712013},\"category_id\":1028,\"objid\":11014451746,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.66695},\"category_id\":1028,\"objid\":11014165176,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":29.73694},\"category_id\":1028,\"objid\":11014445970,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":34.74966},\"category_id\":1028,\"objid\":11014078731,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":29.776901},\"category_id\":1028,\"objid\":11014313137,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.510986},\"category_id\":1028,\"objid\":11014130596,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.510986},\"category_id\":1028,\"objid\":11014341024,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":32.55387},\"category_id\":1028,\"objid\":11013090607,\"objtype\":1},{\"obj_child_type\":0,\"ltr_logs\":{\"objname_bm25\":29.776901},\"category_id\":1028,\"objid\":11013561631,\"objtype\":1}]}"""

    val str2 ="""{"result_ids":[{"obj_child_type":0,"category_id":1005,"objid":2220938,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11012360094,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11014024292,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11014068365,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11014068573,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11012567631,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11012361122,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11013857790,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11013672848,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11014533256,"objtype":1},{"obj_child_type":0,"category_id":1028,"objid":11013857747,"objtype":1}]} """

    //val position1 = "11014245055"//, 41)

    val position1 = "2220938"

//    val tempjson = MyContext.read.json(str1)
//    tempjson.show(false)


    val childTypeRegex = """("obj_child_type":)\d+(\.\d+)?""".r
    val categoryIdRegex = """("category_id":)\d+(\.\d+)?""".r
    val objIdRegex = """("objid":)\d+(\.\d+)?""".r
    val objTypeRegex = """("objtype":)\d+(\.\d+)?""".r
    val bm25Regex = """("objname_bm25":)\d+(\.\d+)?""".r

    val str1Array = str2.replaceAll("""\\""", "").replace("},{", "}#{").split("#")

    val sss = str1Array.map { x =>
      var childType = childTypeRegex.findAllMatchIn(x).toList.toString.replaceAll("List", "")
        .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\"", "").split(":").filter(x => x.nonEmpty)
      var categoryId = categoryIdRegex.findAllMatchIn(x).toList.toString.replaceAll("List", "")
        .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\"", "").split(":").filter(x => x.nonEmpty)
      var objId = objIdRegex.findAllMatchIn(x).toList.toString.replaceAll("List", "")
        .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\"", "").split(":").filter(x => x.nonEmpty)
      var objType = objTypeRegex.findAllMatchIn(x).toList.toString.replaceAll("List", "")
        .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\"", "").split(":").filter(x => x.nonEmpty)
      var bm25 = bm25Regex.findAllMatchIn(x).toList.toString.replaceAll("List", "")
        .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\"", "").split(":").filter(x => x.nonEmpty)
      if (bm25.isEmpty) {
        bm25 = Array("objname_bm25", "0.0")
      }

      if (objId.apply(1)==position1){
        (Map("obj_child_type" -> childType.apply(1)
          , "category_id" -> categoryId.apply(1)
          , "objid" -> objId.apply(1)
          , "objtype" -> objType.apply(1)
          , "objname_bm25" -> bm25.apply(1)),1)
      }else{
        (Map("obj_child_type" -> childType.apply(1)
          , "category_id" -> categoryId.apply(1)
          , "objid" -> objId.apply(1)
          , "objtype" -> objType.apply(1)
          , "objname_bm25" -> bm25.apply(1)),0)
      }

    }
    sss.foreach(println)
    sss
    //println(sss(41)._1.get("objid"))



    //    val sss = str1Array.map { x =>
    //      val bm25 = bm25Regex.findAllMatchIn(x)
    //        //.toList.toString
    //        //.replaceAll("List", "")
    //       // .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\"", "").split(":")
    //      bm25.foreach(println)
    //
    //    }
    //    sss.foreach(println)

    //用数组
    //    val featureArray = Array("obj_child_type", "category_id", "objid", "objtype")
    //
    //
    //    val str1Array = str2.replaceAll("""\\""", "").replace("},{", "}#{").split("#")
    //
    //    val sss = str1Array.map { x =>
    //      val repPrase = featureArray.map { y =>
    //        val regexTemp = ("""("""" + y +"""":)\d+(\.\d+)?""").r
    //        val res = regexTemp.findAllMatchIn(x).toList.toString.replaceAll("List", "")
    //          .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\"", "").split(":")
    //        Map(res.apply(0) -> res.apply(1))
    //      }
    //
    //      if (repPrase.length==4){
    //        (repPrase(0), repPrase(1),repPrase(2),repPrase(3))
    //      }else{
    //        (repPrase(0), repPrase(1),repPrase(2),repPrase(3),repPrase(4))
    //      }
    //    }
    //
    //    sss.foreach(println)
    //    val ttt = "objname_bm25:34.712013"
    //
    //    val spp = ttt.split(":")
    //    val spk = Map(spp.apply(0) -> spp.apply(1))
    //
    //    spk.foreach(println)

    //    val res = filterRegexDouble.findAllMatchIn(str1.replaceAll("""\\""","")).toList
    //    res.foreach(println)
    //
    //    val regex ="""\\\"12\\\"""".r
    //    regex.findAllMatchIn(strs).foreach(println)

  }

}
