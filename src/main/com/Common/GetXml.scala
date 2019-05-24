package com.Common

import java.io.InputStream

import scala.xml.{Elem, XML}

/**
  * @author zhaoming on 2018-08-01 17:04
  **/
object GetXml {

  def getXml(configName: String = "test.xml"): Elem = {
    val inputStream: InputStream = this.getClass.getClassLoader
      .getResourceAsStream(configName)
    XML.load(inputStream)
  }

  def parseXml(someXml: Elem, property: String, name: String, value: String): Map[String, String] = {
    var mapXml: Map[String, String] = Map()
    val getFields = someXml \ property

    getFields.foreach { item =>
      val getName = item \ name
      val getValue = item \ value
      mapXml += (getName.text -> getValue.text)
    }
    mapXml
  }

  val regexMap: Map[String, String] = GetXml.parseXml(
    GetXml.getXml()
    , "property"
    , "name"
    , "value")


}
