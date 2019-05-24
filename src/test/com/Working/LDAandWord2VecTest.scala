package com.Working

import org.junit.{Assert, Test}
import org.jmock
import org.jmock.Mockery


/**
  * @author zhaoming on 2018-07-16 16:42
  **/
class LDAandWord2VecTest  extends  Assert{

//  @Test
//  def getVectorTest:Unit={
//    //LDAandWord2Vec.getVector()
//  }

  @Test
  def getDoubleTest:Unit={

//    val contextx = new Mockery
//    val ss = contextx.mock(TestVector.getClass)

    println(LDAandWord2Vec.getValue(100.0))
  }

}
