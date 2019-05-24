package AllTest.BigDL

import com.intel.analytics.bigdl.dataset.DataSet
import com.intel.analytics.bigdl.dataset.image.{BytesToGreyImg, GreyImgNormalizer, GreyImgToBatch}
import com.intel.analytics.bigdl.models.lenet.LeNet5
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.utils.Engine
import AllTest.BigDL.Utils._
import com.intel.analytics.bigdl.nn.ClassNLLCriterion


/**
  * @author zhaoming on 2018-10-18 16:56
  **/
object BigDLLocalTest {

  def main(args: Array[String]): Unit = {
    System.setProperty("bigdl.localMode", "true")
    System.setProperty("bigdl.coreNumber", "2")
    Engine.init


    val trainData = "D:\\data\\minist\\MNIST\\train-images-idx3-ubyte"
    val trainLabel = "D:\\data\\minist\\MNIST\\train-labels-idx1-ubyte"
    val validationData = "D:\\data\\minist\\MNIST\\t10k-images-idx3-ubyte"
    val validationLabel = "D:\\data\\minist\\MNIST\\t10k-labels-idx1-ubyte"

    val model = LeNet5(classNum = 10)

    val optimMethod = new SGD[Float](learningRate = 0.5,
      learningRateDecay = 0.5)

    val trainSet = DataSet.array(load(trainData, trainLabel)) ->
      BytesToGreyImg(28, 28) -> GreyImgNormalizer(trainMean, trainStd) -> GreyImgToBatch(
      100)

    val optimizer = Optimizer(
      model = model,
      dataset = trainSet,
      criterion = ClassNLLCriterion[Float]())

    val validationSet = DataSet.array(load(validationData, validationLabel)) ->
      BytesToGreyImg(28, 28) -> GreyImgNormalizer(testMean, testStd) -> GreyImgToBatch(
      100)

    val sss = optimizer.setValidation(
      trigger = Trigger.everyEpoch,
      dataset = validationSet,
      vMethods = Array(new Top1Accuracy, new Top5Accuracy[Float], new Loss[Float]))
      .setOptimMethod(optimMethod)
      .setEndWhen(Trigger.maxEpoch(20))
      .optimize()

    println(sss)


  }

}
