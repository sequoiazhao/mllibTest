package com.deeplearning4j;

import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

import java.io.IOException;


public class Test {

     public static  void  main(String[] args) throws IOException {
         int nChannels = 1;      //black & white picture, 3 if color image
         int outputNum = 10;     //number of classification
         int batchSize = 64;     //mini batch size for sgd
         int nEpochs = 10;       //total rounds of training
         int iterations = 1;     //number of iteration in each traning round
         int seed = 123;         //random seed for initialize weights


         DataSetIterator mnistTrain = null;
         DataSetIterator mnistTest = null;

         //mnistTrain = new MnistDataSetIterator(batchSize, true, 12345);
       //  mnistTest = new MnistDataSetIterator(batchSize, false, 12345);
         mnistTrain = new MnistDataSetIterator(batchSize,true,12345);

     }
}
