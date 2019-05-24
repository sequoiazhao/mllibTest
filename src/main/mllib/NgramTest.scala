
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author zhaoming on 2018-10-24 10:57
  **/
object NgramTest {
  def main(args: Array[String]): Unit = {
    //environment
    val conf = new SparkConf()
      .setAppName("LDATest")
      .setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    //    val wordDataFrame = sqlContext.createDataFrame(Seq(
    //      (0, Array("Hi", "I", "heard", "about", "Spark")),
    //      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
    //      (2, Array("Logistic", "regression", "models", "are", "neat"))
    //    )).toDF("label", "words")
    //
    //    wordDataFrame.show(false)
    //
    //    val ngram = new NGram()
    //      .setInputCol("words")
    //      .setOutputCol("ngrams")
    //      .setN(4)
    //    val ngramDataFrame = ngram.transform(wordDataFrame)
    //    ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
    //  }

    //    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    //    val dataFrame: DataFrame = sqlContext.createDataFrame(data).toDF("label", "feature")
    //    val binarizer: Binarizer = new Binarizer()
    //      .setInputCol("feature")
    //      .setOutputCol("binarized_feature")
    //      .setThreshold(0.5)
    //
    //    val binarizedDataFrame = binarizer.transform(dataFrame)
    //    binarizedDataFrame.select("binarized_feature")
    //      .collect().foreach(println)


    //    val data = Array(
    //      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
    //      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
    //      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    //    )
    //    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //    val pca = new PCA()
    //      .setInputCol("features")
    //      .setOutputCol("pcaFeatures")
    //      .setK(3)
    //      .fit(df)
    //
    //
    //    val pcaDF = pca.transform(df)
    //    val result = pcaDF.show(false)


    //    val datasetOr = sqlContext.createDataFrame(
    //      Seq((0, 18, "1.0", 1.0))
    //    ).toDF("id", "hour", "mobile", "userFeatures")
    //
    //    val dataset = datasetOr.select(
    //      col("id")
    //      , col("hour")
    //      , col("mobile").cast(DoubleType)
    //      , col("userFeatures"))
    //
    //    val assembler = new VectorAssembler()
    //      .setInputCols(Array("id", "hour", "mobile", "userFeatures"))
    //      .setOutputCol("features")
    //
    //    val output = assembler.transform(dataset)
    //    output.show(false)
    //    //println(output.select("features").first())
    //
    //    output.printSchema()
    //
    //    val vectorToColumn = udf((x: DenseVector) => x.toArray)
    //
    //    //    val splitFeature:UserDefinedFunction = udf((tag:Array[Double]) =>{
    //    //       tag.toString
    //    //    })
    //
    //    output.withColumn("n1", vectorToColumn(col("features"))).
    //      select(col("n1").getItem(0)
    //        , col("n1").getItem(1)).show(false)

    //
    //    val data = Array(
    //
    //      Vectors.dense(0.40, 8.07)
    //    )
    //    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //    df.printSchema()
    //    df.show(false)
    //
    //    df.select("features").rdd.map { x => x.get(0).toString.replaceAll("\\[|\\]", "").split(",").length }.foreach(println)
    //
    //
    //    val polynomialExpansion = new PolynomialExpansion()
    //      .setInputCol("features")
    //      .setOutputCol("polyFeatures")
    //      .setDegree(2)
    //
    //    val polyDF = polynomialExpansion.transform(df)
    //
    //    polyDF.printSchema()
    //
    //    polyDF.select("polyFeatures").rdd.take(3).foreach(println)

    //polyDF.show(10, false)

    //    polyDF.select("polyFeatures").rdd.map{ x=>x.get(0).asInstanceOf[DenseVector].toArray.length}.foreach(println)
    //    polyDF.select("polyFeatures").rdd.map { x => x.get(0).toString.replaceAll("\\[|\\]", "").split(",").length }.foreach(println)
    //
    //    val vectorToColumn = udf((x: DenseVector) => x.toArray)
    //
    //    val pp = polyDF.withColumn("n1", vectorToColumn(col("polyFeatures")))
    //    pp.printSchema()
    //    polyDF.select("polyFeatures").take(3).foreach(println)


    //        val data = Seq(
    //          Vectors.dense(0.0, 1.0, -2.0, 3.0),
    //          Vectors.dense(-1.0, 2.0, 4.0, -7.0),
    //          Vectors.dense(14.0, -2.0, -5.0, 1.0))
    //
    //        val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //
    //        val dct = new DCT()
    //          .setInputCol("features")
    //          .setOutputCol("featuresDCT")
    //          .setInverse(false)
    //
    //        val dctDf = dct.transform(df)
    //        dctDf.select("featuresDCT").show(3,false)


//    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)
    //    val data2 = Array(-0.5, -0.3, 0.0, 0.2)
    //    val dataFrame = sqlContext.createDataFrame(data2.map(Tuple1.apply)).toDF("features")
    //
    //    dataFrame.show()
    //
    //    val bucketizer = new Bucketizer()
    //      .setInputCol("features")
    //      .setOutputCol("bucketed")
    //      .setSplits(splits)
    //
    //    val buck = bucketizer.transform(dataFrame)
    //
    //    buck.show()

//    val dataFrame = sqlContext.read.format("libsvm").load("d:\\data\\sample_libsvm_data.txt")
    //
    //    dataFrame.show(false)
    //
    //    // Normalize each Vector using $L^1$ norm.
    //    val normalizer = new Normalizer()
    //      .setInputCol("features")
    //      .setOutputCol("normFeatures")
    //      .setP(1.0)
    //
    //    val l1NormData = normalizer.transform(dataFrame)
    //    l1NormData.show(false)
    //
    //    // Normalize each Vector using $L^\infty$ norm.
    //    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    //    lInfNormData.show(false)

    val dataFrame = sqlContext.read.format("libsvm").load("d:\\data\\sample_libsvm_data.txt")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    //scaledData.show(false)

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val bucketizer = new Bucketizer()
      .setInputCol("label")
      .setOutputCol("newlabel")
      .setSplits(splits)

    val budata = bucketizer.transform(scaledData)

    budata.show()

    val discretizer = new QuantileDiscretizer()
      .setInputCol("label")
      .setOutputCol("newlabel")
      .setNumBuckets(5)

    val result = discretizer.fit(scaledData).transform(scaledData)

    result.show()


    val data = Array(Row(Vectors.dense(-2.0, 2.3, 0.0)))

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataRDD = sc.parallelize(data)

    val dataset = sqlContext.createDataFrame(dataRDD, StructType(Array(attrGroup.toStructField())))

    dataset.show(false)
    dataset.printSchema()

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))

    val out = slicer.transform(dataset)
    out.show(false)


    val datasetT = sqlContext.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")

    val formula = new RFormula()

        .setFormula("clicked ~ country +hour")
        .setFeaturesCol("features")
        .setLabelCol("label")

    val outT = formula.fit(datasetT).transform(datasetT)
    outT.show(false)

    datasetT.show(false)


    val df = sqlContext.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")



    val sqlTrans =new SQLTransformer().setStatement(
      "SELECT * , (v1+v2) AS v3,(v1*v2) AS v4 FROM __THIS__"
    )

    sqlTrans.transform(df).show(false)

    //    val dataFramex = sqlContext.createDataFrame(Seq(
//      ("a", Vectors.dense(1.0, 2.0, 3.0)),
//      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")
//
//    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
//    val transformer = new ElementwiseProduct()
//      .setScalingVec(transformingVector)
//      .setInputCol("vector")
//      .setOutputCol("transformedVector")

//    // Batch transform the vectors to create new column:
//    transformer.transform(dataFramex).show()


    //    val indexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexed")
//      .setMaxCategories(5)
//
//    val indexerModel = indexer.fit(dataFrame)
//
//    val indexData = indexerModel.transform(dataFrame)
//
//    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
//    println(s"Chose ${categoricalFeatures.size} categorical features: " +
//      categoricalFeatures.mkString(", "))
//
//
//    indexData.show(false)
  }



}
