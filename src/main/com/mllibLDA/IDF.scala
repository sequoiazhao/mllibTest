package com.mllibLDA

/**
  * @author zhaoming on 2017-12-06 23:29
  **/

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors, _}
import org.apache.spark.rdd.RDD

/**
  * Inverse document frequency (IDF).
  * The standard formulation is used: `idf = log((m + 1) / (d(t) + 1))`, where `m` is the total
  * number of documents and `d(t)` is the number of documents that contain term `t`.
  *
  * This implementation supports filtering out terms which do not appear in a minimum number
  * of documents (controlled by the variable `minDocFreq`). For terms that are not in
  * at least `minDocFreq` documents, the IDF is found as 0, resulting in TF-IDFs of 0.
  *
  * @param minDocFreq minimum of documents in which a term
  *                   should appear for filtering
  */
class IDF(val minDocFreq: Int) {

  def this() = this(0)

  // TODO: Allow different IDF formulations.

  /**
    * Computes the inverse document frequency.
    *
    * @param dataset an RDD of term frequency vectors
    */
  def fit(dataset: RDD[Vector]): IDFModel = {
    val idf = dataset.treeAggregate(new IDF.DocumentFrequencyAggregator(
      minDocFreq = minDocFreq))(
      seqOp = (df, v) => df.add(v),
      combOp = (df1, df2) => df1.merge(df2)
    ).idf()
    new IDFModel(idf)
  }

  /**
    * Computes the inverse document frequency.
    *
    * @param dataset a JavaRDD of term frequency vectors
    */
  def fit(dataset: JavaRDD[Vector]): IDFModel = {
    fit(dataset.rdd)
  }
}

private object IDF {

  /** Document frequency aggregator. */
  class DocumentFrequencyAggregator(val minDocFreq: Int) extends Serializable {

    /** number of documents */
    private var m = 0L
    /** document frequency vector */
    private var df: BDV[Long] = _


    def this() = this(0)

    /** Adds a new document. */
    def add(doc: Vector): this.type = {
      if (isEmpty) {
        df = BDV.zeros(doc.size)
      }
      doc match {
        case SparseVector(size, indices, values) =>
          val nnz = indices.length
          var k = 0
          while (k < nnz) {
            if (values(k) > 0) {
              df(indices(k)) += 1L
            }
            k += 1
          }
        case DenseVector(values) =>
          val n = values.length
          var j = 0
          while (j < n) {
            if (values(j) > 0.0) {
              df(j) += 1L
            }
            j += 1
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
      m += 1L
      this
    }

    /** Merges another. */
    def merge(other: DocumentFrequencyAggregator): this.type = {
      if (!other.isEmpty) {
        m += other.m
        if (df == null) {
          df = other.df.copy
        } else {
          df += other.df
        }
      }
      this
    }

    private def isEmpty: Boolean = m == 0L

    /** Returns the current IDF vector. */
    def idf(): Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document yet.")
      }
      val n = df.length
      val inv = new Array[Double](n)
      var j = 0
      while (j < n) {
        /*
         * If the term is not present in the minimum
         * number of documents, set IDF to 0. This
         * will cause multiplication in IDFModel to
         * set TF-IDF to 0.
         *
         * Since arrays are initialized to 0 by default,
         * we just omit changing those entries.
         */
        if (df(j) >= minDocFreq) {
          inv(j) = math.log((m + 1.0) / (df(j) + 1.0))
        }
        j += 1
      }
      Vectors.dense(inv)
    }
  }

}

/**
  * Represents an IDF model that can transform term frequency vectors.
  */
class IDFModel(val idf: Vector) extends Serializable {

  /**
    * Transforms term frequency (TF) vectors to TF-IDF vectors.
    *
    * If `minDocFreq` was set for the IDF calculation,
    * the terms which occur in fewer than `minDocFreq`
    * documents will have an entry of 0.
    *
    * @param dataset an RDD of term frequency vectors
    * @return an RDD of TF-IDF vectors
    */
  def transform(dataset: RDD[Vector]): RDD[Vector] = {
    val bcIdf = dataset.context.broadcast(idf)
    dataset.mapPartitions(iter => iter.map(v => IDFModel.transform(bcIdf.value, v)))
  }

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF vector
    *
    * @param v a term frequency vector
    * @return a TF-IDF vector
    */
  def transform(v: Vector): Vector = IDFModel.transform(idf, v)

  /**
    * Transforms term frequency (TF) vectors to TF-IDF vectors (Java version).
    *
    * @param dataset a JavaRDD of term frequency vectors
    * @return a JavaRDD of TF-IDF vectors
    */
  def transform(dataset: JavaRDD[Vector]): JavaRDD[Vector] = {
    transform(dataset.rdd).toJavaRDD()
  }
}

private object IDFModel {

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF vector with a IDF vector
    *
    * @param idf an IDF vector
    * @param v   a term frequence vector
    * @return a TF-IDF vector
    */
  def transform(idf: Vector, v: Vector): Vector = {
    val n = v.size
    v match {
      case SparseVector(size, indices, values) =>
        val nnz = indices.length
        val newValues = new Array[Double](nnz)
        var k = 0
        while (k < nnz) {
         // println(idf.size, indices.length)
          newValues(k) = values(k) * idf(indices(k))
          k += 1
        }
        Vectors.sparse(n, indices, newValues)
      case DenseVector(values) =>
        val newValues = new Array[Double](n)
        var j = 0
        while (j < n) {
          newValues(j) = values(j) * idf(j)
          j += 1
        }
        Vectors.dense(newValues)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}

