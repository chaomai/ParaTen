# ParaTen

* This project implements the CP-ALS algorithm on Spark.
* The aim of this project is to utilize the power of RDD to achieve fast iteration while performing decomposition.
* This Project is also a exploration of Type-Safe tensor operation, which is heavily base on Type-Level programming in Scala.

## Build and Run

1. Environment

* sbt 0.13.15
* Scala 2.11.11
* Spark 2.1.1

2. Compile and package

```bash
sbt package
```

It will produce package `target/scala-2.11/paraten_2.11-1.0.jar`.

3. Usage

```bash

```

## References

* An Investigation of Sparse Tensor Formats for Tensor Libraries. Parker Allen Tew. 2015.
* HaTen2: Billion-scale Tensor Decompositions. Inah Jeon, Evangelos E. Papalexakis, U Kang, Christos Faloutsos. 31st IEEE International Conference on Data Engineering (ICDE) 2015, Seoul, Korea.
* Scalable Tensor Decompositions for Multi-aspect Data Mining. Tamara G. Kolda, Jimeng Sun. Data Mining, 2008. ICDM '08.
* Tensor Decompositions and Applications. Tamara G. Kolda, Brett W. Bader. SIAM Review Volume 51 Issue 3, August 2009.

## Thanks

* [yuyongyang800/SparkDistributedMatrix](https://github.com/yuyongyang800/SparkDistributedMatrix)
* [MrVPlusOne/TensorSafe](https://github.com/MrVPlusOne/TensorSafe)
* [Mega-DatA-Lab/SpectralLDA-Spark](https://github.com/Mega-DatA-Lab/SpectralLDA-Spark)
* [mnick/scikit-tensor](https://github.com/mnick/scikit-tensor)

## License
   
Apache License.
