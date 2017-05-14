import breeze.linalg.{DenseMatrix, DenseVector, VectorBuilder, isClose, norm}


class A(val value: Int) {
  def +(b: Int): A = new A(value + b)
}

object A {
  def >(a: Int): A = new A(a)
}

(A > 1 + 2 + 3).value

trait T

trait T1 extends T

trait T2 extends T

trait T3 extends T

trait Val[D] {
  def value: Int
}

object Val {
  def con[D](v: Int) = new Val[D] {
    override def value: Int = v
  }
}

val v1 = Val.con[T1](1)
val v2 = Val.con[T2](2)
val v3 = Val.con[T3](3)

class B[S](val value: Int) {
  def ^[D](b: Val[D]): B[(S, D)] = new B[(S, D)](value + b.value)
}

object B {
  def >[D](a: Val[D]): B[D] = new B[D](a.value)
}

(B > v1).value
((B > v1) ^ v2).value
(B > v1 ^ v2).value

List(1, 2, 3, 4, 5)

//-------------------------------

val v = DenseVector.zeros[Double](5)
v.update(3, 10)
v.foreach(println)

//-------------------------------

var a: List[Class[_]] = List(classOf[T1], classOf[T2], classOf[T3])

a
  .zip(List(1, 2, 3))
  .map(p => Val.con[p._1.type](p._2))

//-------------------------------

val shape = IndexedSeq(1, 2, 3, 4)
shape.updated(2, 10)
shape

//-------------------------------

val vec1 = IndexedSeq((0, 1), (1, 2), (2, 3))
val vec2 = IndexedSeq((0, 1), (2, 3))

val aligned = for {
  a <- vec1
  b <- vec2
  if a._1 == b._1
} yield (a, b)

//-------------------------------

IndexedSeq(2, 4, 5, 6, 1, 2, 3, 8, 6).sortBy(e => e)

//-------------------------------

val m = DenseMatrix.zeros[Int](0, 0)
//m.t

//-------------------------------

val s = IndexedSeq((1, 3), (4, 2), (6, 1), (1, 8), (5, 9))
s.maxBy(e => e._1)
s.maxBy(e => e._2)

//-------------------------------

val seq = DenseVector(2, 4, 5, 6, 0, 2, 0, 8, 6)
val normVal = norm(seq)
seq.foldLeft(0)((acc, e) => acc + e * e)
seq.map(_ / normVal)

//-------------------------------

val builder = new VectorBuilder[Int](6)
builder.add(2, 4)
builder.add(5, 2)
val sv = builder.toSparseVector
sv.mapPairs((i, v) => (i, v)).foldLeft("")((acc, p) => acc + ", " + p)

//-------------------------------

5L > Int.MaxValue

//-------------------------------

DenseVector(2, 4, 5) * DenseVector(1, 2).t
DenseVector(2, 4, 5).t * DenseVector(1, 2, 3)

//-------------------------------

trait CanUse[T]

object CanUse {

  implicit object canUseInt extends CanUse[Int]

  implicit object canUseString extends CanUse[String]

  // etc.
}

def myFunction[T: CanUse](x: T) = {
  ???
}

//myFunction(1.2)

//-------------------------------

isClose(DenseVector[Double](1, 2, 3, 4), DenseVector[Double](1, 2, 3, 5))
