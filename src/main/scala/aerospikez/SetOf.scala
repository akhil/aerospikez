package aerospikez

import com.aerospike.client.listener._
import com.aerospike.client.query.{ IndexType, RecordSet, ResultSet, Filter ⇒ AFilter }
import com.aerospike.client.{ AerospikeException, Record, Host, Key, Bin ⇒ ABin }
import com.aerospike.client.policy.{ QueryPolicy, WritePolicy, Policy }
import com.aerospike.client.task.{ IndexTask, ExecuteTask }
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.util.Util
import com.aerospike.client.Record

import com.aerospike.client.query.{ Statement, Filter ⇒ AFilter }
import com.aerospike.client.Value

import scala.collection.mutable.{ Map ⇒ _ }
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import scalaz.stream.{ Process, Cause, io }
import scalaz.concurrent.Task
import scalaz.{ -\/, \/- }

import internal.util.TSafe._
import internal.util.Pimp._
import internal.util.Util._
import Operations._

private[aerospikez] class SetOf[@specialized(Int, Long) SetV](
    namespace: Namespace,
    setName: String,
    client: AsyncClient,
    generalPolicy: Policy) {

  private final val queryPolicy = namespace.queryConfig.policy
  private final val writePolicy = namespace.writeConfig.policy
  private final val batchPolicy = namespace.batchConfig.policy

  private def createStmt(filter: AFilter): Statement = {

    val stmt: Statement = new Statement()
    stmt.setNamespace(namespace.name)
    stmt.setSetName(setName)
    stmt.setFilters(filter)
    stmt
  }

  private def parseKey[K](key: K): Key = {

    key match {
      case key: Int         ⇒ new Key(namespace.name, setName, key)
      case key: Long        ⇒ new Key(namespace.name, setName, key)
      case key: String      ⇒ new Key(namespace.name, setName, key)
      case key: Array[Byte] ⇒ new Key(namespace.name, setName, key)
    }
  }

  private def parseValue[V](value: V): Value = {

    value match {
      case value: Int         ⇒ new Value.IntegerValue(value)
      case value: String      ⇒ new Value.StringValue(value)
      case value: Array[Byte] ⇒ new Value.BytesValue(value)
      case value: Long        ⇒ new Value.LongValue(value)
      case value: List[_]     ⇒ new Value.ListValue(value)
      case value: Map[_, _]   ⇒ new Value.MapValue(value)
      case None | null        ⇒ new Value.NullValue()
      case v                  ⇒ new Value.BlobValue(v)
    }
  }

  def put[K: KRestriction, V: VRestriction](key: K, value: V, bin: String = "")(
    implicit ev: V TypeOf SetV): Task[Unit] = {

    println(" 3==> " + writePolicy.sendKey)

    Task.async { register ⇒
      client.put(
        writePolicy,
        new WriteListener {
          def onSuccess(key: Key): Unit = register(\/-(()))
          def onFailure(ae: AerospikeException): Unit = register(-\/(ae))
        },
        parseKey[K](key),
        new ABin(bin, parseOption[V](value))
      )
    }
  }

  def put[K: KRestriction, V](key: K, bins: Tuple2[String, V]*)(
    implicit ev: V TypeOf SetV): Task[Unit] = {

    println(" 2==> " + writePolicy.sendKey)

    Task.async { register ⇒
      client.put(
        writePolicy,
        new WriteListener {
          def onSuccess(key: Key): Unit = register(\/-(()))
          def onFailure(ae: AerospikeException): Unit = register(-\/(ae))
        },
        parseKey[K](key),
        bins.map(t ⇒ new ABin(t._1, t._2)): _*
      )
    }
  }

  def get[K: KRestriction, V](key: K, bins: Array[String])(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V]): Task[Map[String, V]] = {

    Task.async { register ⇒
      client.get(
        queryPolicy,
        new RecordListener {
          def onSuccess(key: Key, record: Record): Unit = {
            lazy val _bin = record.bins
            register(\/-(
              if (record != null && _bin != null)
                _bin.toMapWithNotNull.asInstanceOf[Map[String, V]]
              else
                Map.empty[String, V]
            ))
          }
          def onFailure(ae: AerospikeException): Unit = register(-\/(ae))
        },
        parseKey[K](key),
        bins: _*
      )
    }
  }

  def get[K: KRestriction, V](key: K)(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V]): Task[Option[V]] = get[K, V](key, "")

  def get[K: KRestriction, V](key: K, bin: String)(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V], ctx: distinct4.type): Task[Option[V]] = {

    Task.async { register ⇒
      client.get(
        queryPolicy,
        new RecordListener {
          def onSuccess(key: Key, record: Record): Unit = {
            register(\/-(Some(record).
              filter(_ != null).
              map(_.getValue(bin)).
              filter(_ != null).
              asInstanceOf[Option[V]]
            ))
          }
          def onFailure(ae: AerospikeException): Unit = register(-\/(ae))
        },
        parseKey[K](key)
      )
    }
  }

  def get[K: KRestriction, V](keys: Array[K])(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V], ctx: distinct1.type): Task[Map[K, V]] = get[K, V](keys, "")

  def get[K: KRestriction, V](keys: Array[K], bin: String)(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V], ctx: distinct1.type): Task[Map[K, V]] = {

    Task.async { register ⇒
      client.get(
        queryPolicy,
        new RecordArrayListener {
          def onSuccess(keys: Array[Key], records: Array[Record]): Unit = {
            lazy val length = keys.length
            @annotation.tailrec def go(m: Map[K, V], i: Int): Map[K, V] = {
              lazy val _bin = rec.bins
              lazy val rec = records(i)
              lazy val userValue = _bin.get(bin)
              i match {
                case `length` ⇒ m
                case _ ⇒
                  if (rec != null && _bin != null && userValue != null)
                    go(m + (keys(i).userKey.asInstanceOf[K] -> userValue.asInstanceOf[V]), i + 1)
                  else
                    go(m, i + 1)
              }
            }
            register(\/-(go(Map.empty[K, V], 0)))
          }
          def onFailure(ae: AerospikeException): Unit = register(-\/(ae))
        },
        keys.map(parseKey[K](_)),
        Seq(bin): _*
      )
    }
  }

  def getBatch[K: KRestriction, V](keys: Array[K], bin: String)(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V], ctx: distinct1.type): Task[Map[K, V]] = {
    Task.async { register ⇒
      client.get(
        batchPolicy,
        new RecordSequenceListener {
          var m: Map[K, V] = Map.empty[K,V]
          var idx = 0
          override def onRecord(key: Key, record: Record) = {
            idx += 1
            println(key)
            println()
            lazy val length = keys.length

            lazy val _bin = rec.bins
            lazy val rec = record
            lazy val userValue = _bin.get(bin)

            if (rec != null && _bin != null && userValue != null)
              m += (key.userKey.asInstanceOf[K] -> userValue.asInstanceOf[V])

            if(idx == length) register(\/-(m))

          }
          override def onSuccess(): Unit = {

          }
          override def onFailure(ae: AerospikeException): Unit = register(-\/(ae))
        },
        keys.map(parseKey[K](_)),
        Seq(bin): _*
      )
    }
  }

  def get[K: KRestriction, V](keys: Array[K], bins: Array[String])(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V], ctx: distinct3.type): Task[Map[K, Map[String, V]]] = {

    Task.async { register ⇒
      client.get(
        queryPolicy,
        new RecordArrayListener {
          def onSuccess(keys: Array[Key], records: Array[Record]): Unit = {
            lazy val length = keys.length

            @annotation.tailrec def go(m: Map[K, Map[String, V]], i: Int): Map[K, Map[String, V]] = {
              lazy val _key = keys(i)
              lazy val rec = records(i)
              lazy val _bin = rec.bins
              i match {
                case `length` ⇒ m
                case _ ⇒
                  if (_key != null && (rec != null && _bin != null))
                    go(m + (_key.userKey.asInstanceOf[K] -> _bin.toMapWithNotNull.asInstanceOf[Map[String, V]]), i + 1)
                  else
                    go(m, i + 1)
              }
            }
            register(\/-(go(Map.empty[K, Map[String, V]], 0)))
          }
          def onFailure(ae: AerospikeException): Unit = {
            register(-\/(ae))
          }
        },
        keys.map(parseKey[K](_)),
        bins: _*
      )
    }
  }

  def operate[K: KRestriction, V](key: K, operations: Ops*)(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V]): Task[Option[V]] = {

    println(" 1==> " + writePolicy.sendKey)

    operations.last match {
      case _: touch | _: append | _: put[V] | _: prepend | _: add ⇒
        Task.async { register ⇒
          client.operate(
            writePolicy,
            new RecordListener {
              def onSuccess(key: Key, record: Record): Unit =
                register(\/-(Some(().asInstanceOf[V])))
              def onFailure(ae: AerospikeException): Unit =
                register(-\/(ae))
            },
            parseKey[K](key),
            operations.map(_.toOperation): _*
          )
        }
      case _: getHeader ⇒
        Task.async { register ⇒
          client.operate(
            writePolicy,
            new RecordListener {
              def onSuccess(key: Key, record: Record): Unit =
                register(\/-(
                  Some(record).
                    filter(_ != null).
                    map(r ⇒ (r.generation, r.expiration)).
                    asInstanceOf[Option[V]]
                ))
              def onFailure(ae: AerospikeException): Unit =
                register(-\/(ae))
            },
            parseKey[K](key),
            operations.map(_.toOperation): _*
          )
        }
      case last: get ⇒
        Task.async { register ⇒
          client.operate(
            writePolicy,
            new RecordListener {
              def onSuccess(key: Key, record: Record): Unit =
                register(\/-(
                  Some(record).
                    filter(_ != null).
                    map(_.getValue(last.binName)).
                    filter(_ != null).
                    asInstanceOf[Option[V]]
                ))
              def onFailure(ae: AerospikeException): Unit =
                register(-\/(ae))
            },
            parseKey[K](key),
            operations.map(_.toOperation): _*
          )
        }
    }
  }

  def delete[K: KRestriction](key: K): Task[Unit] = {

    Task.async { register ⇒
      client.delete(
        writePolicy,
        new DeleteListener {
          def onSuccess(key: Key, existed: Boolean): Unit =
            register(\/-(()))
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        parseKey[K](key)
      )
    }
  }

  def touch[K: KRestriction](key: K): Task[Unit] = {

    Task.async { register ⇒
      client.touch(
        writePolicy,
        new WriteListener {
          def onSuccess(key: Key): Unit =
            register(\/-(()))
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        parseKey[K](key)
      )
    }
  }

  def exists[K: KRestriction](key: K): Task[Boolean] = {

    Task.async { register ⇒
      client.exists(
        queryPolicy,
        new ExistsListener {
          def onSuccess(key: Key, exists: Boolean): Unit =
            register(\/-(exists))
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        parseKey[K](key)
      )
    }
  }

  def exists[K: KRestriction](keys: Array[K])(
    implicit ctx: distinct1.type): Task[Map[K, Boolean]] = {

    Task.async { register ⇒
      client.exists(
        queryPolicy,
        new ExistsArrayListener {
          def onSuccess(keys: Array[Key], existsArray: Array[Boolean]): Unit = {
            lazy val length = keys.length

            @annotation.tailrec def go(m: Map[K, Boolean], i: Int): Map[K, Boolean] = {
              i match {
                case `length` ⇒ m
                case _ ⇒
                  go(m + (keys(i).userKey.asInstanceOf[K] -> existsArray(i)), i + 1)
              }
            }
            register(\/-(go(Map.empty[K, Boolean], 0)))
          }
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        keys.map(parseKey[K](_))
      )
    }
  }

  def add[K: KRestriction](key: K, value: Int, bin: String = ""): Task[Unit] = {

    Task.async { register ⇒
      client.add(
        writePolicy,
        new WriteListener {
          def onSuccess(k: Key): Unit =
            register(\/-(()))
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        parseKey[K](key),
        new ABin(bin, parseValue(value))
      )
    }
  }

  def append[K: KRestriction](key: K, value: String, bin: String = ""): Task[Unit] = {

    Task.async { register ⇒
      client.append(
        writePolicy,
        new WriteListener {
          def onSuccess(k: Key): Unit =
            register(\/-(()))
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        parseKey[K](key),
        new ABin(bin, parseValue(value))
      )
    }
  }

  def prepend[K: KRestriction](key: K, value: String, bin: String = ""): Task[Unit] = {

    Task.async { register ⇒
      client.prepend(
        writePolicy,
        new WriteListener {
          def onSuccess(k: Key): Unit =
            register(\/-(()))
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        parseKey[K](key),
        new ABin(bin, parseValue(value))
      )
    }
  }

  def getHeader[K: KRestriction](key: K): Task[Option[Tuple2[Int, Int]]] = {

    Task.async { register ⇒
      client.getHeader(
        queryPolicy,
        new RecordListener {
          def onSuccess(key: Key, record: Record): Unit =
            register(\/-(
              Some(record).
                filter(_ != null).
                map(r ⇒ (r.generation, r.expiration)).
                asInstanceOf[Option[Tuple2[Int, Int]]]
            ))
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        parseKey[K](key)
      )
    }
  }

  def getHeader[K: KRestriction](keys: Array[K])(implicit ctx: distinct1.type): Task[Map[K, Tuple2[Int, Int]]] = {

    Task.async { register ⇒
      client.getHeader(
        queryPolicy,
        new RecordArrayListener {
          def onSuccess(keys: Array[Key], records: Array[Record]): Unit = {

            lazy val length = keys.length
            @annotation.tailrec def go(m: Map[K, Tuple2[Int, Int]], i: Int): Map[K, Tuple2[Int, Int]] = {
              lazy val rec = records(i)

              i match {
                case `length` ⇒ m
                case _ ⇒
                  if (rec != null)
                    go(m + (keys(i).userKey.asInstanceOf[K] -> (rec.generation, rec.expiration)), i + 1)
                  else
                    go(m, i + 1)
              }
            }
            register(\/-(go(Map.empty[K, Tuple2[Int, Int]], 0)))
          }
          def onFailure(ae: AerospikeException): Unit =
            register(-\/(ae))
        },
        keys.map(parseKey[K](_))
      )
    }
  }

  def execute[K: KRestriction, LuaR](key: K, packageName: String, functionName: String, functionArgs: Any*)(
    implicit ev1: LuaR DefaultTypeTo Any, ev2: LRestriction[LuaR]): Task[Option[LuaR]] = {

    Task.delay {
      client.execute(
        generalPolicy, parseKey[K](key), packageName, functionName, functionArgs.map(parseValue(_)): _*
      ).toOption.map(result ⇒
          result match {
            case r: java.util.ArrayList[_] ⇒
              r.asScala.toList
            case r: java.util.HashMap[_, _] ⇒
              r.asScala.toMap
            case _ ⇒ result
          }
        ).asInstanceOf[Option[LuaR]]
    }
  }

  def execute(filter: AFilter, packageName: String, functionName: String, functionArgs: Any*): Task[Unit] = {

    Task.delay {
      val t: ExecuteTask = client.execute(
        generalPolicy, createStmt(filter), packageName, functionName, functionArgs.map(parseValue(_)): _*
      )

      while (!t.isDone) Util.sleep(500)
    }
  }

  def query[V](filter: AFilter)(
    implicit ev1: V DefaultTypeTo SetV, ev2: VRestriction[V]): Process[Task, Map[String, V]] = {

    io.resource(
      Task.delay(
        client.query(queryPolicy, createStmt(filter))
      )
    )(rs ⇒ Task.delay(rs.close)) { rs ⇒
        Task.delay {
          if (rs.next)
            rs.getRecord.bins.toMapWithNotNull.asInstanceOf[Map[String, V]]
          else
            throw Cause.Terminated(Cause.End)
        }
      }
  }

  def queryAggregate[LuaR](filter: AFilter, packageName: String, functionName: String, functionArgs: Any*)(
    implicit ev1: LuaR DefaultTypeTo Empty, ev2: LuaR =!= Empty, ev3: LRestriction[LuaR]): Process[Task, LuaR] = {

    io.resource(Task.delay(
      client.queryAggregate(
        queryPolicy,
        createStmt(filter),
        packageName,
        functionName,
        functionArgs.map(parseValue(_)): _*
      )
    ))(rs ⇒ Task.delay(rs.close)) { rs ⇒
      Task.delay {
        if (rs.next)
          (rs.getObject match {
            case r: java.util.HashMap[_, _] ⇒
              r.toMapWithNotNull
            case r: java.util.ArrayList[_] ⇒
              r.asScala.toList
            case other ⇒ other
          }).asInstanceOf[LuaR]
        else
          throw Cause.Terminated(Cause.End)
      }
    }
  }

  def createIndex[I](indexName: String, binName: String)(
    implicit ev1: I DefaultTypeTo Empty, ev2: I =!= Empty, evIndexType: IRestriction[I]): Task[Unit] = {

    Task.delay {
      val t: IndexTask = client.createIndex(
        generalPolicy, namespace.name, setName, indexName, binName, (evIndexType: @unchecked) match {
          case _: IRestriction.string.type ⇒ IndexType.STRING
          case _: IRestriction.int.type    ⇒ IndexType.NUMERIC
        }
      )

      while (!t.isDone) Util.sleep(500)
    }
  }

  def dropIndex(indexName: String): Unit = {

    client.dropIndex(generalPolicy, namespace.name, setName, indexName)
  }
}
