package aerospikez

import com.aerospike.client.policy._
import com.aerospike.client.async.{ MaxCommandAction, AsyncClientPolicy }

import com.typesafe.config.{ Config, ConfigFactory }

import internal.util.Util._

private[aerospikez] object ConfigFile {
  var file: Config = ConfigFactory.load()
}

object ClientConfig {

  def apply(
    timeout: Int = 0,
    asyncMaxCommands: Int = 400,
    maxSocketIdle: Int = 14,
    asyncSelectorThreads: Int = 1,
    asyncSelectorTimeout: Int = 0,
    sharedThreadPool: Boolean = false,
    failIfNotConnected: Boolean = true,
    asyncMaxCommandAction: MaxCommandAction = MaxCommandAction.REJECT): ClientConfig = {

    trySome(ConfigFile.file.getConfig("aerospike.client-policy")).map(c ⇒
      new ClientConfig(
        trySome(c.getInt("async-selector-timeout")).getOrElse(asyncSelectorTimeout),
        trySome(c.getInt("timeout")).getOrElse(timeout),
        trySome(c.getInt("async-max-commands")).getOrElse(asyncMaxCommands),
        trySome(c.getInt("max-socket-idle")).getOrElse(maxSocketIdle),
        trySome(c.getInt("async-selector-threads")).getOrElse(asyncSelectorThreads),
        trySome(c.getBoolean("shared-thread-pool")).getOrElse(sharedThreadPool),
        trySome(c.getBoolean("fail-if-not-connected")).getOrElse(failIfNotConnected),
        trySome(c.getString("async-max-command-action")).map {
          parseAction(_, asyncMaxCommandAction)
        }.getOrElse(asyncMaxCommandAction)
      )
    ).getOrElse(
      new ClientConfig(
        asyncSelectorTimeout,
        timeout,
        asyncMaxCommands,
        maxSocketIdle,
        asyncSelectorThreads,
        sharedThreadPool,
        failIfNotConnected,
        asyncMaxCommandAction
      )
    )
  }

  private[aerospikez] def parseAction(action: String, defaultAction: MaxCommandAction): MaxCommandAction = {

    action.toUpperCase match {
      case "ACCEPT" ⇒ MaxCommandAction.ACCEPT
      case "REJECT" ⇒ MaxCommandAction.REJECT
      case "BLOCK"  ⇒ MaxCommandAction.BLOCK
      case _        ⇒ defaultAction
    }
  }
}

private[aerospikez] class ClientConfig(
    asyncSelectorTimeout: Int,
    timeout: Int,
    asyncMaxCommands: Int,
    maxSocketIdle: Int,
    asyncSelectorThreads: Int,
    sharedThreadPool: Boolean,
    failIfNotConnected: Boolean,
    asyncMaxCommandAction: MaxCommandAction) {

  private[aerospikez] val policy: AsyncClientPolicy = {
    val clientPolicy = new AsyncClientPolicy()
    clientPolicy.timeout = timeout
    clientPolicy.maxSocketIdle = maxSocketIdle
    clientPolicy.sharedThreadPool = sharedThreadPool
    clientPolicy.asyncMaxCommands = asyncMaxCommands
    clientPolicy.failIfNotConnected = failIfNotConnected
    clientPolicy.asyncSelectorThreads = asyncSelectorThreads
    clientPolicy.asyncSelectorTimeout = asyncSelectorTimeout
    clientPolicy.asyncMaxCommandAction = asyncMaxCommandAction
    clientPolicy
  }
}

object QueryConfig {

  def apply(
    timeout: Int = 0,
    maxRetries: Int = 2,
    recordQueueSize: Int = 5000,
    maxConcurrentNodes: Int = 0,
    sleepBetweenRetries: Int = 500): QueryConfig = {

    trySome(ConfigFile.file.getConfig("aerospike.query-policy")).map(c ⇒
      new QueryConfig(
        trySome(c.getInt("timeout")).getOrElse(timeout),
        trySome(c.getInt("max-retries")).getOrElse(maxRetries),
        trySome(c.getInt("record-queue-size")).getOrElse(recordQueueSize),
        trySome(c.getInt("max-concurrent-nodes")).getOrElse(maxConcurrentNodes),
        trySome(c.getInt("sleep-between-retries")).getOrElse(sleepBetweenRetries)
      )
    ).getOrElse(
      new QueryConfig(
        timeout,
        maxRetries,
        recordQueueSize,
        maxConcurrentNodes,
        sleepBetweenRetries
      )
    )
  }
}

private[aerospikez] class QueryConfig(
    timeout: Int,
    maxRetries: Int,
    recordQueueSize: Int,
    maxConcurrentNodes: Int,
    sleepBetweenRetries: Int) {

  private[aerospikez] val policy: QueryPolicy = {
    val queryPolicy = new QueryPolicy()
    queryPolicy.timeout = timeout
    queryPolicy.maxRetries = maxRetries
    queryPolicy.recordQueueSize = recordQueueSize
    queryPolicy.maxConcurrentNodes = maxConcurrentNodes
    queryPolicy.sleepBetweenRetries = sleepBetweenRetries
    queryPolicy
  }
}

object WriteConfig {

  def apply(
    timeout: Int = 0,
    expiration: Int = 0,
    generation: Int = 0,
    maxRetries: Int = 2,
    sendKey: Boolean = true,
    sleepBetweenRetries: Int = 500,
    priority: Priority = Priority.DEFAULT,
    generationPolicy: GenerationPolicy = GenerationPolicy.NONE,
    commitLevel: CommitLevel = CommitLevel.COMMIT_MASTER,
    consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
    recordExistsAction: RecordExistsAction = RecordExistsAction.UPDATE): WriteConfig = {

    trySome(ConfigFile.file.getConfig("aerospike.write-policy")).map(c ⇒
      new WriteConfig(
        trySome(c.getInt("write-policy.timeout")).getOrElse(timeout),
        trySome(c.getInt("write-policy.expiration")).getOrElse(expiration),
        trySome(c.getInt("write-policy.generation")).getOrElse(generation),
        trySome(c.getInt("write-policy.max-retries")).getOrElse(maxRetries),
        trySome(c.getBoolean("write-policy.send-key")).getOrElse(sendKey),
        trySome(c.getInt("write-policy.sleep-between-retries")).getOrElse(sleepBetweenRetries),
        trySome(c.getString("write-policy.priority")).map {
          parsePriority(_, priority)
        }.getOrElse(priority),
        trySome(c.getString("write-policy.generation-policy")).map {
          parseGenerationPolicy(_, generationPolicy)
        }.getOrElse(generationPolicy),
        trySome(c.getString("commit-level")).map {
          parseCommitLevel(_, commitLevel)
        }.getOrElse(commitLevel),
        trySome(c.getString("consistency-level")).map {
          parseConsistencyLevel(_, consistencyLevel)
        }.getOrElse(consistencyLevel),
        trySome(c.getString("write-policy.record-exists-action")).map {
          parseRecordExistsAction(_, recordExistsAction)
        }.getOrElse(recordExistsAction)
      )
    ).getOrElse(
      new WriteConfig(
        timeout,
        expiration,
        generation,
        maxRetries,
        sendKey,
        sleepBetweenRetries,
        priority,
        generationPolicy,
        commitLevel,
        consistencyLevel,
        recordExistsAction
      )
    )
  }

  private[aerospikez] def parseRecordExistsAction(recordExistsAction: String, defaultRecordExistsAction: RecordExistsAction): RecordExistsAction = {

    recordExistsAction.toUpperCase match {
      case "UPDATE"       ⇒ RecordExistsAction.UPDATE
      case "UPDATE_ONLY"  ⇒ RecordExistsAction.UPDATE_ONLY
      case "REPLACE"      ⇒ RecordExistsAction.REPLACE
      case "REPLACE_ONLY" ⇒ RecordExistsAction.REPLACE_ONLY
      case "CREATE_ONLY"  ⇒ RecordExistsAction.CREATE_ONLY
      case _              ⇒ defaultRecordExistsAction
    }
  }

  private[aerospikez] def parseGenerationPolicy(generationPolicy: String, defaultGenerationPolicy: GenerationPolicy): GenerationPolicy = {

    generationPolicy.toUpperCase match {
      case "NONE"             ⇒ GenerationPolicy.NONE
      case "EXPECT_GEN_GT"    ⇒ GenerationPolicy.EXPECT_GEN_GT
      case "EXPECT_GEN_EQUAL" ⇒ GenerationPolicy.EXPECT_GEN_EQUAL
      case _                  ⇒ defaultGenerationPolicy
    }
  }

  private[aerospikez] def parsePriority(priority: String, defaultPriority: Priority): Priority = {

    priority.toUpperCase match {
      case "DEFAULT" ⇒ Priority.DEFAULT
      case "MEDIUM"  ⇒ Priority.MEDIUM
      case "HIGH"    ⇒ Priority.HIGH
      case "LOW"     ⇒ Priority.LOW
      case _         ⇒ defaultPriority
    }
  }

  private[aerospikez] def parseConsistencyLevel(consistencyLevel: String, defaultConsistencyLevel: ConsistencyLevel): ConsistencyLevel = {
    consistencyLevel.toUpperCase match {
      case "CONSISTENCY_ALL" ⇒ ConsistencyLevel.CONSISTENCY_ALL
      case "CONSISTENCY_ONE"  ⇒ ConsistencyLevel.CONSISTENCY_ONE
      case _         ⇒ defaultConsistencyLevel
    }
  }

  private[aerospikez] def parseCommitLevel(commitLevel: String, defaultCommitLevelLevel: CommitLevel): CommitLevel = {
    commitLevel.toUpperCase match {
      case "COMMIT_ALL" ⇒ CommitLevel.COMMIT_ALL
      case "COMMIT_MASTER"  ⇒ CommitLevel.COMMIT_MASTER
      case _         ⇒ defaultCommitLevelLevel
    }
  }
}

private[aerospikez] class WriteConfig(
    timeout: Int,
    expiration: Int,
    generation: Int,
    maxRetries: Int,
    sendKey: Boolean,
    sleepBetweenRetries: Int,
    priority: Priority,
    generationPolicy: GenerationPolicy,
    commitLevel: CommitLevel,
    consistencyLevel: ConsistencyLevel,
    recordExistsAction: RecordExistsAction) {

  private[aerospikez] val policy: WritePolicy = {
    val writePolicy = new WritePolicy()
    writePolicy.timeout = timeout
    writePolicy.expiration = expiration
    writePolicy.generation = generation
    writePolicy.maxRetries = maxRetries
    writePolicy.sendKey = sendKey
    writePolicy.recordExistsAction = recordExistsAction
    writePolicy.sleepBetweenRetries = sleepBetweenRetries
    writePolicy.generationPolicy = generationPolicy
    writePolicy.commitLevel = commitLevel
    writePolicy.consistencyLevel = consistencyLevel
    writePolicy.priority = priority
    writePolicy
  }
}

object BatchConfig {

  def apply(
             maxConcurrentThreads: Int = 1,
             timeout: Int = 0,
             maxRetries: Int = 2,
             sleepBetweenRetries: Int = 500,
             priority: Priority = Priority.DEFAULT,
             consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE): BatchConfig = {

    trySome(ConfigFile.file.getConfig("aerospike.batch-policy")).map(c ⇒
      new BatchConfig(
        trySome(c.getInt("max-concurrent-threads")).getOrElse(timeout),
        trySome(c.getInt("timeout")).getOrElse(timeout),
        trySome(c.getInt("max-retries")).getOrElse(maxRetries),
        trySome(c.getInt("sleep-between-retries")).getOrElse(sleepBetweenRetries),
        trySome(c.getString("priority")).map {
          parsePriority(_, priority)
        }.getOrElse(priority),
        trySome(c.getString("consistency-level")).map {
          parseConsistencyLevel(_, consistencyLevel)
        }.getOrElse(consistencyLevel)
      )
    ).getOrElse(
        new BatchConfig(
          maxConcurrentThreads,
          timeout,
          maxRetries,
          sleepBetweenRetries,
          priority,
          consistencyLevel
        )
      )
  }

  private[aerospikez] def parsePriority(priority: String, defaultPriority: Priority): Priority = {
    priority.toUpperCase match {
      case "DEFAULT" ⇒ Priority.DEFAULT
      case "MEDIUM"  ⇒ Priority.MEDIUM
      case "HIGH"    ⇒ Priority.HIGH
      case "LOW"     ⇒ Priority.LOW
      case _         ⇒ defaultPriority
    }
  }

  private[aerospikez] def parseConsistencyLevel(consistencyLevel: String, defaultConsistencyLevel: ConsistencyLevel): ConsistencyLevel = {
    consistencyLevel.toUpperCase match {
      case "CONSISTENCY_ALL" ⇒ ConsistencyLevel.CONSISTENCY_ALL
      case "CONSISTENCY_ONE"  ⇒ ConsistencyLevel.CONSISTENCY_ONE
      case _         ⇒ defaultConsistencyLevel
    }
  }
}

private[aerospikez] class BatchConfig(
    maxConcurrentThreads: Int,
    timeout: Int,
    maxRetries: Int,
    sleepBetweenRetries: Int,
    priority: Priority,
    consistencyLevel: ConsistencyLevel) {

  private[aerospikez] val policy: BatchPolicy = {
    val batchPolicy = new BatchPolicy()
    batchPolicy.timeout = timeout
    batchPolicy.maxConcurrentThreads = maxConcurrentThreads
    batchPolicy.maxRetries = maxRetries
    batchPolicy.sleepBetweenRetries = sleepBetweenRetries
    batchPolicy.priority = priority
    batchPolicy.consistencyLevel = consistencyLevel
    batchPolicy
  }
}