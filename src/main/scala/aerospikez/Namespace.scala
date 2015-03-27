package aerospikez

case class Namespace(name: String = "test",
                     queryConfig: QueryConfig = QueryConfig(),
                     writeConfig: WriteConfig = WriteConfig(),
                     batchConfig: BatchConfig = BatchConfig())
