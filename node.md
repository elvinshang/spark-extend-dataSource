数据源 *****
    load
    save

DataFrame/Dataset
    text
    json

大数据作业基本流程
    input  业务逻辑  output
    MR/Hive/Spark/Flink/Storm

Spark能处理多种数据源的数据，而且这些数据源可以是在不同的地方
    file/HDFS/S3/OSS/COS/RDBMS
    json/ORC/Parquet/JDBC

通过Spark将各种不同数据源的数据加载成DF/DS，后续的所有操作都是基于DF/DS



Data Source API标准写法
    spark.read.format("").load()
    df.write.format("").save("")














