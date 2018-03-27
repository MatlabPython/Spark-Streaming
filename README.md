#llStreamUdap
##how to use
###1.配置文件说明
	kafka.topic	spark streaming消费的kafka topics，多个topic之间用","连接；
	kafka.broker.list	kafka集群的broker地址，eg：udap1:9092,udap5:9092；
	kafka.groupid	spark streaming消费kafka的group id；
	spark.interval	spark streaming计算频率；
	spark.checkpointPath	spark streaming的checkpoint目录；
	hbase.tables	写入的Hbase表，与代码绑定，不要改动；	
	hbase.hbaseZookeeperPort	Hbase依赖的Zookeeper的端口号；
	hbase.zookeeperquorum	Hbase依赖的Zookeeper集群的地址，多个地址之间用","连接；
	
###2.程序启动
	使用bin目录下的脚本即可后台启动，启动后可在logs目录下查看对应日志。
