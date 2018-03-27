spark-submit --master yarn-client --driver-memory 2G --num-executors 2 --executor-cores 2 --executor-memory 4G --class com.gsafety.lifeline.bigdata.streaming.water.WaterFlowStreaming /app/streaming/test/lifelineStreaming-1.0-SNAPSHOT-jar-with-dependencies.jar /app/streaming/conf/waterFlow.properties >> /app/streaming/logs/WaterFlowStreaming.log 2>&1 &
