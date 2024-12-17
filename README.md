# spark

HDFS NameNode	9870	http://<namenode-ip>:9870	查看 HDFS 文件系统状态
HDFS DataNode	9864	http://<datanode-ip>:9864	查看数据节点的存储状态
YARN ResourceManager	8088	http://<resourcemanager-ip>:8088	查看作业调度和资源分配
YARN NodeManager	8042	http://<nodemanager-ip>:8042	查看节点的任务和资源使用
Spark Master	8080	http://<spark-master-ip>:8080	查看 Spark 集群和 Worker 状态
Spark Worker	8081+	http://<spark-worker-ip>:8081	查看 Worker 资源使用情况
Spark Driver (实时 UI)	4040+	http://<driver-ip>:4040	查看当前作业执行状态
Spark History Server	18080	http://<history-server-ip>:18080	查看已完成作业的执行历史
