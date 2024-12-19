# spark

HDFS NameNode	9870	http://<namenode-ip>:9870	查看 HDFS 文件系统状态
HDFS DataNode	9864	http://<datanode-ip>:9864	查看数据节点的存储状态
YARN ResourceManager	8088	http://<resourcemanager-ip>:8088	查看作业调度和资源分配
YARN NodeManager	8042	http://<nodemanager-ip>:8042	查看节点的任务和资源使用
Spark Master	8080	http://<spark-master-ip>:8080	查看 Spark 集群和 Worker 状态
Spark Worker	8081+	http://<spark-worker-ip>:8081	查看 Worker 资源使用情况
Spark Driver (实时 UI)	4040+	http://<driver-ip>:4040	查看当前作业执行状态
Spark History Server	18080	http://<history-server-ip>:18080	查看已完成作业的执行历史


hive 启动 manager.sh hive stop
     停止 manager.sh hive start



django 使用

django-admin startproject <projectname>
启动开发服务器 python manage.py runserver
python manage.py startapp myapp

django特点
遵循MVT架构 Model-View-Template模式
Model 定义数据的结构 负责与数据库交互
View 处理业务逻辑并返回相应
Template 定义页面显示内容

项目结构
myproject/
    manage.py        # 项目管理脚本
    myproject/
        __init__.py  # 表示该目录是一个 Python 包
        settings.py  # 项目配置
        urls.py      # URL 路由
        wsgi.py      # 部署 WSGI 应用

创建应用
在 Django 中，一个项目可以包含多个应用：

应用结构：
bash
Copy code
myapp/
    admin.py        # 管理后台注册
    apps.py         # 应用配置
    models.py       # 数据模型
    views.py        # 视图逻辑
    urls.py         # 应用路由（需要手动创建）
    migrations/     # 数据库迁移文件
配置应用
在 settings.py 中添加应用到 INSTALLED_APPS：

python
Copy code
INSTALLED_APPS = [
    ...
    'myapp',
]



Python File 方法

open()方法
python open()方法用于打开一个文件,并返回文件对象
使用open()方法一定要保证关闭文件蚊香,即调用close()方法.open()函数常用形式是接受两个参数:文件名(file)和模式(mode)
open(file,mode='r')
with oepn()是open的语法糖 使用了上下文管理器 context manager,可以自动管理资源.即使代码抛出异常,with也会确保文件被正确关闭.
with open('test.txt','w') as file:
    file.write('hello,world')

mysql 数据库导入

mysql -u username -p game < game.sql