# movieLens
基于movieLens数据的推荐，其中ml-10M100K中的rating.dat太大了无法上传，请前往[百度网盘](https://pan.baidu.com/s/1iOY_9DsN062UdvS7s9B7Vg)(密码:j83x)下载

#使用方法
首先配置mysql.conf文件，然后启动dataProcess/data2Mysql.py,将电影数据输入数据库；

再启动SparkAlsRecommend/sparkAlsRecommend.py,可以在Pycharm中配置spark环境然后启动(Pycharm配置spark请参考[这篇文章](https://www.jianshu.com/p/65aec07dea32))

关于提交spark任务，已经写在了SparkAlsRecommend/sparkAlsRecommend.py开头的备注中