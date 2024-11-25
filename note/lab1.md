map-reduce:
    ①从数据集合中先将数据与设计的键值组合成一条条的键值对
    ②根据分区算法，将键值对放入不同分区中等待reduce，形成类似如下结构：
    map1->(reduce1,reduce2,reduce3),map2->(reduce1,reduce2,reduce3)....每个reduce i 包含的键值是相同的
    ③reduce程序将每个map任务的相同序号的reduce组合起来，然后通过sort排序，最后使用reduce函数进行统计并输出到对应文件

hints;
    ①go的rpc要求请求和返回的结构体成员命名必须遵循驼峰命名
    ②rpc的call函数应该是异步写入缓冲区的，所以最好每次请求都新建一个数据缓冲区，避免造成数据没完成写入就被读取导致程序出错