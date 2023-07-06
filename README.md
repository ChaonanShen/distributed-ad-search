## 目前架构

从csv文件中读出数据，以Data结构格式持久化到一个文件中，索引记录keyword->offset (unordered_multimap) \
并且三个节点每个只保存1/3的数据，所以查询时可能要同时查询三个节点返回结果

每台server上都运行两个rpc服务，一个Search，一个SearchLocal
SearchLocal会把Request根据keywords归于那个节点拆成三个sub-Request(结构其实跟原先Request一样)，然后每个节点都返回topN+1(可能不足)个排序最高的广告单元，然后Search中将三个节点返回的所有广告单元再进行排序，取出前topN+1(可能不足)个再进行最后排序，然后计算出价格后返回

所以目前是每个节点都能进行Request的split和拆分，但是对外只能暴露一个端口，于是在server1设置一个负载均衡的balancer均匀的将Request分开给三个节点(如果之后发现server1压力太大，负载均衡算法可以少给server1一些压力)


etcd相关设置：三个server节点读取文件和设置索引完成后，往etcd的/nodeX记录Search Server和SearchLocal Server的ip:port，每个server都会读取其他节点的SearchLocal Server的ip:port以便之后发送sub-Request；balancer会读取所有节点的Search Server的ip:port之后再对外暴露ip:port供调用



## 可能优化思路
自己模拟大数据，用火焰图看瓶颈

大方向存储结构 & 索引结构的修改（多看些分布式查询的算法，往往是算法和底层结构一并修改） 如果可以索引直接保存keyword->Data，而不是Data保存在磁盘

对文件中keywords进行排序（那样读取的10min要优化 - *并行读取* 充分利用16核） - 可以先生成索引，然后按照索引重新将保存的文件一个个读取出来然后重新生成一个keywords有序的文件   多用并行读取  --> 并行读取、外部排序(按keyword排序)、建立索引

数据压缩，磁盘文件压缩和索引的hashtable压缩（参见第四届polardb大赛冠军队对索引存储空间的压榨）

咋感觉campaign_id和item_id这两个u64完全没用到，是不是就不用存了？

不要忘了改成分布式的目的是为了更快！ - 各台机子上的执行开销要比其他通信开销等要大得多，不然分布式意义就不大！如果目前这个情况，读取本机的时间开销远远不如其他个各种开销，那性能会很差
——> 更大的batch操作
## 其他
感觉真实测试数据的分布也是很重要的
如果一个Request中的keywords普遍比较多，并且每个keywords会对应非常多的广告单元，这样可能SearchLocal本身的执行时间就比较长，那么rpc通信时间相对就小了，rpc的开销就可以忽略；但是如果SearchLocal本身执行时间不长，那样rpc通信开销可能占大头，这时候就行更大程度的batch操作(比如多个不同Request的发往server1的sub-Request聚合一批再统一发送，一次rpc带上更多组数据)就能降低rpc开销

所以可能自己生成的大数据也可以模拟下不同情况，不同数据分布可能瓶颈不同

## 目前提交分析
提交id 59398 分数 49.579 qps 149.053\
就是目前的这种架构 balancer - 三个Search Server - 三个SearchLocal Server；读取csv建立索引的时间是8分钟(跟10分钟很接近！最好优化下) \
提交id 60020 分数 49.589 qps 506.468 \ 
上一个提交中删去了Data结构中两个无用的u64作用域，qps确实有提升 - 而且其实索引里已经保存了keyword，磁盘文件中也不需要再保存 \
提交id 60088 分数 49.5881 qps 281.409 \
注意这个提交是在59398基础上改的，没有去掉两个无用的u64，尝试进行rpc的channel和stub的复用（之后可以尝试异步），这次在balancer上尝试，确实有提升，但整体qps依然极低 \
提交id 60089 分数 49.5889 qps 340.037 \
在上一个基础上去掉了三个u64（两个无用的，还有个keyword索引中保存了，磁盘文件中就不再保存），但是看样子qps提升不大啊

提交id 60091 qps 344 \
server端复用channel和stub，但是效果并不明显，我怀疑是没有用连接池，但是这么几个channel用不够 \

感觉复用channel和stub效果都不明显 - 我怀疑是没有用连接池
不过好像一个stub和channel创建后背后会对应很多真实物理连接，所以其实只复用一个stub也是可以的

提交id 60093 qps 616.188 \
这里其实不需要split Request，直接原样发送即可，然后SearchLocal如果遇到不在本地的keyword直接跳过 \




## 模拟数据
广告数据大小说明：
总文件大小69G - 6亿条数据 \
关键词的数量： ～100万 \
广告单元的数量： ～500万 \
广告计划的数量：～100万 \
广告单元上的关键词数量： ～100
