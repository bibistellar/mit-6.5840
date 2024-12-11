所有rpc调用都应该是并行执行的以提高效率，且应该拥有超时返回的逻辑

lab3A:
    ①所有server启动后进入followers状态，监听来自leader的heartbeat rpc调用。
    ②如果一段时间没有收到任何来自leader的调用，将自身状态置为candidate，term+1，进入选举状态
    ③选举时server选举自己，并发射request vote rpc调用到其他节点上。选举将以以下三种状态结束
    a）赢得选举
    b）其他节点赢得选举
    c）投票无法决定唯一leader，选举重新开始
    ④选举规则：先到先得？term相同的情况下，收到谁的request vote就投给谁
    ⑤存在分票规则split

    如果节点接收到低term的选举请求，则将自己的currentTerm返回告知candidiate节点先更新到最新的状态

term增长的规则：
    ①选举开始后，发起选举的server term+1
    ②如果接收到拥有更高term的rpc请求，更新term
    ③拒绝比自己的term更低的请求

避免死锁：
    ①锁的持有覆盖的逻辑不要太长，只有在必要的读写数据时再上锁（读能不上就不上吧）
    ②一种死锁情况是两个server，每个server都处于candidate状态，如果此时都持有自身state的锁，那么互相朝对方发送voterequest，voterequest由于无法修改state（被ticker持有中），导致无法返回，那么candidate处的逻辑就卡在等待rpc返回这里，两个server都卡住了。
    ③减少锁的覆盖范围可以缓解死锁情况，同时提高程序的运行速度，根本上解决还需要修改call的代码，给其增加一个超时自动返回false的逻辑。