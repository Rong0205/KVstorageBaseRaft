#include "raft.h"

void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
    m_peers = peers;     // 与其他结点沟通的rpc类
    m_persister = persister;   // 持久化类
    m_me = me;    // 标记自己，毕竟不能给自己发送rpc吧

    {
        std::unique_lock<std::mutex> Lock(m_mtx);
        // applier
        this->applyChan = applyCh;   // 与kv-server沟通
        // rf.ApplyMsgQueue = make(chan ApplyMsg)

        m_currentTerm = 0;   // 初始化term为0
        m_status = Follower;   // 初始化身份为follower
        m_commitIndex = 0;  
        m_lastApplied = 0;
        m_logs.clear();

        for (int i = 0; i < m_peers.size(); i++) {
            m_matchIndex.push_back(0);
            m_nextIndex.push_back(0);
        }

        m_votedFor = -1;    // 当前term没有给其他人投过票就用-1表示
        m_lastSnapshotIncludeIndex = 0;
        m_lastSnapshotIncludeTerm = 0;
        m_lastResetElectionTime = now();
        m_lastResetHearBeatTime = now();

        // initialize from state persisted before a crash
        readPersist(m_persister->ReadRaftState());

        if (m_lastSnapshotIncludeIndex > 0) {
            m_lastApplied = m_lastSnapshotIncludeIndex;
            // rf.commitIndex = rf.lastSnapshotIncludeIndex 崩溃恢复不能读取commitIndex
        }
    }

    // start ticker  开始三个定时器
    // std::thread t1(&Raft::leaderHearBeatTicker, this);
    // t1.detach();

    // std::thread t2(&Raft::electionTimeOutTicker, this);
    // t2.detach();

    // std::thread t3(&Raft::applierTicker, this);
    // t3.detach();
}

void Raft::electionTimeOutTicker() {
  while (true) {
    // 如果当前节点是 Leader，则睡眠一段时间后继续检查 //用条件变量优化
    {
      std::unique_lock<std::mutex> stateLock(m_mtx);
      if (m_status == Leader) {
        stateLock.unlock(); // 释放锁，避免阻塞其他线程
        std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
        continue;
      }
    }

    // 计算合适的睡眠时间
    std::chrono::nanoseconds suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
        std::unique_lock<std::mutex> stateLock(m_mtx);
        wakeTime = std::chrono::system_clock::now();
        suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
    }

    if (suitableSleepTime > std::chrono::milliseconds(1)) {
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      std::this_thread::sleep_for(suitableSleepTime);

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                << std::endl;
      std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                << std::endl;
    }

    {
        std::unique_lock<std::mutex> stateLock(m_mtx);
        if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
        //说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
        continue;
        }
    }
    doElection();
  }
}

//加线程池！！！！
void Raft::doElection() {
    // 局部变量，用于存储选举过程中需要的数据
    int currentTerm;
    int lastLogIndex = -1, lastLogTerm = -1;
    std::vector<std::shared_ptr<raftRpcProctoc::RequestVoteArgs>> requestVoteArgsList;
    std::vector<std::shared_ptr<raftRpcProctoc::RequestVoteReply>> requestVoteReplyList;
    std::shared_ptr<int> votedNum;

    {
        // 加锁保护共享资源
        std::unique_lock<std::mutex> stateLock(m_mtx);

        // 如果当前节点已经是 Leader，则不需要选举
        if (m_status == Leader) {
            return;
        }

        DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader,开始选举 \n", m_me);
        // 重置选举定时器
        m_lastResetElectionTime = now();
        // 更新节点状态
        m_status = Candidate;
        m_currentTerm += 1;  // 增加任期号
        m_votedFor = m_me;   // 给自己投票
        persist();           // 持久化状态
        
        // 初始化投票计数器
        votedNum = std::make_shared<int>(1);
        // 记录当前任期号，用于后续 RPC 请求
        currentTerm = m_currentTerm;
        // 获取最后一条日志的信息
        getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);

        // 锁内初始化 RequestVote 参数
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                continue;
            }
            auto requestVoteArgs = std::make_shared<raftRpcProctoc::RequestVoteArgs>();
            requestVoteArgs->set_term(m_currentTerm);
            requestVoteArgs->set_candidateid(m_me);
            requestVoteArgs->set_lastlogindex(lastLogIndex);
            requestVoteArgs->set_lastlogterm(lastLogTerm);
            requestVoteArgsList.push_back(requestVoteArgs);

            auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();
            requestVoteReplyList.push_back(requestVoteReply);
        }
    }

    // 在锁外发送 RequestVote RPC
    for (int i = 0; i < m_peers.size(); i++) {
        if (i == m_me) {
            continue;
        }
        std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgsList[i], requestVoteReplyList[i], votedNum);
        t.detach();
    }
}

bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args, std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply,
                           std::shared_ptr<int> votedNum) {
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());

    if (!ok) {
        return ok; // rpc通信失败就立即返回，避免资源消耗
    }

    std::unique_lock<std::mutex> stateLock(m_mtx);

    if (reply->term() > m_currentTerm) {
        // 回复的term比自己大，说明自己落后了，那么就更新自己的状态并且退出
        m_status = Follower; // 三变：身份，term，和投票
        m_currentTerm = reply->term();
        m_votedFor = -1;  // term更新了，那么这个term自己肯定没投过票，为-1
        persist(); // 持久化
        return true;
    } else if (reply->term() < m_currentTerm) {
        // 回复的term比自己的term小，不应该出现这种情况
        return true;
    }

    if (!reply->votegranted()) {  // 这个节点因为某些原因没给自己投票，没啥好说的，结束本函数
        return true;
    }

    // 给自己投票了
    *votedNum = *votedNum + 1; // voteNum多一个

    if (*votedNum >= m_peers.size() / 2 + 1) {
        // 变成leader
        *votedNum = 0;   // 重置voteDNum，如果不重置，那么就会变成leader很多次，是没有必要的，甚至是错误的！！！
        // 第一次变成leader，初始化状态和nextIndex、matchIndex
        m_status = Leader;
        m_cv.notify_all();
        int lastLogIndex = getLastLogIndex();

        for (int i = 0; i < m_nextIndex.size(); i++) {
            m_nextIndex[i] = lastLogIndex + 1; // 有效下标从1开始，因此要+1
            m_matchIndex[i] = 0;               // 每换一个领导都是从0开始，见论文的fig2
        }

        std::thread t(&Raft::doHeartBeat, this); // 马上向其他节点宣告自己就是leader
        t.detach();
        persist();
    }

    return true;
}

void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply) {
    std::unique_lock<std::mutex> stateLock(m_mtx);

    // Your code here (2A, 2B).
    DEFER {
        //应该先持久化，再撤销lock
        persist();
    };
    // 对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
    // reason: 出现网络分区，该竞选者已经OutOfDate(过时）
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Expire);
        reply->set_votegranted(false);
        return;
    }

    // 论文fig2:右下角，如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
    if (args->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;
        // 重置定时器：收到leader的ae，开始选举，透出票
        // 这时候更新了term之后，votedFor也要置为-1
    }

    // 现在节点任期都是相同的(任期小的也已经更新到新的args的term了)
    // 要检查log的term和index是不是匹配的了
    int lastLogTerm = getLastLogTerm();

    // 只有没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会授票
    if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
        // 日志太旧了
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);
        return;
    }

    // 当因为网络质量不好导致的请求丢失重发就有可能！！！！
    // 因此需要避免重复投票
    if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);
        return;
    } else {
        // 同意投票
        m_votedFor = args->candidateid();
        m_lastResetElectionTime = now(); // 认为必须要在投出票的时候才重置定时器，
        reply->set_term(m_currentTerm);
        reply->set_votestate(Normal);
        reply->set_votegranted(true);
        return;
    }
}

void Raft::leaderHearBeatTicker() {
    while (true) {
        // 如果当前节点不是 Leader，则等待
        if (m_status != Leader) {
        std::unique_lock<std::mutex> lock(m_mtx);
        m_cv.wait_for(lock, std::chrono::milliseconds(HeartBeatTimeout), [this] {
            return m_status == Leader;  // 等待状态变为 Leader
        });
        continue;
        }

        auto nowTime = now();
        m_mtx.lock();
        auto suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - nowTime;
        m_mtx.unlock();

        if (suitableSleepTime.count() < 1) {
            suitableSleepTime = std::chrono::milliseconds(1);
        }

        std::this_thread::sleep_for(suitableSleepTime);//用优化条件变量

        m_mtx.lock();
        if ((m_lastResetHearBeatTime - nowTime).count() > 0) { // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
            continue;
        }
        m_mtx.unlock();

        doHeartBeat();
    }
}

//待优化，发送日志进行封装和线程池
void Raft::doHeartBeat() {
    std::unique_lock<std::mutex> stateLock(m_mtx);

    if (m_status == Leader) {
        auto appendNums = std::make_shared<int>(1); // 正确返回的节点的数量

        // 对Follower（除了自己外的所有节点发送AE）
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) { // 不对自己发送AE
                continue;
            }

            // 日志压缩加入后要判断是发送快照还是发送AE
            if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
                // 改发送的日志已经被做成快照，必须发送快照了
                std::thread t(&Raft::leaderSendSnapShot, this, i);
                t.detach();
                continue;
            }

            // 发送心跳，构造发送值
            int preLogIndex = -1;
            int PrevLogTerm = -1;
            getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);  // 获取本次发送的一系列日志的上一条日志的信息，以判断是否匹配

            std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs = std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
            appendEntriesArgs->set_term(m_currentTerm);
            appendEntriesArgs->set_leaderid(m_me);
            appendEntriesArgs->set_prevlogindex(preLogIndex);
            appendEntriesArgs->set_prevlogterm(PrevLogTerm);
            appendEntriesArgs->clear_entries();
            appendEntriesArgs->set_leadercommit(m_commitIndex);

            // 作用是携带上prelogIndex的下一条日志及其之后的所有日志
            // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
            if (preLogIndex != m_lastSnapshotIncludeIndex) {
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
                    raftRpcProctoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = m_logs[j];
                }
            } else {
                for (const auto& item : m_logs) {
                    raftRpcProctoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = item;
                }
            }

            int lastLogIndex = getLastLogIndex();

            // 初始化返回值
            const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply = std::make_shared<raftRpcProctoc::AppendEntriesReply>();
            std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply,
                          appendNums); // 创建新线程并执行b函数，并传递参数
            t.detach();
        }

        m_lastResetHearBeatTime = now(); // leader发送心跳，重置心跳时间，
    }
}

bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args, std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums) {
    // todo： paper中5.3节第一段末尾提到，如果append失败应该不断的retries ,直到这个log成功的被store
    bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

    if (!ok) {
        return ok;
    }

    std::unique_lock<std::mutex> stateLock(m_mtx);

    // 对reply进行处理
    // 对于rpc通信，无论什么时候都要检查term
    if (reply->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        return ok;
    } else if (reply->term() < m_currentTerm) { // 正常不会发生
        return ok;
    }

    if (m_status != Leader) { // 如果不是leader，那么就不要对返回的情况进行处理了
        return ok;
    }

    // term相等
    if (!reply->success()) {
        // 日志不匹配，正常来说就是index要往前-1，既然能到这里，第一个日志（idnex = 1）发送后肯定是匹配的，因此不用考虑变成负数
        // 因为真正的环境不会知道是服务器宕机还是发生网络分区了
        if (reply->updatenextindex() != -100) {  // -100只是一个特殊标记而已，没有太具体的含义
            // 优化日志匹配，让follower决定到底应该下一次从哪一个开始尝试发送
            m_nextIndex[server] = reply->updatenextindex();
        }
        // 如果感觉rf.nextIndex数组是冗余的，看下论文fig2，其实不是冗余的
    } else {
        *appendNums = *appendNums + 1;   // 到这里代表同意接收了本次心跳或者日志

        m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());  // 同意了日志，就更新对应的m_matchIndex和m_nextIndex
        m_nextIndex[server] = m_matchIndex[server] + 1;

        int lastLogIndex = getLastLogIndex();

        if (*appendNums >= 1 + m_peers.size() / 2) { // 可以commit了
            // 两种方法保证幂等性，1.赋值为0 	2.上面≥改为==
            *appendNums = 0;  // 置0

            // 日志的安全性保证！！！！！ leader只有在当前term有日志提交的时候才更新commitIndex，因为raft无法保证之前term的Index是否提交
            // 只有当前term有日志提交，之前term的log才可以被提交，只有这样才能保证“领导人完备性{当选领导人的节点拥有之前被提交的所有log，当然也可能有一些没有被提交的}”
            // 说白了就是只有当前term有日志提交才会提交
            if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
                m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
            }
        }
    }

    return ok;
}

void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply) {
    std::lock_guard<std::mutex> locker(m_mtx);

    // 不同的人收到AppendEntries的反应是不同的，要注意无论什么时候收到rpc请求和响应都要检查term
    if (args->term() < m_currentTerm) {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100); // 论文中：让领导人可以及时更新自己
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(), args->term(), m_me, m_currentTerm);
        return; // 注意从过期的领导人收到消息不要重设定时器
    }

    DEFER { persist(); };  //由于这个局部变量创建在锁之后，因此执行persist的时候应该也是拿到锁的.

    if (args->term() > m_currentTerm) {
        // 三变 ,防止遗漏，无论什么时候都是三变
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1; // 这里设置成-1有意义，如果突然宕机然后上线理论上是可以投票的
        // 这里可不返回，应该改成让改节点尝试接收日志
        // 如果是领导人和candidate突然转到Follower好像也不用其他操作
        // 如果本来就是Follower，那么其term变化，相当于“不言自明”的换了追随的对象，因为原来的leader的term更小，是不会再接收其消息了
    }

    // 如果发生网络分区，那么candidate可能会收到同一个term的leader的消息，要转变为Follower，为了和上面，因此直接写
    m_status = Follower; // 这里是有必要的，因为如果candidate收到同一个term的leader的AE，需要变成follower

    // term相等
    m_lastResetElectionTime = now(); // 重置选举超时定时器

    // 不能无脑的从prevlogIndex开始阶段日志，因为rpc可能会延迟，导致发过来的log是很久之前的
    // 那么就比较日志，日志有3种情况
    if (args->prevlogindex() > getLastLogIndex()) {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(getLastLogIndex() + 1);
        return;
    } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) { // 如果prevlogIndex还没有更上快照
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
    }

    // 本机日志有那么长，冲突(same index,different term),截断日志
    // 注意：这里目前当args.PrevLogIndex == rf.lastSnapshotIncludeIndex与不等的时候要分开考虑，可以看看能不能优化这块
    if (matchLog(args->prevlogindex(), args->prevlogterm())) {
        // 日志匹配，那么就复制日志
        for (int i = 0; i < args->entries_size(); i++) {
            auto log = args->entries(i);
            if (log.logindex() > getLastLogIndex()) { // 超过就直接添加日志
                m_logs.push_back(log);
            } else { // 没超过就比较是否匹配，不匹配再更新，而不是直接截断
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) { // 不匹配就更新
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
                }
            }
        }

        if (args->leadercommit() > m_commitIndex) {
            m_commitIndex = std::min(args->leadercommit(), getLastLogIndex()); // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
        }

        // 领导会一次发送完所有的日志
        reply->set_success(true);
        reply->set_term(m_currentTerm);
        return;
    } else {
        // 不匹配，不匹配不是一个一个往前，而是有优化加速
        // PrevLogIndex 长度合适，但是不匹配，因此往前寻找 矛盾的term的第一个元素
        // 为什么该term的日志都是矛盾的呢？也不一定都是矛盾的，只是这么优化减少rpc而已
        // ？什么时候term会矛盾呢？很多情况，比如leader接收了日志之后马上就崩溃等等
        reply->set_updatenextindex(args->prevlogindex());
        for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
            if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
                reply->set_updatenextindex(index + 1);
                break;
            }
        }

        reply->set_success(false);
        reply->set_term(m_currentTerm);
        return;
    }
}


  //=========恒久化函数==============//
  //持久化节点当前信息
void Raft::persist() {
  auto data = persistData();
  m_persister->SaveRaftState(data);
}

//将节点信息进行序列化
std::string Raft::persistData() {   
  raftPersistDate::PersistRaftNode* persistRaftNode;
  // 填充 protobuf 消息
  persistRaftNode->set_current_term(m_currentTerm);
  persistRaftNode->set_voted_for(m_votedFor);
  persistRaftNode->set_last_snapshot_include_index(m_lastSnapshotIncludeIndex);
  persistRaftNode->set_last_snapshot_include_term(m_lastSnapshotIncludeTerm);

  for (raftRpcProctoc::LogEntry& item : m_logs) {
    raftPersistDate::LogEntry* logEntry = persistRaftNode->add_logs();
    logEntry->set_term(item.logterm());
    logEntry->set_index(item.logindex());
    logEntry->set_command(item.command());
  }

  // 序列化为字符串
  return persistRaftNode->SerializeAsString();
}


void Raft::readPersist(std::string data) {

}

  //===============================//

  //==========辅助函数===============//
  void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  } else {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}

int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}