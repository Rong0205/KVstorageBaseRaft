#include "Raft.h"

void Raft::init(std::vector<std::shared_ptr<RaftRpc>> peers, int me, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
    // 初始化非共享资源
    m_peers = peers;     // 与其他节点通信的 RPC 客户端
    m_persister = persister;   // 持久化存储对象
    m_me = me;    // 当前节点的 ID
    this->applyChan = applyCh;   // 与上层应用（如 KV 服务器）通信的队列

    // 加锁保护共享资源
    {
        std::unique_lock<std::mutex> lock(m_mtx);  // 使用 unique_lock 自动管理锁
        m_currentTerm = 0;   // 初始化当前任期号为 0
        m_status = Follower;   // 初始化节点状态为 Follower
        m_commitIndex = 0;   // 初始化已提交日志条目的索引
        m_lastApplied = 0;   // 初始化已应用到状态机的日志条目的索引
        m_logs.clear();   // 清空日志条目列表
        for (int i = 0; i < m_peers.size(); i++) {
            m_matchIndex.push_back(0);   // 初始化每个节点的 matchIndex
            m_nextIndex.push_back(0);    // 初始化每个节点的 nextIndex
        }
        m_votedFor = -1;    // 当前任期内没有投票给任何节点
        m_lastSnapshotIncludeIndex = 0;   // 快照中包含的最后一条日志条目的索引
        m_lastSnapshotIncludeTerm = 0;    // 快照中包含的最后一条日志条目的任期号
        m_lastResetElectionTime = now();  // 重置选举计时器
        m_lastResetHearBeatTime = now();  // 重置心跳计时器
    }  // lock 在这里自动释放

    // 从持久化存储中恢复状态（需要加锁）
    {
        std::unique_lock<std::mutex> lock(m_mtx);  // 使用 unique_lock 自动管理锁
        readPersist(m_persister->ReadRaftState());
        if (m_lastSnapshotIncludeIndex > 0) {
            m_lastApplied = m_lastSnapshotIncludeIndex;  // 更新已应用到状态机的日志条目索引
        }
    }  // lock 在这里自动释放

    // 启动后台线程（不需要加锁）
    std::thread t(&Raft::leaderHearBeatTicker, this);
    t.detach();
    std::thread t2(&Raft::electionTimeOutTicker, this);
    t2.detach();
    std::thread t3(&Raft::applierTicker, this);
    t3.detach();
}

void Raft::electionTimeOutTicker() {
    while (true) {
        // 加锁保护共享资源
        {
            std::unique_lock<std::mutex> lock(m_mtx);
            auto nowTime = std::chrono::high_resolution_clock::now();
            auto sleepUntil = nowTime + getRandomizedElectionTimeout();
        }

        // 睡眠
        if (suitableSleepTime.count() > 1) {
            // 使用条件变量等待
            m_electionCond.wait_until(lock, sleepUntil, [this, nowTime] {
                return m_lastResetElectionTime > nowTime;  // 检查是否需要提前唤醒
            });
        }

        // 检查是否需要发起选举
        {
            std::unique_lock<std::mutex> lock(m_mtx);
            if ((m_lastResetElectionTime - nowTime).count() > 0) {
                continue;  // 选举计时器被重置，继续下一次循环
            }

        }

        // 发起选举
        doElection();
    }
}
//加线程池！！！！
void Raft::doElection() {
    // 局部变量，用于存储选举过程中需要的数据
    int currentTerm;
    int lastLogIndex, lastLogTerm;
    std::vector<std::shared_ptr<mprrpc::RequestVoteArgs>> requestVoteArgsList;
    std::vector<std::shared_ptr<mprrpc::RequestVoteReply>> requestVoteReplyList;
    std::shared_ptr<int> votedNum;

    {
        // 加锁保护共享资源
        std::lock_guard<std::mutex> lock(m_mtx);

        // 如果当前节点已经是 Leader，则不需要选举
        if (m_status == Leader) {
            return;
        }

        DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader,开始选举 \n", m_me);

        // 更新节点状态
        m_status = Candidate;
        m_currentTerm += 1;  // 增加任期号
        m_votedFor = m_me;   // 给自己投票
        persist();           // 持久化状态

        // 初始化投票计数器
        votedNum = std::make_shared<int>(1);

        // 获取最后一条日志的信息
        getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);

        // 重置选举定时器
        m_lastResetElectionTime = now();

        // 初始化 RequestVote 参数
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                continue;
            }
            auto requestVoteArgs = std::make_shared<mprrpc::RequestVoteArgs>();
            requestVoteArgs->set_term(m_currentTerm);
            requestVoteArgs->set_candidateid(m_me);
            requestVoteArgs->set_lastlogindex(lastLogIndex);
            requestVoteArgs->set_lastlogterm(lastLogTerm);
            requestVoteArgsList.push_back(requestVoteArgs);

            auto requestVoteReply = std::make_shared<mprrpc::RequestVoteReply>();
            requestVoteReplyList.push_back(requestVoteReply);
        }

        // 记录当前任期号，用于后续 RPC 请求
        currentTerm = m_currentTerm;
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

bool Raft::sendRequestVote(int server, std::shared_ptr<mprrpc::RequestVoteArgs> args, std::shared_ptr<mprrpc::RequestVoteReply> reply,
                           std::shared_ptr<int> votedNum) {
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
    if (!ok) {
        return ok; // RPC 通信失败就立即返回，避免资源消耗
    }

    // 加锁保护共享资源
    lock_guard<mutex> lg(m_mtx);

    if (reply->term() > m_currentTerm) {
        // 回复的 term 比自己大，说明自己落后了，更新状态并退出
        m_status = Follower; // 三变：身份、term 和投票
        m_currentTerm = reply->term();
        m_votedFor = -1;  // term 更新了，那么这个 term 自己肯定没投过票，为 -1
        persist(); // 持久化
        return true;
    } else if (reply->term() < m_currentTerm) {
        // 回复的 term 比自己的 term 小，不应该出现这种情况
        return true;
    }

    if (!reply->votegranted()) {
        // 这个节点因为某些原因没给自己投票，没啥好说的，结束本函数
        return true;
    }

    // 给自己投票了
    *votedNum = *votedNum + 1; // voteNum 多一个
    if (*votedNum >= m_peers.size() / 2 + 1) {
        // 变成 Leader
        *votedNum = 0;   // 重置 voteNum，避免重复成为 Leader
        // 第一次变成 Leader，初始化状态和 nextIndex、matchIndex
        m_status = Leader;
        int lastLogIndex = getLastLogIndex();
        for (int i = 0; i < m_nextIndex.size(); i++) {
            m_nextIndex[i] = lastLogIndex + 1; // 有效下标从 1 开始，因此要 +1
            m_matchIndex[i] = 0;               // 每换一个领导都是从 0 开始，见论文的 fig2
        }
        std::thread t(&Raft::doHeartBeat, this); // 马上向其他节点宣告自己就是 Leader
        t.detach();
        persist();
    }

    return true;
}

void Raft::RequestVote(const mprrpc::RequestVoteArgs *args, mprrpc::RequestVoteReply *reply) {
    // 处理过期的请求（不需要锁）
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Expire);
        reply->set_votegranted(false);
        return;
    }

    // 更新节点状态（需要锁）
    {
        std::lock_guard<std::mutex> lg(m_mtx);

        if (args->term() > m_currentTerm) {
            m_status = Follower;
            m_currentTerm = args->term();
            m_votedFor = -1;
        }
    }  // 锁在这里自动释放

    // 检查日志的新旧程度（不需要锁）
    if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);
        return;
    }

    // 避免重复投票（需要锁）
    {
        std::lock_guard<std::mutex> lg(m_mtx);

        if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
            reply->set_term(m_currentTerm);
            reply->set_votestate(Voted);
            reply->set_votegranted(false);
            return;
        }

        // 同意投票
        m_votedFor = args->candidateid();
        m_lastResetElectionTime = now();
        reply->set_term(m_currentTerm);
        reply->set_votestate(Normal);
        reply->set_votegranted(true);

        // 持久化状态
        persist();
    }  // 锁在这里自动释放
}
