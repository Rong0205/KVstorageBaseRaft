#ifndef RAFT_H
#define RAFT_H
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "raftRPC.pb.h"
#include "util.h"
#include "config.h"
//#include "monsoon.h"            //协程相关
#include "ApplyMsg.h"           //应用消息类，
#include "Persister.h"          //持久化
#include "raftRpcUtil.h"        //提供rpc通信功能
#include "raftPersistDate.pb.h" 

constexpr int Disconnected = 0; // 方便网络分区的时候debug，网络异常的时候为disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;

//节点处于什么投票状态
constexpr int Killed = 0;       //挂了
constexpr int Voted = 1;        //本轮投过票了
constexpr int Expire = 2;       //投票（消息、竞选者）过期
constexpr int Normal = 3;       //? 正常啥意思，未投票？


//重点关注成员变量的作用，成员函数很多都是辅助功能
class Raft : public raftRpcProctoc::raftRpc {
private:
  enum Status { Follower, Candidate, Leader };
  Status m_status;
  std::mutex m_mtx;
  std::condition_variable m_cv;
  std::condition_variable m_applyCond;

  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers; //与其他结点通信的rpc入口
  std::shared_ptr<Persister> m_persister;            //持久化
  int m_me;                                          //节点编号
  int m_currentTerm;                                 //当前term
  int m_votedFor;                                    //当前term给谁投票过
  std::vector<raftRpcProctoc::LogEntry> m_logs;      //日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

  // 这两个状态所有结点都在维护，易失
  int m_commitIndex;                                 //已经提交的最高日志索引
  int m_lastApplied;                                 //已经应用到状态机的索引

  // 这两个状态是由服务器来维护；这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
  //在日志复制过程中，Leader 会根据 m_nextIndex 向 Follower 发送相应的日志条目，并更新 m_nextIndex 的值。
  //如果 Follower 返回的响应指示该日志条目已经成功复制，Leader 会将 m_nextIndex 向前推进；
  //如果 Follower 返回日志不匹配错误，Leader 会将 m_nextIndex 向后回退，直到找到匹配的位置。
  std::vector<int> m_nextIndex;                      //记录每个 Follower 节点下一条需要发送的日志条目索引。
  std::vector<int> m_matchIndex;                     //记录每个 Follower 节点已经复制的最高日志条目索引。即同步了多少条了

  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;                     //client从这里取日志，client与raft通信的接口
  std::chrono::_V2::system_clock::time_point m_lastResetElectionTime; //记录上一次选举的时间点
  std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime; //记录上一次发送心跳的时间点

  int m_lastSnapshotIncludeIndex;  //记录快照中的最后一个日志的Index
  int m_lastSnapshotIncludeTerm;   //记录快照中的最后一个日志的Term

  //std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;  //协程

public:

  //初始化
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

  //=========Leader选举流程=========//
  void electionTimeOutTicker(); // 定期检查选举超时
  void doElection();            //发起选举
  bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);  //请求其他结点的投票
  void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);        //拉票请求

  //=========日志复制|心跳函数=========//
  void leaderHearBeatTicker();              //检查是否需要发起心跳：先检查可是leader,再算时间
  void doHeartBeat();                       //leader定时发起心跳
  bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums); //Leader发送心跳后，对心跳的回复进行对应的处理
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);      //收到心跳后的回应


  //=========恒久化函数==============//
  void persist();                           //持久化调用
  void readPersist(std::string data);       //从持久化中恢复数据
  std::string persistData();                //持久化数据
  int getRaftStateSize();                   //获取持久化数据的大小

  //==========辅助函数===============//
  void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);       //获取最后一个日志的index和term
  int getLastLogIndex();                                                  //获取最后一个日志的index
  int getLastLogTerm();                                                   //获取最后一个日志的term 
  bool UpToDate(int index, int term);                                     //判断当前节点是否含有最新的日志
  void getState(int *term, bool *isLeader);                               //看当前节点是否是leader
  int getLogTermFromLogIndex(int logIndex);                               //根据给定的日志索引logIndex，返回该索引对应的日志条目的任期term
  int getSlicesIndexFromLogIndex(int logIndex);                           //设计快照之后logIndex不能与在日志中的数组下标相等了，根据logIndex找到其在日志数组中的索引
  void getPrevLogInfo(int server, int *preIndex, int *preTerm);           //为指定的服务器server获取需要发送的前一条日志的索引preIndex和任期preTerm
  int getNewCommandIndex();                                               //获取新命令应该分配的Index
  bool matchLog(int logIndex, int logTerm);                               //对应index的日志是否匹配，只需要Index和Term就可以知道是否匹配
  void leaderSendSnapShot(int server);                                    //leader发送快照信息给follower
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                      raftRpcProctoc::InstallSnapshotResponse *reply);    //接受leader的快照信息更新自身日志
  void leaderUpdateCommitIndex();                                         //leader更新commitIndex
  void Snapshot(int index, std::string snapshot);                         //应用快照
  bool CondInstallSnapshot(int lastIncludedTerm, 
                          int lastIncludedIndex, std::string snapshot);   //和Snapshot功能相同
  std::vector<ApplyMsg> getApplyLogs();                                   //获取待应用的日志条目，将已经提交但尚未应用到状态机的日志条目打包成 ApplyMsg 消息
  void pushMsgToKvServer(ApplyMsg msg);                                   //给上层的kvserver层发送消息
  void applierTicker();                                                   //定期向状态机写入日志，非重点
  // rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread
  // ，避免使用pthread_create，因此专门写一个函数来执行

  //==========重写基类的rpc方法,因为rpc远程调用真正调用的是该方法==========//
  //其实就是调一下本地的方法，然后done->run()
  void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
  void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest *request,
                       ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
  void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;



  void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);
  // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
  // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
  // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
  



};

#endif  // RAFT_H