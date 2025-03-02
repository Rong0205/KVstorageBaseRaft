#include <iostream>
#include <string>
#include <cassert>
#include "Persister.h" // 包含 Persister 类的头文件
#include "raft.h"

void testPersister() {
    //Persister persister(1); // 假设节点 ID 为 1
    auto persister = std::make_shared<Persister>(1);
    Raft mraft;
    raftRpcProctoc::LogEntry log1;
    log1.set_command("test command1");
    log1.set_logindex(1);
    log1.set_logterm(1);

    raftRpcProctoc::LogEntry log2;
    log2.set_command("test command2");
    log2.set_logindex(2);
    log2.set_logterm(2);

    mraft.init(std::vector<std::shared_ptr<RaftRpcUtil>>(), 1, persister, std::shared_ptr<LockQueue<ApplyMsg>>(new LockQueue<ApplyMsg>()));
    //mraft.m_logs.push_back(log1);
    //mraft.m_logs.push_back(log2);
    //std::cout<<mraft.m_logs.size()<<std::endl;

    mraft.persist();


}

int main() {
    testPersister();
    return 0;
}