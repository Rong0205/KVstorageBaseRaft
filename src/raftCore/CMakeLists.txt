# 创建一个名为 mprpc 的库目标，暂时不指定源文件
add_library(mraftCore "")

# 将指定的源文件添加到 mprpc 目标中
target_sources(mraftCore PRIVATE
    kvServerRPC.pb.cc
    raftPersistDate.pb.cc
    raftRPC.pb.cc
    raft.cc
    Persister.cc
    raftRpcUtil.cc
    util.cc
)

# 链接依赖库
target_link_libraries(mraftCore mprpc muduo_net muduo_base pthread protobuf)