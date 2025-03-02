# 创建一个名为 mprpc 的库目标，暂时不指定源文件
add_library(mprpc "")

# 将指定的源文件添加到 mprpc 目标中
target_sources(mprpc PRIVATE
    rpcheader.pb.cc
    rpcprovider.cc
    mprpcchannel.cc
    mprpccontroller.cc
    mprpcconfig.cc
)

# 链接依赖库
target_link_libraries(mprpc muduo_net muduo_base pthread)