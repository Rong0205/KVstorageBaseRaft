#set(SRC_LIST userservice.cc)

set(SRC_LIST test.cc)

add_executable(test ${SRC_LIST})
target_link_libraries(test mraftCore mprpc protobuf)