::# 生成 Python 代码
protoc --python_out=../src/fetchdata/message --pyi_out=../src/fetchdata/message methodid.proto -I=.

::# 生成 Python + gRPC 代码
::#protoc --python_out=. --grpc_python_out=. -I=. example.proto