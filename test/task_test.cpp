// client.cpp
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

void phase_2_task() {
    std::cout << "phase 2 started..." << std::endl;
    sleep(5);  // 模拟C++耗时任务
    std::cout << "phase 2 completed." << std::endl;
}

void phase_3_task() {
    std::cout << "phase 3  started..." << std::endl;
    sleep(5);  // 模拟C++耗时任务
    std::cout << "phase 3  completed." << std::endl;
}

int main() {
    int client_fd;
    struct sockaddr_in server_address;
    char buffer[1024] = {0};

    // 创建套接字
    if ((client_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cerr << "Socket creation error" << std::endl;
        return -1;
    }

    // 服务器地址配置
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(8080);

    // 将IP地址转换为二进制形式
    if (inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) <= 0) {
        std::cerr << "Invalid address/ Address not supported" << std::endl;
        return -1;
    }

    // 连接到服务器
    if (connect(client_fd, (struct sockaddr *)&server_address, sizeof(server_address)) < 0) {
        std::cerr << "Connection failed" << std::endl;
        return -1;
    }
    phase_2_task();
    // 等待来自服务器的信号
    read(client_fd, buffer, 1024);
    std::cout << "Message from server: " << buffer << std::endl;

    // 执行C++任务
    phase_3_task();

    // 向服务器发送任务完成信号
    const char *message = "C++ task completed";
    send(client_fd, message, strlen(message), 0);

    // 关闭套接字
    close(client_fd);

    return 0;
}
