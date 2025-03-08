#!/bin/bash

# 配置参数
TEST_COUNT=1000               # 总测试次数
TEST_COMMAND="go test -run 3A -race"  # 测试命令（启用竞态检测）
FAIL_LOG="raft_test_fail.log" # 失败日志文件
SUCCESS_COUNT=0               # 成功计数器
FAIL_COUNT=0                  # 失败计数器

# 清空旧日志
> "$FAIL_LOG"

echo "Starting $TEST_COUNT iterations of Raft Lab 3A tests..."
for ((i=1; i<=TEST_COUNT; i++)); do
    echo -n "Run $i: "

    # 执行测试并捕获输出和退出码
    output=$($TEST_COMMAND 2>&1)
    exit_code=$?

    # 判断结果
    if [ $exit_code -eq 0 ]; then
        echo "PASS"
        ((SUCCESS_COUNT++))
    else
        echo "FAIL"
        ((FAIL_COUNT++))
        # 记录失败日志
        echo "===== Failed Run $i =====" >> "$FAIL_LOG"
        echo "$output" >> "$FAIL_LOG"
        echo >> "$FAIL_LOG"
    fi
done

# 输出统计结果
echo
echo "===== Stress Test Summary ====="
echo "Total Runs:   $TEST_COUNT"
echo "Success:      $SUCCESS_COUNT"
echo "Failure:      $FAIL_COUNT"
echo "Failure Rate: $(( (FAIL_COUNT * 100) / TEST_COUNT ))%"
echo "Failure Log:  $FAIL_LOG"