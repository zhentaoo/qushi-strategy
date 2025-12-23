#!/usr/bin/env bash

# 中文版运行脚本：在 Ubuntu 服务器上用 crontab 管理定时任务
# 功能：
# 1）每次执行本脚本，先杀掉当前运行的两个 Python 任务
# 2）清理旧的 crontab 条目并重新写入新的定时规则
# 3）日志输出到 logs/ 目录
# 定时规则：
# - 每小时的 执行 s1_runtime_dapan.py
# - 每小时的 执行 s1_runtime.py（通过 sleep 20 实现秒级偏移）

set -e

# 固定工作目录（脚本与代码所在目录）
DIR="/data/qushi-strategy"

# 固定使用服务器上的 python3 路径
PYTHON_BIN="/usr/bin/python3"

# 固定日志目录（与工作目录一致）
LOG_DIR="$DIR"

echo "[run.sh] 工作目录: $DIR"
echo "[run.sh] Python 路径: $PYTHON_BIN"
echo "[run.sh] 日志目录: $LOG_DIR"

# 创建日志目录（固定路径）
mkdir -p "$LOG_DIR"

# 杀掉正在运行的目标脚本进程（不使用被禁的 pkill/killall）
SCRIPTS=("s1_runtime_open.py" "s1_runtime_dapan.py" "s1_runtime_close.py")
for script in "${SCRIPTS[@]}"; do
  PIDS=$(ps -ef | grep -E "[p]ython3 .*${script}" | awk '{print $2}' || true)
  if [ -n "${PIDS:-}" ]; then
    echo "[run.sh] 结束进程: $script (PID: $PIDS)"
    kill ${PIDS} || true
    sleep 1
    for pid in $PIDS; do
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" || true
      fi
    done
  else
    echo "[run.sh] 未发现运行中的 $script"
  fi
done

echo "[run.sh] 刷新 crontab 任务"

# 读取当前 crontab，并移除旧条目
CURRENT_CRON=$(crontab -l 2>/dev/null || true)
FILTERED_CRON=$(printf "%s\n" "$CURRENT_CRON" | grep -v -E 's1_runtime\.py|s1_runtime_dapan\.py|s1_guard\.py' || true)

# 新的 crontab 条目（变量在写入前展开）
read -r -d '' NEW_ENTRIES <<EOF || true
# === 交易策略任务（由 $DIR/run.sh 于 $(date) 安装） ===

# 每个整小时：运行大盘脚本，拉取数据
0  * * * * sleep 10 && cd "$DIR" && $PYTHON_BIN s1_runtime_dapan.py >> "$LOG_DIR/s1_runtime_dapan.py.log" 2>&1

# 每个整小时：运行策略脚本，执行交易
1  * * * * cd "$DIR" && $PYTHON_BIN s1_runtime_open.py >> "$LOG_DIR/s1_runtime_open.log" 2>&1

# 每20s：运行止损守护脚本
* * * * * cd "$DIR" && $PYTHON_BIN s1_runtime_close.py >> "$LOG_DIR/s1_runtime_close.log" 2>&1
* * * * * sleep 20 && cd "$DIR" && $PYTHON_BIN s1_runtime_close.py >> "$LOG_DIR/s1_runtime_close.log" 2>&1
* * * * * sleep 40 && cd "$DIR" && $PYTHON_BIN s1_runtime_close.py >> "$LOG_DIR/s1_runtime_close.log" 2>&1
# === 结束 ===
EOF

# 安装新的 crontab
printf "%s\n%s\n" "$FILTERED_CRON" "$NEW_ENTRIES" | crontab -

echo "[run.sh] 已更新 crontab。"
crontab -l | sed -n '1,200p'

echo "[run.sh] 完成。日志将写入 $LOG_DIR/"