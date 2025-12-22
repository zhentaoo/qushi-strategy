# 查看日志
tail -f nohup.out
tail -f runtime.log
tail -f s1_runtime.log


<!-- 刷新 crontab 任务 -->
chmod +x run.sh


查看当前用户的定时任务
crontab -l

编辑当前用户的定时任务
crontab -e	


删除当前用户的所有定时任务
crontab -r	