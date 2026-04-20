@echo off
chcp 65001 >nul
echo 开始执行飞书群消息采集...
python "%~dp0external_group_collector.py" collect --hours 96 --headed
pause
