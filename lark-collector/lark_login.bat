@echo off
chcp 65001 >nul
echo 正在打开 Lark 网页，请在浏览器里完成登录（扫码或账号密码）...
echo 最多等待 180 秒，检测到你真正进入 messenger 后才会保存登录状态。
python external_group_collector.py login --wait-seconds 180
pause
