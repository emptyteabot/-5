# 外部群方案

## 现实边界

- 外部群不能按你现在这套企业应用 webhook 方式直接监听全群。
- 自定义机器人适合发消息，不适合替代企业应用做被动全量读群。
- 如果成员不会统一 `@机器人` 或手动触发，唯一可控方案就是浏览器采集。

## 当前实现

保留原有中台：

- `storage.py`：消息持久化
- `auditor.py`：锐评分析
- `group_summary.py`：按时间窗口汇总
- `group_report_artifacts.py`：文本与 JSON 报告落盘

新增外部群采集层：

- `external_group_collector.py`
- `custom_bot_sender.py`

## 推荐执行顺序

### 1. 本地登录一次，保存 Lark Web 会话

```bash
python external_group_collector.py login
```

成功后会生成登录态文件：

```text
data/playwright/lark_storage_state.json
```

### 2. 只读采集外部群，先本地落盘文本与 JSON

```bash
python external_group_collector.py collect \
  --group-url "你的外部群网页地址" \
  --hours 72 \
  --inspect-output data/playwright/external_group_nodes.json
```

默认行为：

- 只读群消息
- 生成 Markdown / JSON 报告
- 不自动回群

输出会包含：

- `Report text path`
- `Report json path`
- `Ingested`
- `People`

### 3. 确认 HTML 效果后，再决定是否回发到外部群

如果外部群里已经加了自定义机器人 webhook：

```bash
python external_group_collector.py collect \
  --group-url "你的外部群网页地址" \
  --hours 72 \
  --send-custom-bot
```

## 云服务器运行

外部群方案想跑 24h，别自欺。你还要处理：

- Playwright 依赖安装
- 浏览器安装
- 登录态续期
- Cookie 过期
- DOM 结构变化

最稳的做法不是直接在云上扫码登录，而是：

1. 本地跑 `login`
2. 把 `data/playwright/lark_storage_state.json` 传到服务器
3. 服务器只跑 `collect`

## 先别做的事

- 别一开始就自动发群
- 别一开始就定时 24h
- 别一开始就假设 DOM 永远不变

先跑通：

`读取 -> 入库 -> 汇总 -> 文本落盘`

然后再接：

`自定义机器人发文本 -> 定时任务`
