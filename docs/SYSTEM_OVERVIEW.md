# System Overview

## 端到端流程

1. 飞书网页登录并保存会话
2. 发现/补抓核心群消息
3. 将消息、文档和批次信息写入本地数据库
4. 做覆盖检查与补采判断
5. 生成日报/周报 digest
6. 改写为管理层版本
7. 渲染 CEO PDF

## 目录分工

- `main-system/bydfi-audit-bot`
  报告生产与渲染主系统。
- `lark-collector`
  飞书侧的登录、抓取和摘要。
- `desktop-entrypoints`
  对日常使用者的直接入口。
- `codex-skills`
  在 Codex 中复用的技能。
