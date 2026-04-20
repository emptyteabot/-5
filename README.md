# BYDFI Management Reporting System

公开可交接版仓库。

这份仓库保留了“飞书群消息抓取 -> 覆盖检查 -> 日报/周报生成 -> CEO PDF 渲染”的完整系统代码、脚本、技能和运行说明，同时去掉了不适合公开仓库的敏感资产。

## 仓库结构

- `main-system/bydfi-audit-bot/`
  日报/周报主系统。
- `desktop-entrypoints/`
  一键运行入口和技能包。
- `lark-collector/`
  飞书网页登录、群消息抓取、摘要和 webhook 发送相关核心代码。
- `codex-skills/`
  直接参与日报/周报生成的 Codex 技能。
- `docs/`
  架构、运行顺序、数据库结构和敏感资产说明。

## 公开仓库里刻意不包含的内容

- 飞书登录态
- 真实群配置与 chat_id
- 原始聊天数据库数据
- 内部生成报告正文与 PDF 成品
- webhook、密钥、`.env`

这些内容在本机的正式交付包中单独保留，不放进公开仓库。

## 快速开始

1. 阅读 `docs/SETUP.md`
2. 在 `lark-collector/` 下准备环境变量并重新登录飞书
3. 在 `main-system/bydfi-audit-bot/config/` 下补入内部版 `lark_group_registry.json`
4. 使用 `desktop-entrypoints/` 里的脚本跑日报、周报和 PDF

## 说明

如果要给公司做完整交接：

- 公开 GitHub：使用本仓库
- 完整私有资产：使用本机导出的正式交付包
