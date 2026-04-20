# Setup

## 1. Python 依赖

- 安装 `main-system/bydfi-audit-bot/requirements.txt`
- 安装 `lark-collector/requirements.txt`

## 2. 飞书抓取器

1. 进入 `lark-collector/`
2. 复制 `.env.example` 为本地环境配置
3. 运行 `lark_login.bat` 或：

```bash
python external_group_collector.py login
```

登录成功后，登录态会写入 `lark-collector/data/playwright/`。

## 3. 群配置

仓库里只有公开占位版 `main-system/bydfi-audit-bot/config/lark_group_registry.json`。

接手后请用内部真实版本替换它。

## 4. 日报/周报入口

- `desktop-entrypoints/01_跑今日CEO日报.cmd`
- `desktop-entrypoints/02_跑本周CEO周报.cmd`
- `desktop-entrypoints/03_强刷8个核心群并重跑本周CEO周报.cmd`
- `desktop-entrypoints/04_检查Lark覆盖.cmd`
- `desktop-entrypoints/05_渲染当前终版PDF.cmd`

## 5. GitHub 版限制

公开仓库没有附带原始数据库和正式输出。
如果需要完整数据，请从私有交付包恢复。
