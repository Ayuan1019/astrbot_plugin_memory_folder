# astrbot_plugin_memory_folder

给 AstrBot 用的**长期记忆**插件：把聊天记录定期蒸馏成记忆文件，聊天前按需召回，还会记住"两个人之间发生过什么"。

## 它解决什么

- 机器人聊两天就忘：每次对话都像第一次见
- 只记得"你是什么样的人"，不记得"我们之间发生过什么"
- 关键词搜不到：问"你还记得我吗"永远召回不到东西

## 三层记忆

| 层 | 内容 | 说明 |
|---|---|---|
| 档案 | 特点 / 判断 / 状态 | 定期蒸馏出来的人物画像 |
| 事件线 | 约定、转折、冲突、玩笑 | 有日期，能回答"我们约定了什么" |
| 原话 | 原始对话 | 按日期或关键词直接翻出来 |

## 复式检索

```
关键词（中文 2 字切片）  ┐
向量（bge-small-zh，本地）├─ RRF 融合 → 取前 N 条注入
时间（"10月27号""昨天"） ┘
```

- 向量模型可选：没有模型就自动降级成关键词 + 时间两路
- 模型放 `data/memory/vec_model/`，支持 `bge-small-zh-v1.5` 的 ONNX int8 版
- 全程本地，不调用任何 embedding API

## 跨群记忆

记忆跨群共享，但每行都带来源标签（`YU-GI-OH·某人` / `私聊·某人`），
所以它分得清"这话是在哪个群听到的"，不会把私聊内容当群公告。

## 安装

```bash
cd AstrBot/data/plugins
git clone <本仓库地址>
# 重启 AstrBot
```

可选：装向量依赖

```bash
pip install onnxruntime tokenizers numpy
```

## 配置

在 AstrBot 面板的插件配置里改，或直接改 `data/memory/config.json`：

| 键 | 说明 |
|---|---|
| `distill_min_msgs` | 攒多少条触发蒸馏 |
| `group_names` | 会话 id → 群名，填了才显示群名 |
| `owner_names` | 哪些人的档案写进全局 |
| `event_keys` | 哪些会话额外提炼事件线 |
| `enable_vector` | 是否启用向量 |

## 命令

| 命令 | 作用 |
|---|---|
| `/记忆` | 看当前会话存了什么 |
| `/回忆 <关键词>` | 手动搜一次 |
| `/整理` | 立刻蒸馏 |

## 数据位置

```
AstrBot/data/memory/
├── <会话id>/
│   ├── people/   人物档案
│   ├── daily/    每天的梗
│   └── events.md 事件线
└── _global/      跨群共享
```

## 依赖

- 必须：AstrBot >= 4.11
- 可选：`onnxruntime` `tokenizers` `numpy`（向量检索用）

## License

MIT
