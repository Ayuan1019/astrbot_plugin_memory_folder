"""长期记忆文件夹插件（按群隔离）。

- 每个会话（群/私聊）一套独立记忆：memory/<会话ID>/daily|people|topics
- 定时蒸馏：按会话分别攒消息、分别蒸馏
- 请求前只检索本会话的记忆
- 上下文过长时压缩旧消息
- 上线回看：挑本会话里值得接的话（含未回复的）
"""

import asyncio
import json
import os
import re
import sqlite3
import time
from datetime import datetime

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.provider.entities import ProviderRequest

def _guess_astrbot_root() -> str:
    """猜 AstrBot 安装目录：优先环境变量，其次从本文件往上找。"""
    env = os.environ.get("ASTRBOT_ROOT")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    cur = here
    for _ in range(6):
        if os.path.isdir(os.path.join(cur, "data")) and \
                os.path.isfile(os.path.join(cur, "data", "data_v4.db")):
            return cur
        cur = os.path.dirname(cur)
    return os.getcwd()


ASTRBOT_ROOT = _guess_astrbot_root()
DB_PATH = os.environ.get("ASTRBOT_DB") or os.path.join(ASTRBOT_ROOT, "data", "data_v4.db")
MEM_ROOT = os.environ.get("MEMORY_ROOT") or os.path.join(ASTRBOT_ROOT, "data", "memory")

DISTILL_INTERVAL = 1800
DISTILL_MIN_MSGS = 300
DISTILL_BATCH = 300
DISTILL_DAILY_SEC = 86400
RECALL_TOP_K = 12

# 会话名映射，可在 config.json 的 group_names 里覆盖
GROUP_NAMES = {}


def _where(key: str) -> str:
    """把会话 id 换成人能认的名字。"""
    if key in GROUP_NAMES:
        return GROUP_NAMES[key]
    return f"群{key}"
GLOBAL_DIR = "_global"
TASTES_FILE = os.path.join(MEM_ROOT, "tastes.md")   # 她自己长出来的口味
OWNER_NAMES = ()                                    # 这些人的档案同时写全局
CONTEXT_SOFT_LIMIT = 130
STARTUP_SEND = os.environ.get("MEMORY_STARTUP_SEND", "0") == "1"
STARTUP_CONFIDENCE = 0.6
STARTUP_REPLY_MAX = 80


EVENT_PROMPT = """你在整理"我和大哥之间发生过的事"。只要事件，不要性格分析。

把下面的聊天记录里**真正发生过的事**列出来，只输出 JSON 数组，不要解释：
[
  {"date": "10-27", "what": "一句话说清发生了什么（25 字内）",
   "detail": "细一点：谁先提的、当时怎么说的、结果怎样（60 字内）",
   "kind": "约定/转折/玩笑/冲突/关心/告白/其他"}
]

什么算"事"：
- 他要求你做什么、你答应了什么、你拒绝了什么
- 你们之间的约定、承诺、"以后再说"
- 你改过名字、换过称呼、调过功能
- 他给你开了什么权限、造了什么家
- 吵过架、和好、被夸、被怼到说不出话
- 反复出现的玩笑、梗是怎么来的
- 他明确说过喜欢/讨厌什么（要带原话大意）

不要记：
- 单纯的知识问答（"ACE扫盘伤硬盘吗"这种不算）
- 他对 AI 的一般性讨论
- 没有下文的闲聊

规则：
- date 用记录里的日期（月-日）
- 一天里有几件就写几件
- 没有真事就输出 []
"""

PROMPT = """你在整理一个群聊的长期记忆。把下面的记录提炼成 JSON，只输出 JSON，不要解释。

核心原则：**记事不如记人，流水账不如判断**。
别人几点说了什么不重要；重要的是他是个什么样的人、你对他什么看法。

要求的 JSON 结构：
{
  "people": [
    {
      "name": "昵称",
      "traits": ["性格、说话风格、好恶（12 字内，越具体越好）"],
      "state": "他当下正在经历什么（20 字内，没有就留空）",
      "notes": "你对这个人的判断和印象（60 字内）",
      "sure": true 或 false
    }
  ],
  "topics": [{"name": "话题", "summary": "这事到哪一步了、你怎么看（50 字内）"}],
  "memes": ["群里的梗（15 字内）"],
  "tastes": ["你自己喜欢或讨厌的东西（15 字内，只写你自己的，不是别人的）"]
}

规则：
- **不要记流水账**：不写"几点谁说了什么"，只写你看出来的判断。
  例：不要写"22:30 老王说他加班到十点"，要写"老王常加班，抱怨但不辞职"。
- **性格和状态分开**：
  · traits 写长期的东西——"爱阴阳""说话冲""较真"
  · state 写短期的东西——"最近在低谷""刚失恋""在准备考试"
  这两样绝不能混在一起。爱阴阳不等于他过得不好，过得不好也不代表他阴阳。
- **拿不准就标 sure=false**：你只是猜的、信息不够的、可能是反讽的，都标 false。
- tastes 只记你自己的口味（喜欢什么歌、什么类型的人、讨厌什么），不记别人的。
- 不要把机器人自己（AstrBot / 阿渊 / 大肥鱼 / 小鲸鱼）写进 people。
- 每类最多 12 条，重要的放前面。traits 每人最多 8 条。
- 一个人的 traits 不要互相矛盾；矛盾说明你没看懂，就少写两条。
"""


# ---------------- 基础工具 ----------------

def _key_of(umo: str) -> str:
    """qq-napcat:GroupMessage:123456789 -> 123456789"""
    if not umo:
        return "unknown"
    return str(umo).split(":")[-1] or "unknown"



EVENT_KEYS = set()                  # 要做事件线的会话 id，可在配置里填


def _events_file(key: str) -> str:
    d = _mem_dir(key)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "events.md")


def _append_events(key: str, events) -> int:
    if not events:
        return 0
    path = _events_file(key)
    old = ""
    if os.path.isfile(path):
        old = open(path, encoding="utf-8").read()
    added = 0
    with open(path, "a", encoding="utf-8") as f:
        if not old:
            f.write("# 事件线：我和大哥之间发生过的事\n\n")
        for e in events:
            if not isinstance(e, dict):
                continue
            what = str(e.get("what", "")).strip()[:80]
            if not what:
                continue
            if what in old:
                continue
            detail = str(e.get("detail", "")).strip()[:160]
            date = str(e.get("date", "")).strip()[:10]
            kind = str(e.get("kind", "")).strip()[:6]
            f.write(f"- `{date}` **[{kind}]** {what} —— {detail}\n")
            added += 1
    return added

def _mem_dir(key: str) -> str:
    base = os.path.join(MEM_ROOT, key)
    for sub in ("daily", "people", "topics"):
        os.makedirs(os.path.join(base, sub), exist_ok=True)
    return base


def _migrate_legacy():
    """把老版本全局目录挪到主群目录下（只做一次）。"""
    legacy_daily = os.path.join(MEM_ROOT, "daily")
    if not os.path.isdir(legacy_daily):
        return
    target = os.path.join(MEM_ROOT, str(os.environ.get("MEMORY_MIGRATE_KEY", "")) or "default")
    try:
        for sub in ("daily", "people", "topics"):
            src = os.path.join(MEM_ROOT, sub)
            if not os.path.isdir(src):
                continue
            dst = os.path.join(target, sub)
            os.makedirs(dst, exist_ok=True)
            for fn in os.listdir(src):
                s = os.path.join(src, fn)
                if os.path.isfile(s):
                    os.replace(s, os.path.join(dst, fn))
            os.rmdir(src)
        logger.info(f"[memory] 旧记忆已迁移到 {target}")
    except Exception as e:
        logger.warning(f"[memory] 迁移旧记忆失败: {e}")


def _load_state():
    p = os.path.join(MEM_ROOT, "state.json")
    if os.path.exists(p):
        try:
            d = json.load(open(p, encoding="utf-8"))
            if "groups" not in d:
                d = {"groups": {}}
            return d
        except Exception:
            pass
    return {"groups": {}}


def _save_state(state):
    os.makedirs(MEM_ROOT, exist_ok=True)
    json.dump(state, open(os.path.join(MEM_ROOT, "state.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)


def _apply_cfg(c: dict):
    """把 config.json 里的设置套到模块变量上。"""
    global GROUP_NAMES, TASTES_FILE, OWNER_NAMES, EVENT_KEYS
    if isinstance(c.get("group_names"), dict):
        GROUP_NAMES = {str(k): str(v) for k, v in c["group_names"].items()}
    if c.get("tastes_file"):
        TASTES_FILE = str(c["tastes_file"])
    if isinstance(c.get("owner_names"), list):
        OWNER_NAMES = tuple(str(x) for x in c["owner_names"] if str(x).strip())
    if isinstance(c.get("event_keys"), list):
        EVENT_KEYS = {str(x) for x in c["event_keys"]}


def _load_cfg():
    p = os.path.join(MEM_ROOT, "config.json")
    if os.path.exists(p):
        try:
            return json.load(open(p, encoding="utf-8-sig"))
        except Exception:
            pass
    return {}


def _is_muted(umo: str) -> bool:
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "astrbot_plugin_mute", "mute_state.json")
    try:
        with open(p, encoding="utf-8") as f:
            return umo in json.load(f)
    except Exception:
        return False


def _extract_text(content_json) -> str:
    try:
        parts = json.loads(content_json)
    except Exception:
        return ""
    out = []
    for p in parts if isinstance(parts, list) else []:
        if isinstance(p, dict) and p.get("type") == "text":
            out.append(p.get("data", {}).get("text", ""))
    return " ".join(out).strip()


def _read_new_messages(key: str, last_id: int, limit: int):
    if not os.path.exists(DB_PATH):
        return [], last_id
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT id, created_at, sender_name, content FROM platform_message_history "
            "WHERE user_id = ? AND id > ? ORDER BY id ASC LIMIT ?",
            (key, last_id, limit),
        )
        rows = cur.fetchall()
    finally:
        con.close()

    msgs, max_id = [], last_id
    for rid, created, sender, content in rows:
        max_id = max(max_id, rid)
        text = _extract_text(content)
        if not text:
            continue
        text = re.sub(r"^\[[^\]]{6,40}\]\s*[^:：]{1,20}\(ID:\d+\):\s*", "", text).strip()
        if len(text) < 2:
            continue
        msgs.append({"time": str(created)[:19], "sender": sender or "?", "text": text[:300]})
    return msgs, max_id


def _active_keys(limit=30):
    if not os.path.exists(DB_PATH):
        return []
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT user_id, MAX(id) AS m FROM platform_message_history "
            "GROUP BY user_id ORDER BY m DESC LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        con.close()
    # 只要纯数字的群号，过滤 webchat 之类的 UUID
    return [r[0] for r in rows if str(r[0]).isdigit()]


def _append_unique(path: str, line: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        if line in open(path, encoding="utf-8").read():
            return False
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    return True


def _write_memory(key: str, data: dict):
    base = _mem_dir(key)
    today = datetime.now().strftime("%Y-%m-%d")
    daily = os.path.join(base, "daily", f"{today}.md")
    if not os.path.exists(daily):
        open(daily, "w", encoding="utf-8").write(f"# {today}\n\n")

    stamp = datetime.now().strftime("%H:%M")
    src = f"群{key}"
    for e in data.get("events", [])[:12]:
        _append_unique(daily, f"- `{stamp}` **{e.get('who','?')}**：{e.get('what','')}（{src}）")
    for mem in data.get("memes", [])[:12]:
        _append_unique(daily, f"- `{stamp}` 梗：{mem}（{src}）")

    for p in data.get("people", [])[:12]:
        name = re.sub(r"[\\/:*?\"<>|]", "_", str(p.get("name") or "未知"))[:24]
        if name.lower() in {"astrbot", "阿渊", "大肥鱼", "小鲸鱼", "胡桃", "bot"}:
            continue
        path = os.path.join(base, "people", f"{name}.md")
        if not os.path.exists(path):
            open(path, "w", encoding="utf-8").write(f"# {name}\n\n")
        for t in p.get("traits", [])[:8]:
            _append_unique(path, f"- 特点：{t}（{today} {src}）")
        if p.get("state"):
            _append_unique(path, f"- 状态（{today}，会过期）：{p['state']}")
        unsure = "（可能不对）" if p.get("sure") is False else ""
        if p.get("notes"):
            _append_unique(path, f"- 判断{unsure}：{p['notes']}（{today} {src}）")

        # 主人：同时写一份到全局，所有群都能看到
        if name in OWNER_NAMES:
            gpath = os.path.join(MEM_ROOT, GLOBAL_DIR, "people", f"{name}.md")
            os.makedirs(os.path.dirname(gpath), exist_ok=True)
            if not os.path.exists(gpath):
                open(gpath, "w", encoding="utf-8").write(f"# {name}\n\n")
            for t in p.get("traits", [])[:10]:
                _append_unique(gpath, f"- 特点：{t}（{today} {src}）")
            if p.get("notes"):
                _append_unique(gpath, f"- 记事：{p['notes']}（{today} {src}）")

    # 自己的口味：只追加，永不覆盖
    tastes = data.get("tastes") or []
    if tastes:
        try:
            old = open(TASTES_FILE, encoding="utf-8").read() if os.path.exists(TASTES_FILE) else "# 我的口味\n\n"
            add = []
            for t in tastes[:6]:
                line = f"- {t}（{today}）"
                if t not in old:
                    add.append(line)
            if add:
                open(TASTES_FILE, "w", encoding="utf-8").write(old.rstrip() + "\n" + "\n".join(add) + "\n")
        except Exception:
            pass

    for t in data.get("topics", [])[:12]:
        name = re.sub(r"[\\/:*?\"<>|]", "_", str(t.get("name") or "杂项"))[:24]
        path = os.path.join(base, "topics", f"{name}.md")
        if not os.path.exists(path):
            open(path, "w", encoding="utf-8").write(f"# {name}\n\n")
        _append_unique(path, f"- `{today}` {t.get('summary','')}（{src}）")


def _all_group_keys():
    """所有群目录（纯数字）+ _global。"""
    keys = []
    try:
        for name in os.listdir(MEM_ROOT):
            p = os.path.join(MEM_ROOT, name)
            if os.path.isdir(p) and (name.isdigit() or name == GLOBAL_DIR):
                keys.append(name)
    except Exception:
        pass
    return keys


def _iter_memory_lines(key: str):
    base = os.path.join(MEM_ROOT, key) if key else os.path.join(MEM_ROOT, GLOBAL_DIR)
    for sub in ("daily", "people", "topics"):
        d = os.path.join(base, sub)
        if not os.path.isdir(d):
            continue
        for fn in sorted(os.listdir(d), reverse=True)[:30]:
            if not fn.endswith(".md"):
                continue
            try:
                for line in open(os.path.join(d, fn), encoding="utf-8"):
                    line = line.strip()
                    if line.startswith("- "):
                        yield fn, line[2:].strip()
            except Exception:
                continue


def _keywords(text: str):
    text = re.sub(r"[\s，。！？、,.!?~…:：;；\"'（）()\[\]]+", " ", text)
    grams = set()
    for chunk in text.split():
        if 2 <= len(chunk) <= 8:
            grams.add(chunk)
        if len(chunk) > 8:
            for i in range(len(chunk) - 2):
                grams.add(chunk[i:i + 3])
    return grams



DATE_PATTERNS = [
    (r"(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})", "full"),
    (r"(?<!\d)(\d{1,2})[-/.月](\d{1,2})(?!\d)", "md"),
]


def _parse_day(text: str):
    """从问句里认出日期，返回 (起, 止) 的 UTC 字符串，认不出返回 None。"""
    from datetime import datetime as _dt, timedelta as _td
    now = _dt.now()
    if "前天" in text:
        d = (now - _td(days=2)).date()
    elif "昨天" in text or "昨晚" in text:
        d = (now - _td(days=1)).date()
    elif "今天" in text or "今晚" in text or "刚才" in text:
        d = now.date()
    else:
        d = None
        for pat, kind in DATE_PATTERNS:
            m = re.search(pat, text)
            if m:
                try:
                    if kind == "full":
                        y, mo, da = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    else:
                        y, mo, da = now.year, int(m.group(1)), int(m.group(2))
                        if mo > 12:
                            mo, da = da, mo
                    if mo > 12:
                        continue
                    d = _dt(y, mo, da).date()
                    break
                except Exception:
                    continue
    if not d:
        return None
    # 北京时间当天 0 点，转 UTC
    start = _dt(d.year, d.month, d.day) - _td(hours=8)
    end = start + _td(days=1)
    return (start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"))


def _grams(text: str, n: int = 2):
    """中文按 2 字切，去掉纯虚词的碎片。"""
    t = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", text or "")
    stop = {"能不", "不能", "我们", "什么", "怎么", "这个", "那个", "一下",
            "现在", "就是", "还是", "可以", "你说", "说什", "了吗", "时候"}
    out = []
    for i in range(len(t) - n + 1):
        g = t[i:i + n]
        if g in stop:
            continue
        out.append(g)
    return out



def _rrf(lists, k=60, limit=20):
    """倒数排名融合：多路结果合起来排序。"""
    score = {}
    payload = {}
    for lst in lists:
        for rank, item in enumerate(lst):
            key = item if isinstance(item, str) else item[0]
            score[key] = score.get(key, 0.0) + 1.0 / (k + rank + 1)
            payload[key] = item
    ranked = sorted(score.items(), key=lambda x: -x[1])
    return [payload[k] for k, _ in ranked[:limit]]


def _vec_down():
    """向量检索能不能用。"""
    try:
        import sys as _s
        _d = os.path.dirname(os.path.abspath(__file__))
        if _d not in _s.path:
            _s.path.insert(0, _d)
        import vec as _v
        return _v
    except Exception as e:
        logger.warning(f"[memory] 向量不可用: {e}")
        return None


def _hybrid_recall(key: str, query: str, top: int = 18):
    """三路召回：关键词、向量、时间(原话)。返回 (记忆行, 原话行)。"""
    kw_mem = _recall(key, query, RECALL_TOP_K)
    kw_raw = _recall_raw(key, query, 8)

    v = _vec_down()
    v_mem, v_raw = [], []
    if v:
        try:
            for sim, src, ky, label in v.search(query, 8, key=None):
                if sim < 0.55:
                    continue
                if src == "memory":
                    v_mem.append(f"{label}")
                elif src == "raw":
                    v_raw.append(label)
        except Exception as e:
            logger.warning(f"[memory] 向量检索失败: {e}")

    mem = _rrf([kw_mem, v_mem], limit=top)
    raw = _rrf([kw_raw, v_raw], limit=min(12, top))
    return mem, raw


def _recall_raw(key: str, query: str, limit: int = 14):
    """翻原始聊天记录，返回带时间和说话人的原话。"""
    from datetime import timedelta
    text = (query or "").strip()
    if not text:
        return []
    span = _parse_day(text)

    sql = ("SELECT created_at, sender_name, content FROM platform_message_history "
           "WHERE user_id=? ")
    args = [str(key)]
    if span:
        sql += "AND created_at >= ? AND created_at < ? ORDER BY id DESC LIMIT 120"
        args += [span[0], span[1]]
        scored_rows = None
    else:
        sql += "AND created_at > datetime('now','-10 day') ORDER BY id DESC LIMIT 3000"
        scored_rows = _grams(text)

    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)
    try:
        rows = con.execute(sql, args).fetchall()
    except Exception as e:
        logger.warning(f"[memory] 原话检索失败: {e}")
        return []
    finally:
        con.close()

    picked = []
    for c, sender, content in rows:
        try:
            body = json.loads(content)[0]["data"]["text"].strip()
        except Exception:
            continue
        body = body.replace("\n", " ").strip()
        if not body or len(body) < 2:
            continue
        try:
            dt = datetime.strptime(c[:19], "%Y-%m-%d %H:%M:%S") + timedelta(hours=8)
        except Exception:
            continue
        line = f"[{dt.strftime('%m-%d %H:%M')}] {sender}: {body[:120]}"
        if scored_rows:
            score = sum(1 for g in scored_rows if g in body)
            if score <= 0:
                continue
            picked.append((score, line))
        else:
            if line not in [x for x in picked]:
                picked.append(line)
        if len(picked) >= limit * 3:
            break

    if scored_rows:
        picked.sort(key=lambda x: -x[0])
        out = [line for score, line in picked[:limit]]
    else:
        out = picked[:limit]
    return out


def _recall(key: str, query: str, k: int = RECALL_TOP_K):
    kws = _keywords(query)
    if not kws:
        return []
    scored = []
    # 本群命中权重 ×2，但其他群不再隔断，一起参与
    for other in _all_group_keys():
        weight = 2 if other == key else 1
        for fn, line in _iter_memory_lines(other):
            score = sum(1 for kw in kws if kw in line)
            if score:
                scored.append((score * weight, fn, line, other))
    scored.sort(key=lambda x: -x[0])
    out = []
    for score, fn, line, src in scored[:k]:
        if src == GLOBAL_DIR:
            tag = "全局"
        elif src == key:
            tag = fn.replace(".md", "")
        else:
            tag = f"{_where(src)}·{fn.replace('.md', '')}"
        warn = "｜她标了可能不准" if ("可能不对" in line or "可能不准" in line) else ""
        warn += "｜状态会过期" if "会过期" in line else ""
        out.append(f"{line}（{tag}{warn}）")
    # 关于主人自己的档案，永远带两条上下文，不管有没有命中关键词
    if kws:
        owner_path = os.path.join(MEM_ROOT, GLOBAL_DIR, "people", f"{OWNER_NAMES[0]}.md")
        if os.path.isfile(owner_path):
            lines = [l.strip() for l in open(owner_path, encoding="utf-8") if l.startswith("-")]
            import re as _re
            for l in lines[-4:]:
                clean = _re.sub(r"（\d{4}-\d{2}-\d{2}[^）]*）", "", l).strip()
                item = f"{clean}（全局·{OWNER_NAMES[0]}）"
                if item not in out:
                    out.append(item)
    # 自己的口味常驻：避免被别的内容挤掉
    if kws:
        try:
            tl = [l.strip() for l in open(TASTES_FILE, encoding="utf-8")
                  if l.strip().startswith("-")]
            for line in tl[-2:]:
                item = f"{line}（我的口味）"
                if item not in out:
                    out.append(item)
        except Exception:
            pass
    return out


@register(
    "astrbot_plugin_memory_folder",
    "local",
    "长期记忆文件夹（按会话隔离）：定时蒸馏聊天记录，请求前按关键词回忆",
    "1.1.0",
)
class MemoryFolderPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        os.makedirs(MEM_ROOT, exist_ok=True)
        _migrate_legacy()
        self.state = _load_state()
        self._task = asyncio.create_task(self._loop())
        self._review_task = asyncio.create_task(self._startup_review())
        self._persona = ""
        cfg = _load_cfg()
        self.send_enabled = bool(cfg.get("startup_send", False)) or STARTUP_SEND
        logger.info(f"[memory] 记忆目录: {MEM_ROOT}（跨群共享，压缩阈值={CONTEXT_SOFT_LIMIT} 条，保留最近 45 条，上线回看真发={self.send_enabled}）")

    async def _force_distill_once(self):
        """手动触发一次蒸馏（有个标记文件控制，跑一次就删）。"""
        flag = os.path.join(os.path.dirname(os.path.abspath(__file__)), "force_distill.flag")
        if not os.path.exists(flag):
            return
        try:
            os.remove(flag)
        except Exception:
            pass
        logger.info("[memory] 收到手动蒸馏指令，等 45 秒让 provider 就绪")
        await asyncio.sleep(45)
        provider = self.context.get_using_provider()
        logger.info(f"[memory] provider 状态: {type(provider).__name__ if provider else 'None'}")
        try:
            await self._distill_all()
            logger.info("[memory] 手动蒸馏完成")
        except Exception as e:
            logger.warning(f"[memory] 手动蒸馏失败: {e}")

    async def _loop(self):
        asyncio.create_task(self._force_distill_once())
        asyncio.create_task(self._export_on_start())
        await asyncio.sleep(60)
        while True:
            cfg = _load_cfg()
            interval = int(cfg.get("distill_interval_sec", DISTILL_INTERVAL))
            try:
                await self._distill_all()
            except Exception as e:
                logger.warning(f"[memory] 蒸馏失败: {e}")
            await asyncio.sleep(interval)

    async def _distill_all(self):
        cfg = _load_cfg()
        min_msgs = int(cfg.get("distill_min_msgs", DISTILL_MIN_MSGS))
        batch = int(cfg.get("distill_batch", DISTILL_BATCH))
        daily_sec = int(cfg.get("distill_daily_sec", DISTILL_DAILY_SEC))

        provider = self.context.get_using_provider()
        if not provider:
            return

        for key in _active_keys():
            st = self.state["groups"].setdefault(key, {"last_id": 0, "last_distill": 0})
            msgs, max_id = _read_new_messages(key, int(st.get("last_id", 0)), batch)
            if not msgs:
                continue

            since = time.time() - int(st.get("last_distill", 0) or 0)
            daily_due = since >= daily_sec
            if len(msgs) < min_msgs and not daily_due:
                logger.info(f"[memory] 会话 {key}: {len(msgs)}/{min_msgs} 条，未达标（兜底还有 {daily_sec/3600:.0f}h）")
                continue

            lines = "\n".join(f"[{m['time']}] {m['sender']}: {m['text']}" for m in msgs)
            try:
                resp = await provider.text_chat(
                    prompt=PROMPT + "\n\n聊天记录：\n" + lines,
                    system_prompt="你是一个只输出 JSON 的整理器。",
                )
                text = getattr(resp, "completion_text", "") or ""
                m = re.search(r"\{.*\}", text, re.S)
                if not m:
                    logger.warning(f"[memory] 会话 {key}: 模型没返回 JSON")
                    continue
                data = json.loads(m.group(0))
            except Exception as e:
                logger.warning(f"[memory] 会话 {key} 蒸馏出错: {e}")
                continue

            _write_memory(key, data)
            st["last_id"] = max_id
            st["last_distill"] = int(time.time())
            _save_state(self.state)
            logger.info(f"[memory] 会话 {key}: 已蒸馏 {len(msgs)} 条 → {_mem_dir(key)}")

            # 私聊额外提炼"事件线"：她和大哥之间发生过什么
            if key in EVENT_KEYS:
                try:
                    eresp = await provider.text_chat(
                        prompt=EVENT_PROMPT + "\n\n聊天记录：\n" + lines,
                        system_prompt="你是一个只输出 JSON 数组的整理器。",
                    )
                    etext = getattr(eresp, "completion_text", "") or ""
                    em = re.search(r"\[.*\]", etext, re.S)
                    if em:
                        evs = json.loads(em.group(0))
                        added = _append_events(key, evs)
                        logger.info(f"[memory] 事件线 +{added} 条 → {_events_file(key)}")
                    else:
                        logger.warning("[memory] 事件线：模型没返回数组")
                except Exception as e:
                    logger.warning(f"[memory] 事件线失败: {e}")
            try:
                self._export_overview()
            except Exception:
                pass

    def _load_persona(self) -> str:
        if self._persona:
            return self._persona
        try:
            con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
            cur = con.cursor()
            cur.execute("SELECT system_prompt FROM personas WHERE persona_id = ?", ("大肥鱼DeepSeek",))
            row = cur.fetchone()
            con.close()
            self._persona = (row[0] if row else "")[:2500]
        except Exception:
            self._persona = ""
        return self._persona

    async def _startup_review(self):
        cfg = _load_cfg()
        if not cfg.get("startup_review", True):
            logger.info("[memory] 上线回看已关闭（startup_review=false）")
            return
        await asyncio.sleep(90)
        try:
            if time.time() - self.state.get("last_startup_review", 0) < 7200:
                logger.info("[memory] 距上次上线回看不到 2 小时，跳过")
                return
            cand = await self._pick_reply()
            if not cand:
                logger.info("[memory] 上线回看：没有合适接的话")
                return

            umo, reply, picked, conf, reason = cand
            if _is_muted(umo):
                logger.info(f"[memory] {umo} 处于静音状态，跳过上线回看")
                return

            self.state["last_startup_review"] = int(time.time())
            _save_state(self.state)
            logger.info(f"[memory] 上线回看 选中「{picked}」conf={conf:.2f} 理由={reason} → 回复「{reply}」")

            if not self.send_enabled:
                logger.info("[memory] 【演练·未发送】在 memory\\config.json 里把 startup_send 设成 true 才会真发")
                return

            from astrbot.core.message.components import Plain
            from astrbot.core.message.message_event_result import MessageChain
            # 模型可能用 || 分多条，这里要拆开，不能把符号直接发出去
            import re as _re
            parts = [x.strip() for x in _re.split(r"\|\||\n\s*\n", reply) if x.strip()]
            if not parts:
                return
            ok = True
            for i, part in enumerate(parts[:4]):
                if i:
                    await asyncio.sleep(1.5 + i * 0.7)
                ok = await self.context.send_message(umo, MessageChain([Plain(text=part)]))
            logger.info(f"[memory] 上线回看已发送({ok}) {len(parts)} 条: {umo}")
        except Exception as e:
            logger.warning(f"[memory] 上线回看失败: {e}")

    @staticmethod
    def _parse_conv(msgs, limit=60):
        seq = []
        for m in msgs[-limit:]:
            role = m.get("role", "user")
            c = m.get("content")
            if isinstance(c, list):
                c = " ".join(str(p.get("text", "")) for p in c if isinstance(p, dict))
            c = str(c)
            mm = re.search(r"\]\s*([^\(（]{1,20})\(ID:\d+\)[:：]\s*", c)
            who = mm.group(1).strip() if mm else ("你" if role == "assistant" else "某人")
            if not who or who.strip("_ -") == "":
                who = "你" if role == "assistant" else "某人"
            body = re.sub(r"^\[[^\]]{6,40}\]\s*[^:：]{1,24}\(ID:\d+\)[:：]\s*", "", c).strip()
            if body:
                seq.append({"role": role, "who": who, "body": body[:200]})
        return seq

    @staticmethod
    def _mark_unanswered(seq):
        for i, it in enumerate(seq):
            if it["role"] != "user":
                continue
            it["answered"] = False
            for j in range(i + 1, len(seq)):
                if seq[j]["role"] == "assistant":
                    it["answered"] = True
                    break
                if seq[j]["role"] == "user":
                    break
        return seq

    async def _pick_reply(self):
        if not os.path.exists(DB_PATH):
            return None
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
        try:
            cur = con.cursor()
            cur.execute(
                "SELECT user_id, content FROM conversations "
                "WHERE platform_id='qq-napcat' AND datetime(updated_at) > datetime('now','-24 hours') "
                "ORDER BY updated_at DESC LIMIT 10"
            )
            rows = cur.fetchall()
        finally:
            con.close()
        if not rows:
            return None

        best = None
        for umo, content in rows:
            try:
                seq = self._mark_unanswered(self._parse_conv(json.loads(content)))
            except Exception:
                continue
            if len(seq) < 3:
                continue
            pending = [x for x in seq if x["role"] == "user" and not x.get("answered")]
            if pending:
                best = (umo, seq, len(pending))
                break

        if best is None:
            umo, content = rows[0]
            try:
                seq = self._mark_unanswered(self._parse_conv(json.loads(content)))
            except Exception:
                return None
            if len(seq) < 3:
                return None
            best = (umo, seq, 0)

        umo, seq, pending_cnt = best
        numbered = "\n".join(
            f"{i+1}. {'【未回复】' if x['role'] == 'user' and not x.get('answered') else ''}"
            f"{x['who']}: {x['body']}"
            for i, x in enumerate(seq)
        )
        logger.info(f"[memory] 回看素材：{umo}，{len(seq)} 条，其中未回复 {pending_cnt} 条")

        persona = self._load_persona()
        system = (
            (persona + "\n\n" if persona else "")
            + "你现在刚重新上线，正在看群里你不在时的聊天记录。"
        )
        prompt = (
            "下面是你不在时群里的聊天（编号. 发送者: 内容）。"
            "带【未回复】的是别人说了、但你当时没接的话。\n\n"
            "优先挑带【未回复】的：有人提问、有人找你、话题明显没聊完。\n"
            "没有【未回复】时，再按普通标准挑一条自然能接的。\n"
            "别挑：纯表情或单字（哈哈/好/行/？/收到）、别人的私事、已经结束的话题、你自己说过的话。\n"
            "如果最后一句已经是你在说，或者实在没有合适的，pick 填 null。\n\n"
            + numbered
            + "\n\n只输出 JSON，不要解释：\n"
            '{"pick": 编号或null, "who": "谁说的", "quote": "那条消息前20字", '
            '"reason": "为什么接这条（10字内）", "confidence": 0到1之间的小数, '
            '"reply": "你要发的话（15~60字，口语，符合人设，不要@任何人，不要解释你在做什么）"}'
        )

        provider = self.context.get_using_provider()
        if not provider:
            return None
        resp = await provider.text_chat(prompt=prompt, system_prompt=system)
        raw = (getattr(resp, "completion_text", "") or "").strip()

        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            logger.info(f"[memory] 回看模型没给 JSON：{raw[:80]}")
            return None
        try:
            data = json.loads(m.group(0))
        except Exception:
            return None

        if not data.get("pick"):
            return None
        reply = str(data.get("reply") or "").strip()
        conf = float(data.get("confidence") or 0)
        if not reply or len(reply) > STARTUP_REPLY_MAX or len(reply) < 5:
            return None
        if any(k in reply for k in ("NONE", "作为AI", "根据聊天记录")):
            return None
        if conf < STARTUP_CONFIDENCE:
            logger.info(f"[memory] 置信度不足 {conf:.2f}，放弃")
            return None

        picked = f"{data.get('who','?')}: {data.get('quote','')}"
        return umo, reply, picked, conf, str(data.get("reason", ""))[:20]

    async def _expand(self, query: str):
        """把问句换成几个相关的说法，用来兜底检索。"""
        try:
            provider = self.context.get_using_provider()
            if not provider:
                return ""
            resp = await provider.text_chat(
                prompt=("下面这句话可能在问某个群友或某件旧事。"
                        "列出 3 到 5 个相关的说法或近义词，用来在记忆里搜索，"
                        "用空格隔开，只输出词语，不要解释。\\n" + query[:200]),
                system_prompt="你是一个只输出近义词的助手。",
            )
            return (getattr(resp, "completion_text", "") or "").strip()[:100]
        except Exception:
            return ""


    def _export_overview(self):
        """把记忆库导成一份可读的 markdown（给大哥看的）。"""
        import glob as _g
        out_path = r"E:\\鱼鱼的家\\记忆总览.md"
        names = dict(GROUP_NAMES)
        names["_global"] = "全局"
        try:
            lines = ["# 阿渊的记忆总览", "",
                     f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}", ""]
            gp = os.path.join(MEM_ROOT, GLOBAL_DIR, "people",
                              f"{OWNER_NAMES[0]}.md")
            if os.path.isfile(gp):
                lines += ["## 关于大哥（全局）", ""]
                lines += [x.rstrip() for x in open(gp, encoding="utf-8") if x.strip().startswith("-")]
                lines.append("")
            for gid, gname in names.items():
                if gid == "_global":
                    continue
                files = sorted(_g.glob(os.path.join(MEM_ROOT, gid, "people", "*.md")))
                if not files:
                    continue
                lines += [f"## {gname}", ""]
                for p in files:
                    body = [x.rstrip() for x in open(p, encoding="utf-8")
                            if x.strip().startswith("-")]
                    if body:
                        lines.append(f"### {os.path.basename(p)[:-3]}")
                        lines += body
                        lines.append("")
            lines += ["## 话题脉络", ""]
            for gid, gname in names.items():
                if gid == "_global":
                    continue
                tops = sorted(_g.glob(os.path.join(MEM_ROOT, gid, "topics", "*.md")),
                              key=os.path.getmtime, reverse=True)[:20]
                if not tops:
                    continue
                lines += [f"### {gname}", ""]
                for p in tops:
                    body = [x.strip() for x in open(p, encoding="utf-8")
                            if x.strip().startswith("-")]
                    if body:
                        lines.append(f"- **{os.path.basename(p)[:-3]}**：{body[-1][2:][:80]}")
                lines.append("")
            lines += ["## 她自己的口味", ""]
            if os.path.isfile(TASTES_FILE):
                lines += [x.rstrip() for x in open(TASTES_FILE, encoding="utf-8")
                          if x.strip().startswith("-")]
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            open(out_path, "w", encoding="utf-8").write("\n".join(lines))
            self.state["last_export"] = time.time()
            logger.info(f"[memory] 记忆总览已更新（{len(lines)} 行）")
        except Exception as e:
            logger.warning(f"[memory] 导出总览失败: {e}")


    async def _export_on_start(self):
        import time as _t
        await asyncio.sleep(90)
        last = self.state.get("last_export", 0)
        if _t.time() - last > 12 * 3600:
            self._export_overview()

    @staticmethod
    def _digest_for(key: str, n_people=10, n_daily=6):
        """不靠关键词，直接把这个人/这个群的档案要点拿出来。"""
        import glob as _g
        base = _mem_dir(key)
        out = []
        try:
            pdir = os.path.join(base, "people")
            for f in _g.glob(os.path.join(pdir, "*.md")):
                lines = [x.strip() for x in open(f, encoding="utf-8")
                         if x.strip().startswith("-")]
                out += lines[-n_people:]
            ev = os.path.join(base, "events.md")
            if os.path.isfile(ev):
                elines = [x.strip() for x in open(ev, encoding="utf-8")
                          if x.strip().startswith("-")]
                out += elines[-14:]
            ddir = os.path.join(base, "daily")
            dails = sorted(_g.glob(os.path.join(ddir, "*.md")))[-3:]
            for f in dails:
                lines = [x.strip() for x in open(f, encoding="utf-8")
                         if x.strip().startswith("-")]
                out += lines[-n_daily:]
        except Exception:
            pass
        return out

    @filter.on_llm_request()
    async def inject_memory(self, event: AstrMessageEvent, req: ProviderRequest):
        key = _key_of(getattr(event, "unified_msg_origin", ""))
        try:
            query = (req.prompt or "") + " " + (getattr(event, "message_str", "") or "")
            hits, raw = _hybrid_recall(key, query)
            digest = []
            try:
                if event.is_private_chat():
                    digest = self._digest_for(key)
            except Exception:
                digest = []
            # 关键词没命中时，让模型给几个近义说法再搜一次
            if not hits and not raw:
                extra = await self._expand(query)
                if extra:
                    hits = _recall(key, query + " " + extra)
                    if hits:
                        logger.info(f"[memory] 语义扩展命中: {extra[:60]}")
            merged = list(digest) + [h for h in hits if h not in digest]

            if merged:
                block = ("\n\n【记忆】（你以前记下的，可能不准，别当成刚发生的事）\n"
                         + "\n".join(f"- {h}" for h in merged[:30]))
                req.system_prompt = (req.system_prompt or "") + block
                logger.info(f"[memory] 注入 {len(merged)} 条（key={key}，档案 {len(digest)}，复式命中 {len(hits)}，原话 {len(raw)}）")
                if raw:
                    rblock = ("\n\n【原话】（真实发生过的对话，别改口）\n"
                              + "\n".join(raw))
                    req.system_prompt = (req.system_prompt or "") + rblock
                    logger.info(f"[memory] 原话 {len(raw)} 条（key={key}）")
            else:
                logger.info(f"[memory] 注入 0 条（key={key}）")
        except Exception as e:
            logger.warning(f"[memory] 回忆失败: {e}")

        try:
            if req.contexts and len(req.contexts) > CONTEXT_SOFT_LIMIT:
                old, keep = req.contexts[:-45], req.contexts[-45:]
                text = "\n".join(
                    f"{c.get('role')}: {str(c.get('content'))[:200]}" for c in old if isinstance(c, dict)
                )[:4000]
                provider = self.context.get_using_provider()
                if provider and text.strip():
                    resp = await provider.text_chat(
                        prompt="把下面这段旧对话压成 5 条要点，中文，每行一条，不要编号：\n" + text,
                        system_prompt="你是一个对话摘要器。",
                    )
                    summary = (getattr(resp, "completion_text", "") or "").strip()[:800]
                    if summary:
                        req.contexts = [{"role": "system", "content": "【更早的对话摘要】\n" + summary}] + keep
                        logger.info(f"[memory] 压缩了 {len(old)} 条旧上下文")
        except Exception as e:
            logger.warning(f"[memory] 压缩失败: {e}")
