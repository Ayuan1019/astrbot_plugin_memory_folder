"""本地向量检索：bge-small-zh-v1.5 (int8 ONNX) + SQLite 向量表。

不依赖任何 API，CPU 跑，384 维。
"""
import json
import os
import sqlite3
import threading

import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

MODEL_DIR = os.environ.get("MEMORY_VEC_MODEL") or os.path.join(
    os.environ.get("MEMORY_ROOT") or os.path.dirname(os.path.abspath(__file__)),
    "vec_model")
VEC_DB = os.environ.get("MEMORY_VEC_DB") or os.path.join(
    os.environ.get("MEMORY_ROOT") or os.path.dirname(os.path.abspath(__file__)),
    "vectors.db")

_lock = threading.Lock()
_sess = None
_tok = None


def _load():
    global _sess, _tok
    if _sess is not None:
        return
    with _lock:
        if _sess is not None:
            return
        opt = ort.SessionOptions()
        opt.intra_op_num_threads = 2
        opt.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        _sess = ort.InferenceSession(
            os.path.join(MODEL_DIR, "onnx", "model_int8.onnx"),
            sess_options=opt, providers=["CPUExecutionProvider"])
        _tok = Tokenizer.from_file(os.path.join(MODEL_DIR, "tokenizer.json"))
        _tok.enable_truncation(max_length=256)
        _tok.enable_padding(length=256)


def embed(texts):
    """把一批文本变成归一化向量。"""
    if isinstance(texts, str):
        texts = [texts]
    _load()
    encs = _tok.encode_batch([t or " " for t in texts])
    ids = np.array([e.ids for e in encs], dtype=np.int64)
    mask = np.array([e.attention_mask for e in encs], dtype=np.int64)
    feed = {"input_ids": ids, "attention_mask": mask}
    names = [i.name for i in _sess.get_inputs()]
    if "token_type_ids" in names:
        feed["token_type_ids"] = np.zeros_like(ids)
    out = _sess.run(None, {k: v for k, v in feed.items() if k in names})
    vec = out[0][:, 0, :]                      # CLS pooling（bge 用 CLS）
    norm = np.linalg.norm(vec, axis=1, keepdims=True)
    norm[norm == 0] = 1.0
    return (vec / norm).astype(np.float32)


def _db():
    os.makedirs(os.path.dirname(VEC_DB), exist_ok=True)
    con = sqlite3.connect(VEC_DB, timeout=10)
    con.execute("""CREATE TABLE IF NOT EXISTS vec (
        id INTEGER PRIMARY KEY,
        source TEXT,      -- memory / raw
        key TEXT,         -- 哪个会话
        label TEXT,       -- 展示用的行
        vec BLOB,
        UNIQUE(source, key, label)
    )""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_vec_src ON vec(source, key)")
    return con


def index_rows(rows, batch=64):
    """rows: [(source, key, label)]，增量写入。"""
    con = _db()
    cur = con.cursor()
    exist = set()
    for src, key, label in cur.execute("SELECT source, key, label FROM vec"):
        exist.add((src, key, label))
    todo = [r for r in rows if r not in exist]
    if not todo:
        con.close()
        return 0
    for i in range(0, len(todo), batch):
        chunk = todo[i:i + batch]
        vecs = embed([c[2][:400] for c in chunk])
        for (src, key, label), v in zip(chunk, vecs):
            cur.execute("INSERT OR IGNORE INTO vec(source,key,label,vec) VALUES (?,?,?,?)",
                        (src, key, label, v.tobytes()))
    con.commit()
    con.close()
    return len(todo)


def search(query: str, k: int = 10, source: str = None, key: str = None):
    """向量检索，返回 [(相似度, source, key, label)]。"""
    con = _db()
    sql = "SELECT source, key, label, vec FROM vec"
    where, args = [], []
    if source:
        where.append("source=?")
        args.append(source)
    if key:
        where.append("key=?")
        args.append(key)
    if where:
        sql += " WHERE " + " AND ".join(where)
    rows = con.execute(sql, args).fetchall()
    con.close()
    if not rows:
        return []
    q = embed([query])[0]
    mat = np.frombuffer(b"".join(r[3] for r in rows)[:0] or b"", dtype=np.float32) if False else None
    sims = []
    for src, ky, label, blob in rows:
        v = np.frombuffer(blob, dtype=np.float32)
        sims.append((float(np.dot(q, v)), src, ky, label))
    sims.sort(key=lambda x: -x[0])
    return sims[:k]


def stats():
    con = _db()
    n = con.execute("SELECT COUNT(*) FROM vec").fetchone()[0]
    con.close()
    return n
