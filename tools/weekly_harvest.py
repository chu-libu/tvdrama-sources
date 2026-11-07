#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""周收割流水线（GitHub Actions 每周调度 + 手动 dispatch；本地亦可手动跑）。

链路：上游 GitHub 配置仓收集候选 → 与仓内 sources.json 现有源并集
     → 探活（ac=list，code==1 = 存活——母仓 D-028 勘误定版：wd 命中有关键词偏置会误杀，
       wd=亮剑 查询「无结果≠API 死」，故判活不用 wd；wd 命中如需统计只作 hits 附注）
     → 导出存活源（存量死源连续 2 轮才除名；新候选探死不入清单；
       存活数骤降熔断不导出，防 runner IP 被拦全灭）
     → sources.json + .github/heartbeat.json 落盘（心跳 commit 保持仓库活性，
       防 GitHub 60 天无提交自动停调度）。

移植自母仓 tvdrama-tool：collect_sources.py（stdlib 探活骨架）+
backend/app/sources_harvest.py（GH_FILES 通道清单、Base58、URL 归一、成人名过滤）。
口径对齐：VOD_RE / PROBE_TIMEOUT=10s / 并发 16 / 三通道回落 / 探活=ac=list code==1（D-028 勘误）。
纯标准库（探活/抓取/解析零三方依赖，Actions runner 免安装）。
"""
import concurrent.futures as cf
import datetime
import json
import os
import re
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES_PATH = os.path.join(ROOT, "sources.json")
STATE_PATH = os.path.join(ROOT, "tools", "state_dead_streak.json")
REMOVED_PATH = os.path.join(ROOT, "tools", "removed.txt")
HEARTBEAT_PATH = os.path.join(ROOT, ".github", "heartbeat.json")

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
RAW_BASE = "https://raw.githubusercontent.com/"
GHPROXY_BASE = "https://gh-proxy.com/https://raw.githubusercontent.com/"
JSD_BASE = "https://cdn.jsdelivr.net/gh/"

FILE_TIMEOUT = 10.0          # 单通道单文件超时（母仓 sources_harvest 口径）
SUB_TIMEOUT = 8.0            # 订阅二级配置超时
PROBE_TIMEOUT = 10.0
PROBE_CONCURRENCY = 16
SUB_FOLLOW_LIMIT = 96        # 订阅跟进上限（母仓 T013-08 口径）

# 探活测试词兜底：runner 出口若对特定 wd 异常，alive 全灭会触发熔断保护，不会写坏清单
VOD_RE = re.compile(r'https?://[^\s"\'<>,\\]+?/api\.php/provide/vod')
ADULT_NAME_RE = re.compile(r"伦理|情色|成人|18禁|色影|禁看|未减|redruid|jin18")
B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# 上游配置仓清单（母仓 sources_harvest.py GH_FILES；jin18 成人通道整组剔除，
# 新候选不再混入成人源；存量 77 源不受影响——只保不删，除名须写 removed.txt 或连续 2 轮死）
GH_FILES = [
    "TVboxorg/TVbox@main:sources.yaml",
    "TVboxorg/TVbox@main:dist/official.json",
    "TVboxorg/TVbox@main:dist/list.json",
]
for owner in ("hafrey1", "lwchc", "Hikaru946", "jcy012"):
    GH_FILES += [
        f"{owner}/LunaTV-config@main:LunaTV-config.txt",
        f"{owner}/LunaTV-config@main:LunaTV-config.json",
        f"{owner}/LunaTV-config@main:jingjian.txt",
        f"{owner}/LunaTV-config@main:jingjian.json",
    ]
GH_FILES += [
    "keluo8824-cell/tvbox@main:ok.json",
    "keluo8824-cell/tvbox@main:fan.json",
    "keluo8824-cell/tvbox@main:mao.json",
    "keluo8824-cell/tvbox@main:cai.json",
    "keluo8824-cell/tvbox@main:4k.json",
    "yoursmile66/TVBox@main:XC.json",
    "liu673cn/box@main:dc.txt",
    "liu673cn/box@main:dm.txt",
    "qist/tvbox@master:0707.json",
    "qist/tvbox@master:0821.json",
    "qist/tvbox@master:0825.json",
    "qist/tvbox@master:0826.json",
    "qist/tvbox@master:0827.json",
    "qist/tvbox@master:367.json",
    "qist/tvbox@master:9918.json",
    "qist/tvbox@master:99188.json",
    "qist/tvbox@master:XYQ.json",
    "TVboxorg/TVbox@main:README.md",
    "hafrey1/LunaTV-config@main:README.md",
    "lwchc/LunaTV-config@main:README.md",
    "yoursmile66/TVBox@main:README.md",
    "liu673cn/box@main:README.md",
    "qist/tvbox@master:README.md",
    "laoma528/awesome-zhuiju-free@main:resources/resources.json",
]
SUBSCRIPTION_FILES = {"liu673cn/box@main:dc.txt", "TVboxorg/TVbox@main:sources.yaml"}

# 非GitHub 直连配置（母仓 collect_sources.py CONFIG_URLS；失败容忍逐条跳过）
DIRECT_CONFIG_URLS = [
    "http://rihou.cc:66",
    "http://itvbox.cc/1.txt",
    "http://home.jundie.top:81/top98.json",
]

stats = {"files_ok": 0, "files_fail": 0, "sub_followed": 0,
         "candidates_new": 0, "adult_skipped": 0}


def fetch(url, timeout=FILE_TIMEOUT):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(url, headers=UA)
    with opener.open(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")


def fetch_gh(ref, timeout=FILE_TIMEOUT):
    """repo@branch:path 三通道回落（母仓口径：raw 直连 → gh-proxy → jsDelivr）。"""
    repo_branch, path = ref.split(":", 1)
    repo, branch = repo_branch.split("@", 1)
    errors = []
    for url in (RAW_BASE + repo + "/" + branch + "/" + path,
                GHPROXY_BASE + repo + "/" + branch + "/" + path,
                JSD_BASE + repo + "@" + branch + "/" + path):
        try:
            return fetch(url, timeout)
        except Exception as e:  # noqa: BLE001 逐通道回落
            errors.append(f"{type(e).__name__}")
    raise OSError("all channels failed: " + ",".join(errors))


def b58decode(s):
    s = "".join(str(s).split())
    if not s or any(ch not in B58_ALPHABET for ch in s):
        raise ValueError("invalid base58")
    n = 0
    for ch in s:
        n = n * 58 + B58_ALPHABET.index(ch)
    body = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return b"\x00" * (len(s) - len(s.lstrip("1"))) + body


def normalize(url):
    """苹果CMS 接口归一（母仓口径）：去 query/fragment，尾斜杠形态，netloc+path 去重。"""
    p = urllib.parse.urlsplit(url.strip())
    if p.scheme not in ("http", "https") or not p.netloc:
        return None
    return (p.netloc.lower(), p.path.rstrip("/") + "/")


def scan_text(text, bag):
    """文本散列通道：VOD_RE 全文扫（覆盖 README/dc.txt/任意 JSON/YAML 里的裸 URL）。"""
    for m in VOD_RE.findall(text):
        key = normalize(m)
        if key and key not in bag:
            bag[key] = None  # name 未知 = 非结构化候选，不做成人名过滤


def scan_json_named(text, bag):
    """结构化通道：TVBox sites[] / LunaTV api_site{} 取 (name, api) 对，成人名过滤。"""
    try:
        data = json.loads(text)
    except Exception:
        return
    def visit(node):
        if isinstance(node, dict):
            api = node.get("api") or node.get("url")
            name = node.get("name")
            if isinstance(api, str) and VOD_RE.search(api):
                key = normalize(api)
                if key:
                    if isinstance(name, str) and ADULT_NAME_RE.search(name):
                        stats["adult_skipped"] += 1
                    elif key not in bag or bag[key] is None:
                        bag[key] = name
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            for v in node:
                visit(v)
    visit(data)


def harvest():
    bag = {}  # (netloc,path) -> name|None
    for ref in GH_FILES:
        try:
            text = fetch_gh(ref)
        except Exception:
            stats["files_fail"] += 1
            continue
        stats["files_ok"] += 1
        scan_text(text, bag)
        scan_json_named(text, bag)
        if ref in SUBSCRIPTION_FILES:
            # 订阅跟进：dc.txt=二级配置地址列表；sources.yaml=清单（yaml 缺失则文本扫兜底）
            for line in text.splitlines():
                m = re.match(r"https?://\S+", line.strip())
                if not m:
                    continue
                if stats["sub_followed"] >= SUB_FOLLOW_LIMIT:
                    break
                try:
                    sub_text = fetch(m.group(0), SUB_TIMEOUT)
                except Exception:
                    continue
                stats["sub_followed"] += 1
                scan_text(sub_text, bag)
                scan_json_named(sub_text, bag)
    for cu in DIRECT_CONFIG_URLS:
        try:
            scan_text(fetch(cu, SUB_TIMEOUT), bag)
        except Exception:
            stats["files_fail"] += 1
    return bag


def probe(ep):
    """判活：ac=list code==1 = 存活（母仓 D-028 勘误定版）。

    不用 ac=videolist&wd=XXX 判活——关键词无结果 ≠ API 死（wd 偏置会误杀，
    母仓实测 30/107「死源」ac=list 完全正常）。返回 (ep, alive, class_count)。
    """
    try:
        data = json.loads(fetch(ep + "?ac=list", PROBE_TIMEOUT))
        n = len(data.get("class") or [])
        return (ep, data.get("code") == 1, n)
    except Exception:
        return (ep, False, 0)


def main():
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
    now_str = now.isoformat(timespec="seconds")

    with open(SOURCES_PATH, encoding="utf-8") as f:
        existing_doc = json.load(f)
    existing = [s["url"] for s in existing_doc.get("sources", []) if s.get("url")]

    removed = set()
    if os.path.isfile(REMOVED_PATH):
        with open(REMOVED_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    removed.add(line.rstrip("/"))

    state = {}
    if os.path.isfile(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as f:
            state = json.load(f)

    bag = harvest()
    existing_keys = {normalize(u) for u in existing}
    new_keys = [k for k in bag if k not in existing_keys]
    stats["candidates_new"] = len(new_keys)

    # 候选统一为无 scheme 的 netloc+path 归一形态，探活时统一补 http（存量源原样保留）
    candidates = list(existing)
    for key in new_keys:
        url = key[0] + key[1]
        if url.rstrip("/") in removed:
            continue
        candidates.append(url)
    candidates = sorted(set(c if c.startswith("http") else "http://" + c for c in candidates))
    candidates = sorted(set(c.rstrip("/") + "/" for c in candidates))

    print(f"existing={len(existing)} harvested_files_ok={stats['files_ok']} "
          f"new_candidates={stats['candidates_new']} adult_skipped={stats['adult_skipped']} "
          f"probing={len(candidates)} ...")

    results = {}
    with cf.ThreadPoolExecutor(max_workers=PROBE_CONCURRENCY) as ex:
        for ep, ok, n in ex.map(probe, candidates):
            results[ep] = (ok, n)

    # 存量源死源连续 2 轮才除名（state 跨 run 持久化在仓内，防单轮探活抖动误删）；
    # 新候选首轮探死不入清单（下轮转活自然加入）——清单只收已证活的增量
    existing_set = {u.rstrip("/") + "/" for u in existing}
    kept, dropped = [], []
    new_state = {}
    new_dead_skipped = 0
    for ep in candidates:
        ok = results.get(ep, (False, 0))[0]
        if ok:
            kept.append(ep)
            new_state[ep] = 0
        elif ep in existing_set:
            streak = int(state.get(ep, 0)) + 1
            if streak < 2:
                kept.append(ep)  # 存量首轮死：观察期保留
                new_state[ep] = streak
            else:
                dropped.append(ep)
        else:
            new_dead_skipped += 1
    alive_count = sum(1 for ep in kept if results.get(ep, (False, 0))[0])

    # 熔断：存活数 < max(10, 存量一半) 视为 runner 出口异常，不导出（旧清单原样保留）
    prev_alive = sum(1 for u in existing if results.get(u.rstrip("/") + "/", (False, 0))[0])
    guard_floor = max(10, len(existing) // 2)
    exported = alive_count >= guard_floor
    if not exported:
        print(f"GUARD TRIPPED: alive={alive_count} < floor={guard_floor} "
              f"(prev_alive={prev_alive}) — sources.json 未改动")

    if exported:
        doc = {"updated": now_str, "sources": [{"url": u} for u in sorted(kept)]}
        with open(SOURCES_PATH, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
            f.write("\n")
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(new_state, f, ensure_ascii=False, indent=2)

    os.makedirs(os.path.dirname(HEARTBEAT_PATH), exist_ok=True)
    heartbeat = {
        "last_run": now_str, "exported": exported,
        "existing": len(existing), "candidates": len(candidates),
        "alive": alive_count, "kept": len(kept) if exported else None,
        "new_added": (len(kept) - len(existing)) if exported else None,
        "dropped_this_run": dropped if exported else None,
        "guard_floor": guard_floor,
        "files_ok": stats["files_ok"], "files_fail": stats["files_fail"],
        "adult_skipped": stats["adult_skipped"], "sub_followed": stats["sub_followed"],
        "new_dead_skipped": new_dead_skipped,
    }
    with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
        json.dump(heartbeat, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(json.dumps(heartbeat, ensure_ascii=False))


if __name__ == "__main__":
    main()
