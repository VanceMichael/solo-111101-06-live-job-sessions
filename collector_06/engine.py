"""离线重建直播带岗日程、场次版本与回放归属的工作流引擎。

身份模型（与来源协议一致）：

* ``UID`` 标识场次，``RECURRENCE-ID`` 出现时与 ``UID`` 共同标识场次实例；
* ``SEQUENCE`` 标识场次修订版本，按版本归档后再投影当前日程；
* ``X-TYPE:REPLAY`` 标识回放，``X-SESSION`` 与 ``X-SESSION-SEQUENCE``
  显式指向拍摄时的场次版本，任何情况下都不改挂到改期后的新版本；
* 只有 ``X-ACTIVITY`` 的回放无法推断场次版本，进入
  unresolved / ``missing_session_revision``。

引擎不按场景名称分支，全部判断只依据来源字段与可配置选项。
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .adapter import decode
from .sources import FixtureTransport

EXPLICIT_ONLY = "explicit_only"
CONFIGURABLE = "configurable"
MATCHING_MODES = (EXPLICIT_ONLY, CONFIGURABLE)

# 可配置规则：默认绝不启用；explicit_only 模式下下列规则一律不参与决策。
RULE_EXPLICIT = "explicit_session_revision"
RULE_ACTIVITY_SINGLE = "activity_single_session"
RULE_TIME_PROXIMITY = "time_proximity"
SUPPORTED_RULES = frozenset({RULE_EXPLICIT, RULE_ACTIVITY_SINGLE, RULE_TIME_PROXIMITY})

REASON_MISSING_SESSION_REVISION = "missing_session_revision"

STATE_SCHEDULED = "scheduled"
STATE_CANCELLED = "cancelled"
STATE_CONFLICT = "conflict"

IDENTITY_KEYS = ("UID", "SEQUENCE", "RECURRENCE-ID")
REPLAY_EVIDENCE_KEYS = (
    "UID",
    "RECURRENCE-ID",
    "X-TYPE",
    "X-SESSION",
    "X-SESSION-SEQUENCE",
    "X-ACTIVITY",
    "X-URL",
    "X-RECORDED-AT",
)

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS run_observations (
        id INTEGER PRIMARY KEY,
        run_id TEXT NOT NULL,
        source TEXT NOT NULL,
        observed_order INTEGER NOT NULL,
        fingerprint TEXT NOT NULL UNIQUE,
        payload TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS session_versions (
        id INTEGER PRIMARY KEY,
        uid TEXT NOT NULL,
        recurrence_id TEXT NOT NULL DEFAULT '',
        sequence INTEGER NOT NULL,
        start TEXT,
        end TEXT,
        activity TEXT,
        status TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        late_arrival INTEGER NOT NULL DEFAULT 0,
        source TEXT NOT NULL,
        run_id TEXT NOT NULL,
        observed_order INTEGER NOT NULL,
        payload TEXT NOT NULL,
        UNIQUE(uid, recurrence_id, sequence)
    )""",
    """CREATE TABLE IF NOT EXISTS session_conflicts (
        id INTEGER PRIMARY KEY,
        uid TEXT NOT NULL,
        recurrence_id TEXT NOT NULL DEFAULT '',
        sequence INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        established_hash TEXT NOT NULL,
        source TEXT NOT NULL,
        run_id TEXT NOT NULL,
        observed_order INTEGER NOT NULL,
        payload TEXT NOT NULL,
        UNIQUE(uid, recurrence_id, sequence, content_hash)
    )""",
    """CREATE TABLE IF NOT EXISTS replays (
        id INTEGER PRIMARY KEY,
        uid TEXT NOT NULL,
        recurrence_id TEXT NOT NULL DEFAULT '',
        session_uid TEXT,
        session_sequence INTEGER,
        activity TEXT,
        url TEXT,
        recorded_at TEXT,
        source TEXT NOT NULL,
        run_id TEXT NOT NULL,
        observed_order INTEGER NOT NULL,
        fingerprint TEXT NOT NULL UNIQUE,
        payload TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY,
        notification_key TEXT NOT NULL UNIQUE,
        kind TEXT NOT NULL,
        subject TEXT NOT NULL,
        summary TEXT NOT NULL,
        evidence TEXT NOT NULL,
        created_run TEXT NOT NULL
    )""",
)


def _canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def event_fingerprint(source: str, event: dict) -> str:
    digest = hashlib.sha256()
    digest.update(source.encode("utf-8"))
    digest.update(b"\0")
    digest.update(_canonical(event).encode("utf-8"))
    return digest.hexdigest()


def content_hash(event: dict) -> str:
    body = {key: value for key, value in event.items() if key not in IDENTITY_KEYS}
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def parse_sequence(value, *, source: str, field: str):
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(str(value).strip())
    except ValueError as error:
        raise ValueError(f"{source}: {field} 必须是整数序号，实际为 {value!r}") from error


def parse_event_time(value: str):
    text = value.strip()
    if len(text) == 16 and text[8] == "T" and text.endswith("Z"):
        return datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def validate_options(options: dict) -> dict:
    matching = options.get("matching", EXPLICIT_ONLY)
    if matching not in MATCHING_MODES:
        raise ValueError(f"不支持的 matching 模式: {matching!r}，可选 {MATCHING_MODES}")
    rules = options.get("match_rules")
    if matching == CONFIGURABLE:
        if not isinstance(rules, list) or not rules:
            raise ValueError("configurable 匹配模式必须在 options.match_rules 中显式给出规则列表")
        unknown = sorted(set(rules) - SUPPORTED_RULES)
        if unknown:
            raise ValueError(f"未知匹配规则: {unknown}，可选 {sorted(SUPPORTED_RULES)}")
    elif rules:
        # explicit_only 是强约束：即使携带规则也不允许隐式打开（空列表视为幂等归一化结果）。
        raise ValueError("matching=explicit_only 时不得配置 match_rules")
    return {"matching": matching, "match_rules": list(rules or [])}


def load_scenario(path) -> dict:
    scenario = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(scenario, dict):
        raise ValueError("场景文件必须是 JSON 对象")
    for field in ("name", "run_id", "sources", "fixture_root"):
        if field not in scenario:
            raise ValueError(f"场景文件缺少字段: {field}")
    if not isinstance(scenario["sources"], list) or not scenario["sources"]:
        raise ValueError("场景文件 sources 必须是非空数组")
    if not isinstance(scenario["run_id"], str) or not scenario["run_id"].strip():
        raise ValueError("run_id 必须是非空字符串")
    options = scenario.get("options") or {}
    if not isinstance(options, dict):
        raise ValueError("options 必须是 JSON 对象")
    scenario["options"] = validate_options(options)
    return scenario


class WorkflowStore:
    def __init__(self, database):
        self.database = str(database)

    def connect(self):
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self, connection):
        for statement in SCHEMA:
            connection.execute(statement)


def _present_fields(event, keys) -> dict:
    return {key: event[key] for key in keys if key in event}


def _notify(connection, run_id, key, kind, subject, summary, evidence) -> int:
    cursor = connection.execute(
        "INSERT OR IGNORE INTO notifications"
        "(notification_key, kind, subject, summary, evidence, created_run) VALUES (?,?,?,?,?,?)",
        (key, kind, subject, summary, _canonical(evidence), run_id),
    )
    return cursor.rowcount


def _ingest_session(connection, run_id, source, order, event):
    created = 0
    uid = str(event.get("UID", "")).strip()
    if not uid:
        raise ValueError(f"{source}: 场次事件缺少 UID: {event!r}")
    recurrence_id = event.get("RECURRENCE-ID", "").strip()
    sequence = parse_sequence(event.get("SEQUENCE"), source=source, field="SEQUENCE")
    if sequence is None:
        sequence = 0
    signature = content_hash(event)
    status = STATE_CANCELLED if event.get("STATUS", "").upper() == "CANCELLED" else STATE_SCHEDULED

    existing = connection.execute(
        "SELECT id, content_hash FROM session_versions "
        "WHERE uid=? AND recurrence_id=? AND sequence=?",
        (uid, recurrence_id, sequence),
    ).fetchone()
    if existing is not None:
        if existing["content_hash"] == signature:
            return 0  # 同一版本的重复观测，幂等忽略
        cursor = connection.execute(
            "INSERT OR IGNORE INTO session_conflicts"
            "(uid, recurrence_id, sequence, content_hash, established_hash, "
            "source, run_id, observed_order, payload) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                uid,
                recurrence_id,
                sequence,
                signature,
                existing["content_hash"],
                source,
                run_id,
                order,
                _canonical(event),
            ),
        )
        if cursor.rowcount:
            created += _notify(
                connection,
                run_id,
                f"conflict:{uid}:{recurrence_id}:{sequence}:{signature}",
                "session_conflict",
                uid,
                f"场次 {uid} 序号 {sequence} 出现不同日程，已隔离为 conflict",
                {
                    "decision": "same_sequence_different_schedule_isolated",
                    "source": source,
                    "fields": _present_fields(event, REPLAY_EVIDENCE_KEYS),
                    "established_content_hash": existing["content_hash"],
                    "conflicting_content_hash": signature,
                },
            )
        return created

    late_row = connection.execute(
        "SELECT 1 FROM session_versions WHERE uid=? AND recurrence_id=? AND sequence > ? LIMIT 1",
        (uid, recurrence_id, sequence),
    ).fetchone()
    late_arrival = 1 if late_row is not None else 0
    connection.execute(
        "INSERT INTO session_versions"
        "(uid, recurrence_id, sequence, start, end, activity, status, content_hash, "
        "late_arrival, source, run_id, observed_order, payload) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            uid,
            recurrence_id,
            sequence,
            event.get("DTSTART"),
            event.get("DTEND"),
            event.get("X-ACTIVITY"),
            status,
            signature,
            late_arrival,
            source,
            run_id,
            order,
            _canonical(event),
        ),
    )
    evidence = {
        "decision": "ics_uid_sequence_revision",
        "source": source,
        "fields": _present_fields(
            event,
            ("UID", "RECURRENCE-ID", "SEQUENCE", "DTSTART", "DTEND", "STATUS", "X-ACTIVITY"),
        ),
        "content_hash": signature,
    }
    if status == STATE_CANCELLED:
        created += _notify(
            connection,
            run_id,
            f"cancel:{uid}:{recurrence_id}:{sequence}",
            "session_cancelled",
            uid,
            f"场次 {uid} 序号 {sequence} 标记取消，已归档回放保留",
            evidence,
        )
        return created
    previous = connection.execute(
        "SELECT start FROM session_versions WHERE uid=? AND recurrence_id=? AND sequence < ? "
        "ORDER BY sequence DESC LIMIT 1",
        (uid, recurrence_id, sequence),
    ).fetchone()
    if previous is not None and previous["start"] and event.get("DTSTART") and previous["start"] != event["DTSTART"]:
        created += _notify(
            connection,
            run_id,
            f"reschedule:{uid}:{recurrence_id}:{sequence}",
            "session_rescheduled",
            uid,
            f"场次 {uid} 改期至 {event['DTSTART']}（序号 {sequence}）",
            evidence,
        )
    return created


def _ingest_replay(connection, run_id, source, order, event):
    uid = str(event.get("UID", "")).strip()
    if not uid:
        raise ValueError(f"{source}: 回放事件缺少 UID: {event!r}")
    recurrence_id = event.get("RECURRENCE-ID", "").strip() or event.get("X-RECURRENCE-ID", "").strip()
    session_uid = event.get("X-SESSION", "").strip() or None
    session_sequence = parse_sequence(
        event.get("X-SESSION-SEQUENCE"), source=source, field="X-SESSION-SEQUENCE"
    )
    fingerprint = event_fingerprint(source, event)
    connection.execute(
        "INSERT OR IGNORE INTO replays"
        "(uid, recurrence_id, session_uid, session_sequence, activity, url, recorded_at, "
        "source, run_id, observed_order, fingerprint, payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            uid,
            recurrence_id,
            session_uid,
            session_sequence,
            event.get("X-ACTIVITY"),
            event.get("X-URL"),
            event.get("X-RECORDED-AT"),
            source,
            run_id,
            order,
            fingerprint,
            _canonical(event),
        ),
    )


def _session_index(connection):
    """返回当前投影所需的场次索引：身份 -> 当前版本与全部版本。"""
    rows = connection.execute(
        "SELECT uid, recurrence_id, sequence, start, end, activity, status, late_arrival, "
        "source, content_hash, observed_order FROM session_versions "
        "ORDER BY uid, recurrence_id, sequence, id"
    ).fetchall()
    identities = {}
    for row in rows:
        key = (row["uid"], row["recurrence_id"])
        identities.setdefault(key, []).append(row)
    index = {}
    for key, versions in identities.items():
        current = versions[-1]
        start = current["start"]
        if start is None:  # 取消事件不带 DTSTART 时，沿历史版本回填，取消只改变状态
            for previous in reversed(versions[:-1]):
                if previous["start"]:
                    start = previous["start"]
                    break
        index[key] = {"current": current, "start": start, "versions": versions}
    return index


def _resolve_replay(replay, session_index, options):
    source = replay["source"]
    payload = json.loads(replay["payload"])
    fields = _present_fields(payload, REPLAY_EVIDENCE_KEYS)
    base_evidence = {
        "matching": options["matching"],
        "source": source,
        "fields": fields,
    }
    recurrence_id = replay["recurrence_id"] or ""

    if replay["session_uid"] and replay["session_sequence"] is not None:
        target_key = (replay["session_uid"], recurrence_id)
        target_observed = any(
            version["uid"] == replay["session_uid"]
            and version["recurrence_id"] == recurrence_id
            and version["sequence"] == replay["session_sequence"]
            for version in (session_index.get(target_key, {}).get("versions") or [])
        )
        evidence = dict(base_evidence)
        evidence.update(
            {
                "decision": RULE_EXPLICIT,
                "target_version_observed": target_observed,
                "anchored_fields": ["X-SESSION", "X-SESSION-SEQUENCE"],
            }
        )
        return {
            "resolution": "explicit",
            "session": replay["session_uid"],
            "session_sequence": replay["session_sequence"],
            "evidence": evidence,
        }

    missing = [name for name in ("X-SESSION", "X-SESSION-SEQUENCE") if name not in payload]
    evidence = dict(base_evidence)
    evidence["missing_fields"] = missing

    if options["matching"] == CONFIGURABLE:
        rules = options["match_rules"]
        if RULE_ACTIVITY_SINGLE in rules and replay["activity"]:
            candidates = [
                key
                for key, info in session_index.items()
                if info["current"]["activity"] == replay["activity"]
            ]
            evidence["activity_rule_candidates"] = len(candidates)
            if len(candidates) == 1:
                uid, rec = candidates[0]
                current = session_index[(uid, rec)]["current"]
                evidence.update(
                    {
                        "decision": f"configured_rule:{RULE_ACTIVITY_SINGLE}",
                        "anchored_fields": ["X-ACTIVITY"],
                        "rule": RULE_ACTIVITY_SINGLE,
                    }
                )
                return {
                    "resolution": "explicit",
                    "session": uid,
                    "session_sequence": current["sequence"],
                    "evidence": evidence,
                }
        if RULE_TIME_PROXIMITY in rules and replay["recorded_at"]:
            reference = parse_event_time(replay["recorded_at"])
            pool = [
                info
                for key, info in session_index.items()
                if not replay["activity"] or info["current"]["activity"] == replay["activity"]
            ]
            ranked = []
            for info in pool:
                if not info["start"]:
                    continue
                delta = abs((parse_event_time(info["start"]) - reference).total_seconds())
                ranked.append((delta, info))
            ranked.sort(key=lambda item: (item[0], item[1]["current"]["uid"]))
            if ranked and (len(ranked) == 1 or ranked[0][0] != ranked[1][0]):
                delta, info = ranked[0]
                current = info["current"]
                evidence.update(
                    {
                        "decision": f"configured_rule:{RULE_TIME_PROXIMITY}",
                        "rule": RULE_TIME_PROXIMITY,
                        "reference_time": replay["recorded_at"],
                        "nearest_start": info["start"],
                        "delta_seconds": delta,
                    }
                )
                return {
                    "resolution": "explicit",
                    "session": current["uid"],
                    "session_sequence": current["sequence"],
                    "evidence": evidence,
                }
            evidence["time_proximity_result"] = "tie_or_no_candidate"

    evidence["decision"] = "activity_only_cannot_infer_session_revision"
    return {
        "resolution": "unresolved",
        "reason": REASON_MISSING_SESSION_REVISION,
        "evidence": evidence,
    }


def _history(connection):
    entries = []
    version_rows = connection.execute(
        "SELECT * FROM session_versions ORDER BY uid, recurrence_id, sequence, id"
    ).fetchall()
    grouped = {}
    for row in version_rows:
        grouped.setdefault((row["uid"], row["recurrence_id"]), []).append(row)
    for (uid, recurrence_id), versions in grouped.items():
        first_observed_order = min(version["observed_order"] for version in versions)
        for index, row in enumerate(versions):
            if row["observed_order"] == first_observed_order:
                kind = "scheduled"
            elif row["late_arrival"]:
                kind = "late_archive"
            elif row["status"] == STATE_CANCELLED:
                kind = "cancelled"
            elif row["start"] and versions[index - 1]["start"] and row["start"] != versions[index - 1]["start"]:
                kind = "rescheduled"
            else:
                kind = "revised"
            entry = {
                "uid": uid,
                "sequence": row["sequence"],
                "start": row["start"],
                "state": row["status"],
                "kind": kind,
                "source": row["source"],
                "evidence": {
                    "decision": "ics_uid_sequence_revision",
                    "source": row["source"],
                    "fields": _present_fields(json.loads(row["payload"]), (
                        "UID", "RECURRENCE-ID", "SEQUENCE", "DTSTART", "DTEND",
                        "STATUS", "X-ACTIVITY",
                    )),
                    "content_hash": row["content_hash"],
                },
            }
            if recurrence_id:
                entry["recurrence_id"] = recurrence_id
            entries.append(entry)
    for row in connection.execute(
        "SELECT * FROM session_conflicts ORDER BY uid, recurrence_id, sequence, id"
    ):
        entry = {
            "uid": row["uid"],
            "sequence": row["sequence"],
            "start": None,
            "state": STATE_CONFLICT,
            "kind": STATE_CONFLICT,
            "source": row["source"],
            "evidence": {
                "decision": "same_sequence_different_schedule_isolated",
                "source": row["source"],
                "fields": _present_fields(json.loads(row["payload"]), (
                    "UID", "RECURRENCE-ID", "SEQUENCE", "DTSTART", "DTEND",
                    "STATUS", "X-ACTIVITY",
                )),
                "established_content_hash": row["established_hash"],
                "conflicting_content_hash": row["content_hash"],
            },
        }
        if row["recurrence_id"]:
            entry["recurrence_id"] = row["recurrence_id"]
        entries.append(entry)
    return entries


def project(connection, options, notifications_created):
    session_index = _session_index(connection)
    sessions = []
    for (uid, recurrence_id), info in sorted(session_index.items()):
        current = info["current"]
        item = {
            "uid": uid,
            "sequence": current["sequence"],
            "start": info["start"],
            "state": current["status"],
        }
        if recurrence_id:
            item["recurrence_id"] = recurrence_id
        sessions.append(item)

    replays = []
    unresolved = []
    for row in connection.execute("SELECT * FROM replays ORDER BY id"):
        replay = dict(row)
        decision = _resolve_replay(replay, session_index, options)
        if decision["resolution"] == "explicit":
            item = {
                "uid": replay["uid"],
                "session": decision["session"],
                "session_sequence": decision["session_sequence"],
                "evidence": decision["evidence"],
            }
            if replay["recurrence_id"]:
                item["recurrence_id"] = replay["recurrence_id"]
            if replay["url"]:
                item["url"] = replay["url"]
            replays.append(item)
        else:
            item = {
                "uid": replay["uid"],
                "reason": decision["reason"],
                "evidence": decision["evidence"],
            }
            if replay["activity"]:
                item["activity"] = replay["activity"]
            unresolved.append(item)

    return {
        "sessions": sessions,
        "replays": replays,
        "unresolved": unresolved,
        "history": _history(connection),
        "notifications": notifications_created,
    }


def run_scenario(scenario: dict, database) -> dict:
    if not isinstance(scenario, dict):
        raise ValueError("场景必须是 JSON 对象")
    run_id = str(scenario["run_id"])
    options = validate_options(scenario.get("options") or {})
    fixture_root = scenario["fixture_root"]
    transport = FixtureTransport(fixture_root)
    store = WorkflowStore(database)
    connection = store.connect()
    notifications_created = 0
    try:
        store.initialize(connection)
        order = 0
        with connection:
            for source in scenario["sources"]:
                response = transport.request(source)
                if response.status != 200:
                    raise ValueError(f"来源 {source} 返回状态 {response.status}")
                for event in decode(response.body):
                    fingerprint = event_fingerprint(source, event)
                    connection.execute(
                        "INSERT OR IGNORE INTO run_observations"
                        "(run_id, source, observed_order, fingerprint, payload) "
                        "VALUES (?,?,?,?,?)",
                        (run_id, source, order, fingerprint, _canonical(event)),
                    )
                    if event.get("X-TYPE", "").upper() == "REPLAY":
                        _ingest_replay(connection, run_id, source, order, event)
                    else:
                        notifications_created += _ingest_session(
                            connection, run_id, source, order, event
                        )
                    order += 1
            replay_rows = connection.execute("SELECT * FROM replays ORDER BY id").fetchall()
            index = _session_index(connection)
            for row in replay_rows:
                decision = _resolve_replay(dict(row), index, options)
                if decision["resolution"] == "unresolved":
                    notifications_created += _notify(
                        connection,
                        run_id,
                        f"unresolved:{row['uid']}:{decision['reason']}",
                        "replay_unresolved",
                        row["uid"],
                        f"回放 {row['uid']} 缺少场次版本线索，进入 unresolved",
                        decision["evidence"],
                    )
        result = project(connection, options, notifications_created)
    finally:
        connection.close()
    result["run_id"] = run_id
    name = scenario.get("name")
    if name:
        result["name"] = name
    return result
