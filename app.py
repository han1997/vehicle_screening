import os
import json
import sqlite3
import time
import uuid
from datetime import datetime, timedelta
from io import BytesIO
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_file, flash

app = Flask(__name__)
app.config["SECRET_KEY"] = "change-this-secret"  # 本地使用即可，如需部署请修改
app.config["UPLOAD_FOLDER"] = os.path.join(os.path.dirname(__file__), "uploads")
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100 MB

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

ALLOWED_EXTENSIONS = {"xls", "xlsx"}
CHECKPOINT_LIBRARY_FILE = os.path.join(os.path.dirname(__file__), "checkpoint_library.json")
FILTER_MODE_PAIR = "pair"
FILTER_MODE_FREQUENT = "frequent"
DEFAULT_FREQUENT_OCCURRENCE = 2
DEFAULT_FREQUENT_START_CLOCK = "00:00"
DEFAULT_FREQUENT_END_CLOCK = "23:59"
SOURCE_COLUMN_PREFIX = "__source__"
PAGE_SIZE = 100

# 简单的内存数据存储，适合本地单用户使用
DATA_STORE = {}

# 会话过期时间（秒）
SESSION_TTL_SECONDS = 2 * 3600  # 2小时


def _db_path(data_id):
    """返回会话对应的 SQLite 文件路径。"""
    return os.path.join(app.config["UPLOAD_FOLDER"], f"{data_id}.db")


def _save_df(df, data_id, table):
    """将 DataFrame 写入 SQLite 表。"""
    conn = sqlite3.connect(_db_path(data_id))
    try:
        df.to_sql(table, conn, if_exists="replace", index=False)
    finally:
        conn.close()


def _load_df(data_id, table):
    """从 SQLite 读取整张表为 DataFrame。"""
    conn = sqlite3.connect(_db_path(data_id))
    try:
        return pd.read_sql(f"SELECT * FROM [{table}]", conn)
    finally:
        conn.close()


def _restore_raw_dtypes(df):
    """raw_data 从 SQLite 读回后恢复 time 列类型。"""
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df


def _restore_pair_dtypes(df):
    """pair 筛选结果恢复时间列类型。"""
    for col in ("first_time", "second_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _restore_frequent_dtypes(df):
    """频繁模式结果恢复时间列和布尔列。"""
    for col in ("first_time", "last_time", "event_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "group_first" in df.columns:
        df["group_first"] = df["group_first"].astype(bool)
    return df


def _touch_session(data_id):
    """更新会话最后访问时间。"""
    if data_id in DATA_STORE:
        DATA_STORE[data_id]["last_access"] = time.time()

RISK_LEVEL_META = {
    "red": {"label": "高风险", "style": 4},
    "yellow": {"label": "中风险", "style": 5},
    "blue": {"label": "低风险", "style": 6},
}


def allowed_file(filename):
    """检查文件扩展名是否允许。"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def normalize_text_list(values):
    """标准化文本列表，去除空值并去重排序。"""
    normalized = set()
    for value in values:
        text = normalize_text_value(value)
        if not text:
            continue
        normalized.add(text)
    return sorted(normalized)


def normalize_text_value(value):
    """将任意值标准化为可比较文本。"""
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "nat"}:
        return ""
    return text


def normalize_choice_list(values, allowed_values=None):
    """按原顺序去重并过滤空值，可选限制在 allowed_values 内。"""
    normalized = []
    seen = set()
    allowed = set(allowed_values) if allowed_values is not None else None

    for value in values or []:
        text = normalize_text_value(value)
        if not text or text in seen:
            continue
        if allowed is not None and text not in allowed:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def normalize_excel_headers(columns):
    """标准化 Excel 表头，空列名补全并保证唯一。"""
    normalized = []
    seen = {}

    for idx, column in enumerate(columns, start=1):
        base = normalize_text_value(column)
        if not base:
            base = f"未命名列{idx}"

        suffix = seen.get(base, 0)
        if suffix > 0:
            name = f"{base}_{suffix + 1}"
        else:
            name = base

        seen[base] = suffix + 1
        normalized.append(name)

    return normalized


def source_column_key(column_name):
    """将原始列名映射到内部列名。"""
    return f"{SOURCE_COLUMN_PREFIX}{column_name}"


def pick_default_export_columns(source_columns):
    """频繁出现模式默认导出列。"""
    priorities = [
        "车牌号",
        "车牌号码",
        "号牌号码",
        "抓拍时间",
        "通过时间",
        "通行时间",
        "抓拍地点",
        "地点",
        "号牌种类",
        "号牌类型",
    ]
    selected = [column for column in priorities if column in source_columns]
    if len(selected) >= 6:
        return selected[:6]

    for column in source_columns:
        if column not in selected:
            selected.append(column)
        if len(selected) >= 6:
            break

    return selected


def parse_time_window(start_time_str, end_time_str):
    """解析前端传入的时间区间。"""
    if not start_time_str or not end_time_str:
        raise ValueError("请输入完整的筛选时间段。")

    try:
        start_time = datetime.fromisoformat(start_time_str)
        end_time = datetime.fromisoformat(end_time_str)
    except ValueError:
        raise ValueError("时间段格式不正确，请重新选择。")

    # datetime-local 通常精度到分钟，这里将结束时间扩展到该分钟末尾，避免秒级数据被误排除。
    if end_time.second == 0 and end_time.microsecond == 0:
        end_time = end_time + timedelta(minutes=1) - timedelta(microseconds=1)

    if start_time > end_time:
        raise ValueError("请确保开始时间早于或等于结束时间。")

    return start_time, end_time


def parse_clock_window(start_clock_str, end_clock_str):
    """解析日内时段（HH:MM-HH:MM），支持跨天。"""
    if not start_clock_str or not end_clock_str:
        raise ValueError("频繁出现模式请填写完整的日内时段。")

    try:
        start_clock = datetime.strptime(start_clock_str, "%H:%M").time()
        end_clock = datetime.strptime(end_clock_str, "%H:%M").time()
    except ValueError:
        raise ValueError("日内时段格式不正确，请按 24 小时制填写，例如 20:00-04:00。")

    return start_clock, end_clock


def split_clock_value(clock_str, default_hour="00", default_minute="00"):
    """将 HH:MM 拆分为小时和分钟文本。"""
    text = normalize_text_value(clock_str)
    if text:
        try:
            parsed = datetime.strptime(text, "%H:%M").time()
            return f"{parsed.hour:02d}", f"{parsed.minute:02d}"
        except ValueError:
            pass
    return default_hour, default_minute


def compose_clock_value(hour_text, minute_text):
    """由小时与分钟文本拼接 HH:MM。"""
    hour_raw = normalize_text_value(hour_text)
    minute_raw = normalize_text_value(minute_text)
    if not hour_raw or not minute_raw:
        return ""

    try:
        hour = int(hour_raw)
        minute = int(minute_raw)
    except ValueError:
        return ""

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return ""

    return f"{hour:02d}:{minute:02d}"


def clock_to_minutes(clock_value):
    """将时分秒转为分钟（向下取整到分钟）。"""
    return clock_value.hour * 60 + clock_value.minute


def merge_distinct_values(values, limit=8):
    """将一组值去重后合并展示。"""
    merged = []
    seen = set()

    for value in values:
        text = normalize_text_value(value)
        if not text or text in seen:
            continue
        seen.add(text)
        merged.append(text)

    if not merged:
        return ""

    if len(merged) <= limit:
        return " | ".join(merged)

    return " | ".join(merged[:limit]) + f" | ... 共 {len(merged)} 项"


def format_datetime_string(value):
    """格式化时间用于展示和导出。"""
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")


def load_checkpoint_library():
    """从本地文件加载卡口库。"""
    if not os.path.exists(CHECKPOINT_LIBRARY_FILE):
        return []

    try:
        with open(CHECKPOINT_LIBRARY_FILE, "r", encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, json.JSONDecodeError):
        return []

    if isinstance(payload, dict):
        values = payload.get("checkpoints", [])
    else:
        values = payload

    if not isinstance(values, list):
        return []

    return normalize_text_list(values)


def save_checkpoint_library(checkpoints):
    """将卡口库保存到本地文件。"""
    normalized = normalize_text_list(checkpoints)
    payload = {"checkpoints": normalized, "updated_at": datetime.now().isoformat(timespec="seconds")}
    with open(CHECKPOINT_LIBRARY_FILE, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    return normalized


def prune_removed_checkpoints_from_config(config, removed_checkpoints):
    """移除配置中已删除的卡口引用。"""
    if not isinstance(config, dict):
        return {}

    removed_set = set(removed_checkpoints or [])
    if not removed_set:
        return config

    first_checkpoint = normalize_text_value(config.get("first_checkpoint"))
    second_checkpoint = normalize_text_value(config.get("second_checkpoint"))
    entry_checkpoint = normalize_text_value(config.get("entry_checkpoint"))
    exit_checkpoint = normalize_text_value(config.get("exit_checkpoint"))

    if first_checkpoint in removed_set:
        config["first_checkpoint"] = ""
    if second_checkpoint in removed_set:
        config["second_checkpoint"] = ""
    if entry_checkpoint in removed_set:
        config["entry_checkpoint"] = ""
    if exit_checkpoint in removed_set:
        config["exit_checkpoint"] = ""

    frequent_checkpoints = normalize_choice_list(config.get("frequent_checkpoints", []))
    config["frequent_checkpoints"] = [
        checkpoint for checkpoint in frequent_checkpoints if checkpoint not in removed_set
    ]
    return config


def save_uploaded_excel(file_storage):
    """保存上传的 Excel 文件并返回路径。"""
    ext = file_storage.filename.rsplit(".", 1)[1].lower()
    filename = f"{uuid.uuid4()}.{ext}"
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file_storage.save(filepath)
    return filepath


def format_datetime_local(value):
    """将日期时间格式化为 datetime-local 输入控件可用的值。"""
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).strftime("%Y-%m-%dT%H:%M")


def get_risk_label(level):
    """将内部风险级别映射为展示文案。"""
    return RISK_LEVEL_META.get(level, {}).get("label", "")


def get_frequent_level(occurrence_count, threshold):
    """根据出现次数返回频繁模式分级。"""
    if occurrence_count >= threshold + 3:
        return "red", "高频"
    if occurrence_count >= threshold + 1:
        return "yellow", "关注"
    return "blue", "达标"


def build_results_summary(df):
    """统计筛选结果的风险分布与关键指标。"""
    summary = {"total": 0, "red": 0, "yellow": 0, "blue": 0, "max_score": 0, "avg_delta": 0.0}

    if df is None or df.empty:
        return summary

    summary["total"] = int(len(df))

    if "level" in df.columns:
        level_counts = df["level"].value_counts().to_dict()
        for level in ("red", "yellow", "blue"):
            summary[level] = int(level_counts.get(level, 0))

    if "score" in df.columns:
        score_series = pd.to_numeric(df["score"], errors="coerce").dropna()
        if not score_series.empty:
            summary["max_score"] = int(score_series.max())

    if "delta_minutes" in df.columns:
        delta_series = pd.to_numeric(df["delta_minutes"], errors="coerce").dropna()
        if not delta_series.empty:
            summary["avg_delta"] = round(float(delta_series.mean()), 2)

    return summary


def build_frequent_results_summary(df, matched_records, threshold):
    """统计频繁出现模式的结果概览。"""
    summary = {
        "total_vehicles": 0,
        "matched_records": int(matched_records),
        "threshold": int(threshold),
        "max_occurrence": 0,
        "avg_occurrence": 0.0,
        "total_occurrences": 0,
        "multi_checkpoint_vehicles": 0,
    }

    if df is None or df.empty:
        return summary

    summary["total_occurrences"] = int(len(df))

    if "plate" in df.columns:
        vehicle_df = df.drop_duplicates(subset=["plate"])
    else:
        vehicle_df = df.copy()

    summary["total_vehicles"] = int(len(vehicle_df))

    if "occurrence_count" in vehicle_df.columns:
        occurrence_series = pd.to_numeric(vehicle_df["occurrence_count"], errors="coerce").dropna()
    else:
        occurrence_series = pd.Series(dtype=float)

    if "checkpoint_count" in vehicle_df.columns:
        checkpoint_series = pd.to_numeric(vehicle_df["checkpoint_count"], errors="coerce").dropna()
    else:
        checkpoint_series = pd.Series(dtype=float)

    if not occurrence_series.empty:
        summary["max_occurrence"] = int(occurrence_series.max())
        summary["avg_occurrence"] = round(float(occurrence_series.mean()), 2)

    if not checkpoint_series.empty:
        summary["multi_checkpoint_vehicles"] = int((checkpoint_series >= 2).sum())

    return summary


def build_pair_filtered_dataframe(
    df,
    start_time,
    end_time,
    active_first_locations,
    active_second_locations,
    target_minutes,
):
    """第一/第二卡口配对模式计算。"""
    valid_locations = active_first_locations.union(active_second_locations)
    df_valid = df[df["location"].isin(valid_locations)].copy()
    df_valid = df_valid[(df_valid["time"] >= start_time) & (df_valid["time"] <= end_time)]
    df_valid = df_valid.sort_values("time")

    results = []

    for plate, group in df_valid.groupby("plate"):
        first_events = group[group["location"].isin(active_first_locations)]
        second_events = group[group["location"].isin(active_second_locations)]

        if first_events.empty or second_events.empty:
            continue

        second_list = list(second_events.itertuples(index=False))

        for first_row in first_events.itertuples(index=False):
            first_time = first_row.time
            best_second = None
            best_delta = None

            for second_row in second_list:
                second_time = second_row.time
                if second_time <= first_time:
                    continue
                delta = second_time - first_time
                best_second = second_row
                best_delta = delta
                break

            if best_second and best_delta is not None:
                delta_minutes = best_delta.total_seconds() / 60.0
                diff = abs(delta_minutes - target_minutes)
                normalized = diff / target_minutes
                raw_score = max(0.0, 1.0 - normalized)
                score = int(round(raw_score * 100))

                if score >= 70:
                    level = "red"
                elif score >= 40:
                    level = "yellow"
                else:
                    level = "blue"

                results.append(
                    {
                        "plate": plate,
                        "plate_type": getattr(first_row, "plate_type", ""),
                        "first_time": first_row.time,
                        "first_location": first_row.location,
                        "second_time": best_second.time,
                        "second_location": best_second.location,
                        "delta_minutes": delta_minutes,
                        "score": score,
                        "level": level,
                    }
                )

    if results:
        filtered_df = pd.DataFrame(results)
        return filtered_df.sort_values("score", ascending=False)

    return pd.DataFrame(
        columns=[
            "plate",
            "plate_type",
            "first_time",
            "first_location",
            "second_time",
            "second_location",
            "delta_minutes",
            "score",
            "level",
        ]
    )


def build_frequent_filtered_dataframe(
    df,
    start_clock,
    end_clock,
    active_checkpoints,
    min_occurrence,
):
    """多卡口频繁出现模式计算。"""
    df_valid = df[df["location"].isin(set(active_checkpoints))].copy()
    start_minutes = clock_to_minutes(start_clock)
    end_minutes = clock_to_minutes(end_clock)

    clock_minutes = df_valid["time"].dt.hour * 60 + df_valid["time"].dt.minute
    if start_minutes <= end_minutes:
        time_mask = (clock_minutes >= start_minutes) & (clock_minutes <= end_minutes)
    else:
        # 例如 20:00-04:00，表示跨天时段
        time_mask = (clock_minutes >= start_minutes) | (clock_minutes <= end_minutes)

    df_valid = df_valid[time_mask]
    df_valid = df_valid.sort_values("time")
    matched_records = int(len(df_valid))

    detail_rows = []
    filtered_vehicle_count = 0

    for plate, group in df_valid.groupby("plate", sort=False):
        occurrence_count = int(len(group))
        if occurrence_count < min_occurrence:
            continue

        filtered_vehicle_count += 1
        group_sorted = group.sort_values("time")
        first_time = group_sorted["time"].iloc[0]
        last_time = group_sorted["time"].iloc[-1]
        duration_minutes = (last_time - first_time).total_seconds() / 60.0

        checkpoint_counts = group_sorted["location"].value_counts()
        checkpoint_summary = "、".join(
            f"{location} × {int(count)}" for location, count in checkpoint_counts.items()
        )
        group_size = int(len(group_sorted))
        summary_plate_type = merge_distinct_values(group_sorted["plate_type"].tolist())

        for idx, row in enumerate(group_sorted.to_dict(orient="records")):
            detail_rows.append(
                {
                    "plate": plate,
                    "plate_type_summary": summary_plate_type,
                    "occurrence_count": occurrence_count,
                    "first_time": first_time,
                    "last_time": last_time,
                    "duration_minutes": round(duration_minutes, 2),
                    "checkpoint_count": int(checkpoint_counts.size),
                    "checkpoint_summary": checkpoint_summary,
                    "event_time": row.get("time"),
                    "event_location": normalize_text_value(row.get("location")),
                    "event_plate_type": normalize_text_value(row.get("plate_type")),
                    "group_row_index": idx,
                    "group_size": group_size,
                    "group_first": idx == 0,
                }
            )

            for column in df.columns:
                if not str(column).startswith(SOURCE_COLUMN_PREFIX):
                    continue
                detail_rows[-1][column] = row.get(column)

    if detail_rows:
        filtered_df = pd.DataFrame(detail_rows)
        filtered_df = filtered_df.sort_values(
            ["occurrence_count", "checkpoint_count", "plate", "event_time"],
            ascending=[False, False, True, True],
        )
    else:
        filtered_df = pd.DataFrame(
            columns=[
                "plate",
                "plate_type_summary",
                "occurrence_count",
                "first_time",
                "last_time",
                "duration_minutes",
                "checkpoint_count",
                "checkpoint_summary",
                "event_time",
                "event_location",
                "event_plate_type",
                "group_row_index",
                "group_size",
                "group_first",
            ]
        )

    return filtered_df, matched_records, filtered_vehicle_count


def build_pair_display_results(filtered_df):
    """构造第一/第二卡口模式的前端展示数据。"""
    display_results = []
    for row in filtered_df.itertuples(index=False):
        level_val = str(row.level) if hasattr(row, "level") and pd.notnull(row.level) else ""
        delta_raw = getattr(row, "delta_minutes", 0.0)
        delta_value = float(delta_raw) if pd.notnull(delta_raw) else 0.0
        score_raw = getattr(row, "score", 0)
        score_value = int(score_raw) if pd.notnull(score_raw) else 0
        display_results.append(
            {
                "plate": row.plate,
                "plate_type": normalize_text_value(getattr(row, "plate_type", "")),
                "first_time": format_datetime_string(getattr(row, "first_time", None)),
                "first_location": normalize_text_value(getattr(row, "first_location", "")),
                "second_time": format_datetime_string(getattr(row, "second_time", None)),
                "second_location": normalize_text_value(getattr(row, "second_location", "")),
                "delta_minutes": delta_value,
                "score": score_value,
                "level": level_val,
                "level_label": get_risk_label(level_val),
            }
        )
    return display_results


def build_frequent_display_results(filtered_df, threshold, selected_export_columns):
    """构造频繁出现模式的前端展示数据。"""
    # 过滤掉与内置列语义重复的导出列（仅影响展示，导出文件仍保留全量）
    display_export_columns = [c for c in selected_export_columns if not _is_overlapping_column(c)]

    display_results = []

    for row in filtered_df.to_dict(orient="records"):
        occurrence_count = int(row.get("occurrence_count", 0) or 0)
        level, level_label = get_frequent_level(occurrence_count, threshold)
        group_size = int(row.get("group_size", 1) or 1)
        group_first = bool(row.get("group_first", False))

        detail_columns = []
        for column in display_export_columns:
            detail_columns.append(
                {
                    "name": column,
                    "value": normalize_text_value(row.get(source_column_key(column), "")),
                }
            )

        display_results.append(
            {
                "plate": normalize_text_value(row.get("plate", "")),
                "plate_type": normalize_text_value(row.get("plate_type_summary", "")),
                "occurrence_count": occurrence_count,
                "first_time": format_datetime_string(row.get("first_time")),
                "last_time": format_datetime_string(row.get("last_time")),
                "duration_minutes": float(row.get("duration_minutes", 0.0) or 0.0),
                "checkpoint_count": int(row.get("checkpoint_count", 0) or 0),
                "checkpoint_summary": normalize_text_value(row.get("checkpoint_summary", "")),
                "event_time": format_datetime_string(row.get("event_time")),
                "event_location": normalize_text_value(row.get("event_location", "")),
                "level": level,
                "level_label": level_label,
                "group_size": group_size,
                "group_first": group_first,
                "detail_columns": detail_columns,
            }
        )

    return display_results


def paginate_results(results, page, filter_mode):
    """对结果列表分页，频繁模式按车辆组边界切割避免断行。"""
    if not results:
        return [], 1, False, False

    total = len(results)

    if filter_mode == FILTER_MODE_PAIR:
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(1, min(page, total_pages))
        start = (page - 1) * PAGE_SIZE
        end = min(start + PAGE_SIZE, total)
        return results[start:end], total_pages, page > 1, page < total_pages

    # 频繁模式：按车辆组边界分页
    group_starts = [i for i, r in enumerate(results) if r.get("group_first")]
    total_groups = len(group_starts)
    if total_groups == 0:
        return [], 1, False, False
    total_pages = max(1, (total_groups + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, total_pages))
    start_group_idx = (page - 1) * PAGE_SIZE
    end_group_idx = min(start_group_idx + PAGE_SIZE, total_groups)
    start_row = group_starts[start_group_idx]
    end_row = group_starts[end_group_idx] if end_group_idx < total_groups else total
    return results[start_row:end_row], total_pages, page > 1, page < total_pages


def build_export_dataframe(filtered_df):
    """构造导出使用的数据表，并保留每行风险级别。"""
    export_df = filtered_df.copy()
    risk_levels = (
        export_df["level"].fillna("").astype(str).tolist()
        if "level" in export_df.columns
        else ["" for _ in range(len(export_df))]
    )

    export_df["车牌号"] = export_df["plate"].astype(str)

    if "plate_type" in export_df.columns:
        export_df["号牌种类"] = export_df["plate_type"].astype(str)
    else:
        export_df["号牌种类"] = ""

    export_df["第一卡口通行时间"] = pd.to_datetime(
        export_df["first_time"], errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")
    export_df["第一卡口通行时间"] = export_df["第一卡口通行时间"].fillna("")

    export_df["第一卡口"] = export_df["first_location"].astype(str)

    export_df["第二卡口通行时间"] = pd.to_datetime(
        export_df["second_time"], errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")
    export_df["第二卡口通行时间"] = export_df["第二卡口通行时间"].fillna("")

    export_df["第二卡口"] = export_df["second_location"].astype(str)

    export_df["时间间隔（分钟）"] = export_df["delta_minutes"].astype(float).round(2)

    if "score" in export_df.columns:
        export_df["评分"] = export_df["score"]
    else:
        export_df["评分"] = 0

    export_df["风险等级"] = [get_risk_label(level) for level in risk_levels]

    columns = [
        "车牌号",
        "号牌种类",
        "第一卡口通行时间",
        "第一卡口",
        "第二卡口通行时间",
        "第二卡口",
        "时间间隔（分钟）",
        "评分",
        "风险等级",
    ]
    return export_df[columns], risk_levels


def build_frequent_export_dataframe(filtered_df, selected_export_columns, threshold):
    """构造频繁出现模式导出表，并返回需要合并的单元格信息和风险级别列表。"""
    summary_columns = [
        "车牌号",
        "号牌种类",
        "出现次数",
        "首次出现时间",
        "最后出现时间",
        "覆盖时长（分钟）",
        "涉及卡口数",
        "卡口分布",
        "频次级别",
    ]
    detail_columns = [
        "本条抓拍时间",
        "本条卡口",
        "本条号牌种类",
    ]
    all_columns = summary_columns + detail_columns + selected_export_columns

    if filtered_df is None or filtered_df.empty:
        return pd.DataFrame(columns=all_columns), [], []

    export_rows = []
    merge_ranges = []
    risk_levels = []
    excel_row = 2

    for row in filtered_df.to_dict(orient="records"):
        occurrence_count = int(row.get("occurrence_count", 0) or 0)
        level, level_label = get_frequent_level(occurrence_count, threshold)
        risk_levels.append(level)

        export_row = {
            "车牌号": normalize_text_value(row.get("plate", "")),
            "号牌种类": normalize_text_value(row.get("plate_type_summary", "")),
            "出现次数": occurrence_count,
            "首次出现时间": format_datetime_string(row.get("first_time")),
            "最后出现时间": format_datetime_string(row.get("last_time")),
            "覆盖时长（分钟）": round(float(row.get("duration_minutes", 0.0) or 0.0), 2),
            "涉及卡口数": int(row.get("checkpoint_count", 0) or 0),
            "卡口分布": normalize_text_value(row.get("checkpoint_summary", "")),
            "频次级别": level_label,
            "本条抓拍时间": format_datetime_string(row.get("event_time")),
            "本条卡口": normalize_text_value(row.get("event_location", "")),
            "本条号牌种类": normalize_text_value(row.get("event_plate_type", "")),
        }

        for column in selected_export_columns:
            export_row[column] = normalize_text_value(row.get(source_column_key(column), ""))

        export_rows.append(export_row)
        if bool(row.get("group_first", False)):
            group_size = int(row.get("group_size", 1) or 1)
            if group_size > 1:
                start_row = excel_row
                end_row = excel_row + group_size - 1
                for column in summary_columns:
                    col_idx = all_columns.index(column) + 1
                    merge_ranges.append((start_row, end_row, col_idx))
        excel_row += 1

    export_df = pd.DataFrame(export_rows)
    return export_df[all_columns], merge_ranges, risk_levels


def excel_column_name(index):
    """将 1-based 列号转换为 Excel 列名。"""
    result = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def build_xlsx_inline_cell(ref, value, style_id):
    """构造 xlsx 的内联字符串单元格。"""
    if value is None or pd.isna(value):
        text = ""
    else:
        text = str(value)
    return (
        f'<c r="{ref}" s="{style_id}" t="inlineStr">'
        f'<is><t xml:space="preserve">{escape(text)}</t></is>'
        f"</c>"
    )


# ---- OOXML 模板常量（build_warning_workbook / build_frequent_warning_workbook 共用） ----

WORKBOOK_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="筛选结果" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
"""

WORKBOOK_RELS_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

RELS_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

CONTENT_TYPES_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""

STYLES_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="3">
    <font><sz val="11"/><color rgb="FF1F2937"/><name val="Microsoft YaHei UI"/><family val="2"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Microsoft YaHei UI"/><family val="2"/></font>
    <font><b/><sz val="15"/><color rgb="FF0F172A"/><name val="Microsoft YaHei UI"/><family val="2"/></font>
  </fonts>
  <fills count="7">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF0F766E"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFE6FFFA"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFE4E6"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF7D6"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFE0F2FE"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"><color rgb="FFD1D5DB"/></left>
      <right style="thin"><color rgb="FFD1D5DB"/></right>
      <top style="thin"><color rgb="FFD1D5DB"/></top>
      <bottom style="thin"><color rgb="FFD1D5DB"/></bottom>
      <diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="7">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" applyBorder="1" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="2" fillId="3" borderId="1" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="center" horizontal="left"/></xf>
    <xf numFmtId="0" fontId="0" fillId="3" borderId="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="center" horizontal="left"/></xf>
    <xf numFmtId="0" fontId="1" fillId="2" borderId="1" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="center" horizontal="center"/></xf>
    <xf numFmtId="0" fontId="0" fillId="4" borderId="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="5" borderId="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="6" borderId="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""


def build_warning_workbook(export_df, risk_levels, summary):
    """生成带风险底色的 xlsx 文件。"""
    output = BytesIO()
    columns = export_df.columns.tolist()
    last_col = excel_column_name(len(columns))
    last_row = len(export_df) + 4
    exported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary_text = (
        f"高风险 {summary['red']} 条  |  中风险 {summary['yellow']} 条  |  "
        f"低风险 {summary['blue']} 条  |  共 {summary['total']} 条"
    )

    row_parts = []
    row_parts.append(
        '<row r="1" ht="28" customHeight="1">'
        + build_xlsx_inline_cell("A1", "车辆进出筛选风险结果", 1)
        + "</row>"
    )
    row_parts.append(
        '<row r="2" ht="22" customHeight="1">'
        + build_xlsx_inline_cell("A2", summary_text, 2)
        + "</row>"
    )
    row_parts.append(
        '<row r="3" ht="22" customHeight="1">'
        + build_xlsx_inline_cell("A3", f"导出时间：{exported_at}", 2)
        + "</row>"
    )

    header_cells = []
    for idx, column in enumerate(columns, start=1):
        header_cells.append(build_xlsx_inline_cell(f"{excel_column_name(idx)}4", column, 3))
    row_parts.append('<row r="4" ht="26" customHeight="1">' + "".join(header_cells) + "</row>")

    for row_index, (row, level) in enumerate(
        zip(export_df.itertuples(index=False), risk_levels), start=5
    ):
        style_id = RISK_LEVEL_META.get(level, {}).get("style", 0)
        data_cells = []
        for col_index, value in enumerate(row, start=1):
            cell_ref = f"{excel_column_name(col_index)}{row_index}"
            data_cells.append(build_xlsx_inline_cell(cell_ref, value, style_id))
        row_parts.append(
            f'<row r="{row_index}" ht="24" customHeight="1">{"".join(data_cells)}</row>'
        )

    merge_refs = (
        f'<mergeCells count="3">'
        f'<mergeCell ref="A1:{last_col}1"/>'
        f'<mergeCell ref="A2:{last_col}2"/>'
        f'<mergeCell ref="A3:{last_col}3"/>'
        f"</mergeCells>"
    )
    column_widths = [14, 14, 21, 18, 21, 18, 14, 10, 12]
    cols_xml = "".join(
        f'<col min="{idx}" max="{idx}" width="{width}" customWidth="1"/>'
        for idx, width in enumerate(column_widths, start=1)
    )

    sheet_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <dimension ref="A1:{last_col}{last_row}"/>
  <sheetViews>
    <sheetView workbookViewId="0">
      <pane ySplit="4" topLeftCell="A5" activePane="bottomLeft" state="frozen"/>
    </sheetView>
  </sheetViews>
  <sheetFormatPr defaultRowHeight="22"/>
  <cols>{cols_xml}</cols>
  <sheetData>{"".join(row_parts)}</sheetData>
  <autoFilter ref="A4:{last_col}{last_row}"/>
  {merge_refs}
  <pageMargins left="0.4" right="0.4" top="0.5" bottom="0.5" header="0.2" footer="0.2"/>
</worksheet>
"""

    created_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{created_at}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{created_at}</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Microsoft Excel</Application>
</Properties>
"""

    with ZipFile(output, "w", ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", CONTENT_TYPES_XML)
        workbook.writestr("_rels/.rels", RELS_XML)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", WORKBOOK_XML)
        workbook.writestr("xl/_rels/workbook.xml.rels", WORKBOOK_RELS_XML)
        workbook.writestr("xl/styles.xml", STYLES_XML)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)

    output.seek(0)
    return output


def build_frequent_warning_workbook(export_df, risk_levels, summary, merge_ranges):
    """生成频繁出现模式带风险底色的 xlsx 文件，支持合并单元格。"""
    output = BytesIO()
    columns = export_df.columns.tolist()
    last_col = excel_column_name(len(columns))
    last_row = len(export_df) + 4
    exported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary_text = (
        f"高频 {summary.get('red', 0)} 条  |  关注 {summary.get('yellow', 0)} 条  |  "
        f"达标 {summary.get('blue', 0)} 条  |  共 {summary.get('total', 0)} 条"
    )

    row_parts = []
    row_parts.append(
        '<row r="1" ht="28" customHeight="1">'
        + build_xlsx_inline_cell("A1", "频繁出现车辆筛选结果", 1)
        + "</row>"
    )
    row_parts.append(
        '<row r="2" ht="22" customHeight="1">'
        + build_xlsx_inline_cell("A2", summary_text, 2)
        + "</row>"
    )
    row_parts.append(
        '<row r="3" ht="22" customHeight="1">'
        + build_xlsx_inline_cell("A3", f"导出时间：{exported_at}", 2)
        + "</row>"
    )

    header_cells = []
    for idx, column in enumerate(columns, start=1):
        header_cells.append(build_xlsx_inline_cell(f"{excel_column_name(idx)}4", column, 3))
    row_parts.append('<row r="4" ht="26" customHeight="1">' + "".join(header_cells) + "</row>")

    for row_index, (row, level) in enumerate(
        zip(export_df.itertuples(index=False), risk_levels), start=5
    ):
        style_id = RISK_LEVEL_META.get(level, {}).get("style", 0)
        data_cells = []
        for col_index, value in enumerate(row, start=1):
            cell_ref = f"{excel_column_name(col_index)}{row_index}"
            data_cells.append(build_xlsx_inline_cell(cell_ref, value, style_id))
        row_parts.append(
            f'<row r="{row_index}" ht="24" customHeight="1">{"".join(data_cells)}</row>'
        )

    merge_cell_parts = [
        f'<mergeCell ref="A1:{last_col}1"/>',
        f'<mergeCell ref="A2:{last_col}2"/>',
        f'<mergeCell ref="A3:{last_col}3"/>',
    ]
    for start_row, end_row, column_idx in merge_ranges:
        col_name = excel_column_name(column_idx)
        merge_cell_parts.append(f'<mergeCell ref="{col_name}{start_row}:{col_name}{end_row}"/>')
    merge_refs = f'<mergeCells count="{len(merge_cell_parts)}">{"".join(merge_cell_parts)}</mergeCells>'

    col_count = len(columns)
    column_widths = [14] * col_count
    cols_xml = "".join(
        f'<col min="{idx}" max="{idx}" width="{w}" customWidth="1"/>'
        for idx, w in enumerate(column_widths, start=1)
    )

    sheet_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <dimension ref="A1:{last_col}{last_row}"/>
  <sheetViews>
    <sheetView workbookViewId="0">
      <pane ySplit="4" topLeftCell="A5" activePane="bottomLeft" state="frozen"/>
    </sheetView>
  </sheetViews>
  <sheetFormatPr defaultRowHeight="22"/>
  <cols>{cols_xml}</cols>
  <sheetData>{"".join(row_parts)}</sheetData>
  <autoFilter ref="A4:{last_col}{last_row}"/>
  {merge_refs}
  <pageMargins left="0.4" right="0.4" top="0.5" bottom="0.5" header="0.2" footer="0.2"/>
</worksheet>
"""

    created_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{created_at}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{created_at}</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Microsoft Excel</Application>
</Properties>
"""

    with ZipFile(output, "w", ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", CONTENT_TYPES_XML)
        workbook.writestr("_rels/.rels", RELS_XML)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", WORKBOOK_XML.replace("筛选结果", "频繁出现车辆"))
        workbook.writestr("xl/_rels/workbook.xml.rels", WORKBOOK_RELS_XML)
        workbook.writestr("xl/styles.xml", STYLES_XML)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)

    output.seek(0)
    return output


def find_matching_column(df, candidates):
    """在 DataFrame 中根据候选列表找到匹配的列名。"""
    normalized = {col: normalize_text_value(col).lower() for col in df.columns}
    for cand in candidates:
        cand_normalized = normalize_text_value(cand).lower()
        for original, norm in normalized.items():
            if norm == cand_normalized:
                return original
    return None


# 与频繁模式内置列语义重叠的候选词，用于过滤导出列中的重复项
_OVERLAP_CANDIDATES = {
    "plate": ["车牌号", "车牌号码", "车牌", "号牌号码", "plate", "plate_no", "license_plate"],
    "time": ["抓拍时间", "通过时间", "时间", "通行时间", "capture_time", "time", "timestamp"],
    "location": ["抓拍地点", "地点", "位置", "地点名称", "location", "site"],
    "plate_type": ["号牌种类", "号牌类型", "车牌种类", "车牌类型", "plate_type", "plate_kind"],
}


def _is_overlapping_column(column_name):
    """判断列名是否与频繁模式内置汇总/明细列语义重复。"""
    col_lower = normalize_text_value(column_name).lower()
    if not col_lower:
        return False
    for candidates in _OVERLAP_CANDIDATES.values():
        for cand in candidates:
            if normalize_text_value(cand).lower() == col_lower:
                return True
    return False


def parse_excel(path):
    """读取并标准化 Excel 数据，返回标准化数据和原始列名。"""
    plate_candidates = [
        "车牌号", "车牌号码", "车牌", "号牌号码",
        "plate", "plate_no", "license_plate",
    ]
    time_candidates = [
        "抓拍时间", "通过时间", "时间", "通行时间",
        "capture_time", "time", "timestamp",
    ]
    location_candidates = [
        "抓拍地点", "地点", "位置", "地点名称",
        "location", "site",
    ]
    plate_type_candidates = [
        "号牌种类", "号牌类型", "车牌种类", "车牌类型",
        "plate_type", "plate_kind", "plate_category",
    ]

    # 第一步：只读表头，识别需要的列
    try:
        header_df = pd.read_excel(path, nrows=0)
    except Exception as exc:
        raise ValueError(f"无法读取Excel文件: {exc}")

    if header_df.empty and len(header_df.columns) == 0:
        raise ValueError("Excel 文件为空。")

    normalized_headers = normalize_excel_headers(header_df.columns)
    source_columns = list(normalized_headers)

    plate_col = _find_matching_from_headers(header_df.columns, normalized_headers, plate_candidates)
    time_col = _find_matching_from_headers(header_df.columns, normalized_headers, time_candidates)
    location_col = _find_matching_from_headers(header_df.columns, normalized_headers, location_candidates)
    plate_type_col = _find_matching_from_headers(header_df.columns, normalized_headers, plate_type_candidates)

    missing = []
    if plate_col is None:
        missing.append("车牌号列")
    if time_col is None:
        missing.append("抓拍时间列")
    if location_col is None:
        missing.append("抓拍地点列")

    if missing:
        cols_str = ", ".join(str(c) for c in normalized_headers)
        raise ValueError(f"无法自动识别列: {', '.join(missing)}。当前表头为: {cols_str}")

    # 第二步：读所有列，指定 dtype=str 避免逐列类型推断（主要性能优化点）
    try:
        df = pd.read_excel(path, dtype=str)
    except Exception as exc:
        raise ValueError(f"无法读取Excel文件: {exc}")

    if df.empty:
        raise ValueError("Excel 文件为空。")

    # 用标准化列名替换原始列名
    col_rename = {orig: norm for orig, norm in zip(header_df.columns, normalized_headers) if orig in df.columns}
    df.rename(columns=col_rename, inplace=True)
    plate_col = col_rename.get(plate_col, plate_col)
    time_col = col_rename.get(time_col, time_col)
    location_col = col_rename.get(location_col, location_col)
    if plate_type_col is not None:
        plate_type_col = col_rename.get(plate_type_col, plate_type_col)

    parsed_df = pd.DataFrame(
        {
            "plate": df[plate_col],
            "time": pd.to_datetime(df[time_col], errors="coerce"),
            "location": df[location_col],
        }
    )

    if plate_type_col is not None:
        parsed_df["plate_type"] = df[plate_type_col]
    else:
        parsed_df["plate_type"] = ""

    # 保留原始列副本，用于频繁模式导出
    for column in source_columns:
        if column in df.columns:
            parsed_df[source_column_key(column)] = df[column]

    parsed_df["plate"] = parsed_df["plate"].map(normalize_text_value)
    parsed_df["location"] = parsed_df["location"].map(normalize_text_value)
    parsed_df["plate_type"] = parsed_df["plate_type"].map(normalize_text_value)

    parsed_df = parsed_df.dropna(subset=["time"])
    parsed_df = parsed_df[parsed_df["plate"] != ""]
    parsed_df = parsed_df[parsed_df["location"] != ""]
    parsed_df = parsed_df[parsed_df["plate"] != "无牌车"]
    parsed_df = parsed_df[parsed_df["plate"] != "未识别"]

    return parsed_df, source_columns


def _find_matching_from_headers(original_columns, normalized_headers, candidates):
    """在已标准化的表头中根据候选列表找到匹配的原始列名。"""
    for cand in candidates:
        cand_normalized = normalize_text_value(cand).lower()
        for orig, norm in zip(original_columns, normalized_headers):
            if normalize_text_value(norm).lower() == cand_normalized:
                return orig
    return None


def import_checkpoints_from_dataframe(df, column_name, source_columns):
    """从已解析的 DataFrame 中按指定列导入卡口（复用内存数据，避免重复读文件）。"""
    selected_column = str(column_name).strip()
    if not selected_column:
        raise ValueError("请选择要导入的卡口列。")

    source_key = source_column_key(selected_column)
    if source_key not in df.columns:
        if selected_column not in source_columns:
            raise ValueError(f"所选列'{selected_column}'不在当前数据中。")
        # 列名在 source_columns 中但未找到 __source__ 前缀，数据中无此列
        raise ValueError(f"所选列'{selected_column}'不在当前数据中。")

    values = df[source_key].dropna().tolist()
    checkpoints = normalize_text_list(values)
    if not checkpoints:
        raise ValueError("指定列中没有可导入的卡口名称。")

    return checkpoints, 1


@app.before_request
def _cleanup_sessions():
    """清理过期的会话数据及对应的 SQLite 文件。"""
    now = time.time()
    expired = [
        did
        for did, meta in DATA_STORE.items()
        if now - meta.get("last_access", 0) > SESSION_TTL_SECONDS
    ]
    for did in expired:
        db_file = DATA_STORE.pop(did, {}).get("db_path")
        if db_file and os.path.exists(db_file):
            try:
                os.remove(db_file)
            except OSError:
                pass


@app.route("/", methods=["GET"])
def upload_form():
    """上传页面。"""
    checkpoint_library = load_checkpoint_library()
    matched_home_checkpoints = checkpoint_library[:12]
    return render_template(
        "upload.html",
        checkpoint_library=checkpoint_library,
        matched_home_checkpoints=matched_home_checkpoints,
    )


@app.route("/upload", methods=["POST"])
def upload():
    """接收并解析上传的 Excel 文件（支持多文件）。"""
    if "files" not in request.files:
        flash("未找到上传文件。")
        return redirect(url_for("upload_form"))

    files = request.files.getlist("files")
    files = [f for f in files if f and f.filename]
    if not files:
        flash("请选择要上传的文件。")
        return redirect(url_for("upload_form"))

    dfs = []
    source_columns = []
    source_column_set = set()

    for file in files:
        if not allowed_file(file.filename):
            flash(f"文件 {file.filename} 不是支持的 Excel 格式。")
            return redirect(url_for("upload_form"))

        filepath = save_uploaded_excel(file)

        try:
            df_part, file_columns = parse_excel(filepath)
        except ValueError as exc:
            flash(f"文件 {file.filename} 解析失败: {exc}")
            return redirect(url_for("upload_form"))

        dfs.append(df_part)
        for column in file_columns:
            if column not in source_column_set:
                source_column_set.add(column)
                source_columns.append(column)

    if not dfs:
        flash("未解析到任何有效数据。")
        return redirect(url_for("upload_form"))

    df = pd.concat(dfs, ignore_index=True)
    locations = sorted(df["location"].dropna().unique().tolist())
    plate_type_values = [normalize_text_value(v) for v in df.get("plate_type", pd.Series(dtype=object)).tolist()]
    plate_types = sorted({value for value in plate_type_values if value})

    data_id = str(uuid.uuid4())
    default_max_minutes = 30.0
    default_start_time = format_datetime_local(df["time"].min())
    default_end_time = format_datetime_local(df["time"].max())
    default_export_columns = pick_default_export_columns(source_columns)

    # DataFrame 持久化到 SQLite，不在内存中保留
    _save_df(df, data_id, "raw_data")
    del df
    del dfs

    DATA_STORE[data_id] = {
        "db_path": _db_path(data_id),
        "locations": locations,
        "plate_types": plate_types,
        "source_columns": source_columns,
        "last_imported_checkpoint_column": "",
        "default_max_minutes": default_max_minutes,
        "default_start_time": default_start_time,
        "default_end_time": default_end_time,
        "filtered_mode": None,
        "summary": None,
        "selected_export_columns": [],
        "config": {
            "filter_mode": FILTER_MODE_PAIR,
            "min_occurrence": DEFAULT_FREQUENT_OCCURRENCE,
            "frequent_start_clock": DEFAULT_FREQUENT_START_CLOCK,
            "frequent_end_clock": DEFAULT_FREQUENT_END_CLOCK,
            "export_columns": default_export_columns,
            "start_time": default_start_time,
            "end_time": default_end_time,
            "target_minutes": default_max_minutes,
        },
        "last_access": time.time(),
    }

    return redirect(url_for("review", data_id=data_id))


@app.route("/checkpoint/import/uploaded/<data_id>", methods=["POST"])
def import_checkpoint_from_uploaded(data_id):
    """从本次已上传通行记录中，按选择列导入卡口库。"""
    data = DATA_STORE.get(data_id)
    if not data:
        flash("数据已过期或不存在，请重新上传文件。")
        return redirect(url_for("upload_form"))

    column_name = request.form.get("checkpoint_source_column", "").strip()
    if not column_name:
        flash("请选择要导入的卡口列。")
        return redirect(url_for("review", data_id=data_id))

    source_columns = data.get("source_columns", [])
    try:
        df = _restore_raw_dtypes(_load_df(data_id, "raw_data"))
    except Exception:
        flash("未找到已解析数据，请重新上传后再试。")
        return redirect(url_for("upload_form"))

    try:
        imported_checkpoints, _ = import_checkpoints_from_dataframe(
            df, column_name, source_columns
        )
    except ValueError as exc:
        flash(str(exc))
        return redirect(url_for("review", data_id=data_id))

    existing_checkpoints = load_checkpoint_library()
    merged_checkpoints = save_checkpoint_library(existing_checkpoints + imported_checkpoints)
    new_count = len(set(merged_checkpoints) - set(existing_checkpoints))

    data["last_imported_checkpoint_column"] = column_name
    _touch_session(data_id)
    flash(
        f"已从'{column_name}'导入卡口，识别 {len(imported_checkpoints)} 个卡口，新增 {new_count} 个，本地卡口库现有 {len(merged_checkpoints)} 个。"
    )
    return redirect(url_for("review", data_id=data_id))


@app.route("/checkpoint/delete/<data_id>", methods=["POST"])
def delete_checkpoints_from_library(data_id):
    """从本地卡口库删除选中的卡口。"""
    data = DATA_STORE.get(data_id)
    if not data:
        flash("数据已过期或不存在，请重新上传文件。")
        return redirect(url_for("upload_form"))

    checkpoint_library = load_checkpoint_library()
    selected_checkpoints = normalize_choice_list(
        request.form.getlist("delete_checkpoints"),
        checkpoint_library,
    )
    if not selected_checkpoints:
        flash("请先选择要删除的卡口。")
        return redirect(url_for("review", data_id=data_id))

    removed_set = set(selected_checkpoints)
    remaining_checkpoints = [
        checkpoint for checkpoint in checkpoint_library if checkpoint not in removed_set
    ]
    updated_library = save_checkpoint_library(remaining_checkpoints)

    config = data.get("config", {})
    data["config"] = prune_removed_checkpoints_from_config(config, selected_checkpoints)

    _touch_session(data_id)
    flash(
        f"已删除 {len(selected_checkpoints)} 个卡口，本地卡口库剩余 {len(updated_library)} 个。"
    )
    return redirect(url_for("review", data_id=data_id))


@app.route("/review/<data_id>", methods=["GET"])
def review(data_id):
    """展示卡口库与筛选条件配置页面。"""
    data = DATA_STORE.get(data_id)
    if not data:
        flash("数据已过期或不存在，请重新上传文件。")
        return redirect(url_for("upload_form"))

    config = data.get("config", {})
    filter_mode = normalize_text_value(config.get("filter_mode", FILTER_MODE_PAIR)).lower()
    if filter_mode not in {FILTER_MODE_PAIR, FILTER_MODE_FREQUENT}:
        filter_mode = FILTER_MODE_PAIR

    start_time_value = normalize_text_value(config.get("start_time")) or data.get(
        "default_start_time", ""
    )
    end_time_value = normalize_text_value(config.get("end_time")) or data.get(
        "default_end_time", ""
    )
    frequent_start_clock_value = normalize_text_value(
        config.get("frequent_start_clock", DEFAULT_FREQUENT_START_CLOCK)
    ) or DEFAULT_FREQUENT_START_CLOCK
    frequent_end_clock_value = normalize_text_value(
        config.get("frequent_end_clock", DEFAULT_FREQUENT_END_CLOCK)
    ) or DEFAULT_FREQUENT_END_CLOCK
    frequent_start_hour_value, frequent_start_minute_value = split_clock_value(
        frequent_start_clock_value, default_hour="00", default_minute="00"
    )
    frequent_end_hour_value, frequent_end_minute_value = split_clock_value(
        frequent_end_clock_value, default_hour="23", default_minute="59"
    )
    checkpoint_library = load_checkpoint_library()
    selected_first_checkpoint = config.get("first_checkpoint", "")
    if not selected_first_checkpoint:
        legacy_first = config.get("entry_checkpoint", "")
        if isinstance(legacy_first, str):
            selected_first_checkpoint = legacy_first
    if not selected_first_checkpoint:
        legacy_entry = config.get("entry_checkpoints", [])
        if isinstance(legacy_entry, list) and legacy_entry:
            selected_first_checkpoint = legacy_entry[0]
        elif isinstance(legacy_entry, str):
            selected_first_checkpoint = legacy_entry

    selected_second_checkpoint = config.get("second_checkpoint", "")
    if not selected_second_checkpoint:
        legacy_second = config.get("exit_checkpoint", "")
        if isinstance(legacy_second, str):
            selected_second_checkpoint = legacy_second
    if not selected_second_checkpoint:
        legacy_exit = config.get("exit_checkpoints", [])
        if isinstance(legacy_exit, list) and legacy_exit:
            selected_second_checkpoint = legacy_exit[0]
        elif isinstance(legacy_exit, str):
            selected_second_checkpoint = legacy_exit

    current_locations = data.get("locations", [])
    matched_checkpoints = sorted(set(current_locations).intersection(checkpoint_library))
    matched_checkpoint_set = set(matched_checkpoints)
    prioritized_checkpoint_library = matched_checkpoints + [
        checkpoint
        for checkpoint in checkpoint_library
        if checkpoint not in matched_checkpoint_set
    ]
    source_columns = data.get("source_columns", [])
    selected_import_column = data.get("last_imported_checkpoint_column", "")
    if not selected_import_column and source_columns:
        selected_import_column = source_columns[0]
    selected_frequent_checkpoints = normalize_choice_list(
        config.get("frequent_checkpoints", []), checkpoint_library
    )
    selected_export_columns = normalize_choice_list(
        config.get("export_columns", []), source_columns
    )
    if not selected_export_columns:
        selected_export_columns = pick_default_export_columns(source_columns)

    min_occurrence_value = config.get("min_occurrence", DEFAULT_FREQUENT_OCCURRENCE)
    try:
        min_occurrence_value = int(min_occurrence_value)
    except (TypeError, ValueError):
        min_occurrence_value = DEFAULT_FREQUENT_OCCURRENCE
    if min_occurrence_value <= 0:
        min_occurrence_value = DEFAULT_FREQUENT_OCCURRENCE

    target_minutes_value = config.get("target_minutes", data.get("default_max_minutes", 30))
    try:
        target_minutes_value = float(target_minutes_value)
    except (TypeError, ValueError):
        target_minutes_value = float(data.get("default_max_minutes", 30))
    if target_minutes_value <= 0:
        target_minutes_value = float(data.get("default_max_minutes", 30))

    _touch_session(data_id)
    return render_template(
        "review.html",
        data_id=data_id,
        locations=current_locations,
        plate_types=data.get("plate_types", []),
        default_max_minutes=data.get("default_max_minutes", 30),
        filter_mode=filter_mode,
        config=config,
        start_time_value=start_time_value,
        end_time_value=end_time_value,
        checkpoint_library=checkpoint_library,
        prioritized_checkpoint_library=prioritized_checkpoint_library,
        selected_first_checkpoint=selected_first_checkpoint,
        selected_second_checkpoint=selected_second_checkpoint,
        selected_frequent_checkpoints=selected_frequent_checkpoints,
        selected_export_columns=selected_export_columns,
        min_occurrence_value=min_occurrence_value,
        target_minutes_value=target_minutes_value,
        frequent_start_clock_value=frequent_start_clock_value,
        frequent_end_clock_value=frequent_end_clock_value,
        frequent_start_hour_value=frequent_start_hour_value,
        frequent_start_minute_value=frequent_start_minute_value,
        frequent_end_hour_value=frequent_end_hour_value,
        frequent_end_minute_value=frequent_end_minute_value,
        matched_checkpoints=matched_checkpoints,
        source_columns=source_columns,
        selected_import_column=selected_import_column,
    )


@app.route("/filter/<data_id>", methods=["POST"])
def filter_results(data_id):
    """根据选择的筛选模式执行过滤。"""
    data = DATA_STORE.get(data_id)
    if not data:
        flash("数据已过期或不存在，请重新上传文件。")
        return redirect(url_for("upload_form"))

    df = _restore_raw_dtypes(_load_df(data_id, "raw_data"))
    config = data.get("config", {}).copy()
    source_columns = data.get("source_columns", [])
    checkpoint_library = set(load_checkpoint_library())
    current_data_locations = set(data.get("locations", []))
    plate_types = data.get("plate_types", [])

    # 号牌种类排除
    exclude_plate_types = normalize_choice_list(request.form.getlist("exclude_plate_types"), plate_types)
    if exclude_plate_types and "plate_type" in df.columns:
        df = df[~df["plate_type"].isin(exclude_plate_types)]

    filter_mode = normalize_text_value(request.form.get("filter_mode", FILTER_MODE_PAIR)).lower()
    if filter_mode not in {FILTER_MODE_PAIR, FILTER_MODE_FREQUENT}:
        filter_mode = FILTER_MODE_PAIR

    config.update(
        {
            "filter_mode": filter_mode,
            "exclude_plate_types": exclude_plate_types,
        }
    )

    if filter_mode == FILTER_MODE_PAIR:
        start_time_str = normalize_text_value(request.form.get("start_time"))
        end_time_str = normalize_text_value(request.form.get("end_time"))
        try:
            start_time, end_time = parse_time_window(start_time_str, end_time_str)
        except ValueError as exc:
            flash(str(exc))
            return redirect(url_for("review", data_id=data_id))

        first_checkpoint = normalize_text_value(request.form.get("first_checkpoint"))
        second_checkpoint = normalize_text_value(request.form.get("second_checkpoint"))
        if not first_checkpoint:
            first_checkpoint = normalize_text_value(request.form.get("entry_checkpoint"))
        if not second_checkpoint:
            second_checkpoint = normalize_text_value(request.form.get("exit_checkpoint"))

        if not first_checkpoint or not second_checkpoint:
            flash("请分别选择第一卡口和第二卡口。")
            return redirect(url_for("review", data_id=data_id))

        if first_checkpoint not in checkpoint_library or second_checkpoint not in checkpoint_library:
            flash("所选卡口不在本地卡口库中，请重新选择。")
            return redirect(url_for("review", data_id=data_id))

        if first_checkpoint == second_checkpoint:
            flash("第一卡口和第二卡口不能相同，请重新选择。")
            return redirect(url_for("review", data_id=data_id))

        active_first_locations = {first_checkpoint}.intersection(current_data_locations)
        active_second_locations = {second_checkpoint}.intersection(current_data_locations)

        if not active_first_locations or not active_second_locations:
            flash("所选第一或第二卡口未出现在当前通行数据中，请重新选择。")
            return redirect(url_for("review", data_id=data_id))

        target_minutes = request.form.get("target_minutes", type=float)
        if target_minutes is None or target_minutes <= 0:
            flash("请填写正确的目标过车间隔（分钟）。")
            return redirect(url_for("review", data_id=data_id))

        filtered_df = build_pair_filtered_dataframe(
            df=df,
            start_time=start_time,
            end_time=end_time,
            active_first_locations=active_first_locations,
            active_second_locations=active_second_locations,
            target_minutes=target_minutes,
        )
        summary = build_results_summary(filtered_df)
        _save_df(filtered_df, data_id, "filtered_data")
        del df, filtered_df

        config.update(
            {
                "first_checkpoint": first_checkpoint,
                "second_checkpoint": second_checkpoint,
                "entry_checkpoint": first_checkpoint,  # 兼容历史字段
                "exit_checkpoint": second_checkpoint,  # 兼容历史字段
                "start_time": start_time_str,
                "end_time": end_time_str,
                "target_minutes": target_minutes,
            }
        )

        DATA_STORE[data_id]["config"] = config
        DATA_STORE[data_id]["filtered_mode"] = FILTER_MODE_PAIR
        DATA_STORE[data_id]["summary"] = summary
        DATA_STORE[data_id]["selected_export_columns"] = []
        DATA_STORE[data_id]["last_access"] = time.time()

        return redirect(url_for("show_results", data_id=data_id))

    selected_checkpoints = normalize_choice_list(
        request.form.getlist("frequent_checkpoints"), checkpoint_library
    )
    if not selected_checkpoints:
        flash("请至少选择一个卡口用于频繁出现筛选。")
        return redirect(url_for("review", data_id=data_id))

    active_checkpoints = sorted(set(selected_checkpoints).intersection(current_data_locations))
    if not active_checkpoints:
        flash("所选卡口未出现在当前通行数据中，请重新选择。")
        return redirect(url_for("review", data_id=data_id))

    min_occurrence = request.form.get("min_occurrence", type=int)
    if min_occurrence is None:
        min_occurrence = DEFAULT_FREQUENT_OCCURRENCE
    if min_occurrence <= 0:
        flash("出现次数必须大于 0。")
        return redirect(url_for("review", data_id=data_id))

    frequent_start_clock_str = normalize_text_value(request.form.get("frequent_start_clock"))
    frequent_end_clock_str = normalize_text_value(request.form.get("frequent_end_clock"))
    if not frequent_start_clock_str:
        frequent_start_clock_str = compose_clock_value(
            request.form.get("frequent_start_hour"),
            request.form.get("frequent_start_minute"),
        )
    if not frequent_end_clock_str:
        frequent_end_clock_str = compose_clock_value(
            request.form.get("frequent_end_hour"),
            request.form.get("frequent_end_minute"),
        )
    try:
        frequent_start_clock, frequent_end_clock = parse_clock_window(
            frequent_start_clock_str, frequent_end_clock_str
        )
    except ValueError as exc:
        flash(str(exc))
        return redirect(url_for("review", data_id=data_id))

    selected_export_columns = normalize_choice_list(
        request.form.getlist("export_columns"), source_columns
    )
    if not selected_export_columns:
        selected_export_columns = pick_default_export_columns(source_columns)

    filtered_df, matched_records, filtered_vehicle_count = build_frequent_filtered_dataframe(
        df=df,
        start_clock=frequent_start_clock,
        end_clock=frequent_end_clock,
        active_checkpoints=active_checkpoints,
        min_occurrence=min_occurrence,
    )
    summary = build_frequent_results_summary(filtered_df, matched_records=matched_records, threshold=min_occurrence)
    summary["total_vehicles"] = filtered_vehicle_count
    _save_df(filtered_df, data_id, "filtered_data")
    del df, filtered_df

    config.update(
        {
            "frequent_checkpoints": selected_checkpoints,
            "min_occurrence": min_occurrence,
            "frequent_start_clock": frequent_start_clock.strftime("%H:%M"),
            "frequent_end_clock": frequent_end_clock.strftime("%H:%M"),
            "export_columns": selected_export_columns,
        }
    )

    DATA_STORE[data_id]["config"] = config
    DATA_STORE[data_id]["filtered_mode"] = FILTER_MODE_FREQUENT
    DATA_STORE[data_id]["summary"] = summary
    DATA_STORE[data_id]["selected_export_columns"] = selected_export_columns
    DATA_STORE[data_id]["last_access"] = time.time()

    return redirect(url_for("show_results", data_id=data_id))


@app.route("/results/<data_id>", methods=["GET"])
def show_results(data_id):
    """分页展示筛选结果。"""
    data = DATA_STORE.get(data_id)
    if not data:
        flash("数据已过期或不存在，请重新上传文件。")
        return redirect(url_for("upload_form"))

    filter_mode = data.get("filtered_mode")
    if not filter_mode:
        flash("请先完成筛选。")
        return redirect(url_for("review", data_id=data_id))

    summary = data.get("summary", {})
    selected_export_columns = data.get("selected_export_columns", [])
    config = data.get("config", {})

    # 从 SQLite 加载筛选结果并重建 display_results
    filtered_df = _load_df(data_id, "filtered_data")
    if filter_mode == FILTER_MODE_PAIR:
        filtered_df = _restore_pair_dtypes(filtered_df)
        display_results = build_pair_display_results(filtered_df)
    else:
        filtered_df = _restore_frequent_dtypes(filtered_df)
        threshold = config.get("min_occurrence", DEFAULT_FREQUENT_OCCURRENCE)
        export_cols = selected_export_columns or config.get("export_columns", [])
        display_results = build_frequent_display_results(
            filtered_df, threshold=threshold, selected_export_columns=export_cols,
        )
    del filtered_df

    page = request.args.get("page", 1, type=int)
    page_results, total_pages, has_prev, has_next = paginate_results(
        display_results, page, filter_mode
    )

    _touch_session(data_id)
    return render_template(
        "results.html",
        data_id=data_id,
        filter_mode=filter_mode,
        results=page_results,
        summary=summary,
        selected_export_columns=selected_export_columns,
        page=page,
        total_pages=total_pages,
        has_prev=has_prev,
        has_next=has_next,
        total_results=len(display_results),
    )


@app.route("/download/<data_id>", methods=["GET"])
def download(data_id):
    """下载筛选结果。"""
    data = DATA_STORE.get(data_id)
    if not data:
        flash("数据已过期或不存在，请重新上传文件。")
        return redirect(url_for("upload_form"))

    filter_mode = normalize_text_value(data.get("filtered_mode") or data.get("config", {}).get("filter_mode"))
    if not filter_mode:
        flash("没有可下载的结果，请先完成筛选。")
        return redirect(url_for("upload_form"))

    filtered_df = _load_df(data_id, "filtered_data")
    if filtered_df is None or filtered_df.empty:
        flash("没有可下载的结果，请先完成筛选。")
        return redirect(url_for("upload_form"))

    config = data.get("config", {})

    if filter_mode == FILTER_MODE_FREQUENT:
        filtered_df = _restore_frequent_dtypes(filtered_df)
        source_columns = data.get("source_columns", [])
        selected_export_columns = normalize_choice_list(
            config.get("export_columns", []), source_columns
        )
        if not selected_export_columns:
            selected_export_columns = pick_default_export_columns(source_columns)

        try:
            threshold = int(config.get("min_occurrence", DEFAULT_FREQUENT_OCCURRENCE))
        except (TypeError, ValueError):
            threshold = DEFAULT_FREQUENT_OCCURRENCE
        if threshold <= 0:
            threshold = DEFAULT_FREQUENT_OCCURRENCE
        export_df, merge_ranges, risk_levels = build_frequent_export_dataframe(
            filtered_df,
            selected_export_columns,
            threshold=threshold,
        )
        frequent_summary = data.get("summary") or build_frequent_results_summary(
            filtered_df, matched_records=len(filtered_df), threshold=threshold,
        )
        output = build_frequent_warning_workbook(
            export_df, risk_levels, frequent_summary, merge_ranges,
        )
        filename = "频繁出现车辆筛选结果.xlsx"
    else:
        filtered_df = _restore_pair_dtypes(filtered_df)
        export_df, risk_levels = build_export_dataframe(filtered_df)
        summary = build_results_summary(filtered_df)
        output = build_warning_workbook(export_df, risk_levels, summary)
        filename = "筛选结果_警戒色.xlsx"

    _touch_session(data_id)
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    app.run(debug=True)
