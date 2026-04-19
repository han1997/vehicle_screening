import os
import json
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
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

ALLOWED_EXTENSIONS = {"xls", "xlsx"}
CHECKPOINT_LIBRARY_FILE = os.path.join(os.path.dirname(__file__), "checkpoint_library.json")

# 简单的内存数据存储，适合本地单用户使用
DATA_STORE = {}

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
        if value is None or pd.isna(value):
            continue
        text = str(value).strip()
        if not text or text.lower() == "nan":
            continue
        normalized.add(text)
    return sorted(normalized)


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


def build_warning_workbook(export_df, risk_levels, summary):
    """生成带风险底色的 xlsx 文件。"""
    output = BytesIO()
    columns = export_df.columns.tolist()
    last_col = excel_column_name(len(columns))
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

    last_row = len(export_df) + 4
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

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
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

    workbook_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="筛选结果" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
"""

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
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
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)

    output.seek(0)
    return output


def find_matching_column(df, candidates):
    """在 DataFrame 中根据候选列表找到匹配的列名。"""
    normalized = {col: str(col).strip() for col in df.columns}
    for cand in candidates:
        for original, norm in normalized.items():
            if norm == cand:
                return original
    return None


def parse_excel(path):
    """读取并标准化 Excel 数据，返回标准化数据和原始列名。"""
    try:
        df = pd.read_excel(path)
    except Exception as exc:
        raise ValueError(f"无法读取Excel文件: {exc}")

    if df.empty:
        raise ValueError("Excel 文件为空。")

    source_columns = [str(column).strip() for column in df.columns]
    source_columns = [column for column in source_columns if column]

    plate_candidates = [
        "车牌号",
        "车牌号码",
        "车牌",
        "号牌号码",
        "plate",
        "plate_no",
        "license_plate",
    ]
    time_candidates = [
        "抓拍时间",
        "通过时间",
        "时间",
        "通行时间",
        "capture_time",
        "time",
        "timestamp",
    ]
    location_candidates = [
        "抓拍地点",
        "地点",
        "位置",
        "地点名称",
        "location",
        "site",
    ]
    plate_type_candidates = [
        "号牌种类",
        "号牌类型",
        "车牌种类",
        "车牌类型",
        "plate_type",
        "plate_kind",
        "plate_category",
    ]

    plate_col = find_matching_column(df, plate_candidates)
    time_col = find_matching_column(df, time_candidates)
    location_col = find_matching_column(df, location_candidates)
    plate_type_col = find_matching_column(df, plate_type_candidates)

    missing = []
    if plate_col is None:
        missing.append("车牌号列")
    if time_col is None:
        missing.append("抓拍时间列")
    if location_col is None:
        missing.append("抓拍地点列")

    if missing:
        cols_str = ", ".join(str(c) for c in df.columns)
        raise ValueError(f"无法自动识别列: {', '.join(missing)}。当前表头为: {cols_str}")

    # 保留需要的列并统一列名
    if plate_type_col is not None:
        df = df[[plate_col, time_col, location_col, plate_type_col]].copy()
        df.columns = ["plate", "time", "location", "plate_type"]
    else:
        df = df[[plate_col, time_col, location_col]].copy()
        df.columns = ["plate", "time", "location"]

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["plate"] = df["plate"].astype(str).str.strip()
    df["location"] = df["location"].astype(str).str.strip()
    if "plate_type" in df.columns:
        df["plate_type"] = df["plate_type"].astype(str).str.strip()

    df = df.dropna(subset=["plate", "time", "location"])
    df = df[df["plate"] != ""]
    df = df[df["plate"] != "无牌车"]
    df = df[df["plate"] != "未识别"]

    return df, source_columns


def import_checkpoints_from_uploaded_files(filepaths, column_name):
    """从本次上传的通行记录文件中按指定列导入卡口。"""
    selected_column = str(column_name).strip()
    if not selected_column:
        raise ValueError("请选择要导入的卡口列。")

    imported_values = []
    matched_file_count = 0

    for path in filepaths:
        try:
            header_df = pd.read_excel(path, nrows=0)
        except Exception as exc:
            raise ValueError(f"读取已上传文件失败: {exc}")

        normalized_map = {str(column).strip(): column for column in header_df.columns}
        actual_column = normalized_map.get(selected_column)
        if actual_column is None:
            continue

        try:
            value_df = pd.read_excel(path, dtype=str, usecols=[actual_column])
        except Exception as exc:
            raise ValueError(f"读取列“{selected_column}”失败: {exc}")

        matched_file_count += 1
        imported_values.extend(value_df[actual_column].tolist())

    if matched_file_count == 0:
        raise ValueError("所选列未出现在本次上传文件中，请重新选择。")

    checkpoints = normalize_text_list(imported_values)
    if not checkpoints:
        raise ValueError("指定列中没有可导入的卡口名称。")

    return checkpoints, matched_file_count


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
    uploaded_filepaths = []
    source_columns = []
    source_column_set = set()

    for file in files:
        if not allowed_file(file.filename):
            flash(f"文件 {file.filename} 不是支持的 Excel 格式。")
            return redirect(url_for("upload_form"))

        filepath = save_uploaded_excel(file)
        uploaded_filepaths.append(filepath)

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
    if "plate_type" in df.columns:
        plate_types = sorted(df["plate_type"].dropna().unique().tolist())
    else:
        plate_types = []

    data_id = str(uuid.uuid4())
    default_max_minutes = 30.0  # 初始默认值，在下一步页面可修改
    default_start_time = format_datetime_local(df["time"].min())
    default_end_time = format_datetime_local(df["time"].max())

    DATA_STORE[data_id] = {
        "df": df,
        "locations": locations,
        "plate_types": plate_types,
        "filtered": None,
        "uploaded_filepaths": uploaded_filepaths,
        "source_columns": source_columns,
        "last_imported_checkpoint_column": "",
        "default_max_minutes": default_max_minutes,
        "default_start_time": default_start_time,
        "default_end_time": default_end_time,
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

    filepaths = data.get("uploaded_filepaths", [])
    if not filepaths:
        flash("未找到本次上传文件，请重新上传后再试。")
        return redirect(url_for("upload_form"))

    try:
        imported_checkpoints, matched_file_count = import_checkpoints_from_uploaded_files(
            filepaths, column_name
        )
    except ValueError as exc:
        flash(str(exc))
        return redirect(url_for("review", data_id=data_id))

    existing_checkpoints = load_checkpoint_library()
    merged_checkpoints = save_checkpoint_library(existing_checkpoints + imported_checkpoints)
    new_count = len(set(merged_checkpoints) - set(existing_checkpoints))

    data["last_imported_checkpoint_column"] = column_name
    flash(
        f"已从“{column_name}”导入卡口，匹配 {matched_file_count} 个文件，识别 {len(imported_checkpoints)} 个卡口，新增 {new_count} 个，本地卡口库现有 {len(merged_checkpoints)} 个。"
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
    start_time_value = config.get("start_time") or data.get("default_start_time", "")
    end_time_value = config.get("end_time") or data.get("default_end_time", "")
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

    return render_template(
        "review.html",
        data_id=data_id,
        locations=current_locations,
        plate_types=data.get("plate_types", []),
        default_max_minutes=data.get("default_max_minutes", 30),
        config=config,
        start_time_value=start_time_value,
        end_time_value=end_time_value,
        checkpoint_library=checkpoint_library,
        prioritized_checkpoint_library=prioritized_checkpoint_library,
        selected_first_checkpoint=selected_first_checkpoint,
        selected_second_checkpoint=selected_second_checkpoint,
        matched_checkpoints=matched_checkpoints,
        source_columns=source_columns,
        selected_import_column=selected_import_column,
    )


@app.route("/filter/<data_id>", methods=["POST"])
def filter_results(data_id):
    """根据本地卡口库中选定的第一/第二卡口和时间窗口筛选车辆记录。"""
    data = DATA_STORE.get(data_id)
    if not data:
        flash("数据已过期或不存在，请重新上传文件。")
        return redirect(url_for("upload_form"))

    df = data["df"]
    checkpoint_library = set(load_checkpoint_library())

    # 号牌种类排除
    exclude_plate_types = request.form.getlist("exclude_plate_types")
    if exclude_plate_types and "plate_type" in df.columns:
        df = df[~df["plate_type"].isin(exclude_plate_types)]

    first_checkpoint = request.form.get("first_checkpoint", "").strip()
    second_checkpoint = request.form.get("second_checkpoint", "").strip()
    if not first_checkpoint:
        first_checkpoint = request.form.get("entry_checkpoint", "").strip()
    if not second_checkpoint:
        second_checkpoint = request.form.get("exit_checkpoint", "").strip()

    if not first_checkpoint or not second_checkpoint:
        flash("请分别选择第一卡口和第二卡口。")
        return redirect(url_for("review", data_id=data_id))

    if first_checkpoint not in checkpoint_library or second_checkpoint not in checkpoint_library:
        flash("所选卡口不在本地卡口库中，请重新选择。")
        return redirect(url_for("review", data_id=data_id))

    if first_checkpoint == second_checkpoint:
        flash("第一卡口和第二卡口不能相同，请重新选择。")
        return redirect(url_for("review", data_id=data_id))

    current_data_locations = set(data.get("locations", []))
    active_first_locations = {first_checkpoint}.intersection(current_data_locations)
    active_second_locations = {second_checkpoint}.intersection(current_data_locations)

    if not active_first_locations or not active_second_locations:
        flash("所选第一或第二卡口未出现在当前通行数据中，请重新选择。")
        return redirect(url_for("review", data_id=data_id))

    start_time_str = request.form.get("start_time")
    end_time_str = request.form.get("end_time")
    target_minutes = request.form.get("target_minutes", type=float)

    if not start_time_str or not end_time_str or target_minutes is None:
        flash("请输入完整的筛选时间段和目标过车间隔。")
        return redirect(url_for("review", data_id=data_id))

    try:
        start_time = datetime.fromisoformat(start_time_str)
        end_time = datetime.fromisoformat(end_time_str)
    except ValueError:
        flash("时间段格式不正确，请重新选择。")
        return redirect(url_for("review", data_id=data_id))

    if start_time >= end_time or target_minutes <= 0:
        flash("请确保开始时间早于结束时间，且目标过车间隔大于 0。")
        return redirect(url_for("review", data_id=data_id))

    DATA_STORE[data_id]["config"] = {
        "exclude_plate_types": exclude_plate_types,
        "first_checkpoint": first_checkpoint,
        "second_checkpoint": second_checkpoint,
        "entry_checkpoint": first_checkpoint,  # 兼容历史字段
        "exit_checkpoint": second_checkpoint,  # 兼容历史字段
        "start_time": start_time_str,
        "end_time": end_time_str,
        "target_minutes": target_minutes,
    }

    valid_locations = active_first_locations.union(active_second_locations)
    df_valid = df[df["location"].isin(valid_locations)].copy()
    df_valid = df_valid[(df_valid["time"] >= start_time) & (df_valid["time"] <= end_time)]
    df_valid = df_valid.sort_values("time")

    results = []

    # 按车牌分组，寻找第一卡口到第二卡口的时间顺序配对
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
                first_time_out = first_time
                first_location_out = first_row.location
                second_time_out = best_second.time
                second_location_out = best_second.location

                if "plate_type" in df.columns:
                    plate_type_out = getattr(first_row, "plate_type", "")
                else:
                    plate_type_out = ""

                delta_minutes = best_delta.total_seconds() / 60.0

                # 根据与目标间隔的接近程度打分并分级
                diff = abs(delta_minutes - target_minutes)
                if target_minutes > 0:
                    # 将差值按目标间隔归一化到 0-1 之间，差值为 0 时得分 100，差值等于目标间隔时得分约为 0
                    normalized = diff / target_minutes
                    raw_score = max(0.0, 1.0 - normalized)
                    score = int(round(raw_score * 100))
                else:
                    score = 0

                if score >= 70:
                    level = "red"
                elif score >= 40:
                    level = "yellow"
                else:
                    level = "blue"

                results.append(
                    {
                        "plate": plate,
                        "plate_type": plate_type_out,
                        "first_time": first_time_out,
                        "first_location": first_location_out,
                        "second_time": second_time_out,
                        "second_location": second_location_out,
                        "delta_minutes": delta_minutes,
                        "score": score,
                        "level": level,
                    }
                )

    # 保存用于下载的 DataFrame
    if results:
        filtered_df = pd.DataFrame(results)
        filtered_df = filtered_df.sort_values("score", ascending=False)
    else:
        filtered_df = pd.DataFrame(
            columns=[
                "plate",
                "plate_type",
                "first_time",
                "first_location",
                "second_time",
                "second_location",
                "delta_minutes",
            ]
        )

    DATA_STORE[data_id]["filtered"] = filtered_df

    # 准备展示用的数据（格式化时间）
    display_results = []
    for row in filtered_df.itertuples(index=False):
        plate_type_val = (
            str(row.plate_type)
            if hasattr(row, "plate_type") and pd.notnull(row.plate_type)
            else ""
        )
        delta_val = float(row.delta_minutes) if pd.notnull(row.delta_minutes) else 0.0
        score_val = int(row.score) if hasattr(row, "score") and pd.notnull(row.score) else 0
        level_val = str(row.level) if hasattr(row, "level") and pd.notnull(row.level) else ""
        level_label = get_risk_label(level_val)
        display_results.append(
            {
                "plate": row.plate,
                "plate_type": plate_type_val,
                "first_time": row.first_time.strftime("%Y-%m-%d %H:%M:%S")
                if pd.notnull(row.first_time)
                else "",
                "first_location": row.first_location,
                "second_time": row.second_time.strftime("%Y-%m-%d %H:%M:%S")
                if pd.notnull(row.second_time)
                else "",
                "second_location": row.second_location,
                "delta_minutes": delta_val,
                "score": score_val,
                "level": level_val,
                "level_label": level_label,
            }
        )

    summary = build_results_summary(filtered_df)

    return render_template(
        "results.html",
        data_id=data_id,
        results=display_results,
        summary=summary,
    )


@app.route("/download/<data_id>", methods=["GET"])
def download(data_id):
    """下载筛选后的结果为带风险底色的 Excel 文件。"""
    data = DATA_STORE.get(data_id)
    if not data:
        flash("数据已过期或不存在，请重新上传文件。")
        return redirect(url_for("upload_form"))

    filtered_df = data.get("filtered")
    if filtered_df is None or filtered_df.empty:
        flash("没有可下载的结果，请先完成筛选。")
        return redirect(url_for("upload_form"))

    export_df, risk_levels = build_export_dataframe(filtered_df)
    summary = build_results_summary(filtered_df)
    output = build_warning_workbook(export_df, risk_levels, summary)
    filename = "筛选结果_警戒色.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    app.run(debug=True)
