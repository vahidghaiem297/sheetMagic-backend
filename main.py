import io
import os
import shutil
import zipfile
import json
import pandas as pd
import re
from fastapi.responses import StreamingResponse
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import logging
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

logger = logging.getLogger(__name__)

# ایجاد app با تنظیمات کامل
app = FastAPI(
    title="SheetMagic API",
    description="Backend for SheetMagic Excel automation tool",
    version="1.0.0",
)

# در main.py بخش CORS را به این صورت به‌روزرسانی کنید:
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173", 
        "https://vahidghaiem297.github.io",
        "https://*.github.io",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://sheetmagic-backend-production.up.railway.app",
        "*"  # برای تست - در تولید محدودتر کنید
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# اضافه کردن endpoint اصلی
@app.get("/")
async def root():
    return {
        "message": "SheetMagic Backend API is running!",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "/merge-files/",
            "/convert-format/",
            "/remove-duplicates/",
            "/get-columns/",
            "/compare-files/",
            "/clean-data/",
            "/create-pivot/",
            "/join-files/",
        ],
    }


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "SheetMagic Backend"}


# ======= توابع کمکی =======
def read_file(upload: UploadFile) -> pd.DataFrame:
    fname = (upload.filename or "").lower()

    # محتوای فایل را یکبار بخوان
    upload.file.seek(0)
    raw = upload.file.read()

    if fname.endswith(".csv"):
        # چند encoding رایج + تشخیص خودکار delimiter
        for enc in ("utf-8", "utf-8-sig", "cp1256", "latin1"):
            try:
                text = raw.decode(enc)
                # sep=None + engine='python' جداکننده را حدس می‌زند
                return pd.read_csv(io.StringIO(text), sep=None, engine="python")
            except Exception:
                continue
        # آخرین تلاش با نادیده گرفتن کاراکترهای ناسازگار
        return pd.read_csv(
            io.StringIO(raw.decode("utf-8", errors="ignore")), sep=None, engine="python"
        )
    else:
        # اکسل (xlsx/xls)
        try:
            upload.file.seek(0)
            return pd.read_excel(io.BytesIO(raw))
        except Exception:
            upload.file.seek(0)
            return pd.read_excel(io.BytesIO(raw), engine="openpyxl")


def save_to_excel(data, sheet_name: str = "Sheet1") -> bytes:
    """
    داده را به فایل اکسل (xlsx) در حافظه تبدیل می‌کند و bytes برمی‌گرداند.
    - اگر data یک DataFrame باشد: در شیتِ sheet_name ذخیره می‌شود.
    - اگر data یک dict[str, DataFrame] باشد: هر کلید یک نام شیت خواهد بود.
    """
    output = io.BytesIO()
    try:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            if isinstance(data, dict):
                for name, df in data.items():
                    (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_excel(
                        writer, sheet_name=name or "Sheet", index=False
                    )
            else:
                (
                    data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
                ).to_excel(writer, sheet_name=sheet_name, index=False)
    except Exception:
        # اگر به هر دلیل openpyxl در دسترس نبود، از engine پیش‌فرض استفاده کن
        output = io.BytesIO()
        with pd.ExcelWriter(output) as writer:
            if isinstance(data, dict):
                for name, df in data.items():
                    (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_excel(
                        writer, sheet_name=name or "Sheet", index=False
                    )
            else:
                (
                    data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
                ).to_excel(writer, sheet_name=sheet_name, index=False)

    output.seek(0)
    return output.getvalue()


# ======= Arabic/Persian helpers =======
_AR_BLOCK = r"\u0600-\u06FF"
_AR_RE = re.compile(f"[{_AR_BLOCK}]")
_AR_RUNS_RE = re.compile(f"([{_AR_BLOCK}]+)")
_AR_BETWEEN_SPACES_RE = re.compile(f"(?<=[{_AR_BLOCK}])\\s+(?=[{_AR_BLOCK}])")
_CTRL_RE = re.compile(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]")


def _strip_controls(s: str) -> str:
    return _CTRL_RE.sub("", s or "")


def _reverse_arabic_runs(s: str) -> str:
    return _AR_RUNS_RE.sub(lambda m: m.group(1)[::-1], s)


def _has_rtl(text: str) -> bool:
    return bool(_AR_RE.search(str(text or "")))


def _fix_rtl_cell(x):
    if x is None:
        return x
    s = str(x)
    s = _strip_controls(s)
    s = _AR_BETWEEN_SPACES_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    if _has_rtl(s):
        s = _reverse_arabic_runs(s)
    return s


# ======= Number helpers =======
_P2E = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def _normalize_digits(s: str) -> str:
    return (s or "").translate(_P2E)


def _as_number_if_possible(x):
    if x is None:
        return x
    s = _normalize_digits(str(x))
    s = re.sub(r"[,\s]", "", s)
    s = re.sub(r"(ریال|Rial|IRR|\$|USD)", "", s, flags=re.IGNORECASE).strip()
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", s):
        try:
            f = float(s)
            return int(f) if f.is_integer() else f
        except Exception:
            return x
    return x


# ======= Column aliasing =======
FA_ALIASES = {
    "row": ["ردیف", "سطر", "شماره", "#"],
    "first": ["نام", "اسم"],
    "last": ["نام خانو", "فامیلی", "شهرت"],
    "salary": ["درآمد", "حقوق", "مزد", "دستمزد", "قیمت", "مبلغ", "جمع", "کل"],
}
EN_ALIASES = {
    "row": ["row", "no", "index", "#"],
    "first": ["first", "firstname", "name"],
    "last": ["last", "lastname", "surname", "family"],
    "salary": ["salary", "wage", "price", "amount", "total"],
}


def _find_by_alias(cols, aliases, is_farsi=False):
    found = {"row": None, "first": None, "last": None, "salary": None}
    for c in cols:
        key = _fix_rtl_cell(c) if is_farsi else str(c or "")
        key_l = key.lower()
        for role, words in aliases.items():
            for w in words:
                if (is_farsi and w in key) or (not is_farsi and w in key_l):
                    if found[role] is None:
                        found[role] = c
    return found


# ======= DF safety & cleanup =======
def _safe_make_df(header, rows):
    rows = rows or []
    max_len = (
        max([len(r) for r in rows] + ([len(header)] if header else [0]))
        if rows or header
        else 0
    )
    if max_len == 0:
        return pd.DataFrame()

    def _norm_row(r):
        r = list(r or [])
        if len(r) < max_len:
            r += [""] * (max_len - len(r))
        elif len(r) > max_len:
            r = r[:max_len]
        return r

    rows_n = [_norm_row(r) for r in rows]
    if header:
        header_n = _norm_row(header)
        header_n = [str(h or f"C{i}") for i, h in enumerate(header_n)]
        # یونیک‌سازی هدرهای تکراری
        seen, uni = {}, []
        for h in header_n:
            if h not in seen:
                seen[h] = 1
                uni.append(h)
            else:
                seen[h] += 1
                uni.append(f"{h}_{seen[h]}")
        return pd.DataFrame(rows_n, columns=uni)
    else:
        return pd.DataFrame(rows_n)


def _drop_duplicate_value_columns(df: pd.DataFrame) -> pd.DataFrame:
    """حذف ستون‌هایی که مقادیرشان دقیقاً یکسان است (خروجی‌های تکراری pdfplumber)."""
    keep, seen = [], {}
    for c in df.columns:
        key = tuple(df[c].astype(str).fillna("").tolist())
        if key in seen:
            continue
        seen[key] = True
        keep.append(c)
    return df[keep]


def _is_simple_index_col(s: pd.Series) -> bool:
    """ستونی که تقریباً 1..n باشد (با کمی خلا/خطا) را تشخیص می‌دهد."""
    vals = [_normalize_digits(str(x)) for x in s.fillna("").tolist()]
    digits = []
    for v in vals:
        v = re.sub(r"[^\d]", "", v)
        digits.append(int(v) if v.isdigit() else None)
    nonan = [d for d in digits if d is not None]
    if len(nonan) < max(3, int(0.5 * len(digits))):
        return False
    # نسبت همخوانی با 1..k
    i, hit = 1, 0
    for d in digits:
        if d is not None and d == i:
            hit += 1
            i += 1
    return hit >= max(3, int(0.7 * len(nonan)))


def _dedupe_index_columns(df: pd.DataFrame, is_farsi: bool) -> pd.DataFrame:
    """اگر بیش از یک ستون ایندکس ساده داریم، فقط یکی را نگه می‌داریم (ترجیح با ستونِ نام‌دارِ «ردیف»)."""
    cands = [c for c in df.columns if _is_simple_index_col(df[c])]
    if len(cands) <= 1:
        return df
    # ترجیح با ستونی که alias «ردیف» دارد
    pick = None
    aliases = FA_ALIASES if is_farsi else EN_ALIASES
    found = _find_by_alias(cands, {"row": aliases["row"]}, is_farsi=is_farsi)
    if found["row"] in cands:
        pick = found["row"]
    else:
        pick = cands[0]
    drop = [c for c in cands if c != pick]
    return df.drop(columns=drop, errors="ignore")


def _is_mostly_numeric_col(sr: pd.Series) -> bool:
    vals = sr.astype(str).head(25).tolist()
    hits = sum(1 for v in vals if re.search(r"\d", _normalize_digits(v or "")))
    return hits >= max(3, int(0.7 * len(vals)))


def _guess_numeric_column(df):
    best, best_score = None, -1
    for col in df.columns:
        vals = df[col].dropna().astype(str).head(15).tolist()
        hit = sum(1 for v in vals if re.search(r"\d", _normalize_digits(v)))
        if hit > best_score:
            best, best_score = col, hit
    return best


def _reorder_columns(df, is_farsi):
    cols = list(df.columns)
    if not cols:
        return df

    if is_farsi:
        found = _find_by_alias(cols, FA_ALIASES, is_farsi=True)
        # فقط اگر alias پیدا نشد، حدس بزنیم
        row_col = found["row"]
        if row_col is None:
            # از بین ستون‌های ایندکس ساده یا عددی، یکی را به‌عنوان ردیف انتخاب کن
            idx_cands = [c for c in cols if _is_simple_index_col(df[c])]
            row_col = idx_cands[0] if idx_cands else _guess_numeric_column(df)

        salary_col = found["salary"] or _guess_numeric_column(df)
        # فقط ستون‌های متنی را برای نام/نام‌خانوادگی استفاده کن
        rest = [c for c in cols if c not in {row_col, salary_col}]
        text_candidates = [c for c in rest if not _is_mostly_numeric_col(df[c])]

        first_col = (
            found["first"]
            if found["first"] in text_candidates
            else (text_candidates[0] if text_candidates else None)
        )
        # بعدی برای نام خانوادگی
        remain_text = [c for c in text_candidates if c != first_col]
        last_col = (
            found["last"]
            if found["last"] in remain_text
            else (remain_text[0] if remain_text else None)
        )

        order = [row_col, first_col, last_col, salary_col]
        order = [c for c in order if c in cols and c is not None]
        order = list(dict.fromkeys(order))  # حذف موارد تکراری
        rest_final = [c for c in cols if c not in order]
        return df[order + rest_final]

    else:
        found = _find_by_alias(cols, EN_ALIASES, is_farsi=False)
        row_col = found["row"]
        if row_col is None:
            idx_cands = [c for c in cols if _is_simple_index_col(df[c])]
            row_col = idx_cands[0] if idx_cands else None
        salary_col = found["salary"] or _guess_numeric_column(df)
        first_col = found["first"]
        last_col = found["last"]
        order = [row_col, first_col, last_col, salary_col]
        order = [c for c in order if c in cols and c is not None]
        order = list(dict.fromkeys(order))
        rest_final = [c for c in cols if c not in order]
        return df[order + rest_final]


# ======= API Endpoints =======
@app.post("/merge-files/")
async def merge_files(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    try:
        print(f"Received files for merging: {file1.filename}, {file2.filename}")

        # خواندن فایل‌ها
        df1 = read_file(file1)
        df2 = read_file(file2)

        print(f"File1 shape: {df1.shape}, columns: {df1.columns.tolist()}")
        print(f"File2 shape: {df2.shape}, columns: {df2.columns.tolist()}")

        # بررسی تطابق ستون‌ها
        if not df1.columns.equals(df2.columns):
            print("Warning: Columns don't match, attempting to align...")
            # اگر ستون‌ها متفاوت هستند، ستون‌های مشترک را پیدا کن
            common_cols = list(set(df1.columns) & set(df2.columns))
            if common_cols:
                df1 = df1[common_cols]
                df2 = df2[common_cols]
                print(f"Using common columns: {common_cols}")
            else:
                return JSONResponse(
                    status_code=400,
                    content={
                        "error": "ستون‌های فایل‌ها کاملاً متفاوت هستند و نمی‌توانند ادغام شوند."
                    },
                )

        # ادغام فایل‌ها
        merged_df = pd.concat([df1, df2], ignore_index=True)

        print(f"Merged successfully. Shape: {merged_df.shape}")

        excel_data = save_to_excel(merged_df, sheet_name="Merged_Data")
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=merged_files.xlsx"},
        )

    except Exception as e:
        logger.exception("merge-files failed")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/convert-format/")
async def convert_format(
    file: UploadFile = File(...),
    target_format: str = Form(...),  # 'excel' یا 'csv'
):
    """
    تبدیل CSV<->Excel
    - target_format='excel'  => خروجی XLSX
    - target_format='csv'    => خروجی CSV (UTF-8-SIG برای سازگاری با Excel)
    """
    try:
        tf = (target_format or "").strip().lower()
        if tf not in {"excel", "csv"}:
            return JSONResponse(
                status_code=400,
                content={"error": "target_format باید 'excel' یا 'csv' باشد."},
            )

        # از هِلپر پروژه استفاده می‌کنیم تا csv/xlsx/xls به درستی خوانده شوند
        df = read_file(file)

        if tf == "excel":
            excel_bytes = save_to_excel(df, sheet_name="Data")
            return Response(
                content=excel_bytes,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": "attachment; filename=converted_file.xlsx"
                },
            )
        else:  # tf == 'csv'
            csv_bytes = df.to_csv(index=False).encode(
                "utf-8-sig"
            )  # BOM برای باز شدن درست در Excel
            return Response(
                content=csv_bytes,
                media_type="text/csv; charset=utf-8",
                headers={
                    "Content-Disposition": "attachment; filename=converted_file.csv"
                },
            )

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/remove-duplicates/")
async def remove_duplicates(
    file: UploadFile = File(...),
    column_name: Optional[str] = Form(None),
):
    """
    حذف ردیف‌های تکراری از کل فایل یا بر اساس یک ستون مشخص (اختیاری).
    خروجی: فایل اکسل deduplicated_file.xlsx
    """
    try:
        # خواندن فایل با هِلپر پروژه
        df = read_file(file)

        # نرمال‌سازی نام ستون‌ها (رفع فاصله/ZWNJ/ی↔ی/ک↔ک)
        def _norm(s: str) -> str:
            if s is None:
                return ""
            s = str(s)
            s = s.replace("\u200c", "")  # ZWNJ
            s = s.replace("ي", "ی").replace("ك", "ک")
            s = re.sub(r"\s+", " ", s).strip()
            return s

        df.columns = [_norm(c) for c in df.columns]

        if column_name:
            col = _norm(column_name)
            if col not in df.columns:
                return JSONResponse(
                    status_code=400,
                    content={
                        "error": f"ستون «{column_name}» یافت نشد. ستون‌های موجود: {df.columns.tolist()}"
                    },
                )
            out_df = df.drop_duplicates(subset=[col])
        else:
            out_df = df.drop_duplicates()

        excel_data = save_to_excel(out_df, sheet_name="Deduplicated")
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=deduplicated_file.xlsx"
            },
        )

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/get-columns/")
async def get_columns(file: UploadFile = File(...)):
    try:
        df = read_file(file)
        cols = df.columns.tolist()
        return {"columns": cols}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/compare-files/")
async def compare_files(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    compare_type: str = Form("all_columns"),  # 'all_columns' یا 'based_on_key'
    key_column: Optional[str] = Form(None),
):
    try:
        # ---------- reader ایمن برای csv/xlsx ----------
        async def read_upload_to_df(f: UploadFile) -> pd.DataFrame:
            name = (f.filename or "").lower()
            raw = await f.read()
            bio = io.BytesIO(raw)
            if name.endswith(".csv"):
                # تلاش با encoding های رایج
                for enc in ("utf-8", "utf-8-sig", "cp1256", "latin1"):
                    try:
                        bio.seek(0)
                        return pd.read_csv(bio, encoding=enc)
                    except Exception:
                        continue
                bio.seek(0)
                return pd.read_csv(bio, encoding_errors="ignore")
            else:  # xlsx/xls
                bio.seek(0)
                # xls قدیمی نیاز به xlrd دارد؛ اگر xls نداری این کافیست
                return pd.read_excel(bio)

        df1 = await read_upload_to_df(file1)
        df2 = await read_upload_to_df(file2)

        # نرمال‌سازی و ایمن‌سازی نام ستون‌ها (حذف فاصله و رفع تکراری)
        def dedup_cols(cols):
            seen = {}
            out = []
            for c in map(lambda x: str(x).strip(), cols):
                if c in seen:
                    seen[c] += 1
                    out.append(f"{c}.{seen[c]}")
                else:
                    seen[c] = 0
                    out.append(c)
            return out

        df1.columns = dedup_cols(df1.columns)
        df2.columns = dedup_cols(df2.columns)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:

            # ====================== حالت بر اساس کلید ======================
            if compare_type == "based_on_key":
                if not key_column:
                    return JSONResponse(
                        status_code=400, content={"error": "ستون کلید انتخاب نشده است."}
                    )
                if key_column not in df1.columns:
                    return JSONResponse(
                        status_code=400,
                        content={"error": f"ستون '{key_column}' در فایل اول یافت نشد."},
                    )
                if key_column not in df2.columns:
                    return JSONResponse(
                        status_code=400,
                        content={"error": f"ستون '{key_column}' در فایل دوم یافت نشد."},
                    )

                d1 = df1.set_index(key_column)
                d2 = df2.set_index(key_column)

                # هم‌ترازی سطرها و ستون‌ها
                all_idx = d1.index.union(d2.index)
                all_cols = sorted(set(d1.columns) | set(d2.columns))
                d1a = d1.reindex(index=all_idx, columns=all_cols)
                d2a = d2.reindex(index=all_idx, columns=all_cols)

                # سطرهای مختص هر فایل
                only1_idx = d1a.index.difference(d2a.dropna(how="all").index)
                only2_idx = d2a.index.difference(d1a.dropna(how="all").index)
                only1 = (
                    pd.DataFrame(only1_idx, columns=[key_column])
                    if len(only1_idx)
                    else pd.DataFrame([{key_column: "(none)"}])
                )
                only2 = (
                    pd.DataFrame(only2_idx, columns=[key_column])
                    if len(only2_idx)
                    else pd.DataFrame([{key_column: "(none)"}])
                )
                only1.to_excel(writer, sheet_name="Only_in_file1", index=False)
                only2.to_excel(writer, sheet_name="Only_in_file2", index=False)

                # تفاوت سلولی در سطرهای مشترک (بدون numpy، به‌صورت stack)
                both_idx = d1a.index.intersection(d2a.index)
                d1c = d1a.loc[both_idx, all_cols]
                d2c = d2a.loc[both_idx, all_cols]

                s1 = d1c.stack(future_stack=True)
                s2 = d2c.stack(future_stack=True)
                # برابر اگر یا مساوی باشند یا هر دو NaN باشند
                neq_mask = ~(s1.eq(s2) | (s1.isna() & s2.isna()))
                if neq_mask.any():
                    idx = s1.index[neq_mask]
                    changes = pd.DataFrame(
                        {
                            key_column: idx.get_level_values(0),
                            "Column": idx.get_level_values(1),
                            "File1": s1[neq_mask].values,
                            "File2": s2[neq_mask].values,
                        }
                    )
                else:
                    changes = pd.DataFrame(
                        [
                            {
                                key_column: "-",
                                "Column": "-",
                                "File1": "No differences found",
                                "File2": "-",
                            }
                        ]
                    )

                changes.to_excel(writer, sheet_name="Changed_cells", index=False)

            # =================== حالت بدون کلید (مقایسه موقعیتی) ===================
            else:
                # هدف: مقایسه ردیف-به-ردیف صرفاً بر اساس موقعیت، با یکسان‌سازی ستون‌ها و طول‌ها
                d1 = df1.copy()
                d2 = df2.copy()

                # 1) اتحاد ستون‌ها و مرتب‌سازی ثابت برای هر دو
                all_cols = sorted(set(map(str, d1.columns)) | set(map(str, d2.columns)))
                d1.columns = list(map(str, d1.columns))
                d2.columns = list(map(str, d2.columns))
                d1 = d1.reindex(columns=all_cols)
                d2 = d2.reindex(columns=all_cols)

                # 2) هم‌طول‌سازی تعداد ردیف‌ها
                n = max(len(d1), len(d2))
                d1a = d1.reindex(range(n))
                d2a = d2.reindex(range(n))

                # 3) استخراج ردیف‌هایی که فقط در یکی از فایل‌ها هستند
                only1_idx = [
                    i
                    for i in range(n)
                    if (i < len(df1)) and (i >= len(df2) or d2a.loc[i].isna().all())
                ]
                only2_idx = [
                    i
                    for i in range(n)
                    if (i < len(df2)) and (i >= len(df1) or d1a.loc[i].isna().all())
                ]

                only1 = (
                    d1a.loc[only1_idx].reset_index().rename(columns={"index": "Row"})
                )
                only2 = (
                    d2a.loc[only2_idx].reset_index().rename(columns={"index": "Row"})
                )
                if only1.empty:
                    only1 = pd.DataFrame([{"Row": "(none)"}])
                if only2.empty:
                    only2 = pd.DataFrame([{"Row": "(none)"}])

                only1.to_excel(writer, sheet_name="Only_in_file1", index=False)
                only2.to_excel(writer, sheet_name="Only_in_file2", index=False)

                # 4) تفاوت سلولی برای ردیف‌های هم‌موقعیت
                try:
                    # pandas جدید (>=2.1) → فقط future_stack=True
                    s1 = d1a.stack(future_stack=True)
                    s2 = d2a.stack(future_stack=True)
                except TypeError:
                    # pandas قدیمی → فقط dropna=False
                    s1 = d1a.stack(dropna=False)
                    s2 = d2a.stack(dropna=False)

                neq_mask = ~(s1.eq(s2) | (s1.isna() & s2.isna()))
                if neq_mask.any():
                    idx = s1.index[neq_mask]
                    changes = pd.DataFrame(
                        {
                            "Row": idx.get_level_values(0) + 1,  # برای خوانایی 1-based
                            "Column": idx.get_level_values(1),
                            "File1": s1[neq_mask].values,
                            "File2": s2[neq_mask].values,
                        }
                    )
                else:
                    changes = pd.DataFrame(
                        [
                            {
                                "Row": "-",
                                "Column": "-",
                                "File1": "No differences found",
                                "File2": "-",
                            }
                        ]
                    )

                changes.to_excel(writer, sheet_name="Changed_cells", index=False)

        output.seek(0)
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=comparison_result.xlsx"
            },
        )

    except Exception as e:
        # پیام واضح به فرانت بده تا در SweetAlert نمایش داده شود
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/clean-data/")
async def clean_data(
    file: UploadFile = File(...),
    operation: str = Form(...),
    column_name: str = Form(...),
    params: str = Form(None),
):
    try:
        df = read_file(file)

        # نرمال‌سازی نام ستون‌ها
        def _norm(s: str) -> str:
            if s is None:
                return ""
            s = str(s)
            s = s.replace("\u200c", "")  # ZWNJ
            s = s.replace("ي", "ی").replace("ك", "ک")
            s = re.sub(r"\s+", " ", s).strip()
            return s

        df.columns = [_norm(c) for c in df.columns]
        col = _norm(column_name)

        if col not in df.columns:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"ستون «{column_name}» یافت نشد. ستون‌های موجود: {df.columns.tolist()}"
                },
            )

        # پارامترها (دیگر نیازی به splitType نداریم)
        clean_params = {}
        if params and params not in ("null", "None", ""):
            try:
                clean_params = json.loads(params)
            except Exception:
                # اگر پارامترها معتبر نیستند، مشکلی نیست چون فقط از فاصله استفاده می‌کنیم
                pass

        op = (operation or "").strip()

        if op == "split_name":
            # فقط جداسازی با فاصله
            parts = df[col].astype(str).str.split(r"\s+", n=1, expand=True)
            df["first_name"] = (
                (parts[0] if 0 in parts.columns else "").astype(str).str.strip()
            )
            df["last_name"] = (
                (parts[1] if 1 in parts.columns else "").astype(str).str.strip()
            )

        elif op == "standardize_phone":
            phone_format = clean_params.get("phoneFormat", "international")

            def clean_phone(v):
                if pd.isna(v):
                    return v
                digits = re.sub(r"\D", "", str(v))
                if not digits:
                    return ""
                if phone_format == "international":  # +98...
                    if digits.startswith("0"):
                        return "+98" + digits[1:]
                    if digits.startswith("98"):
                        return "+" + digits
                    if digits.startswith("9") and len(digits) == 10:
                        return "+98" + digits
                    return "+" + digits
                elif phone_format == "local":  # 09...
                    if digits.startswith("98"):
                        return "0" + digits[2:]
                    if digits.startswith("+98"):
                        return "0" + digits[3:]
                    if digits.startswith("9") and len(digits) == 10:
                        return "0" + digits
                    return digits
                else:  # simple
                    return digits

            df[col] = df[col].apply(clean_phone)

        elif op == "remove_extra_spaces":
            df[col] = (
                df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
            )

        elif op == "standardize_date":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

        else:
            return JSONResponse(
                status_code=400, content={"error": f"عملیات ناشناخته: {operation}"}
            )

        excel_data = save_to_excel(df, sheet_name="Cleaned")
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=cleaned_data.xlsx"},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("clean_data failed")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/create-pivot/")
async def create_pivot(
    file: UploadFile = File(...),
    index_column: str = Form(...),
    values_column: str = Form(...),
    aggregation: str = Form("sum"),
):
    try:
        # فایل را بخوان (هِلپر قبلی خودت)
        df = read_file(file)

        # نرمال‌سازی نام ستون‌ها و ورودی‌ها (رفع مشکل فاصله/حروف عربی/ZWNJ)
        def _norm(s: str) -> str:
            if s is None:
                return ""
            s = str(s)
            s = s.replace("\u200c", "")  # ZWNJ
            s = s.replace("ي", "ی").replace("ك", "ک")
            s = re.sub(r"\s+", " ", s).strip()
            return s

        df.columns = [_norm(c) for c in df.columns]
        idx = _norm(index_column)
        val = _norm(values_column)

        if not idx or not val:
            return JSONResponse(
                status_code=400,
                content={"error": "ستون‌های ایندکس و مقادیر باید انتخاب شوند."},
            )

        if idx not in df.columns:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"ستون ایندکس «{index_column}» یافت نشد. ستون‌های موجود: {df.columns.tolist()}"
                },
            )
        if val not in df.columns:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"ستون مقادیر «{values_column}» یافت نشد. ستون‌های موجود: {df.columns.tolist()}"
                },
            )

        # مقدار ستون مقادیر را عددی کن (غیرقابل‌تبدیل‌ها NaN می‌شوند)
        df[val] = pd.to_numeric(df[val], errors="coerce")

        agg_map = {
            "sum": "sum",
            "mean": "mean",
            "count": "count",
            "min": "min",
            "max": "max",
        }
        aggfunc = agg_map.get(aggregation, "sum")

        pivot = pd.pivot_table(df, index=idx, values=val, aggfunc=aggfunc).reset_index()
        # اگر دوست داری نام ستون خروجی واضح‌تر باشد:
        pivot.columns = [idx, f"{aggregation}_{val}"]

        excel_data = save_to_excel(pivot, sheet_name="Pivot")
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=pivot_table.xlsx"},
        )

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/join-files/")
async def join_files(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    left_key: str = Form(...),
    right_key: str = Form(...),
    join_type: str = Form(...),
):
    try:
        print(f"Received files for join: {file1.filename}, {file2.filename}")
        print(
            f"Join params - left_key: {left_key}, right_key: {right_key}, join_type: {join_type}"
        )

        # خواندن فایل‌ها
        df1 = read_file(file1)
        df2 = read_file(file2)

        print(f"File1 shape: {df1.shape}, columns: {df1.columns.tolist()}")
        print(f"File2 shape: {df2.shape}, columns: {df2.columns.tolist()}")

        # نرمال‌سازی نام ستون‌ها
        def _norm(s: str) -> str:
            if s is None:
                return ""
            s = str(s)
            s = s.replace("\u200c", "")
            s = s.replace("ي", "ی").replace("ك", "ک")
            s = re.sub(r"\s+", " ", s).strip()
            return s

        df1.columns = [_norm(c) for c in df1.columns]
        df2.columns = [_norm(c) for c in df2.columns]
        left_key_norm = _norm(left_key)
        right_key_norm = _norm(right_key)

        print(f"Normalized - left_key: {left_key_norm}, right_key: {right_key_norm}")

        # بررسی وجود ستون کلید در هر جدول
        if left_key_norm not in df1.columns:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"ستون '{left_key}' در فایل اول یافت نشد. ستون‌های موجود: {df1.columns.tolist()}"
                },
            )

        if right_key_norm not in df2.columns:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"ستون '{right_key}' در فایل دوم یافت نشد. ستون‌های موجود: {df2.columns.tolist()}"
                },
            )

        # انجام join بر اساس نوع
        join_kind_map = {
            "inner": "inner",
            "left": "left",
            "right": "right",
            "outer": "outer",
        }

        join_kind = join_kind_map.get(join_type, "inner")

        # انجام join با ستون‌های مختلف برای هر جدول
        result = pd.merge(
            df1,
            df2,
            left_on=left_key_norm,
            right_on=right_key_norm,
            how=join_kind,
            suffixes=("_file1", "_file2"),
        )

        print(f"Join successful. Result shape: {result.shape}")

        excel_data = save_to_excel(result, sheet_name="Joined_Data")
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=joined_data.xlsx"},
        )

    except Exception as e:
        logger.exception("join-files failed")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ایمیل شما - اینجا رو با ایمیل واقعی خودتون جایگزین کنید
DEVELOPER_EMAIL = "vahidghaiem297@gmail.com"  # جایگزین کنید با ایمیل واقعی شما

@app.post("/submit-feedback")
async def submit_feedback(
    rating: int = Form(...),
    comment: str = Form(...),
    email: str = Form(None),
    name: str = Form(None),
    timestamp: str = Form(None),
    userAgent: str = Form(None)
):
    try:
        # ایجاد محتوای ایمیل
        subject = f"📊 بازخورد جدید SheetMagic - امتیاز: {rating}/5"
        
        body = f"""
        بازخورد جدید از کاربر SheetMagic:
        
        📈 امتیاز: {rating}/5
        💬 نظر: {comment}
        
        👤 اطلاعات کاربر:
        - نام: {name or 'ثبت نشده'}
        - ایمیل: {email or 'ثبت نشده'}
        - زمان: {timestamp or 'نامشخص'}
        
        🌐 اطلاعات مرورگر:
        {userAgent or 'نامشخص'}
        
        --
        این پیام به صورت خودکار از سیستم نظرسنجی SheetMagic ارسال شده است.
        """
        
        # در اینجا کد ارسال ایمیل رو اضافه می‌کنیم
        # برای سادگی، فعلاً فقط در کنسول لاگ می‌کنیم
        print("=" * 50)
        print("📧 بازخورد جدید دریافت شد!")
        print(f"📩 ارسال به: {DEVELOPER_EMAIL}")
        print(f"📋 موضوع: {subject}")
        print(f"📝 محتوا: {body}")
        print("=" * 50)
        
        # TODO: اینجا می‌تونید از سرویس‌های مثل SendGrid, SMTP، یا ... استفاده کنید
        # برای نمونه، می‌تونید از smtplib استفاده کنید:
        """
        import smtplib
        from email.mime.text import MIMEText
        
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = 'noreply@sheetmagic.com'
        msg['To'] = DEVELOPER_EMAIL
        
        # ارسال ایمیل با SMTP
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login('your_email@gmail.com', 'your_password')
            server.send_message(msg)
        """
        
        return {"success": True, "message": "نظر شما با موفقیت ثبت شد"}
        
    except Exception as e:
        logger.exception("Error submitting feedback")
        return JSONResponse(
            status_code=500, 
            content={"success": False, "error": str(e)}
        )


if __name__ == "__main__":
    import uvicorn
    import os

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
