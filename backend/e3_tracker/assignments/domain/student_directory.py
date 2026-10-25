"""Display-only NYCU student-number metadata; never used for authorization.

New-format numbers are degree + two-digit ROC admission year + department + serial.
See docs/STUDENT_DIRECTORY.md for provenance and the limits of inferred grades.
"""

import re
import json
import math
from datetime import datetime

from e3_tracker.platform.constants import TAIPEI_TZ


DEGREES = {"1": "學士", "2": "學士後", "3": "碩士", "4": "博士", "5": "碩士在職專班"}

# Public student-number references, checked against the university's department
# directory. Unknown codes deliberately remain unknown; do not use course codes.
DEPARTMENTS = {
    "101": "醫學系", "119": "中醫學系", "150": "牙醫學系", "201": "護理學系",
    "231": "藥學系", "260": "醫學生物暨檢驗學系", "261": "生物醫學工程學系",
    "262": "物理治療暨輔助科技學系", "263": "生物醫學放射科學系",
    "265": "數位醫療學士學位學程", "304": "生命科學系暨基因體科學研究所",
    "350": "生物科技學系", "450": "外國語文學系", "481": "人文社會學系",
    "483": "傳播與科技學系", "504": "電機學院博士班", "506": "電機學院碩士在職專班",
    "510": "電子研究所", "511": "電機工程學系", "512": "電控工程研究所",
    "513": "電信工程研究所", "514": "光電工程學系", "550": "資訊工程學系",
    "551": "資訊科學與工程研究所", "552": "網路工程研究所", "553": "多媒體工程研究所",
    "554": "數據科學與工程研究所", "555": "資訊安全研究所", "556": "資訊學院博士班",
    "580": "前瞻半導體研究所", "581": "智能系統研究所", "591": "國際半導體產業學院",
    "605": "機器人碩士學位學程", "610": "半導體工程學系", "611": "機械工程學系",
    "612": "土木工程學系", "613": "材料科學與工程學系", "615": "環境工程研究所",
    "617": "太空系統工程研究所", "650": "理學院科學學士學位學程", "651": "電子物理學系",
    "652": "應用數學系", "654": "應用化學系", "657": "統計學研究所", "658": "物理研究所",
    "700": "管理科學系", "701": "運輸與物流管理學系", "704": "工業工程與管理學系",
    "705": "資訊管理與財務金融學系", "730": "科技法律研究所碩士在職專班",
    "731": "科技法律研究所", "831": "智慧計算與科技研究所", "832": "智慧系統與應用研究所",
    "833": "智慧與綠能產學研究所", "901": "學士班大一大二不分系", "950": "百川學位學程",
    "951": "系統工程與科技學士學位學程", "952": "學士後電子與光子學士學位學程",
}

DEPARTMENT_ALIASES = {
    '資工系':'資訊工程學系', '電機系':'電機工程學系', '機械系':'機械工程學系',
    '土木系':'土木工程學系', '材料系':'材料科學與工程學系', '應數系':'應用數學系',
    '電物系':'電子物理學系', '應化系':'應用化學系', '外文系':'外國語文學系',
    '人社系':'人文社會學系', '傳科系':'傳播與科技學系', '管科系':'管理科學系',
    '資財系':'資訊管理與財務金融學系', '工工系':'工業工程與管理學系',
    '運管系':'運輸與物流管理學系',
}

PROFILE_REASON_LABELS = {
    'name_missing':'E3 姓名欄位未取得', 'login_required':'E3 登入已失效',
    'access_denied':'E3 拒絕讀取個人資料', 'profile_link_missing':'E3 本人資料連結未取得',
    'profile_owner_missing':'E3 本人識別欄位未取得', 'upstream_timeout':'E3 個人資料讀取逾時',
    'upstream_request':'E3 個人資料連線失敗', 'identity_conflict':'身分資料不一致，未覆寫',
    'dashboard_unavailable':'E3 個人頁暫時無法讀取', 'profile_unavailable':'E3 個人資料暫時無法讀取',
    'edit_unavailable':'E3 個人資料暫時無法讀取', 'internal_error':'個人資料同步待重試',
    'edit_link_missing':'E3 個人資料連結未取得', 'edit_owner_mismatch':'E3 本人資料未通過核對',
    'student_number_field_missing':'E3 學號欄位未取得', 'student_number_unlocked':'E3 學號欄位未通過核對',
    'student_number_invalid':'E3 學號格式未通過核對', 'identity_missing':'個人資料同步待重試',
    'success':'已同步個人資料',
}


def profile_metadata(value):
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or '{}')
        return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
        return {}


def _observed_information(result, details, year):
    department = str(details.get('department') or '').strip()
    if department and len(department) <= 128 and not any(char in department for char in '<>@'):
        result.update(department=DEPARTMENT_ALIASES.get(department, department), department_source='e3')
    actual = details.get('grade')
    degree = result['degree_code']
    observed_degree = details.get('degree_code', degree)
    if (details.get('academic_year') == year and isinstance(actual, int) and not isinstance(actual, bool)
            and 1 <= actual <= 15 and (not degree or observed_degree == degree)):
        if not degree and observed_degree in DEGREES:
            degree = observed_degree
            result.update(degree_code=degree, degree=DEGREES[degree])
        short = {'1':'大', '2':'學士後', '3':'碩', '4':'博', '5':'在職碩'}.get(degree)
        label = short + '一二三四五六七八九十'[actual-1] if short and actual <= 10 else f'{DEGREES.get(degree, "")} {actual} 年級'.strip()
        result.update(grade=label, grade_key=f'{degree}:{actual}', grade_source='e3')
    return result


def academic_year(now=None):
    now = now or datetime.now(TAIPEI_TZ)
    if isinstance(now, datetime):
        now = now.astimezone(TAIPEI_TZ) if now.tzinfo else now
    return now.year - 1911 - (now.month < 8)


def student_information(number, details=None, *, now=None):
    """Decode only supported 109+ formats and label every inferred grade."""
    details = details if isinstance(details, dict) else {}
    year = academic_year(now)
    result = {"number": number or "", "degree": "", "degree_code": "", "admission_year": None,
              "department_code": "", "department": "尚未確認", "department_source": "unknown",
              "grade": "尚未確認", "grade_key": "unknown", "grade_source": "unknown", "academic_year": year}
    if not isinstance(number, str) or not re.fullmatch(r"[1-5][0-9]{8}", number):
        return _observed_information(result, details, year)
    admitted = 100 + int(number[1:3])
    # The merged 1+2+3+3 scheme starts at 109; don't reinterpret older IDs.
    if not 109 <= admitted <= year + 1:
        return _observed_information(result, details, year)
    degree, code = number[0], number[3:6]
    result.update(degree=DEGREES[degree], degree_code=degree, admission_year=admitted, department_code=code)
    if code in DEPARTMENTS:
        result.update(department=DEPARTMENTS[code], department_source="student_number")
    else:
        result["department"] = f"未核對系所（{code}）"
    # Preserve historical names rather than silently renaming an older cohort.
    if code == "610" and admitted < 113:
        result["department_previous"] = "奈米科學及工程學士學位學程"
    if code == "555" and admitted < 113:
        result["department"] = "資通安全碩士學位學程"
    if code in {"832", "833"} and admitted >= 116:
        result.update(department=f"未核對新制系所（{code}）", department_source="unknown")
    elapsed = year - admitted + 1
    if elapsed <= 0:
        result.update(grade=f"{admitted} 學年度入學", grade_key=f"{degree}:future")
    else:
        chinese = "一二三四五六七八九十"
        short = {"1": "大", "2": "學士後", "3": "碩", "4": "博", "5": "在職碩"}[degree]
        # Elapsed years never imply graduation or current enrollment.
        usual = {"1": 6 if code in {"101", "119", "150", "231"} else 4, "2": 2, "3": 3, "4": 7, "5": 4}[degree]
        label = short + chinese[elapsed - 1] if elapsed <= min(usual, 10) else f"{DEGREES[degree]}入學第 {elapsed} 年"
        result.update(grade=label + "（推估）", grade_key=f"{degree}:{elapsed}", grade_source="student_number")
    return _observed_information(result, details, year)


def build_student_directory(accounts, *, now=None):
    """Merge aliases only for display; prefer the newest independently saved fields."""
    from e3_tracker.assignments.domain.usage import student_identity
    grouped = {}
    for row in accounts:
        key = student_identity(row) or row['username']
        grouped.setdefault(key, []).append(profile_metadata(row.get('profile_metadata')))
    result = {}
    for key, rows in grouped.items():
        def checked(row, field='checked_at'):
            value = row.get(field, row.get('checked_at', 0))
            return value if isinstance(value, (int, float)) and math.isfinite(value) else 0
        merged = {}
        for row in sorted(rows, key=checked):
            merged.update(row)
        # A failed retry updates checked_at but must not make retained academic
        # fields look newer than another alias's successful observation.
        for field, attributes in [('department', ('department',)), ('grade', ('grade','academic_year','degree_code'))]:
            observed = [row for row in rows if field in row]
            if observed:
                newest = max(observed, key=lambda row: checked(row, field+'_checked_at'))
                for attribute in attributes:
                    merged.pop(attribute, None)
                    if attribute in newest:
                        merged[attribute] = newest[attribute]
        info = student_information(key, merged, now=now)
        info['sync_status'] = PROFILE_REASON_LABELS.get(merged.get('reason'), '等待背景同步')
        info['checked_at'] = checked(merged)
        result[key] = info
    return result
