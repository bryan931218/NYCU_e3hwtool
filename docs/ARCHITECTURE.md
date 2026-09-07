# 程式架構與修改指南

本庫包含 E3 作業追蹤與研究所備考中心，共用 Flask 與資料儲存層。

## 目錄

| 位置 | 負責內容 |
| --- | --- |
| `backend/e3_tracker/bootstrap.py` | 統一安裝網站功能，供本機與正式站共同使用 |
| `backend/server.py` | 本機伺服器、環境變數與 reload 設定 |
| `backend/wsgi.py` | Railway / Gunicorn 的 WSGI 入口 |
| `backend/e3_tracker/api/web.py` | 應用組裝、認證、共用上下文與排程組裝 |
| `backend/e3_tracker/api/routes/` | 按作業、筆記、搜尋、AI、上傳、影片與管理功能分組的路由 |
| `backend/e3_tracker/services/note_*.py` | 模型呼叫、文字整理、辨識驗證、批次分析、定位與關聯 |
| `backend/e3_tracker/services/study_progress.py` | 完成率、有效觀看秒數、追趕進度與日期計算 |
| `backend/e3_tracker/services/study_upload_batches.py` | 批次進度、檢查點簽章、續跑快取與跨頁索引 |
| `backend/e3_tracker/services/traffic.py` | 流量、使用者活動與持久化統計 |
| `backend/e3_tracker/services/collector.py` | E3 課程及作業蒐集、學期快取 |
| `backend/e3_tracker/services/youtube_*.py` | YouTube 清單同步、音訊與畫面取得 |
| `backend/e3_tracker/shared/storage.py` | 儲存層入口、連線初始化與遷移，相容既有呼叫 |
| `backend/e3_tracker/shared/persistence/` | 共用資料表與按領域拆分的資料庫操作 |
| `backend/e3_tracker/shared/*runtime.py` | 現有功能安裝器與相容層 |
| `backend/e3_tracker/shared/study_math.py` | 公式文字處理 |
| `backend/e3_tracker/shared/source_localization.py` | 原始筆記定位 |
| `frontend/templates/` | 頁面與共用介面片段 |
| `frontend/templates/pages/` | 各大型頁面的 CSS／JavaScript Jinja 片段 |
| `backend/e3_tracker/api/static/` | 前端靜態資源與 Discord 素材 |
| `backend/tests/` | 自動測試 |
| `backend/tools/` | 維護工具 |
| `docs/` | 架構與維護文件 |
| `output/`、`.localdata/` | 本機輸出、測試資料，不提交 Git |

## 新功能放哪裡

- 計算與資料轉換放在對應 service，盡量使用參數輸入、回傳值輸出。
- 路由負責權限、讀取請求與組合回應，不再將獨立計算大量塞入 `create_app()`。
- 資料庫讀寫留在儲存層，service 不匯入 `api.web`，避免循環相依。
- 共用 UI 沿用模板片段；獨立 JavaScript / CSS 可放 static。含 Jinja 的腳本須先抽出資料設定，不能直接搬成靜態檔案。
- 新的安裝器只在 `bootstrap.py` 登記一次。安裝順序會影響模板與工廠包裝，不要任意調換。

## 相容性

`web.py` 仍匯出已拆出的函式、常數與 `TrafficTracker`，讓現有測試與安裝器保持可用。新程式請從實際負責的 service 匯入。

排程重排與休息日安裝器目前會包裝 `web._study_plan_schedule_definitions`，因此排程組裝仍留在 web；單純完成率與追趕計算已拆出。

路由以 `register_*_routes` 註冊，筆記服務以 `build_note_*` 建立。相依透過具名參數傳入，保留每個 app 自己的閉包，不使用全域 app 或反向匯入 web。路由名稱、URL 與權限裝飾器不變。需要支援既有 runtime 包裝的函式，透過延遲 callback 呼叫。

儲存層按作業、影片、學習時間、筆記複習、上傳、助手、社群及帳號分組；`schema.py` 是唯一資料表定義來源。各 mixin 共用原本連線與交易工具，不建立另一套資料庫，也不改變既有資料。

大型頁面使用 Jinja include 拆出含模板變數的 CSS／JavaScript；片段仍在原本標籤位置渲染，不增加網路請求。修改時到 `pages/<頁面名稱>/` 找對應片段。測試的 `source_helpers.read_source` 會展開 include，避免只檢查外層 HTML 而漏掉實際腳本。

## 開發與驗證

在專案根目錄執行：

```sh
python -m pip install -r requirements.txt
python start_servers.py
```

執行全部測試：

```sh
python -m unittest discover -s backend/tests -t backend
```

只測指定功能：

```sh
cd backend
python -m unittest tests.test_study_plan_progress tests.test_study_upload_progress
```

測試多以暫存資料庫及模擬外部服務執行。通過測試不代表正式 YouTube、AI 額度或 Railway 環境已驗證。正式站使用 `backend/wsgi.py`，本機使用 `backend/server.py`，兩者經相同 bootstrap 建立完整應用。

## Git 維護

只提交程式、部署設定、必要的靜態資源、種子資料與文件。影片目錄 JSON 是部署所需資料，不應當成暫存檔刪除。快取、虛擬環境、測試輸出與 `.env` 由 `.gitignore` 排除。

`guest_payload.json` 是使用者匯出的課程資料，不提交 Git；匯出工具本身仍需保留，網站提供下載。`images/` 是 README 使用的圖片，也不是暫存檔。

清理時可刪除 `__pycache__`、`.pytest_cache` 等可重建快取。`output/` 與 `.codex-build/` 可能含未交付的影片、備審或人工測試資料，應先移到專案外封存，不能直接視為垃圾刪除。`.localdata/`、上傳圖片與資料庫必須保留。
