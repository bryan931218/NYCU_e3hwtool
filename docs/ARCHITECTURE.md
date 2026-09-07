# 程式架構與修改指南

本庫包含 E3 作業追蹤與研究所備考中心，共用 Flask 與資料儲存層。

## 目錄

| 位置 | 負責內容 |
| --- | --- |
| `backend/e3_tracker/bootstrap.py` | 統一安裝網站功能，供本機與正式站共同使用 |
| `backend/server.py` | 本機伺服器、環境變數與 reload 設定 |
| `backend/wsgi.py` | Railway / Gunicorn 的 WSGI 入口 |
| `backend/e3_tracker/api/web.py` | Flask 路由、認證與尚未拆出的應用流程 |
| `backend/e3_tracker/services/study_progress.py` | 完成率、有效觀看秒數、追趕進度與日期計算 |
| `backend/e3_tracker/services/study_upload_batches.py` | 批次進度、檢查點簽章、續跑快取與跨頁索引 |
| `backend/e3_tracker/services/traffic.py` | 流量、使用者活動與持久化統計 |
| `backend/e3_tracker/services/collector.py` | E3 課程及作業蒐集、學期快取 |
| `backend/e3_tracker/services/youtube_*.py` | YouTube 清單同步、音訊與畫面取得 |
| `backend/e3_tracker/shared/storage.py` | 資料表、查詢與持久化 |
| `backend/e3_tracker/shared/*runtime.py` | 現有功能安裝器與相容層 |
| `backend/e3_tracker/shared/study_math.py` | 公式文字處理 |
| `backend/e3_tracker/shared/source_localization.py` | 原始筆記定位 |
| `frontend/templates/` | 頁面與共用介面片段 |
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

排程重排與休息日安裝器目前會包裝 `web._study_plan_schedule_definitions`，因此排程組裝仍留在 web；單純完成率與追趕計算已拆出。`storage.py` 與筆記 AI 路由仍偏大，後續應按資料領域與注入相依拆分，不能只用檔案文字切割改變函式作用域。

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
