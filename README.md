# E3 作業追蹤器

一個專門為陽明交通大學（NYCU）學生設計的 E3 作業整理工具，提供 CLI 與 Web 介面。使用者可以快速檢視所有課程作業、導出 Excel / Google 日曆、分享給同學共同使用。

- 網站連結：https://www.e3hwtool.space
- 備註：本工具僅供教育用途，請勿濫用。

---

## 功能一覽

- **作業自動蒐集**：輸入（或貼上） E3 的 `MoodleSession`，系統會爬取所有課程作業，包含截止、狀態、連結。
- **圖形化介面**：Web 版提供「依課程」與「依截止日」兩種視圖，可篩選逾期/已完成作業。
- **Excel / Google 日曆**：可將篩選後的作業導出為 Excel 檔或同步至 Google Calendar（桃紅色標籤、兩天前提醒）。
- **多使用者支援**：登入後每位使用者具有獨立快取，適合多人協作。
- **CLI 工具**：仍保留原始命令列模式，方便自動化或個人使用。

---

## 目錄結構

```
.
├─ e3/
│  ├─ web.py               # Flask Web 應用
│  ├─ cli.py               # CLI 入口
│  ├─ google_calendar.py
│  ├─ http.py / collector.py / parsing.py / utils.py ...
│  └─ templates/
│     ├─ web.html          # 主頁面模板
│     └─ login.html        # 登入頁模板
├─ requirements.txt
├─ e3.py                   # 方便執行的 wrapper
└─ README.md               # 本檔案
```

---

## 環境需求

| 項目                     | 版本建議                             |
| ------------------------ | ------------------------------------ |
| Python                   | 3.11 (與 EB AL2023 相容)             |
| 套件                     | `pip install -r requirements.txt`    |
| Elastic Beanstalk 平台   | Python 3.11 / 64bit Amazon Linux 2023|
| AWS 服務                 | Route 53、ACM、ALB、RDS(可選)        |
| Google API               | OAuth Client + Calendar API          |

---

## 快速開始

### 1. Clone & 安裝

```bash
git clone https://github.com/<your-name>/nycu-e3-tracker.git
cd nycu-e3-tracker
python -m venv .venv
. .venv/Scripts/activate  # Windows
pip install -r requirements.txt
cp .env.example .env      # 依需求修改
```

### 2. CLI 使用

```bash
python e3.py \
  --username 1125xxxx \
  --password <YOUR_PASS> \
  --all-courses \
  --include-completed
```

或輸入 `python e3.py -h` 查看所有參數。

### 3. Web 介面（本地）

```bash
set FLASK_APP=e3/web.py
python -m flask run
```

- 進入 `http://127.0.0.1:5000`
- 首次登入可輸入 E3 帳密或貼上 `MoodleSession`。
- 登入後即可查看作業、匯出 Excel、導入 Google 日曆。

---

## 主要畫面

> 請把實際截圖放到 `docs/images`，並更新下列連結。

1. **登入頁**  
   ![登入頁](docs/images/login.png)

2. **主控台 / 作業列表**  
   ![主控台](docs/images/dashboard.png)

3. **Google 日曆導入選單**  
   ![導入 Google 日曆](docs/images/google-modal.png)

---

## Google Calendar 導入說明

1. 前往 Google Cloud Console → APIs & Services → Credentials → 建立 OAuth 2.0 Client (Web Application)。  
   - Redirect URI 設為 `https://www.e3hwtool.space/google/callback`（或本地測試用 `http://127.0.0.1:5000/google/callback`）。
2. 在 `.env` 設定：
   ```
   E3_GOOGLE_CLIENT_ID=xxx
   E3_GOOGLE_CLIENT_SECRET=xxx
   E3_GOOGLE_REDIRECT_URI=https://www.e3hwtool.space/google/callback
   ```
3. Web 版登入後，點「連結 Google 日曆」進行授權。
4. 在彈出視窗中勾選要導入的作業 → 按「導入至 Google 日曆」。  
   - 事件會統一以 **桃紅色** 顯示，標題前會加 `[作業]`，並設定 **兩天前** 的提醒。

---

## 安全與隱私建議

- **Cookie 模式**：若不想讓使用者輸入密碼，請在 UI 引導他們於 E3 網站登入後，手動貼 `MoodleSession`。  
- **HTTPS 必須啟用**：請使用 AWS ACM 憑證並綁定 ALB，也要在 nginx 加上 HTTP→HTTPS 轉址。  
- **Google OAuth 只用於識別**：目前仍需使用 MoodleSession 抓作業；若未來 E3 開放 API，再改成純 OAuth。

---

## 部署到 Elastic Beanstalk

1. `eb init -p python-3.11 nycu-e3-tracker`（首次設定）  
2. `eb config save --cfg backup-YYYYMMDD` 備份設定  
3. 使用 AL2023 平台建立新環境：`eb create e3-web-prod --cfg backup-YYYYMMDD`  
4. `eb deploy e3-web-prod`  
5. Route 53（A/CNAME）指向新 ALB，ACM 憑證綁在 HTTPS Listener  
6. SSL Labs 驗證：https://www.ssllabs.com/ssltest/analyze.html?d=www.e3hwtool.space  

---

## 常見問題

1. **瀏覽器顯示不安全**：  
   - 確認 `https://www.e3hwtool.space` / `https://e3hwtool.space` 都指向同一個 ALB。  
   - SSL Labs 憑證須同時包含兩個網域，並重整頁面（或清除快取）。

2. **Google 授權後沒變化**：  
   - 確認 Google Console 與 `.env` 的 Redirect URI 完全一致。  
   - 在 `/tmp/e3_tracker_cache/<username>.google.json` 會存 refresh token。

3. **Launch Configuration 失敗**：  
   - AWS 新帳號不支援舊的 Launch Configuration，請換成 Amazon Linux 2023 平台，或在 AWS Support 開 ticket 啟用。

---

## 授權 & 免責

- 僅供學習與個人使用，請勿在未獲授權的情況下蒐集或洩漏他人課程資訊。
- 請遵守 NYCU 及 E3 系統的使用規範，避免高頻率請求導致服務異常。

---

## 網站連結

👉 https://www.e3hwtool.space
