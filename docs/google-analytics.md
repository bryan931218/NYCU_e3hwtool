# GA4 與本站統計

## 建立與連接

1. 到 https://analytics.google.com/ 建立 E3 專用 GA4 資源，時區選 Asia/Taipei。
2. 建立網站資料串流：https://www.e3hwtool.space。**關閉加強型評估**，不要另外安裝 GTM 或貼 Google 自動產生的標籤。本站已管理同意與安全化頁面事件；另貼標籤會重複計數並繞過隱私處理。
3. 記下 G- 開頭的評估 ID，以及管理 → 資源詳細資料的數字資源 ID。
4. 在 Railway production web 設定 `E3_GA4_MEASUREMENT_ID` 和 `E3_GA4_PROPERTY_ID`。
5. Google Cloud 建立專用專案，API 和服務 → 程式庫 → 搜尋 Google Analytics Data API → 啟用。IAM 與管理 → 服務帳戶 → 建立服務帳戶，名稱可用 `e3-ga4-reader`；專案角色及其他使用者存取權留空。
6. 在 GA4 資源存取管理加入服務帳戶電子郵件，僅授予「檢視者」，不要給管理員權限。
7. Google Cloud → 服務帳戶 → 選該帳戶 → 金鑰 → 新增金鑰 → 建立新金鑰 → JSON。下載後自行開啟，將完整 JSON 內容直接存入 Railway production web 的 `E3_GA4_SERVICE_ACCOUNT_JSON`，不是檔名或路徑。不提交 Git、不貼聊天，不共用日曆 OAuth 憑證。限制可讀 Railway 變數的人員，並定期輪替金鑰。若組織禁止建立金鑰，請洽管理員，不要自行放寬組織政策。
8. 儲存變數並重新部署。登入後台，開啟「GA4 報表」(`/admin/ga4`) 驗證；資源不存在、API 未啟用或權限錯誤會顯示無法取得報表，不影響本站功能。
9. 用非管理員瀏覽器同意分析後，在 GA4 即時報表確認事件。標準報表需等待資料處理；接入前不會有歷史 GA4 資料。

## 資料與指標

- 本站帳號分析：學號合併、活躍帳號、功能紀錄、目前綁定與通知啟用狀態。不要將 GA4 訪客數作為精確會員數。
- GA4 報表獨立於 `/admin/ga4`，流量監控不載入 Google 報表。報表授權設定後移除舊來源、頁面與功能統計的重複介面，保留帳號、通知、入學碼／科系碼及操作管理。原頁面分析網址會轉至獨立 GA4 報表，原資料不刪除；尚未設定 GA4 時仍可使用原有統計。
- 區間回訪：選定區間中，至少兩個不同台北日期使用過的帳號。
- 新用戶 7 日內回訪：首次加入後隔日至第 7 日曾使用的帳號 / 完整觀察到第 7 日的新用戶。不計未滿 7 日、追蹤啟用前加入或遷移時無法確認首次加入的帳號。這不是「第 7 日當天留存率」。
- LINE 啟用進度：目前帳號 → 目前綁定 → 綁定且啟用。是狀態採用率，不假設歷史動作順序；包含 LINE 端完成的綁定。
- GA4：來源／活動、熱門頁面、裝置、活躍及新訪客、工作階段、參與率、平均參與秒數／活躍訪客、新／回訪訪客、登入方式、功能事件。
- GA4 閉合漏斗：首頁 → 登入頁 → 登入成功；登入成功 → 網站確認 LINE 綁定 → 網站儲存 LINE 通知啟用。每步限 7 日，同一 GA4 瀏覽器識別。只在 LINE 操作且沒有回到網站確認的綁定不會進入 GA4 漏斗，以本站綁定資料為準。漏斗使用 Google alpha API，失敗時其他報表仍可用。
- 本站操作紀錄不代表作業完成。GA4 事件人數不是同一批人的逐步轉換；真正依序轉換請看漏斗表。

## 隱私與效能

- 無評估 ID 時完全不插入 GA4。管理員、代看模式、考研站與未同意分析的瀏覽器不載入 Google。
- 不使用 `user_id`；不把學號、學號雜湊、姓名、登入 cookie、LINE ID、課程／作業內容上傳 Google。
- 僅允許固定頁面與固定事件參數。送出的 URL 不含 session、view_user、搜尋字詞或自由格式 UTM。Google 仍會接收連線技術資料，隱私政策已揭露。
- 基本同意模式：拒絕前後均不載入 Google，不使用拒絕後的無 Cookie ping。撤回時停用並卸載追蹤、清除 GA Cookie；已經寄出的資料不會由撤回動作自動刪除。
- Google reports 僅後端使用唯讀憑證，管理員 API 才能查閱。每個程序每個區間快取 15 分鐘，背景讀取，錯誤保留舊資料並標示；不在作業頁的請求等待 Google。
- GA4 標籤於同意後使用非同步載入；後台捲到報表區時才取資料。不要啟用額外自動表單／搜尋／影片追蹤。

## 分享來源

允許的 utm_source：dcard、line、google、instagram、facebook、campus。
允許的 utm_medium：social、message、referral、organic。
可選 `E3_GA4_CAMPAIGNS` 為逗號分隔的預先核准活動代碼（小寫英數與連字號，最多 48 字元），例如 `dcard-115-1,line-115-1`。
沒有列入清單的 campaign 一律不送出，不把姓名、學號、班級或私人識別碼放進活動代碼。

範例：https://www.e3hwtool.space/?utm_source=dcard&utm_medium=social&utm_campaign=dcard-115-1

參考：
- https://developers.google.com/analytics/devguides/reporting/data/v1/basics
- https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/batchRunReports
- https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1alpha/properties/runFunnelReport
- https://developers.google.com/analytics/devguides/collection/ga4/views
