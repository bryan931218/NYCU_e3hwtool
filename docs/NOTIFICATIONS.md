# E3 作業通知部署

## 功能

作業頁上方「通知設定」可分別開啟瀏覽器與 LINE。支援當期新作業、截止前 1–30 天（最多 5 個時間點）。第一次啟用建立基準，不補發舊作業；已完成、已評分、已忽略、已過期、舊學期作業不會提醒。
登入帳號的自訂待辦與個人期限會儲存在伺服器，提醒使用個人期限；未修改時使用 E3 原始期限。訪客與舊的本機待辦不會觸發伺服器通知。

### LINE 作業操作

新的作業通知與到期提醒提供「今晚再提醒」、「安排處理時間」、「開啟作業」。晚間提醒預設台北時間 20:00；已過 20:00 時安排隔天，若會超過期限則改為一小時後，仍來不及時不安排。
安排處理時間可選開始日期、15–120 分鐘，並自選同步 Google 日曆。安排的時間必須在截止之前，已完成、已評分、已忽略的作業不會提醒。可在同一頁取消提醒；已同步的 Google 處理時段不會跟著刪除。
LINE 快速操作主要用於手機版，桌面版可使用訊息中的安排連結（[LINE 官方支援說明](https://developers.line.biz/en/docs/messaging-api/using-quick-reply/)）。操作包含有效期限、帳號綁定驗證與重複事件防護，不需要新增 LINE 環境變數或重新綁定。

### 公告／信件期限確認

通知設定可選「可能有作業期限異動」，預設關閉。背景只讀取當期最近一天訊息，每輪每種來源最多補讀一篇內文，避免新增頁面載入等待。
課程訊息內文有明確數字日期與期限相關文字時，會列出待確認日期與來源。支援西元、民國及月／日寫法；「下週五」等相對日期、圖片或附件不會自動判讀。沒有時間時暫填 23:59，必須由用戶確認。只能選同一課程、同一學期的自己的作業，無法確定是哪一份時不會自動選擇。
確認後只更新個人期限，不改寫 E3 資料；列表、日曆、ICS、後續到期提醒使用同一個個人期限。Google 日曆同步須另行勾選，僅更新系統建立的對應作業活動。一般同步也使用同一個活動識別，不會因確認期限後再同步而建立重複活動。同步失敗保留個人期限並提供重試，不會重複建立處理時段。
新資料表透過 `0015_assignment_actions` 遷移建立。期限來源與排程資料加密儲存，刪除帳號時一併清除。確認流程比對快取來源版本與現有個人期限，來源有異動時須重新讀取。

## 瀏覽器推播

在專案根目錄執行：

```powershell
python backend/tools/generate_notification_keys.py
```

產生的 `.localdata/notification-vapid.env` 被 Git 忽略，終端不印出金鑰。
將檔案中的三個變數填入 Railway → E3hwtool → production → web → Variables：

| 變數 | 用途 |
| --- | --- |
| `E3_PUSH_VAPID_PRIVATE_KEY` | 伺服器私鑰 |
| `E3_PUSH_VAPID_PUBLIC_KEY` | 與私鑰成對的訂閱公鑰 |
| `E3_PUSH_VAPID_SUBJECT` | 聯絡網址或信箱 |

保留同一組金鑰，不要在每次部署重新產生；輪替後用戶需要重新啟用裝置。
正式站須使用 HTTPS，用戶在通知設定點選「啟用此裝置」，允許通知後再儲存條件。
背景送達依瀏覽器、作業系統與背景權限而定。iPhone/iPad 須先將網站加入主畫面，再從該網頁 App 啟用；參考 [WebKit 說明](https://webkit.org/blog/13878/web-push-for-web-apps-on-ios-and-ipados/)。

## 建立 LINE 官方帳號

1. 在 [LINE Official Account Manager](https://manager.line.biz/) 建立官方帳號，例如「E3 作業通知」。
2. 官方帳號「設定 → Messaging API」啟用 Messaging API，選擇自己管理的 Provider。
3. 前往 [LINE Developers Console](https://developers.line.biz/console/)，選擇對應的 Messaging API channel。
4. 在 Basic settings 取得 Channel secret；Messaging API 取得可用的 Channel access token 與 Bot basic ID（通常為 `@...`）。
5. 填入 Railway 的變數，不要貼到 Git、前端或對話：

| 變數 | 來源 |
| --- | --- |
| `E3_LINE_CHANNEL_SECRET` | Channel secret |
| `E3_LINE_CHANNEL_ACCESS_TOKEN` | Channel access token |
| `E3_LINE_BOT_BASIC_ID` | Bot basic ID，包含 `@` |

6. 部署本功能與變數後，設定 Webhook URL：`https://www.e3hwtool.space/api/notifications/line/webhook`。
7. 點 Verify 確認成功，開啟 Use webhook，建議開啟 webhook redelivery。
8. 在官方帳號關閉預設自動回覆，避免綁定碼收到無關回覆。
9. 用戶登入網站 → 通知設定 → 綁定 LINE → 加入官方帳號 → 傳送畫面上的 `E3 ...` 綁定碼（10 分鐘有效、一次性）。
10. 網站顯示已綁定後，勾選 LINE 與通知條件並儲存。

官方文件：[建立 Messaging API](https://developers.line.biz/en/docs/messaging-api/getting-started/)、[Webhook 設定](https://developers.line.biz/en/docs/messaging-api/building-bot/)。
推播訊息會使用官方帳號的訊息額度，請查看 [Messaging API 計費說明](https://developers.line.biz/en/docs/messaging-api/pricing/)。

## 排程與限制

現有 web 部署具備任一通道完整設定後會啟動通知工作執行緒，不需另建 Railway 服務。
每分鐘處理通知，用戶的 E3 檢查間隔約 15 分鐘；實際延遲亦取決於 E3 回應與使用量。
`E3_NOTIFICATIONS_WORKER=0` 停用排程，測試時使用此值，正式預設開啟。
背景更新使用既有有效 E3 session，不保存密碼；失效時請重新登入。
E3 無法更新時仍依最後快取提醒，設定頁會提示；無法確認快取後在 E3 完成的繳交。
服務休眠或離線時不執行工作，恢復後只補發仍有效的最近提醒。
通知佇列與發送紀錄最多保留約 30 天，訂閱及綁定在用戶移除時刪除。
網路逾時無法保證端到端 exactly-once；LINE 使用 retry key，瀏覽器使用相同 notification tag 減少重複。

## 驗證

```powershell
$env:PYTHONPATH='backend'
python -m unittest tests.test_assignment_notifications tests.test_assignment_actions tests.test_schema_migrations
```

測試用假的訂閱與模擬傳送，沒有寄出真實通知。正式設定完成後，可在通知設定點選 LINE「測試通知」或瀏覽器「測試推播」；只傳給本人的此裝置或已綁定 LINE，每帳號 10 分鐘最多 10 次（兩個通道共用），不修改作業或提醒條件。畫面顯示已交給服務不等於裝置已送達，仍須確認實際收到訊息。正式驗收亦須用本人帳號驗證新作業及即將到期作業。

若裝置回報已建立通知，卻沒有彈出通知橫幅，請查看 Windows 通知中心、Chrome 的系統通知開關與勿擾模式（[Microsoft 說明](https://support.microsoft.com/en-us/windows/experience/notifications-and-do-not-disturb-in-windows)）。

推播測試會等待最多 20 秒的 service worker 裝置回報，區分服務接受、裝置接收與通知建立；回報只在同瀏覽器通知設定頁的記憶體傳遞，不寫入資料庫或送回伺服器，也不包含作業名稱、訂閱端點或帳號資訊。建立通知成功仍不代表作業系統顯示了橫幅。關閉設定頁時不會有頁面回報，背景推播本身仍可顯示通知。
