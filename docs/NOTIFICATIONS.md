# E3 作業通知部署

## 功能

作業頁上方「通知設定」可分別開啟瀏覽器與 LINE。支援當期新作業、截止前 1–30 天（最多 5 個時間點）。第一次啟用建立基準，不補發舊作業；已完成、已評分、已忽略、已過期、舊學期作業不會提醒。
提醒以 E3 作業資料為準；本機新增待辦與本機修改的截止時間不在通知範圍內。

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
python -m unittest tests.test_assignment_notifications tests.test_schema_migrations
```

測試用假的訂閱與模擬傳送，沒有寄出真實通知。正式設定完成後，可在通知設定點選各通道「測試通知」；只傳給本人的此裝置或已綁定 LINE，每帳號 10 分鐘最多 3 次，不修改作業或提醒條件。畫面顯示已交給服務不等於裝置已送達，仍須確認實際收到訊息。正式驗收亦須用本人帳號驗證新作業及即將到期作業。
