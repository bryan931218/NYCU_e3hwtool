# LINE 官方帳號配置

2026-10-08 已透過 Official Account Manager 套用至「E3 作業通知助手」。本資料夾只保存圖片來源與非敏感文案，不保存 LINE token、綁定碼或用戶資料。

## 六格圖文選單

圖片：`rich-menu.png`，2500 × 1686，兩列三欄。選單列文字為「E3 常用功能」，預設展開。

| 位置 | 按鈕 | 動作 |
| --- | --- | --- |
| A | 我的作業 | `https://www.e3hwtool.space/` |
| B | 課程訊息 | `https://www.e3hwtool.space/courses/messages` |
| C | 提醒設定 | `https://www.e3hwtool.space/settings/notifications` |
| D | 綁定教學 | 傳送文字「綁定教學」 |
| E | 通知說明 | 傳送文字「通知說明」 |
| F | 回報問題 | `https://www.e3hwtool.space/feedback` |

顯示期間：2026/10/08 00:00 至 2036/10/08 23:59。更換圖文選單時應確認時段沒有重疊。重新進入聊天室後才會反映新選單。

圖片使用專案既有 Lucide 圖示，授權見 `frontend/shared/static/icons/lucide-LICENSE.txt` 與 `frontend/assignments/static/vendor/lucide-LICENSE.txt`。在已提供 `sharp` 的 Node 環境執行 `node docs/line-official-account/build-menu.cjs` 可重建圖片；不需變更網站部署依賴。

## 新好友歡迎訊息

僅首次加入好友時傳送，避免解除封鎖時重複歡迎。不插入好友姓名變數。

```text
歡迎使用 E3 作業通知助手！

開始接收個人通知：
1. 登入網站的通知設定。
2. 點「綁定 LINE」，將整段綁定碼傳到這裡。
3. 綁定成功後，回網站開啟 LINE 通知並儲存設定。

https://www.e3hwtool.space/settings/notifications

下方選單可查看作業、課程訊息與教學。
請勿在聊天室傳送 E3 密碼或 MoodleSession。
```

## 關鍵字回應

僅完全符合關鍵字時回覆，不攔截或解讀 `E3 <綁定碼>`；綁定驗證仍由現有 Webhook 處理。保留聊天及 Webhook 開啟，回應時間內使用「手動聊天＋自動回應訊息」，非回應時間使用「自動回應訊息」。原本的 Default「一律回應」已停用，但沒有刪除。

### 綁定教學

關鍵字：`綁定教學`。

```text
綁定 LINE，只需 3 步：

1. 登入網站，開啟通知設定。
2. 點「綁定 LINE」，將產生的整段綁定碼傳到這個聊天室（10 分鐘內有效）。
3. 收到綁定成功後，回網站開啟 LINE 通知、選擇提醒條件，再點「儲存設定」。

https://www.e3hwtool.space/settings/notifications

請勿在聊天室傳送 E3 密碼或 MoodleSession。
```

### 通知說明

關鍵字：`通知說明`。

```text
你可以自行選擇接收：
・新作業、到期前提醒
・作業評分（分數與評語）
・課程公告、課程信件
・可能的作業期限異動

在作業通知點「安排處理時間」，可直接在 LINE 選擇提醒日期與時間。

調整項目、提醒天數或關閉 LINE 通知：
https://www.e3hwtool.space/settings/notifications
修改後記得點「儲存設定」。

沒收到通知？先確認網站顯示已綁定、LINE 通知已開啟，且未封鎖本帳號。若 E3 登入失效，請回網站重新登入。
```

## 驗證與維護

- 未登入時，課程訊息及通知設定會正常導向登入頁；作業首頁與問題回報可開啟。
- 圖文選單、兩則關鍵字回應及歡迎訊息應在後台分別檢查，不可只儲存草稿。
- 手機驗證：離開再進入聊天室，點綁定教學／通知說明，並測試四個網站入口。
- 綁定碼、評分與提醒由網站處理；本次後台設定不增加取得用戶課程或學號的權限。
- 未發送群發訊息、未變更方案、未修改 token 或 Webhook 網址。
- 若日後把這兩個教學指令改由 Webhook 處理，先停用後台同名規則，避免重複回覆。
