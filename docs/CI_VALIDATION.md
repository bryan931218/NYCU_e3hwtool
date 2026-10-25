# Push 前驗證

GitHub 的 Backend validation 與 Security validation 使用 Python 3.12；本機也應在獨立的 Python 3.12 虛擬環境安裝及驗證。Pillow 13 不再支援 Python 3.10，不能使用舊環境的套件結果代替正式依賴的驗證。

在已啟用的 Python 3.12 虛擬環境中，從專案根目錄執行：

```sh
python -m pip install -r requirements-build.txt
python -m pip install -r requirements.txt "pip-audit>=2.10.1,<3"
python -m pip check
python -m unittest discover -s backend/tests -t backend
npm --prefix frontend test
python -m pip_audit
```

介面變更後，測試應驗證目前的使用方式：到期日與課程檢視分別檢查顯示、跳脫與操作權限，保留未選取課程列表的延遲載入。已保存的統計選擇以 Cookie 與頁面設定確認，不依賴已移除的文字元件。

安全套件版本在 `requirements-security-constraints.txt` 鎖定，建置工具在 `requirements-build.txt` 鎖定；更新時一併檢查 `requirements.txt` 的版本範圍，重跑完整測試與稽核。不要以略過測試或忽略漏洞讓檢查變綠。

Security validation 在依賴安裝成功且工作流程未取消時，即使測試失敗也會執行稽核，讓兩種問題同時可見；任一失敗仍會讓整個工作流程失敗。修正後的新提交會產生新檢查，歷史失敗紀錄不會被改寫。
