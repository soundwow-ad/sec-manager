# 從歷史 commit 移除秘密後再 push

GitHub 擋的是「commit 854ed24 裡有金鑰」，需要改寫那個 commit 的內容再 push。

## 步驟（在專案目錄用終端機執行）

### 1. 先提交「安全範本」檔（供 rebase 時覆蓋用）
```powershell
git add .streamlit/secrets_for_streamlit_cloud.SAFE.txt
git commit -m "Add safe template for Streamlit Cloud secrets"
```

### 2. 開始互動式 rebase（改寫 854ed24）
```powershell
git rebase -i 854ed24^
```
- 會開編輯器，看到一行：`pick 854ed24 db`
- 把 **第一個** 的 `pick` 改成 **`edit`**，存檔並關閉編輯器。

### 3. Rebase 停在那個 commit 時，用安全內容覆蓋檔案
```powershell
git show ORIG_HEAD:.streamlit/secrets_for_streamlit_cloud.SAFE.txt | Set-Content -Path .streamlit/secrets_for_streamlit_cloud.txt -Encoding UTF8
```

### 4. 修正該 commit 並繼續 rebase
```powershell
git add .streamlit/secrets_for_streamlit_cloud.txt
git commit --amend --no-edit
git rebase --continue
```
- 若出現編輯器，直接存檔關閉即可。

### 5. 若遠端有新的 commit，先拉再推
```powershell
git pull origin main --rebase
git push origin main
```
- 若遠端沒有新 commit，`git pull` 會說 already up to date，再 `git push origin main` 即可。

---

若某一步報錯，把終端機的完整錯誤貼給我就好。
