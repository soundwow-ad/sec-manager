"""
Excel 檔次稽核核心模組 (Audit V29)
將 Ragic 資料與 Excel CUE 表進行比對，驗證檔次正確性
"""

import pandas as pd
import requests
import os
import warnings
from itertools import combinations
from collections import Counter
import re
import math
import numpy as np
from pathlib import Path
import glob

warnings.filterwarnings("ignore")

# ================= 工具層 =================

SECONDS_BLACKLIST = {5, 10, 15, 20, 30, 40, 60}
YEAR_BLACKLIST = {114, 115, 116, 2025, 2026}

def safe_int(v, target=None):
    """安全地將值轉換為整數，並過濾黑名單值"""
    try:
        f = float(v)
        if abs(f - round(f)) > 1e-3: 
            return None
        f = int(round(f))
        
        if target and f != target:
            if f in SECONDS_BLACKLIST: 
                return None
            if f in YEAR_BLACKLIST: 
                return None
            
        if 0 < f <= 50000:
            return f
    except:
        return None
    return None

def is_noise_row(text):
    """判斷是否為噪音行（包含金額、日期等無關資訊）"""
    noise = ['元','$','含稅','未稅','VAT','COST','PRICE','報價','金額','製作費','費用','日期','結案','發票']
    return any(x in text for x in noise)

def is_store_count_row(text, nums):
    """判斷是否為店數行"""
    keywords = ['門市', '店數', '間門市', '約', '覆蓋', '店家', '家數']
    if any(k in text for k in keywords):
        if len(nums) <= 2 and max(nums) > 100:
            return True
    return False

def semantic_bonus(text):
    """語義加分（與業務相關的行給予加分）"""
    bonus = 0
    if any(x in text for x in ['全家','家樂福','區域','北','中','南','通路','RADIO','VISION','廣播','店舖']): 
        bonus += 3
    if any(x in text for x in ['每日','明細','LIST']): 
        bonus -= 2
    return bonus

def repetition_score(nums):
    """計算數值重複度分數"""
    if not nums: 
        return 0
    c = Counter(nums)
    most_common_count = c.most_common(1)[0][1]
    return most_common_count / len(nums)

# ================= 核心邏輯層 =================

def extract_row_signatures(df, sheet_name, target):
    """從 DataFrame 中提取行簽名（Row Signatures）"""
    rows = []
    for idx in range(len(df)):
        row = df.iloc[idx]
        nums = [safe_int(v, target) for v in row if safe_int(v, target)]
        if len(nums) < 1: 
            continue 

        text = row.astype(str).str.cat(sep=' ').upper()
        if is_noise_row(text): 
            continue
        if is_store_count_row(text, nums): 
            continue 
        
        if len(nums) > 2:
            big_nums = [n for n in nums if n > 1000]
            small_nums = [n for n in nums if n <= 200]
            if big_nums and small_nums:
                if target not in big_nums:
                    nums = small_nums

        if len(nums) >= 8 and repetition_score(nums) < 0.3 and target not in nums: 
            continue

        unit_val = None
        clean_sum = None
        if len(nums) >= 2:
            c = Counter(nums)
            most_common, count = c.most_common(1)[0]
            if count >= 3 or count / len(nums) > 0.3:
                if target and most_common in SECONDS_BLACKLIST and most_common != target: 
                    pass
                elif target and most_common in YEAR_BLACKLIST and most_common != target: 
                    pass
                else: 
                    unit_val = most_common
                    clean_sum = unit_val * count

        level = "L3"
        effective_sum = None
        if len(nums) == 1:
            level = "L1"
        else:
            max_n = max(nums)
            if max_n >= sum(nums) * 0.4:
                level = "L2"
                effective_sum = max_n

        rows.append({
            "sheet": sheet_name, "row_idx": idx, 
            "sum": sum(nums), "clean_sum": clean_sum, "effective_sum": effective_sum,
            "nums": nums, "unit_val": unit_val,
            "count": len(nums), "text": text, "bonus": semantic_bonus(text),
            "level": level
        })
    return rows

def solve_target_v29(rows, target):
    """使用 V29 演算法求解目標值（保留所有核心邏輯）"""
    best = None
    multipliers = list(range(1, 32))
    
    valid_rows = sorted(rows, key=lambda x: min(
        abs(x['sum'] - target), 
        abs((x['clean_sum'] or 99999) - target), 
        abs((x['effective_sum'] or 99999) - target)
    ))[:100]
    
    # 1. Row Sum Solver
    for k in [1, 2, 3]:
        for combo in combinations(valid_rows, k):
            levels = {r["level"] for r in combo}
            if ("L1" in levels or "L2" in levels) and "L3" in levels: 
                continue
            
            value_types = ['sum']
            if any(r['clean_sum'] for r in combo): 
                value_types.append('clean_sum')
            if any(r['effective_sum'] for r in combo): 
                value_types.append('effective_sum')
            
            for v_type in value_types:
                current_raw_sum = 0
                for r in combo:
                    val = r.get(v_type)
                    if val is None: 
                        val = r['sum']
                    current_raw_sum += val
                
                combo_bonus = sum(r["bonus"] for r in combo)
                
                for m in multipliers:
                    if k > 1 and m > 7: 
                        continue 
                    
                    final_val = current_raw_sum * m
                    diff = abs(final_val - target)
                    
                    score = diff * 100 - combo_bonus 
                    if m > 1: 
                        score += 50 
                    
                    if any(r['level'] == 'L2' for r in combo) and v_type == 'sum':
                        score += 1000

                    is_multiple = False
                    if target > 0 and final_val > target:
                        ratio = final_val / target
                        if ratio <= 6 and abs(ratio - round(ratio)) < 0.05:
                            score -= 2000
                            is_multiple = True
                    
                    if diff == 0 and m == 1:
                        score -= 5000 

                    if best is None or score < best["score"]:
                        best = {
                            "type": "RowSolver", "rows": combo, "mult": m, 
                            "sum": final_val, "diff": diff, "score": score, 
                            "val_type": v_type, "is_multiple": is_multiple
                        }
                    if diff == 0 and m == 1 and not is_multiple: 
                        return best

    # 2. Multi-Unit
    rows_by_sheet = {}
    for r in valid_rows:
        if r['unit_val']:
            if r['sheet'] not in rows_by_sheet: 
                rows_by_sheet[r['sheet']] = []
            rows_by_sheet[r['sheet']].append(r)
    
    for sheet, unit_rows in rows_by_sheet.items():
        if len(unit_rows) < 2: 
            continue
        for k in [2, 3]:
            for combo in combinations(unit_rows, k):
                unit_sum = sum(r['unit_val'] for r in combo)
                combo_bonus = sum(r["bonus"] for r in combo)
                for m in multipliers:
                    if m > 35: 
                        continue
                    final_val = unit_sum * m
                    diff = abs(final_val - target)
                    score = diff * 100 - combo_bonus + 20
                    if diff == 0 and m == 1: 
                        score -= 5000
                    
                    if best is None or score < best["score"]:
                        best = {
                            "type": "MultiUnit", "rows": combo, "mult": m, 
                            "sum": final_val, "diff": diff, "score": score, 
                            "is_multiple": False
                        }
                    if diff == 0 and m == 1: 
                        return best

    # 3. Unit Val
    for r in valid_rows:
        if r['unit_val']:
            for m in multipliers:
                if m > 35: 
                    continue
                final_val = r['unit_val'] * m
                diff = abs(final_val - target)
                score = diff * 100 - r['bonus'] + 20
                if diff == 0 and m == 1: 
                    score -= 5000

                if best is None or score < best["score"]:
                    best = {
                        "type": "UnitVal", "rows": (r,), "mult": m, 
                        "sum": final_val, "diff": diff, "score": score, 
                        "is_multiple": False
                    }
                if diff == 0 and m == 1: 
                    return best

    return best

def solve_by_block_fallback(df, target):
    """區塊求解備援方案（掃描整個工作表）"""
    all_nums = []
    for r in range(len(df)):
        for c in range(len(df.columns)):
            val = safe_int(df.iloc[r, c], target)
            if val: 
                all_nums.append(val)
    block_sum = sum(all_nums)
    diff = abs(block_sum - target)
    if diff <= 5 or (target > 0 and diff/target < 0.05):
        return {
            "type": "BlockSolver", "sum": block_sum, "diff": diff, 
            "score": diff * 200, "desc": "Full Sheet Scan", "is_multiple": False
        }
    return None

# ================= 主流程 =================

def get_ragic_api_key():
    """取得 Ragic API Key（從環境變數或 Streamlit secrets）"""
    # 優先從環境變數讀取
    api_key = os.getenv('RAGIC_API_KEY')
    if api_key:
        return api_key
    
    # 其次從 Streamlit secrets 讀取（如果可用）
    try:
        import streamlit as st
        api_key = st.secrets.get('ragic', {}).get('api_key', '')
        if api_key:
            return api_key
    except:
        pass
    
    return None

def fetch_ragic_data_for_audit(api_key=None, api_url=None):
    """從 Ragic API 取得資料（用於稽核）"""
    if not api_key:
        api_key = get_ragic_api_key()
    
    if not api_key:
        raise ValueError("無法取得 Ragic API Key，請設定環境變數或 Streamlit Secrets")
    
    if not api_url:
        api_url = "https://ap13.ragic.com/soundwow/forms12/19/"
    
    headers = {'Authorization': f'Basic {api_key}'}
    params = {'api': '', 'naming': 'EID'}
    
    response = requests.get(api_url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def load_excel_files(download_dir):
    """載入指定資料夾中的所有 Excel 檔案"""
    excel_files = []
    download_path = Path(download_dir)
    
    if not download_path.exists():
        raise FileNotFoundError(f"資料夾不存在: {download_dir}")
    
    # 尋找所有 Excel 檔案
    patterns = ['*.xlsx', '*.xls']
    for pattern in patterns:
        excel_files.extend(download_path.glob(pattern))
        excel_files.extend(download_path.rglob(pattern))  # 遞迴搜尋
    
    if not excel_files:
        raise FileNotFoundError(f"在 {download_dir} 中找不到 Excel 檔案")
    
    return excel_files

def process_excel_file(excel_path, ragic_data, field_map):
    """處理單一 Excel 檔案，與 Ragic 資料比對"""
    results = []
    log_lines = []
    
    try:
        excel_file = pd.ExcelFile(excel_path)
        log_lines.append(f"\n處理檔案: {excel_path.name}")
        
        # 從檔案名稱或路徑推測訂單ID（這裡需要根據實際檔案命名規則調整）
        # 假設檔案名稱包含訂單ID
        file_name = excel_path.stem
        order_id_match = None
        
        # 嘗試從 Ragic 資料中找到對應的訂單
        for rid, record in ragic_data.items():
            order_id = record.get(str(field_map.get('order_id', 1015385)), '')
            if str(order_id) in file_name or file_name in str(order_id):
                order_id_match = rid
                break
        
        if not order_id_match:
            # 如果找不到對應訂單，嘗試所有訂單
            log_lines.append(f"  警告: 無法從檔案名稱找到對應訂單，將嘗試所有訂單")
        
        # 取得目標秒數和檔次
        target_seconds = None
        target_spots = None
        order_info = {}
        
        if order_id_match:
            record = ragic_data[order_id_match]
            target_seconds = safe_int(record.get(str(field_map.get('seconds', 1015412)), 0))
            target_spots = safe_int(record.get(str(field_map.get('spots', 1015411)), 0))
            order_info = {
                'order_id': record.get(str(field_map.get('order_id', 1015385)), ''),
                'platform': record.get(str(field_map.get('platform', 1015390)), ''),
                'client': record.get(str(field_map.get('client', 1015425)), ''),
                'product': record.get(str(field_map.get('product', 1015426)), ''),
            }
        else:
            # 如果找不到對應訂單，嘗試從所有訂單中比對
            for rid, record in ragic_data.items():
                target_seconds = safe_int(record.get(str(field_map.get('seconds', 1015412)), 0))
                target_spots = safe_int(record.get(str(field_map.get('spots', 1015411)), 0))
                if target_seconds and target_spots:
                    order_info = {
                        'order_id': record.get(str(field_map.get('order_id', 1015385)), ''),
                        'platform': record.get(str(field_map.get('platform', 1015390)), ''),
                        'client': record.get(str(field_map.get('client', 1015425)), ''),
                        'product': record.get(str(field_map.get('product', 1015426)), ''),
                    }
                    break
        
        if not target_seconds or not target_spots:
            log_lines.append(f"  錯誤: 無法取得目標秒數或檔次")
            results.append({
                '檔案名稱': excel_path.name,
                '訂單ID': order_info.get('order_id', '未知'),
                '客戶名稱': order_info.get('client', '未知'),
                '素材': order_info.get('product', '未知'),
                '平台': order_info.get('platform', '未知'),
                '目標秒數': target_seconds or 0,
                '目標檔次': target_spots or 0,
                '實際檔次': None,
                '差異': None,
                '狀態': '錯誤：無法取得目標值',
                '求解方法': None,
                '風險等級': '高'
            })
            return results, log_lines
        
        target_total = target_seconds * target_spots
        log_lines.append(f"  目標: 秒數={target_seconds}, 檔次={target_spots}, 總計={target_total}")
        
        # 處理每個工作表
        all_rows = []
        for sheet_name in excel_file.sheet_names:
            try:
                df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
                rows = extract_row_signatures(df, sheet_name, target_total)
                all_rows.extend(rows)
                log_lines.append(f"  工作表 '{sheet_name}': 找到 {len(rows)} 個有效行")
            except Exception as e:
                log_lines.append(f"  工作表 '{sheet_name}' 處理失敗: {str(e)}")
        
        # 求解
        solution = solve_target_v29(all_rows, target_total)
        
        if not solution or solution['diff'] > 5:
            # 嘗試備援方案
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
                    fallback = solve_by_block_fallback(df, target_total)
                    if fallback and (not solution or fallback['score'] < solution['score']):
                        solution = fallback
                        solution['sheet'] = sheet_name
                except:
                    continue
        
        # 計算實際檔次
        if solution and solution['diff'] <= 5:
            actual_total = solution['sum']
            actual_spots = round(actual_total / target_seconds) if target_seconds > 0 else 0
            diff = actual_spots - target_spots
            status = '成功' if diff == 0 else '差異'
            risk = '低' if diff == 0 else ('中' if abs(diff) <= 2 else '高')
            
            log_lines.append(f"  求解成功: 方法={solution['type']}, 總計={actual_total}, 檔次={actual_spots}, 差異={diff}")
        else:
            actual_spots = None
            diff = None
            status = '失敗'
            risk = '高'
            log_lines.append(f"  求解失敗: 無法找到符合的檔次")
        
        results.append({
            '檔案名稱': excel_path.name,
            '訂單ID': order_info.get('order_id', '未知'),
            '客戶名稱': order_info.get('client', '未知'),
            '素材': order_info.get('product', '未知'),
            '平台': order_info.get('platform', '未知'),
            '目標秒數': target_seconds,
            '目標檔次': target_spots,
            '實際檔次': actual_spots,
            '差異': diff,
            '狀態': status,
            '求解方法': solution['type'] if solution else None,
            '風險等級': risk
        })
        
    except Exception as e:
        log_lines.append(f"  處理檔案時發生錯誤: {str(e)}")
        results.append({
            '檔案名稱': excel_path.name,
            '訂單ID': '錯誤',
            '客戶名稱': '錯誤',
            '素材': '錯誤',
            '平台': '錯誤',
            '目標秒數': 0,
            '目標檔次': 0,
            '實際檔次': None,
            '差異': None,
            '狀態': f'錯誤: {str(e)}',
            '求解方法': None,
            '風險等級': '高'
        })
    
    return results, log_lines

def run_audit(download_dir, ragic_data=None, api_key=None, api_url=None, field_map=None):
    """
    執行檔次稽核
    
    參數:
        download_dir: Excel 檔案所在資料夾路徑
        ragic_data: Ragic 資料（字典格式），如果為 None 則從 API 取得
        api_key: Ragic API Key（可選）
        api_url: Ragic API URL（可選）
        field_map: 欄位對照表（可選）
    
    返回:
        (results_df, log_text): (結果 DataFrame, 詳細 Log 文字)
    """
    # 預設欄位對照表
    if field_map is None:
        field_map = {
            'order_id': 1015385,
            'platform': 1015390,
            'spots': 1015411,
            'seconds': 1015412,
            'client': 1015425,
            'product': 1015426,
        }
    
    # 取得 Ragic 資料
    if ragic_data is None:
        ragic_data = fetch_ragic_data_for_audit(api_key, api_url)
    
    # 載入 Excel 檔案
    excel_files = load_excel_files(download_dir)
    
    # 處理所有檔案
    all_results = []
    all_logs = []
    
    for excel_file in excel_files:
        results, logs = process_excel_file(excel_file, ragic_data, field_map)
        all_results.extend(results)
        all_logs.extend(logs)
    
    # 轉換為 DataFrame
    results_df = pd.DataFrame(all_results)
    
    # 產生 Log 文字
    log_text = "\n".join(all_logs)
    
    return results_df, log_text
