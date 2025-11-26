import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns 
import matplotlib as mpl
import numpy as np

# ----------------- ⚠️ 폰트 및 스타일 설정 ⚠️ -----------------
# 챠트 한글 깨짐 방지 설정 및 스타일
plt.rcParams['font.family'] = 'Malgun Gothic'
mpl.rcParams['axes.unicode_minus'] = False 

# ----------------- ⚠️ 상수 정의 ⚠️ -----------------
DB_FILE_NAME = r'./product_quality_db_final_stable-74.db' 
DATE_COLUMN_MAP = {'fw': 'FwStamp', 'rftx': 'RfTxStamp', 'batadc': 'BatadcStamp', 'semi': 'SemiAssyStartTime', 'pcb': 'PcbStartTime'}
ITEM_OPTIONS = ('pcb', 'semi', 'fw', 'rftx', 'batadc')
DB_TABLES = ['T_MASTER_DATA', 'T_ITEM_PCB', 'T_ITEM_SEMI', 'T_ITEM_FW', 'T_ITEM_RFTX', 'T_ITEM_BATADC', 'T_PC_INFO', 'T_SPEC_PCB', 'T_SPEC_SEMI']

# PC 컬럼명 (스키마 확인 결과)
PC_COLUMN_NAME = 'PC_ID'

# ==========================================================
# DB 저장 관련 헬퍼 함수들
# ==========================================================

def calculate_week_number(date_str):
    """'YYYY-MM-DD HH:MM:SS' 형식에서 ISO 주차(YYYY-W##)를 계산합니다."""
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        return f"{dt.year}-W{dt.isocalendar()[1]:02d}"
    except:
        return None

def transform_datetime_columns(df_source, columns_to_transform):
    """Epoch 또는 YYYYMMDDhhmmss.f 형태의 숫자 컬럼을 문자열 날짜로 변환합니다."""
    for col in columns_to_transform:
        if col in df_source.columns:
            
            # ✅ FutureWarning 해결: 명시적으로 컬럼을 object 타입으로 변환 후 문자열 할당
            if df_source[col].dtype in ['float64', 'int64']:
                df_source[col] = df_source[col].astype('object')
                mask = df_source[col].notna()
                df_source.loc[mask, col] = df_source.loc[mask, col].astype(str)

            dt_str_series = df_source[col].astype(str).str.split(r'\.', expand=True)[0]
            is_potential_datetime = (dt_str_series.str.len() >= 14) & (dt_str_series.str.startswith('20'))
            
            try:
                dt_series_format = pd.to_datetime(dt_str_series.where(is_potential_datetime), format='%Y%m%d%H%M%S', errors='coerce')
                mask_success = dt_series_format.notna()
                if mask_success.any():
                    formatted_dates = dt_series_format[mask_success].dt.strftime('%Y-%m-%d %H:%M:%S')
                    df_source.loc[mask_success, col] = formatted_dates.values
            except ValueError: pass
            
            if df_source[col].dtype != 'object':
                df_source[col] = df_source[col].astype(str)
            
            is_numeric_epoch = df_source[col].str.contains(r'^\d+\.\d+$', na=False).fillna(False)
            
            try:
                if is_numeric_epoch.any():
                    dt_series_epoch = pd.to_datetime(df_source.loc[is_numeric_epoch, col].astype(float), unit='ms', origin='unix', errors='coerce', utc=True).dt.tz_convert('Asia/Seoul')
                    str_series_epoch = dt_series_epoch.dt.strftime('%Y-%m-%d %H:%M:%S')
                    mask_success_epoch = str_series_epoch.notna()
                    if mask_success_epoch.any():
                        df_source.loc[is_numeric_epoch, col] = str_series_epoch.values
            except: pass
            
            df_source[col] = df_source[col].astype(str).replace('nan', None, regex=True)
            df_source.loc[df_source[col] == '<NA>', col] = None
                
    return df_source

def create_initial_db_schema(cursor, conn):
    """DB 초기 스키마를 생성합니다."""
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # 1. T_MASTER_DATA
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_MASTER_DATA (
            SNumber TEXT PRIMARY KEY,
            ICount INTEGER,
            Stamp TEXT,
            FwPass TEXT,          
            BatPass TEXT,         
            RfTxPass TEXT,        
            PcbPass TEXT,         
            SemiAssyPass TEXT,    
            BatadcPass TEXT,      
            WEEK_NO TEXT 
        );
    """)
    
    # ✅ 기존 테이블에 WEEK_NO 컬럼이 없으면 추가
    try:
        cursor.execute("SELECT WEEK_NO FROM T_MASTER_DATA LIMIT 1")
    except:
        try:
            cursor.execute("ALTER TABLE T_MASTER_DATA ADD COLUMN WEEK_NO TEXT")
            conn.commit()
            print("✅ T_MASTER_DATA에 WEEK_NO 컬럼을 추가했습니다.")
        except Exception as e:
            print(f"⚠️ WEEK_NO 컬럼 추가 실패 (이미 존재할 수 있음): {e}")

    # 2. T_PC_INFO
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_PC_INFO (
            PC_ID TEXT PRIMARY KEY,
            PC_Type TEXT
        );
    """)
    
    # 3. T_SPEC_PCB
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_SPEC_PCB (
            Spec_ID INTEGER PRIMARY KEY,
            Measure_Item TEXT,
            Min_Value REAL,
            Max_Value REAL,
            Start_Date TEXT,
            Spec_Key TEXT UNIQUE
        );
    """)
    
    # 4. T_SPEC_SEMI
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_SPEC_SEMI (
            Spec_ID INTEGER PRIMARY KEY,
            Measure_Item TEXT,
            Min_Value REAL,
            Max_Value REAL,
            Start_Date TEXT,
            Spec_Key TEXT UNIQUE
        );
    """)
    
    # 5. T_ITEM_PCB
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_ITEM_PCB (
            SNumber TEXT,
            PcbStartTime TEXT,
            PcbStopTime TEXT,
            PcbPass TEXT,
            PcbSleepCurr REAL,
            PcbBatVolt REAL,
            PcbIrCurr REAL,
            PcbIrPwr REAL,
            PcbWirelessVolt REAL,
            PcbUsbCurr REAL,
            PcbWirelessUsbVolt REAL,
            PcbLed REAL,
            SleepCurr_Spec_ID INTEGER,
            pcbPC TEXT,
            PcbMaxIrPwr REAL,
            PRIMARY KEY (SNumber, PcbStartTime),
            FOREIGN KEY (SNumber) REFERENCES T_MASTER_DATA (SNumber)
        );
    """)

    # 6. T_ITEM_SEMI
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_ITEM_SEMI (
            SNumber TEXT, 
            SemiAssyStartTime TEXT, 
            SemiAssyStopTime TEXT, 
            SemiAssyPass TEXT, 
            SemiAssyBatVolt REAL, 
            SemiAssySolarVolt REAL, 
            BatVolt_Spec_ID INTEGER,
            semiPC TEXT,
            SemiAssyMaxBatVolt REAL,
            PRIMARY KEY (SNumber, SemiAssyStartTime),
            FOREIGN KEY (SNumber) REFERENCES T_MASTER_DATA (SNumber)
        );
    """)
    
    # 7. T_ITEM_FW (Pass 컬럼 추가)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_ITEM_FW (
            SNumber TEXT, FwStamp TEXT, FwPC TEXT, FwWrMAC TEXT, FwFile TEXT, FwPass TEXT,
            PRIMARY KEY (SNumber, FwStamp),
            FOREIGN KEY (SNumber) REFERENCES T_MASTER_DATA (SNumber)
        );
    """)
    
    # ✅ 기존 테이블에 FwPass 컬럼 추가 (없으면)
    try:
        cursor.execute("SELECT FwPass FROM T_ITEM_FW LIMIT 1")
    except:
        try:
            cursor.execute("ALTER TABLE T_ITEM_FW ADD COLUMN FwPass TEXT")
            conn.commit()
            print("✅ T_ITEM_FW에 FwPass 컬럼 추가")
        except Exception as e:
            print(f"⚠️ FwPass 컬럼 추가 실패: {e}")
    
    # 8. T_ITEM_RFTX (Pass 컬럼 추가)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_ITEM_RFTX (
            SNumber TEXT, RfTxStamp TEXT, RfTxPC TEXT, RfTxPower REAL, RfTxModul REAL, RfTxCFOD REAL, RfTxPass TEXT,
            PRIMARY KEY (SNumber, RfTxStamp),
            FOREIGN KEY (SNumber) REFERENCES T_MASTER_DATA (SNumber)
        );
    """)
    
    # ✅ 기존 테이블에 RfTxPass 컬럼 추가 (없으면)
    try:
        cursor.execute("SELECT RfTxPass FROM T_ITEM_RFTX LIMIT 1")
    except:
        try:
            cursor.execute("ALTER TABLE T_ITEM_RFTX ADD COLUMN RfTxPass TEXT")
            conn.commit()
            print("✅ T_ITEM_RFTX에 RfTxPass 컬럼 추가")
        except Exception as e:
            print(f"⚠️ RfTxPass 컬럼 추가 실패: {e}")
    
    # 9. T_ITEM_BATADC (Pass 컬럼 추가)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_ITEM_BATADC (
            SNumber TEXT, BatadcStamp TEXT, BatadcPC TEXT, BatadcBtVer TEXT, BatadcLevel REAL, 
            BatadcVoiceTh REAL, BatadcVoiceLvl REAL, BatadcRssiRx REAL, BatadcRssiTx REAL, 
            BatadcOffRaw1 REAL, BatadcOnBase REAL, BatadcOnDiff REAL, BatadcSar TEXT, BatadcPass TEXT,
            PRIMARY KEY (SNumber, BatadcStamp),
            FOREIGN KEY (SNumber) REFERENCES T_MASTER_DATA (SNumber)
        );
    """)
    
    # ✅ 기존 테이블에 BatadcPass 컬럼 추가 (없으면)
    try:
        cursor.execute("SELECT BatadcPass FROM T_ITEM_BATADC LIMIT 1")
    except:
        try:
            cursor.execute("ALTER TABLE T_ITEM_BATADC ADD COLUMN BatadcPass TEXT")
            conn.commit()
            print("✅ T_ITEM_BATADC에 BatadcPass 컬럼 추가")
        except Exception as e:
            print(f"⚠️ BatadcPass 컬럼 추가 실패: {e}")
    
    conn.commit()

def create_or_update_pc_info_streamlit(df_source, conn):
    """T_PC_INFO 테이블을 생성 또는 업데이트합니다 (APPEND 모드)"""
    PC_FIELD_MAP = {
        'FwPC': 'FW', 'RfTxPC': 'RFTX', 'BatadcPC': 'BATADC'
    }

    pc_data = []

    # pcbPC: PcbMaxIrPwr 값(100, 101, 102, 103)을 그대로 PC_ID로 사용
    if 'PcbMaxIrPwr' in df_source.columns:
        df_pcb_values = df_source[['PcbMaxIrPwr']].dropna().drop_duplicates()
        for value in df_pcb_values['PcbMaxIrPwr']:
            if isinstance(value, (int, float, np.float64)):
                pc_id = str(int(value))
                pc_data.append({'PC_ID': pc_id, 'PC_Type': 'PCB'})

    # 기존 PC 코드 로직 (FwPC, RfTxPC, BatadcPC)
    for col_name, item_prefix in PC_FIELD_MAP.items():
        if col_name in df_source.columns:
            unique_values = df_source[col_name].dropna().unique()
            for value in unique_values:
                pc_id = str(value)
                pc_type = item_prefix
                pc_data.append({'PC_ID': pc_id, 'PC_Type': pc_type})

    if not pc_data:
        return {'count': 0, 'message': "⚠️ T_PC_INFO: 추가할 PC 데이터가 없습니다."}

    df_pc_info = pd.DataFrame(pc_data).drop_duplicates(subset=['PC_ID'])
    
    # 기존 PC_INFO 조회
    try:
        df_existing_pc = pd.read_sql("SELECT PC_ID FROM T_PC_INFO", conn)
        if len(df_existing_pc) > 0:
            df_new_pc = df_pc_info[~df_pc_info['PC_ID'].isin(df_existing_pc['PC_ID'])]
            if len(df_new_pc) == 0:
                return {'count': 0, 'message': "ℹ️ T_PC_INFO: 추가할 신규 PC 데이터가 없습니다."}
            df_pc_info = df_new_pc
    except:
        pass
    
    df_pc_info.to_sql('T_PC_INFO', conn, if_exists='append', index=False)
    return {'count': len(df_pc_info), 'message': f"✅ T_PC_INFO: {len(df_pc_info)}행 추가"}

def extract_and_save_spec_streamlit(df_source, prefix, items_map, spec_table_name, conn):
    """Min/Max 추출 및 Spec 테이블 저장 (REPLACE 모드)"""
    all_spec_data = []
    
    for item_name, (min_col_suffix, max_col_suffix) in items_map.items():
        min_col = f'{prefix}{min_col_suffix}{item_name}' 
        max_col = f'{prefix}{max_col_suffix}{item_name}'
        
        if min_col not in df_source.columns or max_col not in df_source.columns:
            continue
            
        df_temp = df_source.dropna(subset=[min_col, max_col]).copy()
        if len(df_temp) == 0:
            continue
            
        unique_specs = df_temp[[min_col, max_col]].drop_duplicates()
        unique_specs['Spec_Key'] = unique_specs.apply(
            lambda row: f"{item_name}_{row[min_col]}_{row[max_col]}", axis=1
        )

        for index, row in unique_specs.iterrows():
            all_spec_data.append({
                'Measure_Item': item_name, 
                'Min_Value': row[min_col], 
                'Max_Value': row[max_col], 
                'Spec_Key': row['Spec_Key'], 
                'Start_Date': '2025-01-01'
            })
            
    if not all_spec_data:
        return {'count': 0, 'message': f"⚠️ {spec_table_name}: 저장할 고유 Min/Max 조합을 찾을 수 없습니다."}

    df_spec = pd.DataFrame(all_spec_data).drop_duplicates(subset=['Spec_Key'])
    df_spec['Spec_ID'] = range(1, len(df_spec) + 1)
    df_spec = df_spec[['Spec_ID', 'Measure_Item', 'Min_Value', 'Max_Value', 'Start_Date', 'Spec_Key']]
    
    df_spec.to_sql(spec_table_name, conn, if_exists='replace', index=False)
    return {'count': len(df_spec), 'message': f"✅ {spec_table_name}: {len(df_spec)}행 저장 (REPLACE)"}

def process_and_save_csv_to_db(df_original, db_file_name):
    """CSV 데이터를 처리하여 DB에 저장합니다. (APPEND 모드)"""
    log_messages = []
    stats = {}
    
    try:
        # 1. DB 연결 및 스키마 생성
        conn = sqlite3.connect(db_file_name)
        cursor = conn.cursor()
        
        # 스키마가 없으면 생성
        create_initial_db_schema(cursor, conn)
        
        cursor.execute("PRAGMA foreign_keys = ON;")
        log_messages.append("✅ DB 연결 및 스키마 확인 성공")
        
        # 2. 날짜 컬럼 변환
        DATE_COLUMNS_TO_CONVERT = [
            'Stamp', 'SemiAssyStartTime', 'SemiAssyStopTime', 'PcbStartTime', 'PcbStopTime', 
            'FwStamp', 'BatStamp', 'RfTxStamp', 'BatadcStamp'
        ]
        df_original = transform_datetime_columns(df_original, DATE_COLUMNS_TO_CONVERT)
        log_messages.append("✅ 날짜 컬럼 변환 완료")
        
        # 3. WEEK_NO 계산
        df_original['WEEK_NO'] = df_original['Stamp'].apply(calculate_week_number)
        log_messages.append("✅ WEEK_NO 계산 완료")
        
        # 4. pcbPC 컬럼 생성
        if 'PcbMaxIrPwr' in df_original.columns:
            df_original['pcbPC'] = df_original['PcbMaxIrPwr'].apply(
                lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float, np.float64)) else None
            )
            log_messages.append("✅ pcbPC 컬럼 생성 완료")
        else:
            df_original['pcbPC'] = None
            log_messages.append("⚠️ PcbMaxIrPwr 컬럼 없음 - pcbPC는 NULL")
        
        # 5. semiPC는 NULL
        df_original['semiPC'] = None
        
        # ✅ 6. T_MASTER_DATA 저장 (Pass 제거 - SNumber, ICount, Stamp, WEEK_NO만)
        # MASTER_COLUMNS = ['SNumber', 'ICount', 'Stamp', 'WEEK_NO']
        MASTER_COLUMNS = ['SNumber', 'ICount', 'Stamp', 'FwPass', 'BatPass', 'RfTxPass', 'PcbPass', 'SemiAssyPass', 'BatadcPass', 'WEEK_NO']

        df_master = df_original.dropna(subset=['SNumber']).copy()
        df_master = df_master[MASTER_COLUMNS].drop_duplicates(subset=['SNumber'])
        
        # 기존 SNumber 확인 후 신규만 추가
        try:
            existing_snumbers = pd.read_sql("SELECT SNumber FROM T_MASTER_DATA", conn)
        except:
            existing_snumbers = pd.DataFrame(columns=['SNumber'])
        
        df_master_new = df_master[~df_master['SNumber'].isin(existing_snumbers['SNumber'])]
        
        if len(df_master_new) > 0:
            df_master_new.to_sql('T_MASTER_DATA', conn, if_exists='append', index=False)
            stats['master'] = len(df_master_new)
            log_messages.append(f"✅ T_MASTER_DATA: {len(df_master_new)}행 추가")
        else:
            stats['master'] = 0
            log_messages.append("ℹ️ T_MASTER_DATA: 신규 데이터 없음")
        
        # ✅ 7. T_ITEM_PCB 저장 (PcbPass 포함)
        PCB_VALUE_COLUMNS = ['SNumber', 'PcbStartTime', 'PcbStopTime', 'PcbPass', 'PcbSleepCurr', 'PcbBatVolt', 'PcbIrCurr', 'PcbIrPwr', 'PcbWirelessVolt', 'PcbUsbCurr', 'PcbWirelessUsbVolt', 'PcbLed', 'pcbPC', 'PcbMaxIrPwr']
        
        df_pcb = df_original.dropna(subset=['SNumber', 'PcbStartTime']).copy()
        
        # ✅ PcbPass 컬럼 추가 (CSV에서 가져오기)
        if 'PcbPass' in df_pcb.columns:
            df_pcb['PcbPass'] = df_pcb['PcbPass'].astype(str).str.lower().replace({'nan': None, 'none': None})
        else:
            df_pcb['PcbPass'] = None
            log_messages.append("⚠️ CSV에 PcbPass 컬럼 없음 - NULL로 저장")
        
        if len(df_pcb) > 0:
            try:
                existing_pcb = pd.read_sql("SELECT SNumber, PcbStartTime FROM T_ITEM_PCB", conn)
            except:
                existing_pcb = pd.DataFrame(columns=['SNumber', 'PcbStartTime'])
            
            if len(existing_pcb) > 0:
                df_pcb = df_pcb.merge(existing_pcb, on=['SNumber', 'PcbStartTime'], how='left', indicator=True)
                df_pcb_new = df_pcb[df_pcb['_merge'] == 'left_only'].drop(columns=['_merge'])
            else:
                df_pcb_new = df_pcb
            
            df_pcb_new = df_pcb_new.drop_duplicates(subset=['SNumber', 'PcbStartTime'], keep='first')
            
            if len(df_pcb_new) > 0:
                df_pcb_new[PCB_VALUE_COLUMNS].to_sql('T_ITEM_PCB', conn, if_exists='append', index=False)
                stats['pcb'] = len(df_pcb_new)
                log_messages.append(f"✅ T_ITEM_PCB: {len(df_pcb_new)}행 추가")
            else:
                stats['pcb'] = 0
                log_messages.append("ℹ️ T_ITEM_PCB: 신규 데이터 없음")
        else:
            stats['pcb'] = 0
            log_messages.append("⚠️ T_ITEM_PCB: 데이터 없음")
        
        # ✅ 8. T_ITEM_SEMI 저장 (SemiAssyPass 포함)
        SEMI_VALUE_COLUMNS = ['SNumber', 'SemiAssyStartTime', 'SemiAssyStopTime', 'SemiAssyPass', 'SemiAssyBatVolt', 'SemiAssySolarVolt', 'semiPC']
        df_semi = df_original.dropna(subset=['SNumber', 'SemiAssyStartTime']).copy()
        
        # ✅ SemiAssyPass 컬럼 추가
        if 'SemiAssyPass' in df_semi.columns:
            df_semi['SemiAssyPass'] = df_semi['SemiAssyPass'].astype(str).str.lower().replace({'nan': None, 'none': None})
        else:
            df_semi['SemiAssyPass'] = None
            log_messages.append("⚠️ CSV에 SemiAssyPass 컬럼 없음 - NULL로 저장")
        
        if len(df_semi) > 0:
            try:
                existing_semi = pd.read_sql("SELECT SNumber, SemiAssyStartTime FROM T_ITEM_SEMI", conn)
            except:
                existing_semi = pd.DataFrame(columns=['SNumber', 'SemiAssyStartTime'])
            
            df_semi = df_semi.merge(existing_semi, on=['SNumber', 'SemiAssyStartTime'], how='left', indicator=True)
            df_semi_new = df_semi[df_semi['_merge'] == 'left_only'].drop(columns=['_merge'])
            
            if len(df_semi_new) > 0:
                df_semi_new[SEMI_VALUE_COLUMNS].to_sql('T_ITEM_SEMI', conn, if_exists='append', index=False)
                stats['semi'] = len(df_semi_new)
                log_messages.append(f"✅ T_ITEM_SEMI: {len(df_semi_new)}행 추가")
            else:
                stats['semi'] = 0
                log_messages.append("ℹ️ T_ITEM_SEMI: 신규 데이터 없음")
        else:
            stats['semi'] = 0
            log_messages.append("⚠️ T_ITEM_SEMI: 데이터 없음")
        
        # ✅ 9. T_ITEM_FW 저장 (FwPass 포함)
        FW_COLUMNS = ['SNumber', 'FwStamp', 'FwPC', 'FwWrMAC', 'FwFile', 'FwPass']
        df_fw = df_original.dropna(subset=['SNumber', 'FwStamp']).copy()
        
        if 'FwPass' in df_fw.columns:
            df_fw['FwPass'] = df_fw['FwPass'].astype(str).str.lower().replace({'nan': None, 'none': None})
        else:
            df_fw['FwPass'] = None
        
        if len(df_fw) > 0:
            try:
                existing_fw = pd.read_sql("SELECT SNumber, FwStamp FROM T_ITEM_FW", conn)
            except:
                existing_fw = pd.DataFrame(columns=['SNumber', 'FwStamp'])
            
            if len(existing_fw) > 0:
                df_fw = df_fw.merge(existing_fw, on=['SNumber', 'FwStamp'], how='left', indicator=True)
                df_fw_new = df_fw[df_fw['_merge'] == 'left_only'].drop(columns=['_merge'])
            else:
                df_fw_new = df_fw
            
            df_fw_new = df_fw_new.drop_duplicates(subset=['SNumber', 'FwStamp'], keep='first')
            
            if len(df_fw_new) > 0:
                df_fw_new[FW_COLUMNS].to_sql('T_ITEM_FW', conn, if_exists='append', index=False)
                stats['fw'] = len(df_fw_new)
                log_messages.append(f"✅ T_ITEM_FW: {len(df_fw_new)}행 추가")
            else:
                stats['fw'] = 0
                log_messages.append("ℹ️ T_ITEM_FW: 신규 데이터 없음")
        else:
            stats['fw'] = 0
            log_messages.append("⚠️ T_ITEM_FW: 데이터 없음")
        
        # ✅ 10. T_ITEM_RFTX 저장 (RfTxPass 포함)
        RFTX_COLUMNS = ['SNumber', 'RfTxStamp', 'RfTxPC', 'RfTxPower', 'RfTxModul', 'RfTxCFOD', 'RfTxPass']
        df_rftx = df_original.dropna(subset=['SNumber', 'RfTxStamp']).copy()
        
        if 'RfTxPass' in df_rftx.columns:
            df_rftx['RfTxPass'] = df_rftx['RfTxPass'].astype(str).str.lower().replace({'nan': None, 'none': None})
        else:
            df_rftx['RfTxPass'] = None
        
        if len(df_rftx) > 0:
            try:
                existing_rftx = pd.read_sql("SELECT SNumber, RfTxStamp FROM T_ITEM_RFTX", conn)
            except:
                existing_rftx = pd.DataFrame(columns=['SNumber', 'RfTxStamp'])
            
            if len(existing_rftx) > 0:
                df_rftx = df_rftx.merge(existing_rftx, on=['SNumber', 'RfTxStamp'], how='left', indicator=True)
                df_rftx_new = df_rftx[df_rftx['_merge'] == 'left_only'].drop(columns=['_merge'])
            else:
                df_rftx_new = df_rftx
            
            df_rftx_new = df_rftx_new.drop_duplicates(subset=['SNumber', 'RfTxStamp'], keep='first')
            
            if len(df_rftx_new) > 0:
                df_rftx_new[RFTX_COLUMNS].to_sql('T_ITEM_RFTX', conn, if_exists='append', index=False)
                stats['rftx'] = len(df_rftx_new)
                log_messages.append(f"✅ T_ITEM_RFTX: {len(df_rftx_new)}행 추가")
            else:
                stats['rftx'] = 0
                log_messages.append("ℹ️ T_ITEM_RFTX: 신규 데이터 없음")
        else:
            stats['rftx'] = 0
            log_messages.append("⚠️ T_ITEM_RFTX: 데이터 없음")
        
        # ✅ 11. T_ITEM_BATADC 저장 (BatadcPass 포함)
        BATADC_COLUMNS = ['SNumber', 'BatadcStamp', 'BatadcPC', 'BatadcBtVer', 'BatadcLevel', 'BatadcVoiceTh', 'BatadcVoiceLvl', 'BatadcRssiRx', 'BatadcRssiTx', 'BatadcOffRaw1', 'BatadcOnBase', 'BatadcOnDiff', 'BatadcSar', 'BatadcPass']
        df_batadc = df_original.dropna(subset=['SNumber', 'BatadcStamp']).copy()
        
        if 'BatadcPass' in df_batadc.columns:
            df_batadc['BatadcPass'] = df_batadc['BatadcPass'].astype(str).str.lower().replace({'nan': None, 'none': None})
        else:
            df_batadc['BatadcPass'] = None
        
        if len(df_batadc) > 0:
            try:
                existing_batadc = pd.read_sql("SELECT SNumber, BatadcStamp FROM T_ITEM_BATADC", conn)
            except:
                existing_batadc = pd.DataFrame(columns=['SNumber', 'BatadcStamp'])
            
            if len(existing_batadc) > 0:
                df_batadc = df_batadc.merge(existing_batadc, on=['SNumber', 'BatadcStamp'], how='left', indicator=True)
                df_batadc_new = df_batadc[df_batadc['_merge'] == 'left_only'].drop(columns=['_merge'])
            else:
                df_batadc_new = df_batadc
            
            df_batadc_new = df_batadc_new.drop_duplicates(subset=['SNumber', 'BatadcStamp'], keep='first')
            
            if len(df_batadc_new) > 0:
                df_batadc_new[BATADC_COLUMNS].to_sql('T_ITEM_BATADC', conn, if_exists='append', index=False)
                stats['batadc'] = len(df_batadc_new)
                log_messages.append(f"✅ T_ITEM_BATADC: {len(df_batadc_new)}행 추가")
            else:
                stats['batadc'] = 0
                log_messages.append("ℹ️ T_ITEM_BATADC: 신규 데이터 없음")
        else:
            stats['batadc'] = 0
            log_messages.append("⚠️ T_ITEM_BATADC: 데이터 없음")
        
        # 12. T_PC_INFO 저장
        log_messages.append("\n📋 T_PC_INFO 테이블 생성/업데이트 시작...")
        pc_info_result = create_or_update_pc_info_streamlit(df_original, conn)
        stats['pc_info'] = pc_info_result['count']
        log_messages.append(pc_info_result['message'])
        
        # 13. T_SPEC_PCB 저장
        PCB_ITEMS_MAP = {
            'SleepCurr': ('Min', 'Max'), 
            'BatVolt': ('Min', 'Max'), 
            'IrCurr': ('Min', 'Max'), 
            'IrPwr': ('Min', 'Max'), 
            'WirelessVolt': ('Min', 'Max'), 
            'UsbCurr': ('Min', 'Max'), 
            'WirelessUsbVolt': ('Min', 'Max'), 
            'Led': ('Min', 'Max')
        }
        spec_pcb_result = extract_and_save_spec_streamlit(df_original, 'Pcb', PCB_ITEMS_MAP, 'T_SPEC_PCB', conn)
        stats['spec_pcb'] = spec_pcb_result['count']
        log_messages.append(spec_pcb_result['message'])
        
        # 14. T_SPEC_SEMI 저장
        SEMI_ITEMS_MAP = {
            'BatVolt': ('Min', 'Max'), 
            'SolarVolt': ('Min', 'Max')
        }
        spec_semi_result = extract_and_save_spec_streamlit(df_original, 'SemiAssy', SEMI_ITEMS_MAP, 'T_SPEC_SEMI', conn)
        stats['spec_semi'] = spec_semi_result['count']
        log_messages.append(spec_semi_result['message'])
        
        conn.commit()
        conn.close()
        log_messages.append("\n✅ DB 저장 완료")
        
        return {
            'success': True,
            'stats': stats,
            'log': '\n'.join(log_messages)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'log': '\n'.join(log_messages)
        }

# ==========================================================
# 1. 핵심 DB 및 쿼리 정의 함수
# ==========================================================

def get_db_connection(db_name):
    """DB 연결을 새로 열어 반환합니다 (스레드 오류 방지)."""
    if not os.path.exists(db_name):
        return None  # DB 파일 없음을 나타냄
    # ⚠️ 스레딩 문제 해결: 연결 객체는 스레드 내에서 생성되어야 함
    return sqlite3.connect(db_name)

@st.cache_data
def get_pc_info_list(_conn): 
    """T_PC_INFO 테이블에서 PC_ID 목록을 로드하여 캐싱합니다."""
    try:
        df_pc = pd.read_sql_query("SELECT PC_ID FROM T_PC_INFO ORDER BY PC_ID", _conn)
        pc_list = df_pc['PC_ID'].tolist()
        return ['전체'] + pc_list
    except Exception as e:
        # T_PC_INFO가 없으면 T_MASTER_DATA에서 추출
        try:
            df_pc = pd.read_sql_query("SELECT DISTINCT PC_ID FROM T_MASTER_DATA WHERE PC_ID IS NOT NULL ORDER BY PC_ID", _conn)
            pc_list = df_pc['PC_ID'].tolist()
            return ['전체'] + pc_list
        except:
            st.warning(f"⚠️ PC 목록 로드 실패: {str(e)}")
            return ['전체']



def get_query_and_columns(item_key, date_col, pc_id):
    """SQL 쿼리 템플릿을 생성하고 마스터 패스 필드를 반환합니다. (T_ITEM 테이블 사용)"""
    item_key = item_key.lower()
    
    # ✅✅✅ FW 쿼리 (677~692줄)
    if item_key == 'fw':
        fw_pc_filter = ""
        if pc_id and pc_id != '전체':
            fw_pc_filter = f" AND T1.FwPC = '{pc_id.replace(chr(39), chr(39)+chr(39))}'"
        
        query_template = f"""
        SELECT T1.SNumber, T1.{date_col} AS StartTime, 'FileCheck' AS Measure_Item, T1.FwFile AS Test_Value, 
               NULL AS MinLimit, NULL AS MaxLimit,
            CASE 
                WHEN T1.FwPass = 'o' OR T1.FwPass = 'O' THEN 'Pass'
                WHEN T1.FwPass = 'x' OR T1.FwPass = 'X' THEN '미달'
                WHEN T1.FwPass IS NULL OR T1.FwPass = '' OR T1.FwPass = 'None' OR T1.FwPass = 'nan' THEN '제외'
                ELSE '제외'
            END AS Spec_Result_Detail
        FROM T_ITEM_FW AS T1
        WHERE T1.{date_col} BETWEEN ? AND ? AND T1.FwFile LIKE ? {fw_pc_filter} LIMIT ?;
        """
        master_pass_field = 'FwPass'
        return query_template, master_pass_field

    # ✅✅✅ RFTX 쿼리 (694~712줄)
    elif item_key == 'rftx':
        rftx_pc_filter = ""
        if pc_id and pc_id != '전체':
            rftx_pc_filter = f" AND T1.RfTxPC = '{pc_id.replace(chr(39), chr(39)+chr(39))}'"
        
        query_template = f"""
        SELECT U.SNumber, U.{date_col} AS StartTime, U.Measure_Item, U.Test_Value, 
               NULL AS MinLimit, NULL AS MaxLimit,
            CASE 
                WHEN U.RfTxPass = 'o' OR U.RfTxPass = 'O' THEN 'Pass'
                WHEN U.RfTxPass = 'x' OR U.RfTxPass = 'X' THEN '미달'
                WHEN U.RfTxPass IS NULL OR U.RfTxPass = '' OR U.RfTxPass = 'None' OR U.RfTxPass = 'nan' THEN '제외'
                ELSE '제외'
            END AS Spec_Result_Detail
        FROM (
            SELECT T1.SNumber, T1.{date_col}, T1.RfTxPower AS Test_Value, 'Power' AS Measure_Item, T1.RfTxPass FROM T_ITEM_RFTX AS T1 WHERE 1=1 {rftx_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.RfTxModul AS Test_Value, 'Modul' AS Measure_Item, T1.RfTxPass FROM T_ITEM_RFTX AS T1 WHERE 1=1 {rftx_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.RfTxCFOD AS Test_Value, 'CFOD' AS Measure_Item, T1.RfTxPass FROM T_ITEM_RFTX AS T1 WHERE 1=1 {rftx_pc_filter}
        ) AS U
        WHERE U.{date_col} BETWEEN ? AND ? AND U.Measure_Item LIKE ? LIMIT ?;
        """
        master_pass_field = 'RfTxPass'
        return query_template, master_pass_field

    # ✅✅✅ BATADC 쿼리 (714~733줄)
    elif item_key == 'batadc':
        batadc_pc_filter = ""
        if pc_id and pc_id != '전체':
            batadc_pc_filter = f" AND T1.BatadcPC = '{pc_id.replace(chr(39), chr(39)+chr(39))}'"
        
        query_template = f"""
        SELECT U.SNumber, U.{date_col} AS StartTime, U.Measure_Item, U.Test_Value, 
               NULL AS MinLimit, NULL AS MaxLimit,
            CASE 
                WHEN U.BatadcPass = 'o' OR U.BatadcPass = 'O' THEN 'Pass'
                WHEN U.BatadcPass = 'x' OR U.BatadcPass = 'X' THEN '미달'
                WHEN U.BatadcPass IS NULL OR U.BatadcPass = '' OR U.BatadcPass = 'None' OR U.BatadcPass = 'nan' THEN '제외'
                ELSE '제외'
            END AS Spec_Result_Detail
        FROM (
            SELECT T1.SNumber, T1.{date_col}, T1.BatadcLevel AS Test_Value, 'Level' AS Measure_Item, T1.BatadcPass FROM T_ITEM_BATADC AS T1 WHERE 1=1 {batadc_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.BatadcVoiceTh AS Test_Value, 'VoiceTh' AS Measure_Item, T1.BatadcPass FROM T_ITEM_BATADC AS T1 WHERE 1=1 {batadc_pc_filter}
        ) AS U
        WHERE U.{date_col} BETWEEN ? AND ? AND U.Measure_Item LIKE ? LIMIT ?;
        """
        master_pass_field = 'BatadcPass'
        return query_template, master_pass_field

    # ✅✅✅ SEMI 쿼리 (735~756줄)
    elif item_key == 'semi':
        query_template = f"""
        SELECT U.SNumber, U.{date_col} AS StartTime, U.Measure_Item, U.Test_Value, 
               S.Min_Value AS MinLimit, S.Max_Value AS MaxLimit,
            CASE 
                WHEN U.SemiAssyPass = 'o' OR U.SemiAssyPass = 'O' THEN 'Pass'
                WHEN U.SemiAssyPass = 'x' OR U.SemiAssyPass = 'X' THEN (
                    CASE
                        WHEN U.Test_Value IS NULL OR U.Test_Value = 0.0 THEN '제외'
                        WHEN S.Min_Value IS NULL AND S.Max_Value IS NULL THEN '제외'
                        WHEN U.Test_Value < S.Min_Value THEN '미달'
                        WHEN U.Test_Value > S.Max_Value THEN '초과'
                        ELSE '제외'
                    END
                )
                WHEN U.SemiAssyPass IS NULL OR U.SemiAssyPass = '' OR U.SemiAssyPass = 'None' OR U.SemiAssyPass = 'nan' THEN '제외'
                ELSE '제외'
            END AS Spec_Result_Detail
        FROM (
            SELECT T1.SNumber, T1.{date_col}, T1.SemiAssyBatVolt AS Test_Value, 'BatVolt' AS Measure_Item, T1.SemiAssyPass FROM T_ITEM_SEMI AS T1
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.SemiAssySolarVolt AS Test_Value, 'SolarVolt' AS Measure_Item, T1.SemiAssyPass FROM T_ITEM_SEMI AS T1
        ) AS U
        LEFT JOIN T_SPEC_SEMI AS S ON U.Measure_Item = S.Measure_Item
        WHERE U.{date_col} BETWEEN ? AND ? AND U.Measure_Item LIKE ? LIMIT ?;
        """
        master_pass_field = 'SemiAssyPass'
        return query_template, master_pass_field
        
    # ✅✅✅ PCB 쿼리 (758~795줄)
    elif item_key == 'pcb':
        pcb_pc_filter = ""
        if pc_id and pc_id != '전체':
            pcb_pc_filter = f" WHERE T1.pcbPC = '{pc_id.replace(chr(39), chr(39)+chr(39))}'"
        
        query_template = f"""
        SELECT 
            U.SNumber, U.{date_col} AS StartTime, U.Measure_Item, U.Test_Value, 
            S.Min_Value AS MinLimit, S.Max_Value AS MaxLimit,
            CASE 
                WHEN U.PcbPass = 'o' OR U.PcbPass = 'O' THEN 'Pass'
                WHEN U.PcbPass = 'x' OR U.PcbPass = 'X' THEN (
                    CASE
                        WHEN U.Test_Value IS NULL OR U.Test_Value = 0.0 THEN '제외'
                        WHEN S.Min_Value IS NULL AND S.Max_Value IS NULL THEN '제외'
                        WHEN U.Test_Value < S.Min_Value THEN '미달'
                        WHEN U.Test_Value > S.Max_Value THEN '초과'
                        ELSE '제외'
                    END
                )
                WHEN U.PcbPass IS NULL OR U.PcbPass = '' OR U.PcbPass = 'None' OR U.PcbPass = 'nan' THEN '제외'
                ELSE '제외'
            END AS Spec_Result_Detail
        FROM (
            SELECT T1.SNumber, T1.{date_col}, T1.PcbSleepCurr AS Test_Value, 'SleepCurr' AS Measure_Item, T1.SleepCurr_Spec_ID AS Spec_ID, T1.PcbPass FROM T_ITEM_PCB AS T1{pcb_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.PcbBatVolt AS Test_Value, 'BatVolt' AS Measure_Item, NULL AS Spec_ID, T1.PcbPass FROM T_ITEM_PCB AS T1{pcb_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.PcbIrCurr AS Test_Value, 'IrCurr' AS Measure_Item, NULL AS Spec_ID, T1.PcbPass FROM T_ITEM_PCB AS T1{pcb_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.PcbIrPwr AS Test_Value, 'IrPwr' AS Measure_Item, NULL AS Spec_ID, T1.PcbPass FROM T_ITEM_PCB AS T1{pcb_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.PcbWirelessVolt AS Test_Value, 'WirelessVolt' AS Measure_Item, NULL AS Spec_ID, T1.PcbPass FROM T_ITEM_PCB AS T1{pcb_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.PcbUsbCurr AS Test_Value, 'UsbCurr' AS Measure_Item, NULL AS Spec_ID, T1.PcbPass FROM T_ITEM_PCB AS T1{pcb_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.PcbWirelessUsbVolt AS Test_Value, 'WirelessUsbVolt' AS Measure_Item, NULL AS Spec_ID, T1.PcbPass FROM T_ITEM_PCB AS T1{pcb_pc_filter}
            UNION ALL SELECT T1.SNumber, T1.{date_col}, T1.PcbLed AS Test_Value, 'Led' AS Measure_Item, NULL AS Spec_ID, T1.PcbPass FROM T_ITEM_PCB AS T1{pcb_pc_filter}
        ) AS U
        LEFT JOIN T_SPEC_PCB AS S 
            ON (U.Spec_ID IS NOT NULL AND U.Spec_ID = S.Spec_ID AND U.Measure_Item = S.Measure_Item)
            OR (U.Spec_ID IS NULL AND U.Measure_Item = S.Measure_Item)
        WHERE 
             U.{date_col} BETWEEN ? AND ?
             AND U.Measure_Item LIKE ?
        LIMIT ?;
        """
        master_pass_field = 'PcbPass'
        return query_template, master_pass_field

    else:
        raise ValueError(f"지원되지 않는 항목: '{item_key}'")



# ----------------- Pandas 스타일링 함수 (불량 강조) -----------------
def style_df_failure(df):
    """Pandas DataFrame에서 '미달'/'초과'/'제외' 결과를 시각적으로 강조합니다."""
    def highlight_failure(val):
        if isinstance(val, str):
            if '초과' in val or '미달' in val or val.lower() == 'x':
                return 'background-color: #ffcccc; color: #cc0000' 
            elif '제외' in val:
                return 'background-color: #e0e0e0; color: #555555'
            elif val.lower() == 'o' or val.lower() == 'pass':
                return 'background-color: #ccffcc'
        return ''
    styled_df = df.style.applymap(highlight_failure)
    return styled_df


# ==========================================================
# 새로 추가: 불량 유형별 SNumber 조회 함수
# ==========================================================

# def show_snumbers_by_defect_type(category_1st, category_2nd, analysis_params):
#     """불량 유형(1차/2차)에 따라 해당하는 SNumber 목록을 조회합니다."""
    
#     st.subheader(f"📋 조회 결과: {category_1st} → {category_2nd}")
    
#     conn = None
#     try:
#         # 분석 파라미터 가져오기
#         start_date = analysis_params['start']
#         end_date = analysis_params['end']
#         item = analysis_params['item']
#         limit = analysis_params['limit']
#         pc_id = analysis_params['pc_id']
#         measure_item_filter = analysis_params.get('measure_item_filter', '전체')
        
#         item_key = item.lower()
#         start_date_str = start_date.strftime('%Y-%m-%d 00:00:00')
#         end_date_str = end_date.strftime('%Y-%m-%d 23:59:59')
#         date_col = DATE_COLUMN_MAP.get(item_key, 'Stamp')
        
#         st.info(f"📊 조건: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} | {item.upper()} | PC: {pc_id} | 유형: {measure_item_filter}")
        
#         conn = get_db_connection(DB_FILE_NAME)
        
#         # ✅ get_query_and_columns 사용 (T_ITEM 테이블 기반)
#         SQL_STEP1_WITH_PC, master_pass_field = get_query_and_columns(item_key, date_col, pc_id)
        
#         item_filter = '%%'
#         params_step1 = (start_date_str, end_date_str, item_filter, limit)
        
#         with st.spinner("데이터 추출 중..."):
#             df_filtered_all = pd.read_sql_query(SQL_STEP1_WITH_PC, conn, params=params_step1)
            
#             # ✅ measure_item_filter 적용
#             if measure_item_filter != '전체':
#                 before_filter = len(df_filtered_all)
#                 df_filtered_all = df_filtered_all[df_filtered_all['Measure_Item'] == measure_item_filter]
#                 after_filter = len(df_filtered_all)
#                 st.info(f"✅ 유형 필터링: {before_filter}건 → {after_filter}건 (유형: {measure_item_filter})")
        
#         if df_filtered_all.empty:
#             st.warning("⚠️ 해당 조건에 맞는 데이터가 없습니다.")
#             return
        
#         # ✅ 데이터 처리 (run_analysis와 동일)
#         df_filtered_all['Date_Only'] = df_filtered_all['StartTime'].str[:10]
#         df_final = df_filtered_all.copy()
        
#         # ✅ SNumber별 Pass 여부 확인
#         snumber_pass_status = df_final[df_final['Spec_Result_Detail'] == 'Pass'].groupby('SNumber').size().reset_index(name='PassCount')
#         snumber_pass_status['Has_Pass'] = snumber_pass_status['PassCount'] > 0
#         df_final = pd.merge(df_final, snumber_pass_status[['SNumber', 'Has_Pass']], on='SNumber', how='left').fillna({'Has_Pass': False})
        
#         # ✅ SNumber별 가성/진성 분류
#         snumber_classification = df_final.groupby('SNumber').agg({
#             'Has_Pass': 'first'
#         }).reset_index()
        
#         snumber_classification['SNumber_Category'] = snumber_classification['Has_Pass'].apply(
#             lambda x: '가성불량' if x else '진성불량'
#         )
        
#         df_final = pd.merge(df_final, snumber_classification[['SNumber', 'SNumber_Category']], on='SNumber', how='left')
        
#         # ✅ Final_Failure_Category 결정
#         def classify_failure_final(row):
#             detail = row['Spec_Result_Detail']
            
#             if detail == 'Pass':
#                 return 'Pass'
#             elif detail in ['미달', '초과', '제외']:
#                 return row['SNumber_Category']
#             else:
#                 return detail
        
#         df_final['Final_Failure_Category'] = df_final.apply(classify_failure_final, axis=1)
        
#         # ✅ 필터링 (최종 1차/2차 분류 조건에 맞는 행만 남김)
#         df_filtered = df_final[
#             (df_final['Final_Failure_Category'] == category_1st) &
#             (df_final['Spec_Result_Detail'] == category_2nd)
#         ].copy()
        
#         if df_filtered.empty:
#             st.warning(f"⚠️ '{category_1st} → {category_2nd}' 조건에 해당하는 데이터가 없습니다.")
#             return

#         # ✅ 통계 정보 표시
#         total_records = len(df_filtered)
#         unique_snumbers = df_filtered['SNumber'].nunique()
        
#         col1, col2 = st.columns(2)
#         with col1:
#             st.metric("📊 전체 측정 건수", f"{total_records:,}건")
#         with col2:
#             st.metric("🔢 고유 SNumber 개수", f"{unique_snumbers:,}개")
        
#         st.markdown("---")
        
#         # 날짜별로 그룹화하여 표시
#         df_filtered['Date_Only'] = df_filtered['StartTime'].str[:10]
        
#         for date in sorted(df_filtered['Date_Only'].unique()):
#             df_date = df_filtered[df_filtered['Date_Only'] == date]
#             records_on_date = len(df_date)
#             snumbers_on_date = df_date['SNumber'].unique().tolist()
#             unique_count = len(snumbers_on_date)
            
#             with st.expander(f"📅 {date} | 측정 {records_on_date:,}건 / 고유 SNumber {unique_count:,}개", expanded=False):
#                 # 상세 정보 테이블 생성
#                 df_date_detailed = df_date.copy()
                
#                 # ✅ 수정: DataFrame에 실제 존재하는 컬럼만 사용
#                 # 먼저 어떤 컬럼이 있는지 확인
#                 available_columns = df_date_detailed.columns.tolist()
                
#                 # 표시할 컬럼 선택 (존재하는 컬럼만)
#                 display_columns = {}
                
#                 # 기본 컬럼 (항상 있음)
#                 if 'SNumber' in available_columns:
#                     display_columns['SNumber'] = 'SNumber'
#                 if 'StartTime' in available_columns:
#                     display_columns['StartTime'] = 'Stamp'
#                 if 'Measure_Item' in available_columns:
#                     display_columns['Measure_Item'] = '측정항목'
#                 if 'Test_Value' in available_columns:
#                     display_columns['Test_Value'] = '측정값'
#                 if 'MinLimit' in available_columns:
#                     display_columns['MinLimit'] = 'Min (기준)'
#                 if 'MaxLimit' in available_columns:
#                     display_columns['MaxLimit'] = 'Max (기준)'
#                 if 'Spec_Result_Detail' in available_columns:
#                     display_columns['Spec_Result_Detail'] = '판정결과'
#                 if 'Final_Failure_Category' in available_columns:
#                     display_columns['Final_Failure_Category'] = '가성/진성'
#                 if 'SNumber_Category' in available_columns:
#                     display_columns['SNumber_Category'] = 'SNumber분류'
                
#                 # ✅ 존재하는 컬럼만 선택
#                 df_display = df_date_detailed[list(display_columns.keys())].rename(columns=display_columns)
                
#                 # 번호 추가
#                 df_display.insert(0, '번호', range(1, len(df_display) + 1))
                
#                 # 스타일 적용하여 표시
#                 st.dataframe(style_df_failure(df_display), use_container_width=True, hide_index=True, height=400)
                
#                 # 선택 가능한 selectbox 추가
#                 selected_sn = st.selectbox(
#                     f"상세 조회할 SNumber 선택 ({date}):",
#                     snumbers_on_date,
#                     key=f"select_sn_{date}_{category_1st}_{category_2nd}"
#                 )
                
#                 if st.button(f"상세 조회", key=f"detail_btn_{date}_{category_1st}_{category_2nd}"):
#                     st.markdown("---")
#                     show_single_snumber_detail(selected_sn, item_key, conn, analysis_params['pc_id'])
    
#     except Exception as e:
#         st.error(f"❌ 조회 중 오류 발생: {e}")
#         import traceback
#         st.code(traceback.format_exc())
#     finally:
#         if conn:
#             conn.close()
def show_snumbers_by_defect_type(category_1st, category_2nd, analysis_params):
    """불량 유형(1차/2차)에 따라 해당하는 SNumber 목록을 조회합니다."""
    
    st.subheader(f"📋 조회 결과: {category_1st} → {category_2nd}")
    
    conn = None
    try:
        # 분석 파라미터 가져오기
        start_date = analysis_params['start']
        end_date = analysis_params['end']
        item = analysis_params['item']
        limit = analysis_params['limit']
        pc_id = analysis_params['pc_id']
        measure_item_filter = analysis_params.get('measure_item_filter', '전체')
        
        item_key = item.lower()
        start_date_str = start_date.strftime('%Y-%m-%d 00:00:00')
        end_date_str = end_date.strftime('%Y-%m-%d 23:59:59')
        date_col = DATE_COLUMN_MAP.get(item_key, 'Stamp')
        
        st.info(f"📊 조건: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} | {item.upper()} | PC: {pc_id} | 유형: {measure_item_filter}")
        
        conn = get_db_connection(DB_FILE_NAME)
        
        # ✅ get_query_and_columns 사용 (T_ITEM 테이블 기반)
        SQL_STEP1_WITH_PC, master_pass_field = get_query_and_columns(item_key, date_col, pc_id)
        
        item_filter = '%%'
        params_step1 = (start_date_str, end_date_str, item_filter, limit)
        
        with st.spinner("데이터 추출 중..."):
            df_filtered_all = pd.read_sql_query(SQL_STEP1_WITH_PC, conn, params=params_step1)
            
            # ✅ measure_item_filter 적용
            if measure_item_filter != '전체':
                before_filter = len(df_filtered_all)
                df_filtered_all = df_filtered_all[df_filtered_all['Measure_Item'] == measure_item_filter]
                after_filter = len(df_filtered_all)
                st.info(f"✅ 유형 필터링: {before_filter}건 → {after_filter}건 (유형: {measure_item_filter})")
        
        if df_filtered_all.empty:
            st.warning("⚠️ 해당 조건에 맞는 데이터가 없습니다.")
            return
        
        # ✅ 데이터 처리 (run_analysis와 동일)
        df_filtered_all['Date_Only'] = df_filtered_all['StartTime'].str[:10]
        df_final = df_filtered_all.copy()
        
        # ✅ SNumber별 Pass 여부 확인
        snumber_pass_status = df_final[df_final['Spec_Result_Detail'] == 'Pass'].groupby('SNumber').size().reset_index(name='PassCount')
        snumber_pass_status['Has_Pass'] = snumber_pass_status['PassCount'] > 0
        df_final = pd.merge(df_final, snumber_pass_status[['SNumber', 'Has_Pass']], on='SNumber', how='left').fillna({'Has_Pass': False})
        
        # ✅ SNumber별 가성/진성 분류
        snumber_classification = df_final.groupby('SNumber').agg({
            'Has_Pass': 'first'
        }).reset_index()
        
        snumber_classification['SNumber_Category'] = snumber_classification['Has_Pass'].apply(
            lambda x: '가성불량' if x else '진성불량'
        )
        
        df_final = pd.merge(df_final, snumber_classification[['SNumber', 'SNumber_Category']], on='SNumber', how='left')
        
        # ✅ Final_Failure_Category 결정
        def classify_failure_final(row):
            detail = row['Spec_Result_Detail']
            
            if detail == 'Pass':
                return 'Pass'
            elif detail in ['미달', '초과', '제외']:
                return row['SNumber_Category']
            else:
                return detail
        
        df_final['Final_Failure_Category'] = df_final.apply(classify_failure_final, axis=1)
        
        # ✅ 필터링 (최종 1차/2차 분류 조건에 맞는 행만 남김)
        df_filtered = df_final[
            (df_final['Final_Failure_Category'] == category_1st) &
            (df_final['Spec_Result_Detail'] == category_2nd)
        ].copy()
        
        if df_filtered.empty:
            st.warning(f"⚠️ '{category_1st} → {category_2nd}' 조건에 해당하는 데이터가 없습니다.")
            return

        # ✅ 통계 정보 표시
        total_records = len(df_filtered)
        unique_snumbers = df_filtered['SNumber'].nunique()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("📊 전체 측정 건수", f"{total_records:,}건")
        with col2:
            st.metric("🔢 고유 SNumber 개수", f"{unique_snumbers:,}개")
        
        st.markdown("---")
        
        # 날짜별로 그룹화하여 표시
        df_filtered['Date_Only'] = df_filtered['StartTime'].str[:10]
        
        for date in sorted(df_filtered['Date_Only'].unique()):
            df_date = df_filtered[df_filtered['Date_Only'] == date]
            records_on_date = len(df_date)
            snumbers_on_date = df_date['SNumber'].unique().tolist()
            unique_count = len(snumbers_on_date)
            
            with st.expander(f"📅 {date} | 측정 {records_on_date:,}건 / 고유 SNumber {unique_count:,}개", expanded=False):
                # 상세 정보 테이블 생성
                df_date_detailed = df_date.copy()
                
                # ✅ 수정: DataFrame에 실제 존재하는 컬럼만 사용
                available_columns = df_date_detailed.columns.tolist()
                
                # 표시할 컬럼 선택 (존재하는 컬럼만)
                display_columns = {}
                
                # 기본 컬럼 (항상 있음)
                if 'SNumber' in available_columns:
                    display_columns['SNumber'] = 'SNumber'
                if 'StartTime' in available_columns:
                    display_columns['StartTime'] = 'Stamp'
                if 'Measure_Item' in available_columns:
                    display_columns['Measure_Item'] = '측정항목'
                if 'Test_Value' in available_columns:
                    display_columns['Test_Value'] = '측정값'
                if 'MinLimit' in available_columns:
                    display_columns['MinLimit'] = 'Min (기준)'
                if 'MaxLimit' in available_columns:
                    display_columns['MaxLimit'] = 'Max (기준)'
                if 'Spec_Result_Detail' in available_columns:
                    display_columns['Spec_Result_Detail'] = '판정결과'
                if 'Final_Failure_Category' in available_columns:
                    display_columns['Final_Failure_Category'] = '가성/진성'
                if 'SNumber_Category' in available_columns:
                    display_columns['SNumber_Category'] = 'SNumber분류'
                
                # ✅ 존재하는 컬럼만 선택
                df_display = df_date_detailed[list(display_columns.keys())].rename(columns=display_columns)
                
                # 번호 추가
                df_display.insert(0, '번호', range(1, len(df_display) + 1))
                
                # 스타일 적용하여 표시
                st.dataframe(style_df_failure(df_display), use_container_width=True, hide_index=True, height=400)
                
                # 선택 가능한 selectbox 추가
                selected_sn = st.selectbox(
                    f"상세 조회할 SNumber 선택 ({date}):",
                    snumbers_on_date,
                    key=f"select_sn_{date}_{category_1st}_{category_2nd}"
                )
                
                if st.button(f"상세 조회", key=f"detail_btn_{date}_{category_1st}_{category_2nd}"):
                    st.markdown("---")
                    # ✅ 날짜 필터 전달
                    show_single_snumber_detail(
                        selected_sn, 
                        item_key, 
                        conn, 
                        pc_id=analysis_params['pc_id'],
                        start_date=start_date,  # ← 추가!
                        end_date=end_date       # ← 추가!
                    )
    
    except Exception as e:
        st.error(f"❌ 조회 중 오류 발생: {e}")
        import traceback
        st.code(traceback.format_exc())
    finally:
        if conn:
            conn.close()

# ==========================================================
# 새로 추가: SNumber 검색 및 상세 조회 함수
# ==========================================================
def search_snumber(search_pattern, conn):
    """SNumber를 패턴으로 검색합니다 (와일드카드 지원)."""
    try:
        # % 와일드카드 자동 추가
        if '*' in search_pattern:
            search_pattern = search_pattern.replace('*', '%')
        else:
            # *가 없으면 자동으로 양쪽에 % 추가
            search_pattern = f"%{search_pattern}%"
        
        query = "SELECT DISTINCT SNumber FROM T_MASTER_DATA WHERE SNumber LIKE ? ORDER BY SNumber LIMIT 100"
        df_results = pd.read_sql_query(query, conn, params=(search_pattern,))
        
        return df_results['SNumber'].tolist()
    except Exception as e:
        st.error(f"❌ SNumber 검색 오류: {e}")
        return []


def show_snumber_detail(snumber_input, item_key):
    """SNumber 상세 조회를 독립적으로 수행합니다."""
    if not snumber_input:
        st.info("⬆️ 위의 입력창에 조회할 SNumber를 입력하고 버튼을 누르세요.")
        st.info("💡 팁: 일부만 입력하면 자동으로 검색됩니다 (예: THSR, *9226)")
        return
    
    conn = get_db_connection(DB_FILE_NAME)
    if conn is None: 
        st.stop()

    try:
        # 1. 먼저 검색 수행
        matching_snumbers = search_snumber(snumber_input, conn)
        
        if not matching_snumbers:
            st.warning(f"⚠️ '{snumber_input}' 패턴과 일치하는 SNumber가 없습니다.")
            return
        
        # 2. 검색 결과가 여러 개인 경우
        if len(matching_snumbers) > 1:
            st.success(f"🔍 검색 결과: {len(matching_snumbers)}건 발견")
            
            # 결과 목록을 DataFrame으로 표시
            df_search_results = pd.DataFrame({
                '번호': range(1, len(matching_snumbers) + 1),
                'SNumber': matching_snumbers
            })
            
            st.dataframe(df_search_results, use_container_width=True, hide_index=True)
            
            # 선택 가능한 selectbox 추가
            st.markdown("---")
            selected_snumber = st.selectbox(
                "상세 조회할 SNumber 선택:",
                matching_snumbers,
                key="selected_snumber_from_search"
            )
            
            if st.button("선택한 SNumber 상세 조회", key="detail_from_selected"):
                # show_single_snumber_detail(selected_snumber, item_key, conn)
                show_single_snumber_detail(selected_snumber, item_key, conn, selected_pc_id)
        
        # 3. 검색 결과가 1개인 경우 바로 상세 조회
        else:
            snumber_to_show = matching_snumbers[0]
            st.info(f"✅ 일치하는 SNumber: {snumber_to_show}")
            show_single_snumber_detail(snumber_to_show, item_key, conn, selected_pc_id)
            
    except Exception as e:
        st.error(f"❌ 조회 중 오류 발생: {e}")
    finally:
        conn.close()


def show_single_snumber_detail(snumber, item_key, conn, pc_id=None, start_date=None, end_date=None):
    """단일 SNumber의 상세 정보를 표시합니다. (날짜 필터 추가)"""
    try:
        # ✅ 날짜 컬럼 매핑
        date_col_map = {
            'fw': 'FwStamp',
            'rftx': 'RfTxStamp',
            'batadc': 'BatadcStamp',
            'pcb': 'PcbStartTime',
            'semi': 'SemiAssyStartTime'
        }
        date_col = date_col_map.get(item_key, 'Stamp')
        
        # T_MASTER_DATA 전체 요약 (날짜 필터 없음)
        master_query = "SELECT * FROM T_MASTER_DATA WHERE SNumber = ?"
        df_master = pd.read_sql_query(master_query, conn, params=(snumber,))
        
        if df_master.empty:
            st.warning(f"⚠️ SNumber '{snumber}'에 대한 마스터 데이터가 없습니다.")
            return

        st.markdown(f"### ✅ SNumber: `{snumber}` 전체 공정 요약")
        st.dataframe(df_master.T, use_container_width=True)
        
        # T_ITEM_XXX 상세 측정 내역 조회
        item_table_name = f"T_ITEM_{item_key.upper()}"
        
        # ✅ 날짜 필터 조건 생성
        date_filter = ""
        params = [snumber]
        
        if start_date and end_date:
            start_date_str = start_date.strftime('%Y-%m-%d 00:00:00')
            end_date_str = end_date.strftime('%Y-%m-%d 23:59:59')
            date_filter = f" AND {date_col} BETWEEN ? AND ?"
            params.extend([start_date_str, end_date_str])
        
        # ✅ PC 필터 추가
        pc_filter = ""
        if pc_id and pc_id != '전체':
            # 품목별 PC 컬럼명 확인
            pc_column_map = {
                'fw': 'FwPC',
                'rftx': 'RfTxPC',
                'batadc': 'BatadcPC',
                'pcb': 'pcbPC',
                'semi': None  # SEMI는 PC 없음
            }
            
            pc_column = pc_column_map.get(item_key)
            
            if pc_column:
                pc_filter = f" AND {pc_column} = ?"
                params.append(pc_id)
        
        # ✅ 쿼리 실행
        item_query = f"SELECT * FROM {item_table_name} WHERE SNumber = ?{date_filter}{pc_filter} ORDER BY {date_col}"
        df_item = pd.read_sql_query(item_query, conn, params=tuple(params))
        
        if not df_item.empty:
            st.markdown(f"### ✅ {item_key.upper()} 상세 측정 내역 ({item_table_name})")
            
            # ✅ 필터 정보 표시
            filter_info = []
            if start_date and end_date:
                filter_info.append(f"📅 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
            if pc_id and pc_id != '전체':
                filter_info.append(f"🖥️ PC: {pc_id}")
            
            if filter_info:
                st.info(" | ".join(filter_info))
            
            # ✅ Pass 컬럼 통계 (품목별 Pass 컬럼명)
            pass_col_map = {
                'fw': 'FwPass',
                'rftx': 'RfTxPass',
                'batadc': 'BatadcPass',
                'pcb': 'PcbPass',
                'semi': 'SemiAssyPass'
            }
            pass_col = pass_col_map.get(item_key)
            
            if pass_col and pass_col in df_item.columns:
                # 중복 제거: 동일 Stamp에서 하나의 Pass만 카운트
                df_unique = df_item.drop_duplicates(subset=[date_col])
                o_count = ((df_unique[pass_col] == 'o') | (df_unique[pass_col] == 'O')).sum()
                x_count = ((df_unique[pass_col] == 'x') | (df_unique[pass_col] == 'X')).sum()
                total_tests = len(df_unique)
                
                st.info(f"📊 측정 횟수: 총 {total_tests}회 (고유 {date_col} 기준) | Pass(o): {o_count}회 | Fail(x): {x_count}회")
                
                # ✅ 판정 검증
                if o_count > 0:
                    judgment = "가성불량"
                    judgment_color = "green"
                    st.success(f"✅ **선택된 기간 내** Pass가 {o_count}회 있으므로 **가성불량**이 맞습니다.")
                else:
                    judgment = "진성불량"
                    judgment_color = "red"
                    st.error(f"❌ **선택된 기간 내** Pass가 없으므로 **진성불량**이어야 합니다!")
            
            st.dataframe(style_df_failure(df_item), use_container_width=True)
        else:
            st.info(f"선택된 조건({item_key.upper()})에 대한 상세 기록이 없습니다.")
        
    except Exception as e:
        st.error(f"❌ 상세 조회 중 오류 발생: {e}")
        import traceback
        st.code(traceback.format_exc())


# ==========================================================
# STREAMLIT APP 실행 함수
# ==========================================================

def run_analysis(start_date, end_date, item, limit, pc_id, measure_item_filter='전체'):
    """데이터 분석을 실행합니다. (T_ITEM 테이블만 사용)"""
    
    conn = None
    try:
        item_key = item.lower()
        limit = int(limit)
        
        start_date_str = start_date.strftime('%Y-%m-%d 00:00:00')
        end_date_str = end_date.strftime('%Y-%m-%d 23:59:59')
        date_col = DATE_COLUMN_MAP.get(item_key, 'Stamp') 

        conn = get_db_connection(DB_FILE_NAME) 
        
        # ✅ T_ITEM 테이블만 사용하는 쿼리
        SQL_STEP1, master_pass_field = get_query_and_columns(item_key, date_col, pc_id) 
        item_filter = '%%'
        params_step1 = (start_date_str, end_date_str, item_filter, limit)
        
        st.info(f"조회 기간: **{start_date_str}** 부터 **{end_date_str}** 까지 | 항목: **{item.upper()}** | PC: **{pc_id}** | 유형: **{measure_item_filter}**")

        # 2. 데이터 추출
        with st.spinner("DB에서 데이터 추출 및 분류 중..."):
            df_filtered_all = pd.read_sql_query(SQL_STEP1, conn, params=params_step1)
            
            # ✅ measure_item_filter 적용
            if measure_item_filter != '전체':
                before_filter = len(df_filtered_all)
                df_filtered_all = df_filtered_all[df_filtered_all['Measure_Item'] == measure_item_filter]
                after_filter = len(df_filtered_all)
                st.info(f"✅ 유형 필터링: {before_filter}건 → {after_filter}건 (유형: {measure_item_filter})")

            # ❌ SQL_STEP2 완전 제거! (T_MASTER_DATA 사용 안 함)
            
        if df_filtered_all.empty:
            st.warning("⚠️ 해당 기간에 조회된 데이터가 없습니다.")
            return

    except Exception as e:
        st.error(f"❌ 데이터 추출 오류: {e}")
        import traceback
        st.code(traceback.format_exc())
        return
    finally:
        if conn:
            conn.close() 

    # ✅✅✅ 3. 데이터 처리 및 가성/진성 분류
    
    df_filtered_all['Date_Only'] = df_filtered_all['StartTime'].str[:10]
    df_final = df_filtered_all.copy()
    
    # ✅ SNumber별 Pass 여부 확인
    snumber_pass_status = df_final[df_final['Spec_Result_Detail'] == 'Pass'].groupby('SNumber').size().reset_index(name='PassCount')
    snumber_pass_status['Has_Pass'] = snumber_pass_status['PassCount'] > 0
    df_final = pd.merge(df_final, snumber_pass_status[['SNumber', 'Has_Pass']], on='SNumber', how='left').fillna({'Has_Pass': False})
    
    # ✅ SNumber별 가성/진성 분류
    snumber_classification = df_final.groupby('SNumber').agg({
        'Has_Pass': 'first'
    }).reset_index()
    
    snumber_classification['SNumber_Category'] = snumber_classification['Has_Pass'].apply(
        lambda x: '가성불량' if x else '진성불량'
    )
    
    df_final = pd.merge(df_final, snumber_classification[['SNumber', 'SNumber_Category']], on='SNumber', how='left')
    
    # ✅ 날짜별 집계
    unique_dates = sorted(df_final['Date_Only'].unique())
    
    st.subheader(f"📈 {item.upper()} 항목 | 기간 ({len(unique_dates)}일) 상세 분석")

    for date_only in unique_dates:
        df_day = df_final[df_final['Date_Only'] == date_only].copy()

        # ✅ Final_Failure_Category 결정
        def classify_failure_final(row):
            detail = row['Spec_Result_Detail']
            
            if detail == 'Pass':
                return 'Pass'
            elif detail in ['미달', '초과', '제외']:
                return row['SNumber_Category']
            else:
                return detail

        df_day['Final_Failure_Category'] = df_day.apply(classify_failure_final, axis=1)

        # 집계 테이블 생성
        df_summary = df_day[df_day['Spec_Result_Detail'].isin(['Pass', '미달', '초과', '제외'])].copy()
        
        if not df_summary.empty:
            summary_table = pd.crosstab(
                index=df_summary['Final_Failure_Category'], 
                columns=df_summary['Spec_Result_Detail'], 
                margins=True, 
                margins_name="Total"
            )
            summary_table = summary_table.reindex(
                index=['Pass', '가성불량', '진성불량', 'Total'],
                columns=['Pass', '미달', '초과', '제외', 'Total'], 
                fill_value=0
            )
            
            st.markdown(f"#### 🗓️ {date_only} ({len(df_summary)} 건)")
            st.dataframe(summary_table, use_container_width=True)

    st.success("✔️ 전체 기간 분석 및 테이블 출력 완료!")

# ==========================================================
# 4. 메인 실행 함수
# ==========================================================

def main():
    st.set_page_config(layout="wide", page_title="TM2360E 생산 품질 분석 대시보드")
    st.title("TM2360E 생산 품질 분석 대시보드")
    st.markdown("---")
    
    # ✅ DB 파일 존재 여부 확인
    db_exists = os.path.exists(DB_FILE_NAME)
    
    if not db_exists:
        st.warning("⚠️ DB 파일이 존재하지 않습니다. CSV 파일을 업로드하여 DB를 생성하세요.")
        st.info("👉 아래에서 CSV 파일을 업로드하면 자동으로 DB가 생성됩니다.")
        
        # CSV 업로드 화면만 표시
        st.header("📁 CSV 파일 업로드 및 DB 생성")
        
        uploaded_file = st.file_uploader(
            "CSV 파일을 선택하세요",
            type=['csv'],
            help="TM2360E 검사 데이터 CSV 파일을 업로드하면 DB가 생성됩니다"
        )
        
        if uploaded_file is not None:
            try:
                df_uploaded = pd.read_csv(uploaded_file, encoding='utf-8', low_memory=False, dtype={'SNumber': str})
                
                st.success(f"✅ 파일 '{uploaded_file.name}' 업로드 성공! (총 {len(df_uploaded):,}행)")
                
                with st.expander("📋 업로드된 데이터 미리보기 (상위 100행)", expanded=False):
                    st.dataframe(df_uploaded.head(100), use_container_width=True, height=400)
                
                if st.button("💾 DB 생성 및 저장", type="primary", key='create_db_btn'):
                    with st.spinner("DB를 생성하고 데이터를 저장하는 중..."):
                        try:
                            # DB 생성 및 저장
                            save_result = process_and_save_csv_to_db(df_uploaded, DB_FILE_NAME)
                            
                            if save_result['success']:
                                st.success("🎉 DB가 생성되고 데이터가 저장되었습니다!")
                                st.info("페이지를 새로고침하면 전체 기능을 사용할 수 있습니다.")
                                
                                st.markdown("### 📊 저장 통계")
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("T_MASTER_DATA", f"{save_result['stats'].get('master', 0):,}행")
                                    st.metric("T_ITEM_PCB", f"{save_result['stats'].get('pcb', 0):,}행")
                                    st.metric("T_ITEM_SEMI", f"{save_result['stats'].get('semi', 0):,}행")
                                with col2:
                                    st.metric("T_ITEM_FW", f"{save_result['stats'].get('fw', 0):,}행")
                                    st.metric("T_ITEM_RFTX", f"{save_result['stats'].get('rftx', 0):,}행")
                                    st.metric("T_ITEM_BATADC", f"{save_result['stats'].get('batadc', 0):,}행")
                                with col3:
                                    st.metric("T_PC_INFO", f"{save_result['stats'].get('pc_info', 0):,}행")
                                    st.metric("T_SPEC_PCB", f"{save_result['stats'].get('spec_pcb', 0):,}행")
                                    st.metric("T_SPEC_SEMI", f"{save_result['stats'].get('spec_semi', 0):,}행")
                                
                                if st.button("🔄 새로고침", type="secondary"):
                                    st.rerun()
                            else:
                                st.error(f"❌ DB 저장 중 오류 발생: {save_result['error']}")
                        except Exception as e:
                            st.error(f"❌ 저장 중 예상치 못한 오류 발생: {e}")
                            import traceback
                            st.code(traceback.format_exc())
            except Exception as e:
                st.error(f"❌ CSV 파일 읽기 실패: {e}")
        
        st.stop()  # DB가 없으면 여기서 중단

    # --- 1. 사이드바: 메인 액션 선택 ---
    with st.sidebar:
        st.header("⚙️ 분석 및 DB 관리")
        
        main_action = st.radio(
            "수행할 작업을 선택하세요:",
            options=["DB 조회 및 분석", "DB 업로드 및 저장", "DB 삭제"],
            index=0, 
            key='main_action_selector'
        )
        st.markdown("---")
        
        # ✅ DB 테이블 미리보기 (라디오버튼 바로 아래)
        st.subheader("🔍 DB 테이블 미리보기")
        
        preview_table = st.selectbox(
            "조회할 테이블 선택",
            ['선택하세요'] + DB_TABLES,
            key='preview_table_select'
        )
        
        preview_rows = st.number_input(
            "조회 행 수",
            min_value=10,
            max_value=910000,
            value=1000,
            step=1000,
            key='preview_rows_input'
        )
        
        if st.button("📋 테이블 미리보기", key='preview_btn'):
            if preview_table != '선택하세요':
                st.session_state['preview_executed'] = True
                st.session_state['preview_table_name'] = preview_table
                st.session_state['preview_rows_count'] = preview_rows
        
        st.markdown("---")
        
        # UI 요소 로딩 (공통)
        conn_ui = get_db_connection(DB_FILE_NAME)
        
        # 날짜/품목 입력 필드
        default_start = datetime(2025, 10, 20).date()
        default_end = datetime(2025, 10, 25).date()
        start_date_ui = st.date_input("🗓️ 시작 날짜 (From)", default_start)
        end_date_ui = st.date_input("🗓️ 종료 날짜 (To)", default_end)
        item_ui = st.selectbox("품목 선택", ITEM_OPTIONS, index=0, key='item_select')
        limit_ui = st.number_input("조회 행 제한 (Limit)", min_value=1000, value=100000, step=1000, key='limit_select')
        
        st.markdown("---")
        
        # ✅ 품목별 PC 구분 동적 변경
        st.subheader("🖥️ PC 구분")
        
        pc_filter_options = ['전체']
        try:
            if item_ui == 'pcb':
                # PCB는 100, 101, 102, 103
                df_pc_filtered = pd.read_sql(
                    "SELECT DISTINCT PC_ID FROM T_PC_INFO WHERE PC_Type = 'PCB' ORDER BY PC_ID",
                    conn_ui
                )
                if len(df_pc_filtered) > 0:
                    pc_filter_options.extend(df_pc_filtered['PC_ID'].tolist())
            elif item_ui == 'fw':
                # FW PC 목록
                df_pc_filtered = pd.read_sql(
                    "SELECT DISTINCT PC_ID FROM T_PC_INFO WHERE PC_Type = 'FW' ORDER BY PC_ID",
                    conn_ui
                )
                if len(df_pc_filtered) > 0:
                    pc_filter_options.extend(df_pc_filtered['PC_ID'].tolist())
            elif item_ui == 'rftx':
                # RFTX PC 목록
                df_pc_filtered = pd.read_sql(
                    "SELECT DISTINCT PC_ID FROM T_PC_INFO WHERE PC_Type = 'RFTX' ORDER BY PC_ID",
                    conn_ui
                )
                if len(df_pc_filtered) > 0:
                    pc_filter_options.extend(df_pc_filtered['PC_ID'].tolist())
            elif item_ui == 'batadc':
                # BATADC PC 목록
                df_pc_filtered = pd.read_sql(
                    "SELECT DISTINCT PC_ID FROM T_PC_INFO WHERE PC_Type = 'BATADC' ORDER BY PC_ID",
                    conn_ui
                )
                if len(df_pc_filtered) > 0:
                    pc_filter_options.extend(df_pc_filtered['PC_ID'].tolist())
            # semi는 PC가 없으므로 '전체'만
        except Exception as e:
            st.warning(f"⚠️ PC 목록 조회 실패: {e}")
        
        selected_pc_id = st.selectbox(
            f"{item_ui.upper()} PC 선택",
            pc_filter_options,
            key='pc_filter_dynamic'
        )
        
        st.markdown("---")
        st.subheader("📊 유형 필터")
        
        # 품목별 유형 목록 정의
        measure_items_map = {
            'pcb': ['전체', 'SleepCurr', 'BatVolt', 'IrCurr', 'IrPwr', 'WirelessVolt', 'UsbCurr', 'WirelessUsbVolt', 'Led'],
            'semi': ['전체', 'BatVolt', 'SolarVolt'],
            'fw': ['전체', 'FileCheck'],
            'rftx': ['전체', 'Power', 'Modul', 'CFOD'],
            'batadc': ['전체', 'Level', 'VoiceTh']
        }
        
        # 선택된 품목에 따른 유형 목록 표시
        available_items = measure_items_map.get(item_ui, ['전체'])
        measure_item_filter = st.selectbox(
            "측정 유형 선택",
            available_items,
            key='measure_item_filter'
        )
        
        st.markdown("---")
        
        # DB 연결 닫기
        conn_ui.close()

    # --- 2. 메인 화면: 선택된 액션에 따른 UI 렌더링 ---
    
    if main_action == "DB 조회 및 분석":
        
        # ✅ 테이블 미리보기 결과 표시 (최상단)
        if st.session_state.get('preview_executed', False):
            st.header("📋 DB 테이블 미리보기")
            
            preview_table = st.session_state['preview_table_name']  # ✅ 변경된 키
            preview_rows = st.session_state['preview_rows_count']  # ✅ 변경된 키
            
            try:
                conn_preview = get_db_connection(DB_FILE_NAME)
                df_preview = pd.read_sql_query(f"SELECT * FROM {preview_table} LIMIT {preview_rows}", conn_preview)
                conn_preview.close()
                
                st.success(f"✅ {preview_table} 테이블 | 총 {len(df_preview):,}행 조회")
                
                # 다운로드 버튼
                csv = df_preview.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 CSV 다운로드",
                    data=csv,
                    file_name=f"{preview_table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
                st.dataframe(df_preview, use_container_width=True, height=400)
                
                # 미리보기 닫기 버튼
                if st.button("❌ 미리보기 닫기", key='close_preview'):
                    st.session_state['preview_executed'] = False
                    st.info("✅ 미리보기를 닫았습니다. 다른 작업을 진행하세요.")
                    
            except Exception as e:
                st.error(f"❌ 테이블 조회 오류: {e}")
            
            st.markdown("---")
        
        st.header("📊 기간별 품질 분석 결과")
        
        # ✅ 분석 실행 버튼
        if st.button("🔍 데이터 분석 실행", key='run_analysis_btn', type="primary"):
            start_datetime = datetime(start_date_ui.year, start_date_ui.month, start_date_ui.day)
            end_datetime = datetime(end_date_ui.year, end_date_ui.month, end_date_ui.day)
            end_datetime_inclusive = end_datetime + timedelta(hours=23, minutes=59, seconds=59)
            
            if end_datetime < start_datetime:  # ✅ < 로 변경 (같은 날짜 허용)
                st.error("⚠️ 종료 날짜는 시작 날짜보다 이전일 수 없습니다.")
            else:
                # session_state에 분석 실행 플래그 설정
                st.session_state['analysis_executed'] = True
                st.session_state['analysis_params'] = {
                    'start': start_datetime,
                    'end': end_datetime_inclusive,
                    'item': item_ui,
                    'limit': limit_ui,
                    'pc_id': selected_pc_id,
                    'measure_item_filter': measure_item_filter
                }
        
        # ✅ 분석이 실행되었으면 결과를 고정된 컨테이너에 표시
        if st.session_state.get('analysis_executed', False):
            params = st.session_state['analysis_params']
            
            # 고정된 컨테이너에 분석 결과 표시
            analysis_container = st.container()
            with analysis_container:
                run_analysis(
                    params['start'], 
                    params['end'], 
                    params['item'], 
                    params['limit'], 
                    params['pc_id'],
                    params.get('measure_item_filter', '전체')
                )
        
        # ✅ 상세 조회 섹션 (항상 표시)
        st.markdown("---")
        st.header("🔎 불량 유형별 SNumber 조회")
        
        # 탭으로 두 가지 조회 방법 제공
        tab1, tab2 = st.tabs(["📊 불량 유형별 조회", "🔍 SNumber 직접 검색"])
     
        with tab1:
            st.markdown("### 불량 분류별 SNumber 목록 조회")
            st.info("💡 먼저 데이터 분석을 실행한 후에 이 기능을 사용하세요!")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                category_1st = st.selectbox(
                    "1차 분류 선택",
                    ["Pass", "가성불량", "진성불량"],
                    key="category_1st_select"
                )
            
            with col2:
                category_2nd = st.selectbox(
                    "2차 분류 선택",
                    ["Pass", "미달", "초과", "제외"],
                    key="category_2nd_select"
                )
            
            with col3:
                st.write("")  # 간격 조정
                st.write("")  # 간격 조정
                query_by_category_btn = st.button(
                    "🔍 조회", 
                    key="query_by_category_btn",
                    type="primary"
                )
            
            # 불량 유형별 조회 실행
            if query_by_category_btn:
                if not st.session_state.get('analysis_executed', False):
                    st.warning("⚠️ 먼저 '데이터 분석 실행' 버튼을 눌러 데이터를 분석해주세요!")
                else:
                    # session_state에 불량 조회 정보 저장
                    st.session_state['defect_query_executed'] = True
                    st.session_state['defect_category_1st'] = category_1st
                    st.session_state['defect_category_2nd'] = category_2nd
            
            # 불량 조회 결과 표시
            defect_query_container = st.container()
            with defect_query_container:
                if st.session_state.get('defect_query_executed', False):
                    show_snumbers_by_defect_type(
                        st.session_state['defect_category_1st'],
                        st.session_state['defect_category_2nd'],
                        st.session_state['analysis_params']
                    )
        
        with tab2:
            st.markdown("### SNumber 직접 검색")
            st.markdown("""
            **💡 검색 팁:**
            - 전체 SNumber 입력: `THSRBN5901480USMJNYAH9226`
            - 일부만 입력: `THSR`, `9226`, `BN590`
            - 와일드카드 사용: `THSR*`, `*9226`, `*BN590*`
            """)
            
            snumber_input_main = st.text_input(
                "조회할 SNumber 입력 (일부만 입력해도 자동 검색)", 
                value="", 
                placeholder="예: THSR, *9226, THSRBN590*",
                key="snumber_input_main"
            )
            
            col1, col2 = st.columns([1, 4])
            with col1:
                search_clicked = st.button("🔍 검색 및 상세 조회", key="detail_query_btn_main", type="secondary")
            
            # 검색/상세 조회 섹션 (독립된 컨테이너)
            detail_container = st.container()
            
            with detail_container:
                if search_clicked:
                    if snumber_input_main:
                        # session_state에 검색 실행 플래그 설정
                        st.session_state['search_executed'] = True
                        st.session_state['search_query'] = snumber_input_main
                    else:
                        st.warning("⚠️ SNumber를 입력해주세요.")
                
                # 검색이 실행되었으면 결과 표시
                if st.session_state.get('search_executed', False):
                    show_snumber_detail(st.session_state['search_query'], item_ui)

    elif main_action == "DB 업로드 및 저장":
        st.header("📁 CSV 파일 업로드 및 DB 누적 저장")
        
        st.info("""
        💡 **사용 방법:**
        1. CSV 파일을 업로드하세요
        2. 업로드된 파일 정보를 확인하세요
        3. "DB에 저장" 버튼을 클릭하면 데이터가 DB에 추가됩니다
        4. 저장 완료 후 DB 파일을 다운로드할 수 있습니다
        """)
        
        st.markdown("---")
        
        # CSV 파일 업로드
        uploaded_file = st.file_uploader(
            "CSV 파일을 선택하세요",
            type=['csv'],
            help="TM2360E 검사 데이터 CSV 파일만 업로드 가능합니다"
        )
        
        if uploaded_file is not None:
            try:
                # CSV 파일 읽기
                df_uploaded = pd.read_csv(uploaded_file, encoding='utf-8', low_memory=False, dtype={'SNumber': str})
                
                st.success(f"✅ 파일 '{uploaded_file.name}' 업로드 성공! (총 {len(df_uploaded):,}행)")
                
                # 데이터 미리보기
                with st.expander("📋 업로드된 데이터 미리보기 (상위 100행)", expanded=False):
                    st.dataframe(df_uploaded.head(100), use_container_width=True, height=400)
                
                # 컬럼 정보
                with st.expander("📊 컬럼 정보", expanded=False):
                    col_info = pd.DataFrame({
                        '컬럼명': df_uploaded.columns,
                        '데이터 타입': df_uploaded.dtypes.astype(str).values,  # ✅ Arrow 에러 방지
                        'NULL 개수': df_uploaded.isnull().sum().values,
                        '고유값 개수': [df_uploaded[col].nunique() for col in df_uploaded.columns]
                    })
                    st.dataframe(col_info, use_container_width=True)
                
                st.markdown("---")
                
                # DB 저장 버튼
                if st.button("💾 DB에 저장 (APPEND 모드)", type="primary", key='save_to_db_btn'):
                    with st.spinner("데이터를 DB에 저장하는 중..."):
                        try:
                            # DB 저장 프로세스 실행
                            save_result = process_and_save_csv_to_db(df_uploaded, DB_FILE_NAME)
                            
                            if save_result['success']:
                                st.success("🎉 데이터가 성공적으로 DB에 저장되었습니다!")
                                
                                # 저장 통계 표시
                                st.markdown("### 📊 저장 통계")
                                
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("T_MASTER_DATA", f"{save_result['stats'].get('master', 0):,}행")
                                    st.metric("T_ITEM_PCB", f"{save_result['stats'].get('pcb', 0):,}행")
                                    st.metric("T_ITEM_SEMI", f"{save_result['stats'].get('semi', 0):,}행")
                                with col2:
                                    st.metric("T_ITEM_FW", f"{save_result['stats'].get('fw', 0):,}행")
                                    st.metric("T_ITEM_RFTX", f"{save_result['stats'].get('rftx', 0):,}행")
                                    st.metric("T_ITEM_BATADC", f"{save_result['stats'].get('batadc', 0):,}행")
                                with col3:
                                    st.metric("T_PC_INFO", f"{save_result['stats'].get('pc_info', 0):,}행")
                                    st.metric("T_SPEC_PCB", f"{save_result['stats'].get('spec_pcb', 0):,}행")
                                    st.metric("T_SPEC_SEMI", f"{save_result['stats'].get('spec_semi', 0):,}행")
                                
                                # 로그 표시
                                with st.expander("📝 저장 로그 보기", expanded=False):
                                    st.code(save_result['log'])
                                    
                            else:
                                st.error(f"❌ DB 저장 중 오류 발생: {save_result['error']}")
                                
                        except Exception as e:
                            st.error(f"❌ 저장 중 예상치 못한 오류 발생: {e}")
                            import traceback
                            st.code(traceback.format_exc())
            
            except Exception as e:
                st.error(f"❌ CSV 파일 읽기 실패: {e}")
        
        st.markdown("---")
        
        # DB 다운로드 섹션
        st.header("📥 DB 파일 다운로드")
        
        if os.path.exists(DB_FILE_NAME):
            file_size = os.path.getsize(DB_FILE_NAME) / (1024 * 1024)  # MB
            st.info(f"현재 DB 파일 크기: **{file_size:.2f} MB**")
            
            with open(DB_FILE_NAME, 'rb') as f:
                db_data = f.read()
            
            download_filename = f"product_quality_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            
            st.download_button(
                label="💾 DB 파일 다운로드",
                data=db_data,
                file_name=download_filename,
                mime="application/x-sqlite3",
                help="현재 DB 파일을 다운로드합니다"
            )
        else:
            st.warning("⚠️ DB 파일이 존재하지 않습니다.")
    
    elif main_action == "DB 삭제":
        st.header("🗑️ DB 데이터 삭제 (주차별)")
        
        st.warning("""
        ⚠️ **주의:**
        - 선택한 주차(WEEK_NO)의 데이터만 삭제됩니다
        - DB 파일과 스키마는 유지됩니다
        - 삭제된 데이터는 복구할 수 없습니다
        """)
        
        st.markdown("---")
        
        # DB에서 WEEK_NO 목록 조회
        try:
            conn_delete = get_db_connection(DB_FILE_NAME)
            df_weeks = pd.read_sql("SELECT DISTINCT WEEK_NO FROM T_MASTER_DATA WHERE WEEK_NO IS NOT NULL ORDER BY WEEK_NO DESC", conn_delete)
            week_list = df_weeks['WEEK_NO'].tolist()
            
            if len(week_list) == 0:
                st.info("ℹ️ 삭제할 데이터가 없습니다. (WEEK_NO가 없음)")
                conn_delete.close()
            else:
                # 주차별 데이터 통계
                st.subheader("📊 주차별 데이터 현황")
                
                week_stats = []
                for week in week_list:
                    master_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM T_MASTER_DATA WHERE WEEK_NO = '{week}'", conn_delete)['cnt'][0]
                    pcb_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM T_ITEM_PCB WHERE SNumber IN (SELECT SNumber FROM T_MASTER_DATA WHERE WEEK_NO = '{week}')", conn_delete)['cnt'][0]
                    semi_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM T_ITEM_SEMI WHERE SNumber IN (SELECT SNumber FROM T_MASTER_DATA WHERE WEEK_NO = '{week}')", conn_delete)['cnt'][0]
                    
                    week_stats.append({
                        'WEEK_NO': week,
                        'MASTER': f"{master_count:,}",
                        'PCB': f"{pcb_count:,}",
                        'SEMI': f"{semi_count:,}"
                    })
                
                df_stats = pd.DataFrame(week_stats)
                st.dataframe(df_stats, use_container_width=True, hide_index=True)
                
                conn_delete.close()
                
                st.markdown("---")
                
                # 삭제할 주차 선택
                st.subheader("🗑️ 삭제할 주차 선택")
                
                selected_weeks = st.multiselect(
                    "삭제할 WEEK_NO를 선택하세요 (복수 선택 가능)",
                    week_list,
                    help="여러 주차를 선택하여 한 번에 삭제할 수 있습니다"
                )
                
                if selected_weeks:
                    st.warning(f"⚠️ 선택된 주차: {', '.join(selected_weeks)}")
                    
                    # 삭제될 데이터 미리보기
                    conn_preview = get_db_connection(DB_FILE_NAME)
                    week_filter = "', '".join(selected_weeks)
                    
                    delete_master = pd.read_sql(f"SELECT COUNT(*) as cnt FROM T_MASTER_DATA WHERE WEEK_NO IN ('{week_filter}')", conn_preview)['cnt'][0]
                    delete_pcb = pd.read_sql(f"SELECT COUNT(*) as cnt FROM T_ITEM_PCB WHERE SNumber IN (SELECT SNumber FROM T_MASTER_DATA WHERE WEEK_NO IN ('{week_filter}'))", conn_preview)['cnt'][0]
                    delete_semi = pd.read_sql(f"SELECT COUNT(*) as cnt FROM T_ITEM_SEMI WHERE SNumber IN (SELECT SNumber FROM T_MASTER_DATA WHERE WEEK_NO IN ('{week_filter}'))", conn_preview)['cnt'][0]
                    
                    conn_preview.close()
                    
                    st.info(f"""
                    📊 **삭제될 데이터:**
                    - T_MASTER_DATA: {delete_master:,}행
                    - T_ITEM_PCB: {delete_pcb:,}행
                    - T_ITEM_SEMI: {delete_semi:,}행
                    - T_ITEM_FW, T_ITEM_RFTX, T_ITEM_BATADC: 관련 데이터
                    """)
                    
                    # 확인 체크박스
                    confirm_delete = st.checkbox(f"위 내용을 확인했으며, 선택한 주차({', '.join(selected_weeks)})의 데이터를 삭제하겠습니다")
                    
                    if confirm_delete:
                        if st.button("🗑️ 선택한 주차 데이터 삭제", type="secondary"):
                            try:
                                conn_del = get_db_connection(DB_FILE_NAME)
                                cursor = conn_del.cursor()
                                
                                # FOREIGN KEY 제약조건 임시 비활성화
                                cursor.execute("PRAGMA foreign_keys = OFF;")
                                
                                week_filter = "', '".join(selected_weeks)
                                
                                # SNumber 목록 추출
                                snumber_query = f"SELECT SNumber FROM T_MASTER_DATA WHERE WEEK_NO IN ('{week_filter}')"
                                df_snumbers = pd.read_sql(snumber_query, conn_del)
                                snumber_list = df_snumbers['SNumber'].tolist()
                                
                                if len(snumber_list) > 0:
                                    snumber_filter = "', '".join(snumber_list)
                                    
                                    # 관련 테이블 삭제
                                    cursor.execute(f"DELETE FROM T_ITEM_PCB WHERE SNumber IN ('{snumber_filter}')")
                                    cursor.execute(f"DELETE FROM T_ITEM_SEMI WHERE SNumber IN ('{snumber_filter}')")
                                    cursor.execute(f"DELETE FROM T_ITEM_FW WHERE SNumber IN ('{snumber_filter}')")
                                    cursor.execute(f"DELETE FROM T_ITEM_RFTX WHERE SNumber IN ('{snumber_filter}')")
                                    cursor.execute(f"DELETE FROM T_ITEM_BATADC WHERE SNumber IN ('{snumber_filter}')")
                                    
                                    # 마스터 데이터 삭제
                                    cursor.execute(f"DELETE FROM T_MASTER_DATA WHERE WEEK_NO IN ('{week_filter}')")
                                
                                # FOREIGN KEY 제약조건 다시 활성화
                                cursor.execute("PRAGMA foreign_keys = ON;")
                                
                                conn_del.commit()
                                conn_del.close()
                                
                                st.success(f"✅ 선택한 주차({', '.join(selected_weeks)})의 데이터가 삭제되었습니다.")
                                st.info("페이지를 새로고침하세요.")
                                
                                if st.button("🔄 새로고침"):
                                    st.rerun()
                                    
                            except Exception as e:
                                st.error(f"❌ 데이터 삭제 실패: {e}")
                                import traceback
                                st.code(traceback.format_exc())
                
        except Exception as e:
            st.error(f"❌ WEEK_NO 조회 실패: {e}")


if __name__ == "__main__":
    main()
