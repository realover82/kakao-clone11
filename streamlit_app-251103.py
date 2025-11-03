
import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import os
import io
import re
from datetime import datetime, timedelta

# ==============================================================================
# 0. 전역 설정 및 함수 정의
# ==============================================================================

# 💡 DB 파일명을 현재 실행 디렉토리에 저장하도록 설정
DB_FILE = r'./db/product_history_d2.db' 
BASE_MEASUREMENTS = [
    'PcbSleepCurr', 'PcbBatVolt', 'PcbIrCurr', 'PcbIrPwr', 'PcbWirelessVolt',
    'PcbUsbCurr', 'PcbWirelessUsbVolt', 'PcbLed'
]

# ------------------------------------------------------------------------------
# 0-1. 유틸리티 함수
# ------------------------------------------------------------------------------
@st.cache_resource
def get_db_connection(db_name):
    """ SQLite 연결을 캐시하여 안정적인 세션을 유지합니다. """
    conn = sqlite3.connect(db_name, check_same_thread=False)
    return conn

def get_table_names(conn):
    """ DB에 존재하는 모든 테이블 이름을 가져옵니다. """
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    df_tables = pd.read_sql_query(query, conn)
    return df_tables['name'].tolist()

@st.cache_data
def clean_excel_string(series):
    """ Excel 형식의 ="..." 문자열 및 앞뒤 공백을 제거합니다. """
    series = series.astype(str).str.replace(r'="', '', regex=True).str.replace(r'"', '', regex=True).replace('nan', np.nan)
    if series.dtype == 'object': 
        series = series.str.strip()
    return series

def get_total_count_for_query(conn, sql_query):
    """ 현재 쿼리에서 LIMIT을 제거하고 총 행 개수를 반환합니다. """
    try:
        import re
        cleaned_query = re.sub(r'LIMIT\s+\d+\s*;?\s*$', '', sql_query.strip(), flags=re.IGNORECASE).rstrip('; ')
        
        if not cleaned_query.lower().startswith('select') and not cleaned_query.lower().startswith('with'):
             return None, "유효하지 않은 쿼리"
             
        # 전체를 서브쿼리로 감싸서 COUNT(*) 실행
        count_query = f"SELECT COUNT(*) FROM ({cleaned_query})"
        
        df_count = pd.read_sql_query(count_query, conn)
        total_count = df_count.iloc[0, 0]
        return total_count, None
    except Exception as e:
        return None, str(e)
    
# ------------------------------------------------------------------------------
# (추가) 날짜 기본값 계산 유틸리티
# ------------------------------------------------------------------------------
@st.cache_data
def get_default_dates():
    """ 날짜 계산 결과를 명시적으로 캐시합니다. """
    today = datetime.now().date()
    # 30일 전 날짜 계산
    default_start = today - timedelta(days=1) 
    return today, default_start


# ------------------------------------------------------------------------------
# 0-2. 데이터 로드 및 저장 함수
# ------------------------------------------------------------------------------
@st.cache_data(show_spinner="CSV 파일 분석 및 전처리 중...")
def load_data_with_dynamic_header(uploaded_file, test_week):
    """ 동적 헤더 탐색 및 데이터 전처리 후 DataFrame 반환 (원본 로직 재현) """
    try:
        file_content = uploaded_file.getvalue()
        file_io = io.StringIO(file_content.decode('utf-8'))
        df = pd.read_csv(file_io, header=4) 
        
        # --- 전처리 로직 ---
        df.columns = [str(col).strip() for col in df.columns] 
        object_cols_to_clean = ['SNumber', 'PcbSleepCurr', 'PcbMaxSleepCurr', 'PcbMinSleepCurr', 'PcbBatVolt', 'PcbMaxBatVolt', 'PcbMinBatVolt', 'PcbIrCurr', 'PcbMaxIrCurr', 'PcbMinIrCurr', 'PcbIrPwr', 'PcbMaxIrPwr', 'PcbMinIrPwr', 'PcbWirelessVolt', 'PcbMaxWirelessVolt', 'PcbMinWirelessVolt', 'PcbUsbCurr', 'PcbMaxUsbCurr', 'PcbMinUsbCurr', 'PcbWirelessUsbVolt', 'PcbMaxWirelessUsbVolt', 'PcbMinWirelessUsbVolt', 'PcbLed', 'PcbMaxLed', 'PcbMinLed', 'PcbPass']
        for col in object_cols_to_clean:
            if col in df.columns: df[col] = clean_excel_string(df[col])
        numeric_pcb_cols = [col for col in object_cols_to_clean if col not in ['SNumber', 'PcbPass']]
        for col in numeric_pcb_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        TIME_FORMAT_COMPACT = '%Y%m%d%H%M%S'
        df['PcbStartTime'] = pd.to_datetime(df['PcbStartTime'].astype(str).str.replace(r'="', '', regex=True).str.replace(r'"', '', regex=True).str.strip(), errors='coerce', format=TIME_FORMAT_COMPACT)
        df['PcbStopTime'] = pd.to_datetime(df['PcbStopTime'].astype(str).str.replace(r'="', '', regex=True).str.replace(r'"', '', regex=True).str.strip(), errors='coerce', format=TIME_FORMAT_COMPACT)
        df['Stamp'] = pd.to_datetime(df['Stamp'], errors='coerce', format='%Y-%m-%d %H:%M:%S', exact=False)

        # 3. DB 테이블용 DataFrame 생성
        model_name = 'SJ_TM2360E'; model_suffix = 'PCB'
        df_product = df[['SNumber']].drop_duplicates().copy()
        df_product['ProductModel_Name'] = model_name; df_product['ProductModel_Suffix'] = model_suffix
            
        specs = [];
        for item in BASE_MEASUREMENTS:
            min_col = f'PcbMin{item[3:]}'; max_col = f'PcbMax{item[3:]}'
            if min_col in df.columns and max_col in df.columns:
                min_val = df[min_col].dropna().iloc[0] if not df[min_col].dropna().empty else np.nan
                max_val = df[max_col].dropna().iloc[0] if not df[max_col].dropna().empty else np.nan
                specs.append({'TestItemName': item, 'MinLimit': min_val, 'MaxLimit': max_val})
        df_spec = pd.DataFrame(specs)
        
        history_cols_base = ['Unnamed: 0', 'SNumber', 'Stamp', 'ICount', 'PcbStartTime', 'PcbStopTime', 'PcbPass']
        valid_history_cols = [col for col in history_cols_base if col in df.columns] + [col for col in BASE_MEASUREMENTS if col in df.columns]
        df_history = df[valid_history_cols].copy()
        
        if 'Unnamed: 0' in df_history.columns: df_history.rename(columns={'Unnamed: 0': 'Original_Local_TestID'}, inplace=True)
        df_history['TestWeek'] = test_week 
        column_mapping = {'Stamp': 'TestStamp'}
        for col in BASE_MEASUREMENTS: 
            if col in df_history.columns: column_mapping[col] = f'{col}_Value'
        df_history = df_history.rename(columns=column_mapping)
        df_history.dropna(axis=1, how='all', inplace=True)
        
        return df_product, df_spec, df_history

    except Exception as e:
        st.error(f"파일 로드 및 전처리 중 오류 발생: {e}")
        return None, None, None


def save_dataframes_to_db(conn, df_product, df_spec, df_history):
    """ DataFrame들을 DB에 추가/덮어쓰기 합니다. """
    try:
        df_product.to_sql('PRODUCT', conn, if_exists='append', index=False)
        df_spec.to_sql('TEST_SPECIFICATION', conn, if_exists='replace', index=False)
        df_history.to_sql('TEST_HISTORY', conn, if_exists='append', index=True, index_label='History_PK')
        return True, len(df_history)
    except Exception as e:
        st.error(f"❌ DB 저장 중 오류 발생: {e}")
        return False, 0

# ------------------------------------------------------------------------------
# 0-3. DB 현황 및 삭제 함수
# ------------------------------------------------------------------------------
def get_db_status(conn):
    """ DB에 저장된 주차별, 모델별, 연도별 데이터 현황을 조회합니다. """
    query = """
    SELECT
        strftime('%Y', T1.TestStamp) AS Year,
        T1.TestWeek,
        T2.ProductModel_Suffix,
        COUNT(T1.History_PK) AS RecordCount,
        COUNT(DISTINCT T1.SNumber) AS UniqueProducts
    FROM TEST_HISTORY AS T1
    JOIN PRODUCT AS T2 ON T1.SNumber = T2.SNumber
    GROUP BY 1, 2, 3
    ORDER BY Year DESC, TestWeek DESC;
    """
    try:
        df_status = pd.read_sql_query(query, conn)
        return df_status
    except Exception:
        return pd.DataFrame(columns=['Year', 'TestWeek', 'ProductModel_Suffix', 'RecordCount', 'UniqueProducts'])

def delete_db_data(conn, year, suffix, week):
    """ TEST_HISTORY와 PRODUCT 테이블에서 조건에 맞는 데이터를 삭제합니다. """
    try:
        cursor = conn.cursor()
        
        find_sn_query = f"""
        SELECT DISTINCT T1.SNumber
        FROM TEST_HISTORY AS T1
        JOIN PRODUCT AS T2 ON T1.SNumber = T2.SNumber
        WHERE T1.TestWeek = '{week}' 
          AND T2.ProductModel_Suffix = '{suffix}' 
          AND strftime('%Y', T1.TestStamp) = '{year}';
        """
        df_sn = pd.read_sql_query(find_sn_query, conn)
        delete_sns = tuple(df_sn['SNumber'].tolist())
        
        if not delete_sns:
            return 0, 0, "삭제 조건에 맞는 SNumber가 없습니다."

        placeholders = ','.join(['?'] * len(delete_sns))

        delete_history_query = f"""
        DELETE FROM TEST_HISTORY
        WHERE TestWeek = '{week}' AND SNumber IN ({placeholders});
        """
        cursor.execute(delete_history_query, delete_sns)
        deleted_history_count = cursor.rowcount
        
        delete_product_query = f"""
        DELETE FROM PRODUCT
        WHERE SNumber IN ({placeholders})
          AND SNumber NOT IN (SELECT SNumber FROM TEST_HISTORY);
        """
        cursor.execute(delete_product_query, delete_sns)
        deleted_product_count = cursor.rowcount
        
        conn.commit()
        return deleted_history_count, deleted_product_count, None

    except Exception as e:
        return 0, 0, str(e)


# ==============================================================================
# 1. Streamlit 앱 레이아웃
# ==============================================================================

st.set_page_config(layout="wide", page_title="PCB DB 관리 앱")

st.title("PCB 제조 이력 DB 관리 및 분석 🛠️")

# --- 세션 상태 초기화 ---
if 'custom_query_result' not in st.session_state: st.session_state.custom_query_result = None
if 'custom_query_text' not in st.session_state: st.session_state.custom_query_text = """
SELECT * FROM (
    WITH All_Test_Results AS (
        -- TEST_HISTORY의 모든 측정값을 TEST_SPECIFICATION의 모든 항목과 결합합니다.
        SELECT
            T1.Original_Local_TestID, T1.SNumber, T1.PcbStartTime, T1.PcbStopTime,
            T2.TestItemName, T2.MinLimit, T2.MaxLimit,

            -- 각 TestItemName에 해당하는 측정값을 선택합니다.
            CASE
                WHEN T2.TestItemName = 'PcbSleepCurr' THEN T1.PcbSleepCurr_Value
                WHEN T2.TestItemName = 'PcbBatVolt' THEN T1.PcbBatVolt_Value
                WHEN T2.TestItemName = 'PcbIrCurr' THEN T1.PcbIrCurr_Value
                WHEN T2.TestItemName = 'PcbIrPwr' THEN T1.PcbIrPwr_Value
                WHEN T2.TestItemName = 'PcbWirelessVolt' THEN T1.PcbWirelessVolt_Value
                WHEN T2.TestItemName = 'PcbUsbCurr' THEN T1.PcbUsbCurr_Value
                WHEN T2.TestItemName = 'PcbWirelessUsbVolt' THEN T1.PcbWirelessUsbVolt_Value
                WHEN T2.TestItemName = 'PcbLed' THEN T1.PcbLed_Value
                -- 다른 항목이 있다면 여기에 추가합니다.
                ELSE NULL
            END AS Test_Value,

            -- 각 TestItemName에 대한 Spec_Result_Detail을 계산합니다.
            CASE
                WHEN T2.TestItemName = 'PcbSleepCurr' AND (T1.PcbSleepCurr_Value IS NULL OR T1.PcbSleepCurr_Value = 0.0) THEN '제외'
                WHEN T2.TestItemName = 'PcbBatVolt' AND (T1.PcbBatVolt_Value IS NULL OR T1.PcbBatVolt_Value = 0.0) THEN '제외'
                -- ... 다른 항목에 대해서도 '제외' 조건을 추가합니다.
                
                WHEN 
                    CASE
                        WHEN T2.TestItemName = 'PcbSleepCurr' THEN T1.PcbSleepCurr_Value
                        WHEN T2.TestItemName = 'PcbBatVolt' THEN T1.PcbBatVolt_Value
                        WHEN T2.TestItemName = 'PcbIrCurr' THEN T1.PcbIrCurr_Value
                        WHEN T2.TestItemName = 'PcbIrPwr' THEN T1.PcbIrPwr_Value
                        WHEN T2.TestItemName = 'PcbWirelessVolt' THEN T1.PcbWirelessVolt_Value
                        WHEN T2.TestItemName = 'PcbUsbCurr' THEN T1.PcbUsbCurr_Value
                        WHEN T2.TestItemName = 'PcbWirelessUsbVolt' THEN T1.PcbWirelessUsbVolt_Value
                        WHEN T2.TestItemName = 'PcbLed' THEN T1.PcbLed_Value
                        ELSE NULL
                    END > T2.MaxLimit THEN '초과'
                
                WHEN 
                    CASE
                        WHEN T2.TestItemName = 'PcbSleepCurr' THEN T1.PcbSleepCurr_Value
                        WHEN T2.TestItemName = 'PcbBatVolt' THEN T1.PcbBatVolt_Value
                        WHEN T2.TestItemName = 'PcbIrCurr' THEN T1.PcbIrCurr_Value
                        WHEN T2.TestItemName = 'PcbIrPwr' THEN T1.PcbIrPwr_Value
                        WHEN T2.TestItemName = 'PcbWirelessVolt' THEN T1.PcbWirelessVolt_Value
                        WHEN T2.TestItemName = 'PcbUsbCurr' THEN T1.PcbUsbCurr_Value
                        WHEN T2.TestItemName = 'PcbWirelessUsbVolt' THEN T1.PcbWirelessUsbVolt_Value
                        WHEN T2.TestItemName = 'PcbLed' THEN T1.PcbLed_Value
                        ELSE NULL
                    END < T2.MinLimit THEN '미달'
                
                WHEN 
                    CASE
                        WHEN T2.TestItemName = 'PcbSleepCurr' THEN T1.PcbSleepCurr_Value
                        WHEN T2.TestItemName = 'PcbBatVolt' THEN T1.PcbBatVolt_Value
                        WHEN T2.TestItemName = 'PcbIrCurr' THEN T1.PcbIrCurr_Value
                        WHEN T2.TestItemName = 'PcbIrPwr' THEN T1.PcbIrPwr_Value
                        WHEN T2.TestItemName = 'PcbWirelessVolt' THEN T1.PcbWirelessVolt_Value
                        WHEN T2.TestItemName = 'PcbUsbCurr' THEN T1.PcbUsbCurr_Value
                        WHEN T2.TestItemName = 'PcbWirelessUsbVolt' THEN T1.PcbWirelessUsbVolt_Value
                        WHEN T2.TestItemName = 'PcbLed' THEN T1.PcbLed_Value
                        ELSE NULL
                    END IS NOT NULL AND 
                    CASE
                        WHEN T2.TestItemName = 'PcbSleepCurr' THEN T1.PcbSleepCurr_Value
                        WHEN T2.TestItemName = 'PcbBatVolt' THEN T1.PcbBatVolt_Value
                        WHEN T2.TestItemName = 'PcbIrCurr' THEN T1.PcbIrCurr_Value
                        WHEN T2.TestItemName = 'PcbIrPwr' THEN T1.PcbIrPwr_Value
                        WHEN T2.TestItemName = 'PcbWirelessVolt' THEN T1.PcbWirelessVolt_Value
                        WHEN T2.TestItemName = 'PcbUsbCurr' THEN T1.PcbUsbCurr_Value
                        WHEN T2.TestItemName = 'PcbWirelessUsbVolt' THEN T1.PcbWirelessUsbVolt_Value
                        WHEN T2.TestItemName = 'PcbLed' THEN T1.PcbLed_Value
                        ELSE NULL
                    END BETWEEN T2.MinLimit AND T2.MaxLimit THEN 'Pass'

                ELSE '제외' -- 측정값이 NULL이거나 0.0인 경우를 포함
            END AS Spec_Result_Detail

        FROM TEST_HISTORY AS T1
        -- 크로스 조인(CROSS JOIN)을 사용하여 T1의 각 행이 T2의 모든 TestItemName과 연결되도록 합니다.
        JOIN TEST_SPECIFICATION AS T2 ON 1=1
        
        -- ⭐⭐⭐ 날짜 필터 추가 ⭐⭐⭐
         WHERE T1.PcbStartTime >= '2025-10-22' AND T1.PcbStartTime < '2025-10-23'
    ),
    
    Product_Status AS (
        -- 시리얼 번호(SNumber)별로 한 번이라도 'Pass'한 이력이 있는지 확인합니다.
        SELECT 
            SNumber, 
            MAX(CASE WHEN Spec_Result_Detail = 'Pass' THEN 1 ELSE 0 END) AS Has_Passed_Flag
        FROM All_Test_Results
        GROUP BY SNumber
    )
    
    SELECT
        ATR.PcbStartTime, ATR.PcbStopTime, ATR.Original_Local_TestID, ATR.SNumber, 
        ATR.TestItemName, ATR.Test_Value, ATR.MinLimit, ATR.MaxLimit,
        ATR.Spec_Result_Detail,
        
        -- 최종 불량 유형을 결정합니다.
        CASE
            WHEN ATR.Spec_Result_Detail = 'Pass' THEN 'Pass'
            WHEN ATR.Spec_Result_Detail IN ('미달', '초과', '제외') AND PS.Has_Passed_Flag = 1 THEN '가성불량'
            WHEN ATR.Spec_Result_Detail IN ('미달', '초과', '제외') AND PS.Has_Passed_Flag = 0 THEN '진성불량'
            ELSE ATR.Spec_Result_Detail
        END AS Final_Failure_Category
        
    FROM All_Test_Results AS ATR
    JOIN Product_Status AS PS ON ATR.SNumber = PS.SNumber
    -- 결과를 제한하여 너무 많은 행이 반환되는 것을 방지합니다.
    
)
"""

# "SELECT TestWeek, COUNT(History_PK) FROM TEST_HISTORY GROUP BY TestWeek;"
if 'mode' not in st.session_state: st.session_state.mode = '1. 데이터 저장/추가 (CSV Insert)'
if 'data_loaded' not in st.session_state: st.session_state.data_loaded = os.path.exists(DB_FILE)
if 'query_limit' not in st.session_state: st.session_state.query_limit = 100
if 'filters' not in st.session_state: st.session_state.filters = []
if 'filtered_df' not in st.session_state: st.session_state.filtered_df = None
if 'uploaded_files_data' not in st.session_state: st.session_state.uploaded_files_data = {}

# # 💡 누락된 DB 경로/파일명 초기화 추가
if 'db_path_input' not in st.session_state: st.session_state.db_path_input = "."
if 'db_name_input' not in st.session_state: st.session_state.db_name_input = DB_FILE
# # ----------------------------------------

# --- 모드 선택 라디오 버튼 (3가지 모드) ---
selected_mode = st.radio( 
    "작업 모드 선택:",
    ['2. DB 조회 및 분석', '1. 데이터 저장/추가 (CSV Insert)', '3. 주차별 DB 데이터 삭제'],
    # index=['1. 데이터 저장/추가 (CSV Insert)', '2. DB 조회 및 분석', '3. 주차별 DB 데이터 삭제'].index(st.session_state.mode),
    index=0,
    horizontal=False
)
st.session_state.mode = selected_mode
st.markdown("---")

conn = get_db_connection(DB_FILE)

# ==============================================================================
# MODE 1: 데이터 저장/추가 로직
# ==============================================================================
if st.session_state.mode == '1. 데이터 저장/추가 (CSV Insert)':
    st.header("1. CSV 파일 업로드 및 DB 누적 저장")

    # 1. DB 현황판 표시
    df_status = get_db_status(conn)
    st.subheader("현재 DB에 저장된 주차별 상세 데이터 현황")
    if not df_status.empty:
        st.dataframe(df_status, use_container_width=True)
    else:
        st.info("DB에 저장된 데이터가 없습니다. CSV 파일을 업로드하여 DB를 생성하세요.")

    st.markdown("---")
    st.subheader("새 CSV 데이터 처리")

    # 2. 업로드할 주차/모델 선택 UI
    col_week, col_suffix = st.columns(2)
    with col_week:
        selected_week = st.selectbox("1. 업로드할 데이터 주차 선택 (WXX)", options=['W38', 'W39', 'W40', 'W41', 'W42', 'W43', 'UNKNOWN'], key='input_week_select')
    with col_suffix:
        selected_suffix = st.text_input("2. ProductModel_Suffix 입력 (예: PCB)", value="PCB", key='input_suffix_text')

    uploaded_files = st.file_uploader(
        "3. CSV 파일 업로드 (복수 파일 가능)",
        type=['csv'],
        accept_multiple_files=True,
        key='insert_uploader',
        on_change=lambda: st.session_state.uploaded_files_data.clear() # 새 파일 업로드 시 이전 데이터 초기화
    )
    
    # 3. 파일 업로드 시 즉시 데이터 처리 및 미리보기
    if uploaded_files:
        st.markdown("---")
        st.subheader("4. 업로드 데이터 미리보기 및 저장 필드 선택")
        
        files_data_to_process = {} # 임시 저장소
        
        for file in uploaded_files:
            file_key = file.name
            
            # 파일이 세션 상태에 없으면 처리 시작
            if file_key not in st.session_state.uploaded_files_data:
                
                # 데이터 로드 및 전처리 (캐시 함수 호출)
                df_prod, df_spec, df_hist = load_data_with_dynamic_header(file, selected_week)
                
                if df_hist is not None:
                    initial_cols = df_hist.columns.tolist()
                    st.session_state.uploaded_files_data[file_key] = {
                        'df_prod': df_prod,
                        'df_spec': df_spec,
                        'df_history_original': df_hist,
                        'selected_cols': initial_cols, # 초기에는 모든 컬럼 선택
                        'test_week': selected_week
                    }
        
        # 4. 미리보기 UI 및 필드 선택
        
        total_rows_to_save = 0
        
        for file_key, data in st.session_state.uploaded_files_data.items():
            total_rows_to_save += len(data['df_history_original'])
            
            # 💡 파일별 Expander와 필드 선택 UI
            with st.expander(f"📁 파일: {file_key} (주차: {data['test_week']}, 총 {len(data['df_history_original'])}행)", expanded=True):
                
                st.markdown("##### 저장할 필드 선택/제거")
                all_cols = data['df_history_original'].columns.tolist()
                
                # Multiselect 위젯을 사용하여 필드 선택
                selected_cols = st.multiselect(
                    "필드명 추가 또는 제거",
                    options=all_cols,
                    default=data['selected_cols'], # 이전 선택값 유지
                    key=f'multiselect_insert_{file_key}' # 고유 키 사용
                )
                
                # 선택된 컬럼을 세션 상태에 저장 (다음 저장 버튼 클릭 시 사용)
                st.session_state.uploaded_files_data[file_key]['selected_cols'] = selected_cols
                
                # 미리보기 테이블 출력 (선택된 컬럼만)
                if selected_cols:
                    df_preview = data['df_history_original'][selected_cols].head(5)
                    st.markdown("##### 📄 미리보기 (선택 필드)")
                    st.dataframe(df_preview, use_container_width=True)
        
        st.markdown("---")
        st.markdown(f"#### 💾 최종 저장 예정 행 수: {total_rows_to_save} 행")
        
        # 5. 최종 저장 버튼
        if st.button("DB에 데이터 저장/추가 실행", key='final_save_btn'):
            
            total_rows_added = 0
            
            for file_key, data in st.session_state.uploaded_files_data.items():
                
                df_hist_filtered = data['df_history_original'][data['selected_cols']].copy()
                data['df_prod']['ProductModel_Suffix'] = selected_suffix
                
                success, added_rows = save_dataframes_to_db(
                    conn, 
                    data['df_prod'], 
                    data['df_spec'], 
                    df_hist_filtered # 필터링된 DF 전달
                )
                if success:
                    st.success(f"✔️ {file_key} ({data['test_week']}) 파일 데이터 {added_rows}행 DB에 성공적으로 추가.")
                    total_rows_added += added_rows

            if total_rows_added > 0:
                st.toast(f"총 {total_rows_added}행의 데이터가 DB에 추가되었습니다.", icon='🎉')
                st.session_state.data_loaded = True
                st.session_state.uploaded_files_data = {} # 저장 완료 후 초기화
                st.rerun() # UI 갱신
            else:
                st.warning("DB에 저장된 데이터가 없습니다. 저장된 파일이 있는지 확인하세요.")
                
    elif not os.path.exists(DB_FILE):
         st.info("DB 파일이 존재하지 않습니다. CSV 파일을 업로드하여 DB를 생성하세요.")


# ==============================================================================
# MODE 2: DB 조회 및 분석 로직
# ==============================================================================
elif st.session_state.mode == '2. DB 조회 및 분석':
    st.header("2. DB 테이블 조회 및 관리")
#  ########   
    # # 💡 DB 파일 경로 입력 UI
    # st.subheader("DB 연결 설정")
    # # col_db_path, col_db_name = st.columns([2, 1])
    # col_db_path, col_db_name, col_db_download = st.columns([2, 1, 1])
    
    # with col_db_path:
    #     # 경로 입력 필드
    #     db_path = st.text_input("DB 파일 경로 (예: C:/data 또는 .)", value=st.session_state.db_path_input, key='db_path_mode2')
    #     st.session_state.db_path_input = db_path
    
    # with col_db_name:
    #     # 파일명 입력 필드
    #     db_name = st.text_input("DB 파일명 (예: analysis.db)", value=st.session_state.db_name_input, key='db_name_mode2')
    #     st.session_state.db_name_input = db_name
# ############
    # 1. DB 현황판 업데이트 및 초기 쿼리 실행
    df_status = get_db_status(conn)
    st.subheader("DB에 저장된 주차별 상세 데이터 현황 (현재 연결 파일: {DB_FILE_DEFAULT})")
    if not df_status.empty:
        st.dataframe(df_status, use_container_width=True)
        # 💡 모드 전환 시 초기 쿼리 실행
        if st.session_state.custom_query_result is None:
            try:
                df_initial = pd.read_sql_query(st.session_state.custom_query_text, conn)
                st.session_state.custom_query_result = df_initial
                st.session_state.filters = []
                st.session_state.filtered_df = df_initial.copy()
            except Exception:
                pass 
    else:
        st.info("DB 파일이 초기화되었거나 TEST_HISTORY 테이블에 데이터가 없습니다.")

#      ########   
#     # 💡 DB 파일 경로 입력 UI
#     st.subheader("DB 연결 설정")
#     col_db_path, col_db_name = st.columns([2, 1])

#     with col_db_path:
#         # 경로 입력 필드
#         db_path = st.text_input("DB 파일 경로 (예: C:/data 또는 .)", value=st.session_state.db_path_input, key='db_path_mode2')
#         st.session_state.db_path_input = db_path
    
#     with col_db_name:
#         # 파일명 입력 필드
#         db_name = st.text_input("DB 파일명 (예: analysis.db)", value=st.session_state.db_name_input, key='db_name_mode2')
#         st.session_state.db_name_input = db_name
# ############
##################
    # 💡 다운로드 버튼 로직
    
    # full_db_path_download = os.path.join(db_path, db_name)
    
    # with col_db_download:
    #     st.markdown("<br>", unsafe_allow_html=True) # UI 정렬용
        
    #     # 파일이 실제로 존재하고 연결이 성공했을 때만 다운로드 버튼 표시
    #     if os.path.exists(full_db_path_download):
    #         with open(full_db_path_download, "rb") as file:
    #             st.download_button(
    #                 label="DB 파일 다운로드 (.db)",
    #                 data=file,
    #                 file_name=db_name, # 입력된 DB 파일명 사용
    #                 mime="application/octet-stream"
    #             )
    #     else:
    #         st.info("DB 파일이 존재하지 않거나 경로가 잘못되었습니다.")
            
################
#############
    # 💡 DB 다운로드 기능 (현황판 밑에 생성)
    st.markdown("---")
    st.subheader("DB 파일 다른 이름으로 저장 (다운로드)")
    
    col_name, col_download_btn = st.columns([3, 1])

    with col_name:
        db_name = st.text_input("다운로드 시 저장할 파일명 입력 (.db 포함)", value=st.session_state.db_name_input, key='db_name_download')
        st.session_state.db_name_input = db_name

    with col_download_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        
        if os.path.exists(DB_FILE):
            try:
                with open(DB_FILE, "rb") as file:
                    st.download_button(
                        label="DB 파일 다운로드 실행",
                        data=file,
                        file_name=db_name,
                        mime="application/octet-stream",
                        key='db_download_final_btn'
                    )
            except Exception as e:
                st.error(f"DB 파일 읽기 오류: {e}")
        else:
            st.warning(f"현재 연결된 DB 파일({DB_FILE})이 디스크에 존재하지 않습니다.")

    st.markdown("---")
        
    ####################333    
    # 2. 메인 조회 및 필터링 로직
    if not get_table_names(conn):
        st.warning(f"DB 파일에 테이블이 존재하지 않습니다. 모드 1에서 먼저 데이터를 저장해주세요.")
    else:
################
        # # 💡 기간 입력 위젯
        # st.markdown("##### 📅 쿼리 기간 설정 (PcbStartTime 기준)")
        # # today = datetime.now().date()
        # # default_start = today - timedelta(days=30)
        # today_date, default_start_date = get_default_dates() # 💡 함수 호출로 변수 생성

        # col_date_from, col_date_to = st.columns(2)
        # with col_date_from:
        #     date_from = st.date_input("FROM Date", value=default_start_date, key='date_from_input')
        # with col_date_to:
        #     date_to = st.date_input("TO Date", value=today_date, key='date_to_input')
 ######################       
        # --- 2-1. 테이블 목록 선택 및 필드 편집 ---
        # (이전 답변의 2-1 로직 재배치)
        st.markdown("---")
        st.subheader("2-1. 테이블 필드 선택 조회 (Raw Data)")
        table_names = get_table_names(conn)
        col_select, col_display = st.columns([1, 2])
        
        with col_select:
            selected_table = st.selectbox("조회할 테이블 선택", table_names, key='table_select_2_1')
        
        try:
            df_full = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 100", conn)
            
            with col_select:
                all_columns = df_full.columns.tolist()
                selected_columns = st.multiselect(
                    "필드명 추가 또는 제거", 
                    all_columns, 
                    default=all_columns,
                    key=f'multiselect_{selected_table}'
                )
            
            if selected_columns:
                columns_str = ", ".join([f'"{c}"' for c in selected_columns])
                query_filtered = f"SELECT {columns_str} FROM {selected_table}"
                df_filtered = pd.read_sql_query(query_filtered, conn)
                
                with col_display:
                    st.markdown(f"**테이블 `{selected_table}` 조회 결과 (선택 필드)**")
                    st.dataframe(df_filtered.head(100), use_container_width=True)
            else:
                with col_display:
                    st.info("조회할 필드를 선택해주세요.")
                    
        except Exception as e:
            st.error(f"테이블 조회 오류: {e}")
  ###################      
        # # 💡 기간 입력 위젯
        # st.markdown("##### 📅 쿼리 기간 설정 (PcbStartTime 기준)")
        # # today = datetime.now().date()
        # # default_start = today - timedelta(days=30)
        # today_date, default_start_date = get_default_dates() # 💡 함수 호출로 변수 생성

        # col_date_from, col_date_to = st.columns(2)
        # with col_date_from:
        #     date_from = st.date_input("FROM Date", value=default_start_date, key='date_from_input')
        # with col_date_to:
        #     date_to = st.date_input("TO Date", value=today_date, key='date_to_input')
 ####################           
        # --- 2-2. 사용자 직접 SQL 쿼리 실행 및 필드 편집 ---
        st.markdown("---")
        st.subheader("2-2. 사용자 직접 SQL 쿼리 실행 및 멀티 필터링")
        # ##########
        # # 💡 기간 입력 위젯
        # st.markdown("##### 📅 쿼리 기간 설정 (PcbStartTime 기준)")
        # today = datetime.now().date()
        # default_start = today - timedelta(days=30)
        
        # col_date_from, col_date_to = st.columns(2)
        # with col_date_from:
        #     date_from = st.date_input("FROM Date", value=default_start, key='date_from_input')
        # with col_date_to:
        #     date_to = st.date_input("TO Date", value=today, key='date_to_input')
        
        # ########
        col_sql, col_limit = st.columns([2, 1])

        with col_sql:
            sql_query = st.text_area("실행할 SQL 쿼리 입력 (SELECT 또는 WITH로 시작)", value=st.session_state.custom_query_text, height=150, key='sql_input')
            
        with col_limit:
            st.markdown("##### 📌 쿼리 결과 LIMIT 설정")
            # (Total Count 및 LIMIT 입력 로직)
            total_rows, error_msg = get_total_count_for_query(conn, sql_query)
            if total_rows is not None: st.markdown(f"**총 예상 행 개수:** (`{total_rows}행`)")
            else: st.info(f"총 행 개수 확인 불가 ({error_msg})")
            
            new_limit_str = st.text_input("LIMIT 값 입력 (기본: 100)", value=str(st.session_state.query_limit), key='custom_limit_input')
            
            if st.button("SQL 쿼리 실행", key='run_sql_button'):
                try:
                    final_limit = int(new_limit_str) if new_limit_str.strip().isdigit() and int(new_limit_str) > 0 else st.session_state.query_limit
                    cleaned_query = re.sub(r'LIMIT\s+\d+\s*;?\s*$', '', sql_query.strip(), flags=re.IGNORECASE).rstrip('; ')
                    final_query = f"{cleaned_query} LIMIT {final_limit}"
                    
                    df_custom = pd.read_sql_query(final_query, conn)
                    st.session_state.custom_query_result = df_custom.copy()
                    st.session_state.filters = []
                    st.session_state.filtered_df = df_custom.copy()
                    st.success(f"쿼리 실행 성공! (LIMIT: {final_limit})")

                except Exception as e:
                    st.error(f"❌ SQL 실행 오류: {e}")
                    st.session_state.custom_query_result = None 
                    
        # ##########
        # # 💡 기간 입력 위젯
        # st.markdown("##### 📅 쿼리 기간 설정 (PcbStartTime 기준)")
        # # today = datetime.now().date()
        # # default_start = today - timedelta(days=30)
        # today_date, default_start_date = get_default_dates() # 💡 함수 호출로 변수 생성

        # col_date_from, col_date_to = st.columns(2)
        # with col_date_from:
        #     date_from = st.date_input("FROM Date", value=default_start_date, key='date_from_input')
        # with col_date_to:
        #     date_to = st.date_input("TO Date", value=today_date, key='date_to_input')

        # col_date_from, col_date_to = st.columns(2)
        # with col_date_from:
        #     date_from = st.date_input("FROM Date", value=default_start, key='date_from_input')
        # with col_date_to:
        #     date_to = st.date_input("TO Date", value=today, key='date_to_input')
        
        # ########

        # 💡 결과 출력 및 필드 편집/검색 로직
        if st.session_state.custom_query_result is not None:
            
            df_full_result = st.session_state.custom_query_result.copy()
            all_result_columns = df_full_result.columns.tolist()
            
            # --- 3. 최종 결과 테이블 출력 (상단) ---
            st.markdown("---")
            st.markdown("##### 📌 SQL 쿼리 실행 결과")
            
            df_to_display = st.session_state.filtered_df if st.session_state.filtered_df is not None else df_full_result
            
            selected_final_columns = st.multiselect("3. 최종 출력 필드 선택", all_result_columns, default=all_result_columns, key='final_output_selector_vertical')
            
            if selected_final_columns:
                df_edited = df_to_display[selected_final_columns]
                st.markdown(f"**SQL 쿼리 결과 (총 {len(df_to_display)}건)**")
                st.dataframe(df_edited, use_container_width=True) 

            st.markdown("---") 
            st.markdown("##### ✨ 1. 검색 조건 추가/관리")

            # --- 필터 조건 추가 UI ---
            col_add_field, col_add_op, col_add_value, col_add_btn = st.columns([1, 0.7, 1.3, 0.5])
            with col_add_field: filter_col_add = st.selectbox("필드", options=all_result_columns, key='filter_col_add_key')
            with col_add_op: filter_op_add = st.selectbox("조건", options=['포함', '일치', '시작', '종료'], key='filter_op_add_key')
            with col_add_value: filter_value_add = st.text_input("검색 값", value="", key='filter_value_add_key')
            with col_add_btn:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("필터 추가", key='add_filter_btn'):
                    if filter_value_add:
                        st.session_state.filters.append({'col': filter_col_add, 'op': filter_op_add, 'val': filter_value_add})
                        st.success(f"필터 추가됨: {filter_col_add} {filter_op_add} '{filter_value_add}'")
                    else:
                        st.warning("검색 값을 입력해주세요.")

            st.markdown("---")
            st.markdown("##### 2. 현재 적용된 필터 목록")

            # --- 현재 필터 목록 및 제거 UI ---
            if st.session_state.filters:
                for i, f in enumerate(st.session_state.filters):
                    col_view, col_remove = st.columns([3, 0.5])
                    with col_view: st.write(f"**{i+1}.** `{f['col']}` {f['op']} **'{f['val']}'**")
                    with col_remove:
                        if st.button("제거", key=f'remove_filter_{i}'):
                            st.session_state.filters.pop(i)
                            st.rerun() 

            else:
                st.info("추가된 검색 조건이 없습니다.")

            st.markdown("---")
            
            # --- 검색 시작 버튼 및 필터링 로직 ---
            if st.button("검색 시작", key='apply_search_btn'):
                df_filtered = df_full_result.copy()
                
                for f in st.session_state.filters:
                    col, op, val = f['col'], f['op'], f['val']
                    if col in df_filtered.columns:
                        series = df_filtered[col].astype(str).str.lower()
                        val_lower = str(val).lower()
                        
                        if op == '포함': df_filtered = df_filtered[series.str.contains(val_lower, na=False)]
                        elif op == '일치': df_filtered = df_filtered[series == val_lower]
                        elif op == '시작': df_filtered = df_filtered[series.str.startswith(val_lower, na=False)]
                        elif op == '종료': df_filtered = df_filtered[series.str.endswith(val_lower, na=False)]
                
                st.session_state.filtered_df = df_filtered.copy()
                st.success(f"검색 결과 적용 완료! 총 {len(df_filtered)}건 조회되었습니다.")


# ==============================================================================
# MODE 3: 주차별 DB 데이터 삭제 로직
# ==============================================================================
elif st.session_state.mode == '3. 주차별 DB 데이터 삭제':
    st.header("3. 주차별 데이터 삭제")
    st.warning("⚠️ 경고: 데이터 삭제는 되돌릴 수 없습니다. 조건을 정확히 확인하세요.")

    df_status = get_db_status(conn)
    
    if df_status.empty:
        st.error("삭제할 데이터가 DB에 존재하지 않습니다. '2. DB 조회 및 분석' 모드에서 확인하세요.")
    else:
        st.markdown("##### 현재 DB 현황 (삭제 기준)")
        st.dataframe(df_status, use_container_width=True)

        # 2. 조건 선택 UI
        st.markdown("---")
        st.subheader("삭제할 주차/모델 선택")

        col_year, col_suffix, col_week = st.columns(3)
        
        unique_years = df_status['Year'].unique().tolist()
        unique_suffixes = df_status['ProductModel_Suffix'].unique().tolist()

        with col_year:
            year_to_delete = st.selectbox("1. 삭제할 연도 선택", unique_years)
        with col_suffix:
            suffix_to_delete = st.selectbox("2. 삭제할 ProductModel_Suffix 선택", unique_suffixes)
        
        filtered_weeks = df_status[
            (df_status['Year'] == year_to_delete) & 
            (df_status['ProductModel_Suffix'] == suffix_to_delete)
        ]['TestWeek'].unique().tolist()

        with col_week:
            week_to_delete = st.selectbox("3. 삭제할 주차(TestWeek) 선택", filtered_weeks)

        # 3. 삭제 확인 버튼
        st.markdown("---")
        if st.button(f"위 조건으로 데이터 삭제 실행", type="primary"):
            
            hist_count, prod_count, error = delete_db_data(
                conn, year_to_delete, suffix_to_delete, week_to_delete
            )
            
            if error is None:
                st.success(f"✅ 데이터 삭제 성공!")
                st.markdown(f"- **TEST_HISTORY** 테이블에서 **{hist_count}행** 삭제됨.")
                st.markdown(f"- **PRODUCT** 테이블에서 **{prod_count}행** 삭제됨 (다른 기록이 없는 제품).")
                st.info("현황을 갱신합니다. '2. DB 조회 및 분석' 모드를 다시 선택하세요.")
                st.rerun() 

            else:
                st.error(f"❌ 데이터 삭제 오류 발생: {error}")
