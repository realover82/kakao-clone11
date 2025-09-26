# csv2.py

import pandas as pd
import numpy as np
import io
from datetime import datetime
import warnings
import streamlit as st

warnings.filterwarnings('ignore')

def clean_string_format(value):
    """'="...' 형식의 문자열을 정리하는 함수"""
    if isinstance(value, str) and value.startswith('="') and value.endswith('"'):
        return value[2:-1]
    return value

# csv2.py 파일의 read_csv_with_dynamic_header 함수 수정

def read_csv_with_dynamic_header(uploaded_file):
    """
    PCB 데이터에 맞는 키워드로 헤더를 찾아 DataFrame을 로드하는 함수.
    헤더를 찾기 위해 파일을 문자열로 읽어와 유연하게 처리합니다.
    """
    try:
        file_content = uploaded_file.getvalue()
        encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr', 'latin1']
        df = None
        
        search_keywords = ['snumber', 'pcbstarttime', 'pcbmaxirpwr', 'pcbpass', 'pcbsleepcurr']

        for encoding in encodings:
            try:
                file_string = file_content.decode(encoding)
                file_io = io.StringIO(file_string)
                
                # 헤더 찾기: 전체 내용을 한 번에 읽어와 분석
                df_temp = pd.read_csv(file_io, header=None, na_filter=False, dtype=str, skipinitialspace=True)
                
                header_row = None
                for i, row in df_temp.iterrows():
                    row_values_lower = [str(x).strip().lower().replace('\t', '') for x in row.values]
                    
                    # 필수 키워드가 모두 포함된 행을 찾습니다.
                    if all(keyword in row_values_lower for keyword in search_keywords):
                        header_row = i
                        break
                
                if header_row is not None:
                    file_io.seek(0)
                    df = pd.read_csv(file_io, header=header_row, dtype=str, skipinitialspace=True)
                    
                    # === 필드 매핑 로직 (기존 로직 유지) ===
                    actual_field_mapping = []
                    actual_cols_lower = {col.strip().lower(): col for col in df.columns}
                    
                    for keyword in search_keywords:
                        if keyword in actual_cols_lower:
                            actual_field_mapping.append(actual_cols_lower[keyword])

                    if 'field_mapping' not in st.session_state:
                        st.session_state.field_mapping = {}
                        
                    st.session_state.field_mapping['Pcb'] = actual_field_mapping
                    # =======================================
                    
                    return df
            except UnicodeDecodeError:
                continue
            except Exception:
                continue
        
        st.error("파일 헤더를 찾을 수 없습니다. 필수 컬럼이 누락되었거나 형식이 다릅니다.")
        return None
    except Exception as e:
        st.error(f"파일을 읽는 중 심각한 오류가 발생했습니다: {e}")
        return None

    
def apply_qc_check(df, main_col):
    """특정 컬럼에 대해 Min/Max 컬럼을 찾아 '미달', '초과', 'Pass'를 분류하는 함수"""
    
    # 1. 대응되는 Min/Max 컬럼 이름 찾기
    min_col_name = main_col.replace('Pcb', 'PcbMin')
    max_col_name = main_col.replace('Pcb', 'PcbMax')
    
    cols_lower = {col.strip().lower(): col for col in df.columns}
    
    try:
        min_col_actual = cols_lower[min_col_name.lower()]
        max_col_actual = cols_lower[max_col_name.lower()]
    except KeyError:
        # Min/Max 컬럼이 없으면 경고 메시지를 출력하고 건너뜁니다.
        st.warning(f"QC 체크 건너뜀: '{main_col}'에 대한 필수 제한 컬럼 ('{min_col_name}' 또는 '{max_col_name}')을 찾을 수 없습니다. 컬럼 이름을 확인해주세요.")
        return df 

    # 2. QC Status 컬럼을 즉시 생성 및 비교를 위한 숫자 변환
    qc_col = main_col + '_QC'
    df[qc_col] = 'Pass'
    
    # 비교를 위해 숫자로 변환합니다 (변환 실패 시 NaN).
    main_values = pd.to_numeric(df[main_col].astype(str).str.strip(), errors='coerce')
    min_limits = pd.to_numeric(df[min_col_actual].astype(str).str.strip(), errors='coerce')
    max_limits = pd.to_numeric(df[max_col_actual].astype(str).str.strip(), errors='coerce')

    # === 3. '측정값 0' 제외 로직 (최우선 적용) ===
    # 측정된 수치가 0인 모든 행을 '제외'로 분류합니다.
    is_zero_value = (main_values == 0)
    df.loc[is_zero_value, qc_col] = '제외'
    
    # 4. 나머지 QC 로직 적용 (0이 아니며, 제외되지 않은 행에 대해서만)
    
    # '제외'로 분류되지 않은 행을 식별합니다.
    is_not_excluded = ~is_zero_value
    
    # 미달 (Below Min)
    below_min = (main_values < min_limits) & is_not_excluded
    df.loc[below_min, qc_col] = '미달'
    
    # 초과 (Above Max)
    above_max = (main_values > max_limits) & is_not_excluded
    df.loc[above_max, qc_col] = '초과'
    
    # 데이터 부족/결측치 처리
    # is_not_excluded인 행 중에서, 값 중 하나라도 NaN인 경우
    is_na = (main_values.isnull() | min_limits.isnull() | max_limits.isnull()) & is_not_excluded
    df.loc[is_na, qc_col] = '데이터 부족' 
    
    return df

def analyze_data(df):
    """
    PCB 데이터의 분석 로직을 담고 있는 함수.
    PcbStartTime 컬럼의 다양한 타임스탬프 형식을 처리하고, 상세 데이터를 전체 컬럼으로 저장합니다.
    """
    # 데이터 전처리
    for col in df.columns:
        df[col] = df[col].apply(clean_string_format)

    # === 새로운 QC 체크 로직 적용 시작 ===
    
    # QC 체크를 원하는 모든 메인 측정 컬럼 목록
    qc_columns = [
        'PcbSleepCurr',  'PcbIrCurr', 'PcbIrPwr', 
        'PcbWirelessVolt', 'PcbBatVolt', 'PcbUsbCurr', 'PcbWirelessUsbVolt','PcbLed'
    ] # 'PcbBatVolt', 'PcbUsbCurr', 'PcbWirelessUsbVolt',

    for col_name in qc_columns:
        df = apply_qc_check(df, col_name)

    # === 새로운 QC 체크 로직 적용 완료 ===
    # === PassStatusNorm 컬럼 생성 (최우선) ===
    pass_col = 'PcbPass'
    try:
        pass_col_actual = next(col for col in df.columns if col.strip().lower() == pass_col.lower())
        df['PassStatusNorm'] = df[pass_col_actual].fillna('').astype(str).str.strip().str.upper()
    except StopIteration:
        st.error(f"'{pass_col}' 컬럼을 찾을 수 없어 'PassStatusNorm' 생성에 실패했습니다.")
        return None, None
    # =========================================================================

    # 타임스탬프 컬럼 이름 찾기
    timestamp_col = 'PcbStartTime'
    try:
        timestamp_col_actual = next(col for col in df.columns if col.strip().lower() == timestamp_col.lower())
    except StopIteration:
        st.error(f"'{timestamp_col}' 컬럼이 데이터에 없습니다.")
        return None, None

    # === 타임스탬프 변환 로직 ===
    
    # 1. YYYYMMDDHHmmss 형식 변환 시도 (문자열 전용)
    df['temp_converted'] = pd.to_datetime(df[timestamp_col_actual].astype(str).str.strip(), format='%Y%m%d%H%M%S', errors='coerce')

    # 2. 유닉스 타임스탬프 (초 단위) 변환 시도
    numeric_series = pd.to_numeric(df[timestamp_col_actual].astype(str).str.strip(), errors='coerce')
    seconds_converted = pd.to_datetime(numeric_series, unit='s', errors='coerce')
    milliseconds_converted = pd.to_datetime(numeric_series, unit='ms', errors='coerce')
    
    final_series = df['temp_converted'].copy()
    is_na = final_series.isnull()
    
    # 초 단위 (seconds) 변환 결과 사용 (1980년 이후의 날짜만 유효한 것으로 간주하여 1970년 에러 방지)
    is_valid_seconds = seconds_converted.notnull() & (seconds_converted.dt.year > 1980) 
    final_series[is_na & is_valid_seconds] = seconds_converted[is_na & is_valid_seconds]
    is_na = final_series.isnull()

    # 밀리초 단위 (milliseconds) 변환 결과 사용
    final_series[is_na] = milliseconds_converted[is_na]
    
    df = df.drop(columns=['temp_converted'], errors='ignore')
    
    if final_series.isnull().all():
        st.warning(f"타임스탬프 변환에 실패했습니다. '{timestamp_col_actual}' 컬럼의 형식을 확인해주세요.")
        return None, None
        
    df[timestamp_col_actual] = final_series
    
    # ======================================
    
    summary_data = {}
    
    jig_col = 'PcbMaxIrPwr'
    if jig_col not in df.columns:
        df[jig_col] = 'DefaultJig'

    jig_pass_history = df[df['PassStatusNorm'] == 'O'].groupby(jig_col)['SNumber'].unique().apply(set).to_dict()

    for jig, group in df.groupby(jig_col):
        group = group.dropna(subset=[timestamp_col_actual])
        if group.empty:
            continue
        
        for d, day_group in group.groupby(group[timestamp_col_actual].dt.date):
            if pd.isna(d):
                continue
            
            date_iso = pd.to_datetime(d).strftime("%Y-%m-%d")
            current_jig_passed_sns = jig_pass_history.get(jig, set())
            
            pass_df = day_group[day_group['PassStatusNorm'] == 'O']
            fail_df = day_group[day_group['PassStatusNorm'] == 'X']
            
            false_defect_df = fail_df[fail_df['SNumber'].isin(current_jig_passed_sns)]
            true_defect_df = fail_df[~fail_df['SNumber'].isin(current_jig_passed_sns)]
            
            # === 핵심 수정: 상세 데이터 저장 시 DataFrame의 모든 컬럼을 포함하도록 to_dict('records') 사용 ===
            pass_data = pass_df.to_dict('records')
            false_defect_data = false_defect_df.to_dict('records')
            true_defect_data = true_defect_df.to_dict('records')
            fail_data = fail_df.to_dict('records')
            # =========================================================================================

            pass_count = len(pass_df)
            false_defect_count = len(false_defect_df)
            true_defect_count = len(true_defect_df)
            fail_count = len(fail_df)
            total_test = len(day_group)
            rate = 100 * pass_count / total_test if total_test > 0 else 0

            if jig not in summary_data:
                summary_data[jig] = {}
            
            summary_data[jig][date_iso] = {
                'total_test': total_test,
                'pass': pass_count,
                'false_defect': false_defect_count,
                'true_defect': true_defect_count,
                'fail': fail_count,
                'pass_rate': f"{rate:.1f}%",
                
                # 수정된 부분 반영: 모든 컬럼이 포함된 리스트 저장
                'pass_data': pass_data,
                'false_defect_data': false_defect_data,
                'true_defect_data': true_defect_data,
                'fail_data': fail_data,

                'pass_unique_count': len(pass_df['SNumber'].unique()),
                'false_defect_unique_count': len(false_defect_df['SNumber'].unique()),
                'true_defect_unique_count': len(true_defect_df['SNumber'].unique()),
                'fail_unique_count': len(fail_df['SNumber'].unique())
            }
    
    all_dates = sorted(list(df[timestamp_col_actual].dt.date.dropna().unique()))
    return summary_data, all_dates
