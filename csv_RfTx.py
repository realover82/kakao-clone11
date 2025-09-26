import pandas as pd
import numpy as np
import io
from datetime import datetime
import warnings
import streamlit as st

warnings.filterwarnings('ignore')

# '="...' 형식의 문자열을 정리하는 함수
def clean_string_format(value):
    if isinstance(value, str) and value.startswith('="') and value.endswith('"'):
        return value[2:-1]
    return value

def read_csv_with_dynamic_header_for_RfTx(uploaded_file):
    """RfTx 데이터에 맞는 키워드로 헤더를 찾아 DataFrame을 로드하는 함수"""
    try:
        file_content = io.BytesIO(uploaded_file.getvalue())
        # 다양한 인코딩 시도
        encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr', 'latin1']
        df = None
        for encoding in encodings:
            try:
                file_content.seek(0)
                df_temp = pd.read_csv(file_content, header=None, nrows=100, encoding=encoding)
                keywords = ['SNumber', 'RfTxStamp', 'RfTxPC', 'RfTxPass']
                # st.session_state에 직접 키워드 리스트 저장
                if 'field_mapping' not in st.session_state:
                    st.session_state.field_mapping = {}
                st.session_state.field_mapping['RfTx'] = keywords
                header_row = None
                for i, row in df_temp.iterrows():
                    row_values = [str(x).strip() for x in row.values if pd.notna(x)]
                    if all(keyword in row_values for keyword in keywords):
                        header_row = i
                        break
                
                if header_row is not None:
                    file_content.seek(0)
                    df = pd.read_csv(file_content, header=header_row, encoding=encoding)
                    return df
            except Exception:
                continue
        return None
    except Exception as e:
        return None

# rftx.py 파일의 analyze_RfTx_data 함수 전체를 아래 코드로 교체하세요.


def analyze_RfTx_data(df):
    """RfTx 데이터의 분석 로직을 담고 있는 함수"""
    # 데이터 전처리
    for col in df.columns:
        df[col] = df[col].apply(clean_string_format)

    # === 수정된 타임스탬프 변환 로직 ===
    original_col_name = 'RfTxStamp'
    if original_col_name in df.columns:
        converted_series = None
        
        # 1. 밀리초(ms) 단위 변환 시도
        try:
            converted_series = pd.to_datetime(df[original_col_name], unit='ms', errors='coerce')
        except Exception:
            pass
        
        # 2. 초(s) 단위 변환 시도
        if converted_series is None or converted_series.isnull().all():
            try:
                converted_series = pd.to_datetime(df[original_col_name], unit='s', errors='coerce')
            except Exception:
                pass

        # 3. YYYY-MM-DD HH:MM:SS 형식 변환 시도
        if converted_series is None or converted_series.isnull().all():
            try:
                converted_series = pd.to_datetime(df[original_col_name], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            except Exception:
                pass

        # 4. YYYY/MM/DD HH:MM:SS 형식 변환 시도
        if converted_series is None or converted_series.isnull().all():
            try:
                converted_series = pd.to_datetime(df[original_col_name], format='%Y/%m/%d %H:%M:%S', errors='coerce')
            except Exception:
                pass
        
        # 5. 변환된 시리즈로 컬럼 업데이트
        if converted_series is not None and not converted_series.isnull().all():
            df[original_col_name] = converted_series
        else:
            st.warning(f"타임스탬프 변환에 실패했습니다. {original_col_name} 컬럼의 형식을 확인해주세요.")
            return None, None # 변환 실패 시 함수 종료
    else:
        st.error(f"'{original_col_name}' 컬럼이 데이터에 없습니다.")
        return None, None # 컬럼이 없을 시 함수 종료
    # ==================================

    df['PassStatusNorm'] = df['RfTxPass'].fillna('').astype(str).str.strip().str.upper()

    summary_data = {}
    
    if 'RfTxPC' not in df.columns:
        df['RfTxPC'] = 'DefaultJig'

    jig_pass_history = df[df['PassStatusNorm'] == 'O'].groupby('RfTxPC')['SNumber'].unique().apply(set).to_dict()

    for jig, group in df.groupby('RfTxPC'):
        group = group.dropna(subset=['RfTxStamp'])
        if group.empty:
            continue
        
        for d, day_group in group.groupby(group['RfTxStamp'].dt.date):
            if pd.isna(d):
                continue
            
            date_iso = pd.to_datetime(d).strftime("%Y-%m-%d")

            current_jig_passed_sns = jig_pass_history.get(jig, set())
            
            pass_df = day_group[day_group['PassStatusNorm'] == 'O']
            fail_df = day_group[day_group['PassStatusNorm'] == 'X']
            
            false_defect_df = fail_df[fail_df['SNumber'].isin(current_jig_passed_sns)]
            
            true_defect_df = fail_df[~fail_df['SNumber'].isin(current_jig_passed_sns)]
            
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
                
                'pass_data': pass_df.to_dict('records'),
                'false_defect_data': false_defect_df.to_dict('records'),
                'true_defect_data': true_defect_df.to_dict('records'),
                'fail_data': fail_df.to_dict('records'),

                'pass_unique_count': len(pass_df['SNumber'].unique()),
                'false_defect_unique_count': len(false_defect_df['SNumber'].unique()),
                'true_defect_unique_count': len(true_defect_df['SNumber'].unique()),
                'fail_unique_count': len(fail_df['SNumber'].unique())
            }
    
    all_dates = sorted(list(df['RfTxStamp'].dt.date.dropna().unique()))
    return summary_data, all_dates
