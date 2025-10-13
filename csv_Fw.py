#
# csv_Fw.py
# 이 파일은 Streamlit 앱에서 모듈로 사용됩니다.
#

import pandas as pd
import numpy as np
import io
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# '="...' 형식의 문자열을 정리하는 함수
def clean_string_format(value):
    if isinstance(value, str) and value.startswith('="') and value.endswith('"'):
        return value[2:-1]
    return value

def read_csv_with_dynamic_header_for_Fw(uploaded_file):
    """Fw 데이터에 맞는 키워드로 헤더를 찾아 DataFrame을 로드하는 함수"""
    try:
        file_content = io.BytesIO(uploaded_file.getvalue())
        # 다양한 인코딩으로 파일 읽기 시도
        encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr', 'latin1']
        df = None
        for encoding in encodings:
            try:
                file_content.seek(0)
                df_temp = pd.read_csv(file_content, header=None, nrows=100, encoding=encoding)
                
                keywords = ['SNumber', 'FwStamp', 'FwPC', 'FwPass']
                
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

def analyze_Fw_data(df):
    """Fw 데이터의 분석 로직을 담고 있는 함수"""
    # 데이터 전처리
    for col in df.columns:
        df[col] = df[col].apply(clean_string_format)

    df['FwStamp'] = pd.to_datetime(df['FwStamp'], errors='coerce')
    df['PassStatusNorm'] = df['FwPass'].fillna('').astype(str).str.strip().str.upper()

    summary_data = {}
    
    if 'FwPC' not in df.columns:
        df['FwPC'] = 'DefaultJig'

    # 전체 데이터에서 한번이라도 PASS한 SNumber들을 미리 계산 (더 정확한 가성불량 판정을 위함)
    jig_pass_history = df[df['PassStatusNorm'] == 'O'].groupby('FwPC')['SNumber'].unique().apply(set).to_dict()

    for jig, group in df.groupby('FwPC'):
        group = group.dropna(subset=['FwStamp'])
        if group.empty:
            continue
        
        for d, day_group in group.groupby(group['FwStamp'].dt.date):
            if pd.isna(d):
                continue
            
            date_iso = pd.to_datetime(d).strftime("%Y-%m-%d")

            # 해당 Jig에서 한번이라도 통과한 전체 SN 목록
            current_jig_passed_sns = jig_pass_history.get(jig, set())
            
            # 각 카테고리별 데이터프레임 필터링
            pass_df = day_group[day_group['PassStatusNorm'] == 'O']
            fail_df = day_group[day_group['PassStatusNorm'] == 'X']
            
            false_defect_df = fail_df[fail_df['SNumber'].isin(current_jig_passed_sns)]
            true_defect_df = fail_df[~fail_df['SNumber'].isin(current_jig_passed_sns)]
            
            # 각 항목별 건수 (테스트 횟수)
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
                
                # 상세 데이터를 DataFrame을 dict 리스트로 변환하여 전달
                'pass_data': pass_df.to_dict('records'),
                'false_defect_data': false_defect_df.to_dict('records'),
                'true_defect_data': true_defect_df.to_dict('records'),
                'fail_data': fail_df.to_dict('records'),

                # 고유 SN 건수
                'pass_unique_count': len(pass_df['SNumber'].unique()),
                'false_defect_unique_count': len(false_defect_df['SNumber'].unique()),
                'true_defect_unique_count': len(true_defect_df['SNumber'].unique()),
                'fail_unique_count': len(fail_df['SNumber'].unique())
            }
    
    all_dates = sorted(list(df['FwStamp'].dt.date.dropna().unique()))
    return summary_data, all_dates