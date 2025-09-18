import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3
import numpy as np
import io
import warnings

warnings.filterwarnings('ignore')

# SQLite 연결 함수
@st.cache_resource(check_same_thread=False)
def get_connection():
    try:
        db_path = "db/SJ_TM2360E.sqlite3"
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결에 실패했습니다: {e}")
        return None

# 데이터베이스에서 테이블을 읽어 DataFrame으로 반환하는 함수
def read_data_from_db(conn, table_name):
    try:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"테이블 '{table_name}'에서 데이터를 불러오는 중 오류가 발생했습니다: {e}")
        return None

def analyze_data(df, keywords):
    # 키워드에 따라 데이터를 필터링하고 분석하는 로직
    df_filtered = df.copy()
    
    # 키워드 필터링
    for keyword in keywords:
        if keyword in df_filtered.columns:
            # SNumber 컬럼은 비어있지 않은 값만 필터링
            if keyword == 'SNumber':
                df_filtered = df_filtered[df_filtered[keyword].notna()]
            else:
                df_filtered = df_filtered[df_filtered[keyword].notna() & (df_filtered[keyword] != 'N/A')]

    # PcbPass, RfTxPass 등 Pass 컬럼을 기준으로 가성/진성 불량 분석
    # 이 부분은 키워드에 따라 동적으로 처리하도록 수정
    pass_col = None
    if 'PcbPass' in df_filtered.columns:
        pass_col = 'PcbPass'
    elif 'FwPass' in df_filtered.columns:
        pass_col = 'FwPass'
    # ... 기타 pass 컬럼에 대한 로직 추가 ...

    summary_data = {}
    all_dates = []

    # 분석 로직 (기존 analyze_data 로직을 재활용)
    if not df_filtered.empty and 'SNumber' in df_filtered.columns and pass_col:
        df_filtered['PcbStartTime'] = pd.to_datetime(df_filtered['PcbStartTime'], errors='coerce')
        df_filtered['PassStatusNorm'] = df_filtered[pass_col].fillna('').astype(str).str.strip().str.upper()

        if not df_filtered['PcbStartTime'].dt.date.dropna().empty:
            for jig, group in df_filtered.groupby('PcbMaxIrPwr'): # PcbMaxIrPwr 컬럼은 PCB 탭에서만 유효
                for d, day_group in group.groupby(group['PcbStartTime'].dt.date):
                    date_iso = pd.to_datetime(d).strftime("%Y-%m-%d")
                    pass_sns_series = day_group.groupby('SNumber')['PassStatusNorm'].apply(lambda x: 'O' in x.tolist())
                    pass_sns = pass_sns_series[pass_sns_series].index.tolist()
                    
                    false_defect_count = len(day_group[(day_group['PassStatusNorm'] == 'X') & (day_group['SNumber'].isin(pass_sns))]['SNumber'].unique())
                    true_defect_count = len(day_group[(day_group['PassStatusNorm'] == 'X') & (~day_group['SNumber'].isin(pass_sns))]['SNumber'].unique())
                    
                    pass_count = len(pass_sns)
                    total_test = len(day_group['SNumber'].unique())
                    fail_count = total_test - pass_count

                    if jig not in summary_data:
                        summary_data[jig] = {}
                    summary_data[jig][date_iso] = {
                        'total_test': total_test,
                        'pass': pass_count,
                        'false_defect': false_defect_count,
                        'true_defect': true_defect_count,
                        'fail': fail_count,
                    }
            all_dates = sorted(list(df_filtered['PcbStartTime'].dt.date.dropna().unique()))
    
    return summary_data, all_dates

def display_analysis_result(analysis_key, table_name, df_raw):
    # 원본 CSV 코드를 활용한 결과 표시 로직
    # df_raw를 사용하여 분석
    
    # 이 부분은 분석 함수에서 반환하는 데이터 구조에 맞게 다시 구성해야 합니다.
    # 이전 analyze_data 함수가 반환하는 형식에 맞춰 재구성했습니다.
    summary_data, all_dates = analyze_data(df_raw, []) # 키워드는 analyze_data 함수 내부에서 처리
    
    if not summary_data:
        st.error(f"테이블 '{table_name}'에는 분석 가능한 데이터가 없습니다.")
        return

    st.markdown(f"### '{table_name}' 분석 리포트")
    
    kor_date_cols = [f"{d.strftime('%y%m%d')}" for d in all_dates]
    
    st.write(f"**분석 시간**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.markdown("---")

    all_reports_text = ""
    
    for jig in sorted(summary_data.keys()):
        st.subheader(f"구분: {jig}")
        
        report_data = {
            '지표': ['총 테스트 수', 'PASS', '가성불량', '진성불량', 'FAIL']
        }
        
        for date_iso, date_str in zip([d.strftime('%Y-%m-%d') for d in all_dates], kor_date_cols):
            data_point = summary_data[jig].get(date_iso)
            if data_point:
                report_data[date_str] = [
                    data_point['total_test'],
                    data_point['pass'],
                    data_point['false_defect'],
                    data_point['true_defect'],
                    data_point['fail']
                ]
            else:
                report_data[date_str] = ['N/A'] * 5
        
        report_df = pd.DataFrame(report_data)
        st.table(report_df)
        all_reports_text += report_df.to_csv(index=False) + "\n"
    
    st.success("분석이 완료되었습니다!")

    st.download_button(
        label="분석 결과 다운로드",
        data=all_reports_text.encode('utf-8-sig'),
        file_name=f"{table_name}_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )

def main():
    st.set_page_config(layout="wide")
    st.title("리모컨 생산 데이터 분석 툴")
    st.markdown("---")

    conn = get_connection()
    if conn is None:
        return

    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = {
            'pcb': None, 'fw': None, 'rftx': None, 'semi': None, 'func': None
        }
    if 'analysis_data' not in st.session_state:
        st.session_state.analysis_data = {
            'pcb': None, 'fw': None, 'rftx': None, 'semi': None, 'func': None
        }
    if 'analysis_time' not in st.session_state:
        st.session_state.analysis_time = {
            'pcb': None, 'fw': None, 'rftx': None, 'semi': None, 'func': None
        }

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["파일 PCB 분석", "파일 Fw 분석", "파일 RfTx 분석", "파일 Semi 분석", "파일 Func 분석"])

    try:
        df_all_data = read_data_from_db(conn, "historyinspection")
        if df_all_data is None:
            st.error("데이터베이스에서 'historyinspection' 테이블을 찾을 수 없습니다. 앱을 실행할 수 없습니다.")
            return

        with tab1:
            st.header("파일 PCB (Pcb_Process)")
            if st.button("파일 PCB 분석 실행", key="analyze_pcb"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    # PCB 관련 키워드로 데이터 필터링 및 분석
                    keywords = ['SNumber', 'PcbPass', 'PcbStartTime', 'PcbMaxIrPwr']
                    df_filtered = df_all_data.copy()
                    for k in keywords:
                        if k in df_filtered.columns:
                            df_filtered = df_filtered[df_filtered[k].notna()]
                    
                    st.session_state.analysis_results['pcb'] = df_filtered
                    st.session_state.analysis_data['pcb'] = analyze_data(df_filtered, [])
                    st.session_state.analysis_time['pcb'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                st.success("분석 완료! 결과가 저장되었습니다.")
            
            if st.session_state.analysis_results['pcb'] is not None:
                display_analysis_result('pcb', 'Pcb_Process', st.session_state.analysis_results['pcb'])

        with tab2:
            st.header("파일 Fw (Fw_Process)")
            if st.button("파일 Fw 분석 실행", key="analyze_fw"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    keywords = ['SNumber', 'FwPass', 'FwStamp', 'FwPC']
                    df_filtered = df_all_data.copy()
                    for k in keywords:
                        if k in df_filtered.columns:
                            df_filtered = df_filtered[df_filtered[k].notna()]

                    st.session_state.analysis_results['fw'] = df_filtered
                    st.session_state.analysis_data['fw'] = analyze_Fw_data(df_filtered, [])
                    st.session_state.analysis_time['fw'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['fw'] is not None:
                display_analysis_result('fw', 'Fw_Process', st.session_state.analysis_results['fw'])

        with tab3:
            st.header("파일 RfTx (RfTx_Process)")
            if st.button("파일 RfTx 분석 실행", key="analyze_rftx"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    keywords = ['SNumber', 'RfTxPass', 'RfTxStamp', 'RfTxPC']
                    df_filtered = df_all_data.copy()
                    for k in keywords:
                        if k in df_filtered.columns:
                            df_filtered = df_filtered[df_filtered[k].notna()]

                    st.session_state.analysis_results['rftx'] = df_filtered
                    st.session_state.analysis_data['rftx'] = analyze_RfTx_data(df_filtered, [])
                    st.session_state.analysis_time['rftx'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['rftx'] is not None:
                display_analysis_result('rftx', 'RfTx_Process', st.session_state.analysis_results['rftx'])

        with tab4:
            st.header("파일 Semi (SemiAssy_Process)")
            if st.button("파일 Semi 분석 실행", key="analyze_semi"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    keywords = ['SNumber', 'SemiAssyPass', 'SemiAssyStartTime']
                    df_filtered = df_all_data.copy()
                    for k in keywords:
                        if k in df_filtered.columns:
                            df_filtered = df_filtered[df_filtered[k].notna()]

                    st.session_state.analysis_results['semi'] = df_filtered
                    st.session_state.analysis_data['semi'] = analyze_Semi_data(df_filtered, [])
                    st.session_state.analysis_time['semi'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['semi'] is not None:
                display_analysis_result('semi', 'SemiAssy_Process', st.session_state.analysis_results['semi'])

        with tab5:
            st.header("파일 Func (Func_Process)")
            if st.button("파일 Func 분석 실행", key="analyze_func"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    keywords = ['SNumber', 'BatadcPass', 'BatadcStamp']
                    df_filtered = df_all_data.copy()
                    for k in keywords:
                        if k in df_filtered.columns:
                            df_filtered = df_filtered[df_filtered[k].notna()]
                    
                    st.session_state.analysis_results['func'] = df_filtered
                    st.session_state.analysis_data['func'] = analyze_Batadc_data(df_filtered, [])
                    st.session_state.analysis_time['func'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                st.success("분석 완료! 결과가 저장되었습니다.")
            
            if st.session_state.analysis_results['func'] is not None:
                display_analysis_result('func', 'Func_Process', st.session_state.analysis_results['func'])

    except Exception as e:
        st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()
