import streamlit as st
import pandas as pd
from datetime import datetime, date
import sqlite3
import numpy as np
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

# 데이터베이스에서 테이블을 읽어 DataFrame으로 반환하는 함수 (날짜 필터 추가)
def read_data_from_db(conn, table_name, start_date=None, end_date=None, date_col=None):
    try:
        query = f"SELECT * FROM {table_name}"
        if start_date and date_col:
            query += f" WHERE date({date_col}) >= '{start_date}'"
        if end_date and date_col:
            query += f" AND date({date_col}) <= '{end_date}'"
            
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"테이블 '{table_name}'에서 데이터를 불러오는 중 오류가 발생했습니다: {e}")
        return None

def analyze_data(df):
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    df = df.replace('N/A', np.nan)
    
    # PcbStartTime 컬럼이 존재할 경우에만 날짜 변환
    if 'PcbStartTime' in df.columns:
        df['PcbStartTime'] = pd.to_datetime(df['PcbStartTime'], errors='coerce')

    df['PassStatusNorm'] = ""
    if 'PcbPass' in df.columns:
        df['PassStatusNorm'] = df['PcbPass'].fillna('').astype(str).str.strip().str.upper()
    elif 'FwPass' in df.columns:
        df['PassStatusNorm'] = df['FwPass'].fillna('').astype(str).str.strip().str.upper()
    elif 'RfTxPass' in df.columns:
        df['PassStatusNorm'] = df['RfTxPass'].fillna('').astype(str).str.strip().str.upper()
    elif 'SemiAssyPass' in df.columns:
        df['PassStatusNorm'] = df['SemiAssyPass'].fillna('').astype(str).str.strip().str.upper()
    elif 'BatadcPass' in df.columns:
        df['PassStatusNorm'] = df['BatadcPass'].fillna('').astype(str).str.strip().str.upper()

    summary_data = {}
    all_dates = []

    jig_col = 'SNumber'
    date_col = None
    if 'PcbMaxIrPwr' in df.columns: jig_col = 'PcbMaxIrPwr'
    if 'BatadcStamp' in df.columns: jig_col = 'BatadcStamp'
    if 'PcbStartTime' in df.columns: date_col = 'PcbStartTime'
    if 'FwStamp' in df.columns: date_col = 'FwStamp'
    if 'RfTxStamp' in df.columns: date_col = 'RfTxStamp'
    if 'SemiAssyStartTime' in df.columns: date_col = 'SemiAssyStartTime'
    if 'BatadcStamp' in df.columns: date_col = 'BatadcStamp'


    if date_col in df.columns and 'SNumber' in df.columns and not df[date_col].dt.date.dropna().empty:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        for jig, group in df.groupby(jig_col):
            for d, day_group in group.groupby(group[date_col].dt.date):
                if pd.isna(d): continue
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
        all_dates = sorted(list(df[date_col].dt.date.dropna().unique()))
    else:
        all_dates = [datetime.now().date()]
        summary_data['N/A'] = {all_dates[0].strftime("%Y-%m-%d"): {'total_test': 0, 'pass': 0, 'false_defect': 0, 'true_defect': 0, 'fail': 0}}

    return summary_data, all_dates

def display_analysis_result(analysis_key, table_name):
    if st.session_state.analysis_results[analysis_key] is None:
        st.error("데이터 로드에 실패했습니다. 파일 형식을 확인해주세요.")
        return

    summary_data, all_dates = st.session_state.analysis_data[analysis_key]
    
    st.markdown(f"### '{table_name}' 분석 리포트")
    
    kor_date_cols = [f"{d.strftime('%y%m%d')}" for d in all_dates]
    
    st.write(f"**분석 시간**: {st.session_state.analysis_time[analysis_key]}")
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
        with tab1:
            st.header("파일 PCB (Pcb_Process)")
            # PCB 탭의 날짜 필터 추가
            start_date_pcb = st.date_input("PCB 분석 시작 날짜", value=datetime.now().date(), key="date_pcb")
            if st.button("파일 PCB 분석 실행", key="analyze_pcb"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    df = read_data_from_db(conn, "historyinspection", start_date=start_date_pcb, end_date=start_date_pcb, date_col='PcbStartTime')
                    if df is not None:
                        st.session_state.analysis_results['pcb'] = df
                        st.session_state.analysis_data['pcb'] = analyze_data(df, [])
                        st.session_state.analysis_time['pcb'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
            
            if st.session_state.analysis_results['pcb'] is not None:
                display_analysis_result('pcb', 'Pcb_Process')

        with tab2:
            st.header("파일 Fw (Fw_Process)")
            # Fw 탭의 날짜 필터 추가
            start_date_fw = st.date_input("Fw 분석 시작 날짜", value=datetime.now().date(), key="date_fw")
            if st.button("파일 Fw 분석 실행", key="analyze_fw"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    df = read_data_from_db(conn, "historyinspection", start_date=start_date_fw, end_date=start_date_fw, date_col='FwStamp')
                    if df is not None:
                        st.session_state.analysis_results['fw'] = df
                        st.session_state.analysis_data['fw'] = analyze_data(df, [])
                        st.session_state.analysis_time['fw'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['fw'] is not None:
                display_analysis_result('fw', 'Fw_Process')

        with tab3:
            st.header("파일 RfTx (RfTx_Process)")
            # RfTx 탭의 날짜 필터 추가
            start_date_rftx = st.date_input("RfTx 분석 시작 날짜", value=datetime.now().date(), key="date_rftx")
            if st.button("파일 RfTx 분석 실행", key="analyze_rftx"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    df = read_data_from_db(conn, "historyinspection", start_date=start_date_rftx, end_date=start_date_rftx, date_col='RfTxStamp')
                    if df is not None:
                        st.session_state.analysis_results['rftx'] = df
                        st.session_state.analysis_data['rftx'] = analyze_data(df, [])
                        st.session_state.analysis_time['rftx'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['rftx'] is not None:
                display_analysis_result('rftx', 'RfTx_Process')

        with tab4:
            st.header("파일 Semi (SemiAssy_Process)")
            # Semi 탭의 날짜 필터 추가
            start_date_semi = st.date_input("Semi 분석 시작 날짜", value=datetime.now().date(), key="date_semi")
            if st.button("파일 Semi 분석 실행", key="analyze_semi"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    df = read_data_from_db(conn, "historyinspection", start_date=start_date_semi, end_date=start_date_semi, date_col='SemiAssyStartTime')
                    if df is not None:
                        st.session_state.analysis_results['semi'] = df
                        st.session_state.analysis_data['semi'] = analyze_data(df, [])
                        st.session_state.analysis_time['semi'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['semi'] is not None:
                display_analysis_result('semi', 'SemiAssy_Process')

        with tab5:
            st.header("파일 Func (Func_Process)")
            # Func 탭의 날짜 필터 추가
            start_date_func = st.date_input("Func 분석 시작 날짜", value=datetime.now().date(), key="date_func")
            if st.button("파일 Func 분석 실행", key="analyze_func"):
                with st.spinner("데이터 분석 및 저장 중..."):
                    df = read_data_from_db(conn, "historyinspection", start_date=start_date_func, end_date=start_date_func, date_col='BatadcStamp')
                    if df is not None:
                        st.session_state.analysis_results['func'] = df
                        st.session_state.analysis_data['func'] = analyze_data(df, [])
                        st.session_state.analysis_time['func'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
            
            if st.session_state.analysis_results['func'] is not None:
                display_analysis_result('func', 'Func_Process')

    except Exception as e:
        st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()
