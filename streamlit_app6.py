import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3
import numpy as np
import io
import warnings

warnings.filterwarnings('ignore')

# SQLite 연결 함수
@st.cache_resource
def get_connection():
    try:
        db_path = "db/SJ_TM2360E.sqlite3"
        # check_same_thread=False 인수를 sqlite3.connect 함수에 직접 전달
        conn = sqlite3.connect(db_path, check_same_thread=False)
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

# 원본 csv2.py의 analyze_data 함수를 재구현 (DB 데이터에 맞춰 수정)
def analyze_data(df):
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    # NaN 값 처리 및 데이터 타입 변환
    df = df.replace('N/A', np.nan)
    df['PcbStartTime'] = pd.to_datetime(df['PcbStartTime'], errors='coerce')
    df['PassStatusNorm'] = df['PcbPass'].fillna('').astype(str).str.strip().str.upper()

    summary_data = {}
    
    # 'PcbMaxIrPwr'은 지그(Jig)를 구분하는 컬럼명으로 추정
    for jig, group in df.groupby('PcbMaxIrPwr'):
        if group['PcbStartTime'].dt.date.dropna().empty:
            continue
        
        for d, day_group in group.groupby(group['PcbStartTime'].dt.date):
            if pd.isna(d):
                continue
            
            date_iso = pd.to_datetime(d).strftime("%Y-%m-%d")

            pass_sns_series = day_group.groupby('SNumber')['PassStatusNorm'].apply(lambda x: 'O' in x.tolist())
            pass_sns = pass_sns_series[pass_sns_series].index.tolist()

            false_defect_df = day_group[(day_group['PassStatusNorm'] == 'X') & (day_group['SNumber'].isin(pass_sns))]
            false_defect_count = len(false_defect_df['SNumber'].unique())

            true_defect_df = day_group[(day_group['PassStatusNorm'] == 'X') & (~day_group['SNumber'].isin(pass_sns))]
            true_defect_count = len(true_defect_df['SNumber'].unique())

            pass_count = len(pass_sns)
            total_test = len(day_group['SNumber'].unique())
            fail_count = false_defect_count + true_defect_count

            if jig not in summary_data:
                summary_data[jig] = {}
            summary_data[jig][date_iso] = {
                'total_test': total_test,
                'pass': pass_count,
                'false_defect': false_defect_count,
                'true_defect': true_defect_count,
                'fail': fail_count,
            }
            
    all_dates = sorted(list(df['PcbStartTime'].dt.date.dropna().unique()))
    return summary_data, all_dates


# 분석 함수들을 모두 analyze_data 함수로 통합합니다.
def analyze_Fw_data(df):
    return analyze_data(df)
def analyze_RfTx_data(df):
    return analyze_data(df)
def analyze_Semi_data(df):
    return analyze_data(df)
def analyze_Batadc_data(df):
    return analyze_data(df)


def display_analysis_result(analysis_key, table_name):
    """ session_state에 저장된 분석 결과를 Streamlit에 표시하는 함수"""
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
            if st.button("파일 PCB 분석 실행", key="analyze_pcb"):
                df = read_data_from_db(conn, "historyinspection")
                if df is not None:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        st.session_state.analysis_results['pcb'] = df
                        st.session_state.analysis_data['pcb'] = analyze_data(df)
                        st.session_state.analysis_time['pcb'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
                else:
                    st.error("PCB 데이터 테이블을 읽을 수 없습니다.")
            
            if st.session_state.analysis_results['pcb'] is not None:
                display_analysis_result('pcb', 'historyinspection')

        with tab2:
            st.header("파일 Fw (Fw_Process)")
            if st.button("파일 Fw 분석 실행", key="analyze_fw"):
                df = read_data_from_db(conn, "Fw_process")
                if df is not None:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        st.session_state.analysis_results['fw'] = df
                        st.session_state.analysis_data['fw'] = analyze_Fw_data(df)
                        st.session_state.analysis_time['fw'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
                else:
                    st.error("Fw 데이터 테이블을 읽을 수 없습니다.")

            if st.session_state.analysis_results['fw'] is not None:
                display_analysis_result('fw', 'Fw_process')

        with tab3:
            st.header("파일 RfTx (RfTx_Process)")
            if st.button("파일 RfTx 분석 실행", key="analyze_rftx"):
                df = read_data_from_db(conn, "RfTx_process")
                if df is not None:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        st.session_state.analysis_results['rftx'] = df
                        st.session_state.analysis_data['rftx'] = analyze_RfTx_data(df)
                        st.session_state.analysis_time['rftx'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
                else:
                    st.error("RfTx 데이터 테이블을 읽을 수 없습니다.")

            if st.session_state.analysis_results['rftx'] is not None:
                display_analysis_result('rftx', 'RfTx_process')

        with tab4:
            st.header("파일 Semi (SemiAssy_Process)")
            if st.button("파일 Semi 분석 실행", key="analyze_semi"):
                df = read_data_from_db(conn, "SemiAssy_process")
                if df is not None:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        st.session_state.analysis_results['semi'] = df
                        st.session_state.analysis_data['semi'] = analyze_Semi_data(df)
                        st.session_state.analysis_time['semi'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
                else:
                    st.error("Semi 데이터 테이블을 읽을 수 없습니다.")

            if st.session_state.analysis_results['semi'] is not None:
                display_analysis_result('semi', 'SemiAssy_process')

        with tab5:
            st.header("파일 Func (Func_Process)")
            if st.button("파일 Func 분석 실행", key="analyze_func"):
                df = read_data_from_db(conn, "BatAdc_process")
                if df is not None:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        st.session_state.analysis_results['func'] = df
                        st.session_state.analysis_data['func'] = analyze_Batadc_data(df)
                        st.session_state.analysis_time['func'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
                else:
                    st.error("Func 데이터 테이블을 읽을 수 없습니다.")
            
            if st.session_state.analysis_results['func'] is not None:
                display_analysis_result('func', 'BatAdc_process')

    except Exception as e:
        st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()
