import streamlit as st
import pandas as pd
from datetime import datetime, date
import sqlite3
import numpy as np
import warnings

warnings.filterwarnings('ignore')

# SQLite connection function
@st.cache_resource
def get_connection():
    try:
        db_path = "db/SJ_TM2360E_v2.sqlite3"
        conn = sqlite3.connect(db_path, check_same_thread=False)
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결에 실패했습니다: {e}")
        return None

# Function to read data from DB with date filter
def read_data_from_db(conn, table_name, start_date=None, end_date=None, date_col=None):
    try:
        query = f"SELECT * FROM {table_name}"
        conditions = []
        if start_date and date_col:
            start_date_str = start_date.strftime('%Y-%m-%d')
            conditions.append(f"date({date_col}) >= '{start_date_str}'")
        if end_date and date_col:
            end_date_str = end_date.strftime('%Y-%m-%d')
            conditions.append(f"date({date_col}) <= '{end_date_str}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"테이블 '{table_name}'에서 데이터를 불러오는 중 오류가 발생했습니다: {e}")
        return None

# Analysis function (refactored to work with DB data)
def analyze_data(df, date_col_name):
    # Data cleaning and type conversion
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    df = df.replace('N/A', np.nan)
    
    if date_col_name in df.columns:
        df[date_col_name] = pd.to_datetime(df[date_col_name], errors='coerce')
    
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
    if 'PcbMaxIrPwr' in df.columns: jig_col = 'PcbMaxIrPwr'
    if 'BatadcStamp' in df.columns: jig_col = 'BatadcStamp'
    
    if date_col_name in df.columns and 'SNumber' in df.columns and not df[date_col_name].dt.date.dropna().empty:
        for jig, group in df.groupby(jig_col):
            for d, day_group in group.groupby(group[date_col_name].dt.date):
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
        all_dates = sorted(list(df[date_col_name].dt.date.dropna().unique()))
    else:
        all_dates = [datetime.now().date()]
        summary_data['N/A'] = {all_dates[0].strftime("%Y-%m-%d"): {'total_test': 0, 'pass': 0, 'false_defect': 0, 'true_defect': 0, 'fail': 0}}

    return summary_data, all_dates


def display_analysis_result(analysis_key, table_name, date_col_name):
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
    
    # Get all data once to determine date ranges
    try:
        df_all_data = pd.read_sql_query("SELECT * FROM my_csv_data;", conn)
        df_all_data = df_all_data.replace('N/A', np.nan)
    except Exception as e:
        st.error(f"데이터베이스에서 'historyinspection' 테이블을 불러오는 중 오류가 발생했습니다: {e}")
        return

    try:
        with tab1:
            st.header("파일 PCB (Pcb_Process)")
            # PCB 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                # 데이터에서 날짜의 최소/최대값 추출 및 범위 선택
                df_dates = pd.to_datetime(df_all_data['PcbStartTime'], errors='coerce')
                min_date = df_dates.min().date() if not df_dates.dropna().empty else date.today()
                max_date = df_dates.max().date() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_pcb")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_pcb") and len(selected_dates) == 2:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        start_date_pcb, end_date_pcb = selected_dates
                        df_filtered = df_all_data[
                            (pd.to_datetime(df_all_data['PcbStartTime']).dt.date >= start_date_pcb) &
                            (pd.to_datetime(df_all_data['PcbStartTime']).dt.date <= end_date_pcb)
                        ]
                        
                        st.session_state.analysis_results['pcb'] = df_filtered
                        st.session_state.analysis_data['pcb'] = analyze_data(df_filtered, 'PcbStartTime')
                        st.session_state.analysis_time['pcb'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
            
            if st.session_state.analysis_results['pcb'] is not None:
                display_analysis_result('pcb', 'Pcb_Process', 'PcbStartTime')

        with tab2:
            st.header("파일 Fw (Fw_Process)")
            # Fw 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = pd.to_datetime(df_all_data['FwStamp'], errors='coerce')
                min_date = df_dates.min().date() if not df_dates.dropna().empty else date.today()
                max_date = df_dates.max().date() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_fw")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_fw") and len(selected_dates) == 2:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        start_date_fw, end_date_fw = selected_dates
                        df_filtered = df_all_data[
                            (pd.to_datetime(df_all_data['FwStamp']).dt.date >= start_date_fw) &
                            (pd.to_datetime(df_all_data['FwStamp']).dt.date <= end_date_fw)
                        ]

                        st.session_state.analysis_results['fw'] = df_filtered
                        st.session_state.analysis_data['fw'] = analyze_data(df_filtered, 'FwStamp')
                        st.session_state.analysis_time['fw'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['fw'] is not None:
                display_analysis_result('fw', 'Fw_Process', 'FwStamp')

        with tab3:
            st.header("파일 RfTx (RfTx_Process)")
            # RfTx 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = pd.to_datetime(df_all_data['RfTxStamp'], errors='coerce')
                min_date = df_dates.min().date() if not df_dates.dropna().empty else date.today()
                max_date = df_dates.max().date() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_rftx")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_rftx") and len(selected_dates) == 2:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        start_date_rftx, end_date_rftx = selected_dates
                        df_filtered = df_all_data[
                            (pd.to_datetime(df_all_data['RfTxStamp']).dt.date >= start_date_rftx) &
                            (pd.to_datetime(df_all_data['RfTxStamp']).dt.date <= end_date_rftx)
                        ]

                        st.session_state.analysis_results['rftx'] = df_filtered
                        st.session_state.analysis_data['rftx'] = analyze_data(df_filtered, 'RfTxStamp')
                        st.session_state.analysis_time['rftx'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['rftx'] is not None:
                display_analysis_result('rftx', 'RfTx_Process', 'RfTxStamp')

        with tab4:
            st.header("파일 Semi (SemiAssy_Process)")
            # Semi 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = pd.to_datetime(df_all_data['SemiAssyStartTime'], errors='coerce')
                min_date = df_dates.min().date() if not df_dates.dropna().empty else date.today()
                max_date = df_dates.max().date() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_semi")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_semi") and len(selected_dates) == 2:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        start_date_semi, end_date_semi = selected_dates
                        df_filtered = df_all_data[
                            (pd.to_datetime(df_all_data['SemiAssyStartTime']).dt.date >= start_date_semi) &
                            (pd.to_datetime(df_all_data['SemiAssyStartTime']).dt.date <= end_date_semi)
                        ]

                        st.session_state.analysis_results['semi'] = df_filtered
                        st.session_state.analysis_data['semi'] = analyze_data(df_filtered, 'SemiAssyStartTime')
                        st.session_state.analysis_time['semi'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['semi'] is not None:
                display_analysis_result('semi', 'SemiAssy_Process', 'SemiAssyStartTime')

        with tab5:
            st.header("파일 Func (Func_Process)")
            # Func 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = pd.to_datetime(df_all_data['BatadcStamp'], errors='coerce')
                min_date = df_dates.min().date() if not df_dates.dropna().empty else date.today()
                max_date = df_dates.max().date() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_func")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_func") and len(selected_dates) == 2:
                    with st.spinner("데이터 분석 및 저장 중..."):
                        start_date_func, end_date_func = selected_dates
                        df_filtered = df_all_data[
                            (pd.to_datetime(df_all_data['BatadcStamp']).dt.date >= start_date_func) &
                            (pd.to_datetime(df_all_data['BatadcStamp']).dt.date <= end_date_func)
                        ]
                    
                        st.session_state.analysis_results['func'] = df_filtered
                        st.session_state.analysis_data['func'] = analyze_data(df_filtered, 'BatadcStamp')
                        st.session_state.analysis_time['func'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
            
            if st.session_state.analysis_results['func'] is not None:
                display_analysis_result('func', 'Func_Process', 'BatadcStamp')

    except Exception as e:
        st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()
