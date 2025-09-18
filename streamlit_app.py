import streamlit as st
import pandas as pd
from datetime import datetime, date
import sqlite3
import numpy as np
import warnings

warnings.filterwarnings('ignore')

# SQLite 연결 함수
@st.cache_resource
def get_connection():
    try:
        db_path = "db/SJ_TM2360E_v2.sqlite3"
        conn = sqlite3.connect(db_path, check_same_thread=False)
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결에 실패했습니다: {e}")
        return None

# 데이터베이스에서 테이블을 읽어 DataFrame으로 반환하는 함수 (날짜 필터 없음)
def read_data_from_db(conn, table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"테이블 '{table_name}'에서 데이터를 불러오는 중 오류가 발생했습니다: {e}")
        return None

# analyze_data 함수 (날짜 필터링 제거)
def analyze_data(df, date_col_name):
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
    
    # 데이터가 비어있거나 날짜 컬럼이 유효하지 않은 경우를 처리
    if df.empty or date_col_name not in df.columns or df[date_col_name].dt.date.dropna().empty:
        return summary_data, all_dates

    if 'SNumber' in df.columns:
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
    
    return summary_data, all_dates


def display_analysis_result(analysis_key, table_name, date_col_name):
    if st.session_state.analysis_results[analysis_key] is None:
        st.error("데이터 로드에 실패했습니다. 파일 형식을 확인해주세요.")
        return

    summary_data, all_dates = st.session_state.analysis_data[analysis_key]
    
    # 분석 데이터가 비어있을 경우 명확한 경고 메시지를 표시
    if not summary_data:
        st.warning("선택한 날짜에 해당하는 분석 데이터가 없습니다.")
        return

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
        df_all_data = pd.read_sql_query("SELECT * FROM historyinspection;", conn)
    except Exception as e:
        st.error(f"데이터베이스에서 'historyinspection' 테이블을 불러오는 중 오류가 발생했습니다: {e}")
        return

    # 모든 날짜 관련 컬럼을 datetime 객체로 미리 변환
    df_all_data['PcbStartTime_dt'] = pd.to_datetime(df_all_data['PcbStartTime'], errors='coerce')
    df_all_data['FwStamp_dt'] = pd.to_datetime(df_all_data['FwStamp'], errors='coerce')
    df_all_data['RfTxStamp_dt'] = pd.to_datetime(df_all_data['RfTxStamp'], errors='coerce')
    df_all_data['SemiAssyStartTime_dt'] = pd.to_datetime(df_all_data['SemiAssyStartTime'], errors='coerce')
    df_all_data['BatadcStamp_dt'] = pd.to_datetime(df_all_data['BatadcStamp'], errors='coerce')

    try:
        with tab1:
            st.header("파일 PCB (Pcb_Process)")
            # PCB 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = df_all_data['PcbStartTime_dt'].dt.date.dropna()
                min_date = df_dates.min() if not df_dates.empty else date.today()
                max_date = df_dates.max() if not df_dates.empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_pcb")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_pcb"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        if len(selected_dates) == 2:
                            start_date, end_date = selected_dates
                            df_filtered = df_all_data[
                                (df_all_data['PcbStartTime_dt'].dt.date >= start_date) &
                                (df_all_data['PcbStartTime_dt'].dt.date <= end_date)
                            ].copy()
                        else:
                            st.warning("날짜 범위를 올바르게 선택해주세요.")
                            df_filtered = pd.DataFrame()
                        
                        st.session_state.analysis_results['pcb'] = df_filtered
                        st.session_state.analysis_data['pcb'] = analyze_data(df_filtered, 'PcbStartTime_dt')
                        st.session_state.analysis_time['pcb'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
            
            if st.session_state.analysis_results['pcb'] is not None:
                display_analysis_result('pcb', 'Pcb_Process', 'PcbStartTime_dt')

        with tab2:
            st.header("파일 Fw (Fw_Process)")
            # Fw 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = df_all_data['FwStamp_dt'].dt.date.dropna()
                min_date = df_dates.min() if not df_dates.empty else date.today()
                max_date = df_dates.max() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_fw")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_fw"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        if len(selected_dates) == 2:
                            start_date, end_date = selected_dates
                            df_filtered = df_all_data[
                                (df_all_data['FwStamp_dt'].dt.date >= start_date) &
                                (df_all_data['FwStamp_dt'].dt.date <= end_date)
                            ].copy()

                        else:
                            st.warning("날짜 범위를 올바르게 선택해주세요.")
                            df_filtered = pd.DataFrame()

                        st.session_state.analysis_results['fw'] = df_filtered
                        st.session_state.analysis_data['fw'] = analyze_data(df_filtered, 'FwStamp_dt')
                        st.session_state.analysis_time['fw'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['fw'] is not None:
                display_analysis_result('fw', 'Fw_Process', 'FwStamp_dt')

        with tab3:
            st.header("파일 RfTx (RfTx_Process)")
            # RfTx 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = df_all_data['RfTxStamp_dt'].dt.date.dropna()
                min_date = df_dates.min() if not df_dates.empty else date.today()
                max_date = df_dates.max() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_rftx")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_rftx"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        if len(selected_dates) == 2:
                            start_date, end_date = selected_dates
                            df_filtered = df_all_data[
                                (df_all_data['RfTxStamp_dt'].dt.date >= start_date) &
                                (df_all_data['RfTxStamp_dt'].dt.date <= end_date)
                            ].copy()

                        else:
                            st.warning("날짜 범위를 올바르게 선택해주세요.")
                            df_filtered = pd.DataFrame()

                        st.session_state.analysis_results['rftx'] = df_filtered
                        st.session_state.analysis_data['rftx'] = analyze_data(df_filtered, 'RfTxStamp_dt')
                        st.session_state.analysis_time['rftx'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['rftx'] is not None:
                display_analysis_result('rftx', 'RfTx_Process', 'RfTxStamp_dt')

        with tab4:
            st.header("파일 Semi (SemiAssy_Process)")
            # Semi 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = df_all_data['SemiAssyStartTime_dt'].dt.date.dropna()
                min_date = df_dates.min() if not df_dates.empty else date.today()
                max_date = df_dates.max() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_semi")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_semi"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        if len(selected_dates) == 2:
                            start_date, end_date = selected_dates
                            df_filtered = df_all_data[
                                (df_all_data['SemiAssyStartTime_dt'].dt.date >= start_date) &
                                (df_all_data['SemiAssyStartTime_dt'].dt.date <= end_date)
                            ].copy()
                        else:
                            st.warning("날짜 범위를 올바르게 선택해주세요.")
                            df_filtered = pd.DataFrame()

                        st.session_state.analysis_results['semi'] = df_filtered
                        st.session_state.analysis_data['semi'] = analyze_data(df_filtered, 'SemiAssyStartTime_dt')
                        st.session_state.analysis_time['semi'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")

            if st.session_state.analysis_results['semi'] is not None:
                display_analysis_result('semi', 'SemiAssy_Process', 'SemiAssyStartTime_dt')

        with tab5:
            st.header("파일 Func (Func_Process)")
            # Func 관련 필터
            col_date, col_button = st.columns([0.8, 0.2])
            with col_date:
                df_dates = df_all_data['BatadcStamp_dt'].dt.date.dropna()
                min_date = df_dates.min() if not df_dates.empty else date.today()
                max_date = df_dates.max() if not df_dates.dropna().empty else date.today()
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key="dates_func")
            with col_button:
                st.markdown("---")
                if st.button("분석 실행", key="analyze_func"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        if len(selected_dates) == 2:
                            start_date, end_date = selected_dates
                            df_filtered = df_all_data[
                                (df_all_data['BatadcStamp_dt'].dt.date >= start_date) &
                                (df_all_data['BatadcStamp_dt'].dt.date <= end_date)
                            ].copy()
                        else:
                            st.warning("날짜 범위를 올바르게 선택해주세요.")
                            df_filtered = pd.DataFrame()
                        
                        st.session_state.analysis_results['func'] = df_filtered
                        st.session_state.analysis_data['func'] = analyze_data(df_filtered, 'BatadcStamp_dt')
                        st.session_state.analysis_time['func'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("분석 완료! 결과가 저장되었습니다.")
            
            if st.session_state.analysis_results['func'] is not None:
                display_analysis_result('func', 'Func_Process', 'BatadcStamp_dt')

    except Exception as e:
        st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()
