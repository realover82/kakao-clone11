import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3
import altair as alt

# 외부 파일에서 가져온 함수들 (여기서는 dummy 함수로 대체)
# 실제 프로젝트에서는 이 함수들이 각 py파일에 정의되어 있어야 합니다.
def read_data_from_db(conn, table_name):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, conn)
    return df

def analyze_data(df):
    # 'historyinspection' 테이블에 맞게 데이터를 분석하는 로직
    # 예시로 가상의 요약 데이터를 반환합니다.
    summary_data = {
        100.00: {
            "2025-09-08": {"total_test": 4157, "pass": 3944, "false_defect": 81, "true_defect": 132, "fail": 213},
            "2025-09-09": {"total_test": 5798, "pass": 5548, "false_defect": 110, "true_defect": 140, "fail": 250},
        }
    }
    all_dates = [datetime(2025, 9, 8).date(), datetime(2025, 9, 9).date()]
    return summary_data, all_dates

def analyze_Fw_data(df):
    # 'fw_process' 테이블에 맞게 데이터를 분석하는 로직
    summary_data = {
        100.00: {
            "2025-09-08": {"total_test": 100, "pass": 90, "false_defect": 5, "true_defect": 5, "fail": 10},
        }
    }
    all_dates = [datetime(2025, 9, 8).date()]
    return summary_data, all_dates

def analyze_RfTx_data(df):
    # 'rftx_process' 테이블에 맞게 데이터를 분석하는 로직
    summary_data = {
        100.00: {
            "2025-09-08": {"total_test": 200, "pass": 180, "false_defect": 10, "true_defect": 10, "fail": 20},
        }
    }
    all_dates = [datetime(2025, 9, 8).date()]
    return summary_data, all_dates

def analyze_Semi_data(df):
    # 'semi_assy_process' 테이블에 맞게 데이터를 분석하는 로직
    summary_data = {
        100.00: {
            "2025-09-08": {"total_test": 300, "pass": 270, "false_defect": 15, "true_defect": 15, "fail": 30},
        }
    }
    all_dates = [datetime(2025, 9, 8).date()]
    return summary_data, all_dates

def analyze_Batadc_data(df):
    # 'bat_adc_process' 테이블에 맞게 데이터를 분석하는 로직
    summary_data = {
        100.00: {
            "2025-09-08": {"total_test": 400, "pass": 360, "false_defect": 20, "true_defect": 20, "fail": 40},
        }
    }
    all_dates = [datetime(2025, 9, 8).date()]
    return summary_data, all_dates

def display_analysis_result(analysis_key, file_name):
    """ session_state에 저장된 분석 결과를 Streamlit에 표시하는 함수"""
    if st.session_state.analysis_results[analysis_key] is None:
        st.error("데이터 로드에 실패했습니다. 파일 형식을 확인해주세요.")
        return

    summary_data, all_dates = st.session_state.analysis_data[analysis_key]
    
    st.markdown(f"### '{file_name}' 분석 리포트")
    
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
        file_name=f"{file_name}_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )

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

def main():
    st.set_page_config(layout="wide")
    st.title("리모컨 생산 데이터 분석 툴")
    st.markdown("---")

    # session_state 초기화
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
    
    conn = get_connection()

    if conn:
        st.sidebar.success("데이터베이스 연결 성공!")

        tab1, tab2, tab3, tab4, tab5 = st.tabs(["파일 PCB 분석", "파일 Fw 분석", "파일 RfTx 분석", "파일 Semi 분석", "파일 Func 분석"])
        
        try:
            with tab1:
                st.header("파일 PCB (Pcb_Process)")
                if st.button("파일 PCB 분석 실행", key="analyze_pcb"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        df = read_data_from_db(conn, "historyinspection")
                        if df is not None:
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
                    with st.spinner("데이터 분석 및 저장 중..."):
                        df = read_data_from_db(conn, "fw_process")
                        if df is not None:
                            st.session_state.analysis_results['fw'] = df
                            st.session_state.analysis_data['fw'] = analyze_Fw_data(df)
                            st.session_state.analysis_time['fw'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            st.success("분석 완료! 결과가 저장되었습니다.")
                        else:
                            st.error("Fw 데이터 테이블을 읽을 수 없습니다.")

                if st.session_state.analysis_results['fw'] is not None:
                    display_analysis_result('fw', 'fw_process')

            with tab3:
                st.header("파일 RfTx (RfTx_Process)")
                if st.button("파일 RfTx 분석 실행", key="analyze_rftx"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        df = read_data_from_db(conn, "rftx_process")
                        if df is not None:
                            st.session_state.analysis_results['rftx'] = df
                            st.session_state.analysis_data['rftx'] = analyze_RfTx_data(df)
                            st.session_state.analysis_time['rftx'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            st.success("분석 완료! 결과가 저장되었습니다.")
                        else:
                            st.error("RfTx 데이터 테이블을 읽을 수 없습니다.")

                if st.session_state.analysis_results['rftx'] is not None:
                    display_analysis_result('rftx', 'rftx_process')

            with tab4:
                st.header("파일 Semi (SemiAssy_Process)")
                if st.button("파일 Semi 분석 실행", key="analyze_semi"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        df = read_data_from_db(conn, "semi_assy_process")
                        if df is not None:
                            st.session_state.analysis_results['semi'] = df
                            st.session_state.analysis_data['semi'] = analyze_Semi_data(df)
                            st.session_state.analysis_time['semi'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            st.success("분석 완료! 결과가 저장되었습니다.")
                        else:
                            st.error("Semi 데이터 테이블을 읽을 수 없습니다.")

                if st.session_state.analysis_results['semi'] is not None:
                    display_analysis_result('semi', 'semi_assy_process')

            with tab5:
                st.header("파일 Func (Func_Process)")
                if st.button("파일 Func 분석 실행", key="analyze_func"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        df = read_data_from_db(conn, "bat_adc_process")
                        if df is not None:
                            st.session_state.analysis_results['func'] = df
                            st.session_state.analysis_data['func'] = analyze_Batadc_data(df)
                            st.session_state.analysis_time['func'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            st.success("분석 완료! 결과가 저장되었습니다.")
                        else:
                            st.error("Func 데이터 테이블을 읽을 수 없습니다.")
                
                if st.session_state.analysis_results['func'] is not None:
                    display_analysis_result('func', 'bat_adc_process')

        except Exception as e:
            st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")

    else:
        st.sidebar.error("데이터베이스 연결에 실패했습니다.")
        st.error("데이터베이스 연결 실패: 앱을 실행할 수 없습니다.")
        

if __name__ == "__main__":
    main()
