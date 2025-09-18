import streamlit as st
import pandas as pd
import sqlite3
import altair as alt
import io
import re

# SQLite 연결 함수
@st.cache_resource
def get_connection():
    try:
        db_path = st.secrets["db_credentials"]["DB_FILE"]
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        st.error(f"SQLite 연결에 실패했습니다: {e}")
        return None

def show_dashboard(df):
    """
    업로드된 데이터프레임을 기반으로 대시보드를 생성
    """
    try:
        # 데이터프레임 전치 및 정리
        df_transposed = df.set_index('지표').T
        df_transposed.index.name = '날짜'
        df_transposed = df_transposed.reset_index()
        
        # 숫자형 데이터로 변환 (N/A 값 포함)
        for col in ['총 테스트 수', 'PASS', '가성불량', '진성불량', 'FAIL']:
            df_transposed[col] = pd.to_numeric(df_transposed[col], errors='coerce')

        # 필터: 날짜 선택
        dates = sorted(df_transposed['날짜'].unique())
        selected_date = st.selectbox("날짜를 선택하세요:", dates, index=len(dates) - 1)
        
        day_data = df_transposed[df_transposed['날짜'] == selected_date].iloc[0]
        
        # 지표 출력
        st.subheader(f"날짜: {selected_date} 요약")
        col1, col2, col3, col4, col5 = st.columns(5)
        
        col1.metric("총 테스트 수", f"{day_data['총 테스트 수']:,}")
        col2.metric("PASS", f"{day_data['PASS']:,}")
        col3.metric("FAIL", f"{day_data['FAIL']:,}")
        
        # 이전 날짜와 비교하여 증감량 계산
        delta_false = None
        delta_true = None
        if len(dates) > 1:
            prev_date = dates[dates.index(selected_date) - 1]
            prev_data = df_transposed[df_transposed['날짜'] == prev_date].iloc[0]
            delta_false = day_data['가성불량'] - prev_data['가성불량']
            delta_true = day_data['진성불량'] - prev_data['진성불량']
        
        col4.metric("가성불량", f"{day_data['가성불량']:,}", delta=f"{int(delta_false)}" if delta_false is not None else None)
        col5.metric("진성불량", f"{day_data['진성불량']:,}", delta=f"{int(delta_true)}" if delta_true is not None else None)

        # 차트: 일자별 불량 추이
        st.divider()
        st.subheader("일자별 불량 추이")
        chart_data = df_transposed.melt(id_vars=['날짜'], value_vars=['가성불량', '진성불량', 'FAIL'], var_name='불량 유형', value_name='수')
        
        line_chart = alt.Chart(chart_data).mark_line(point=True).encode(
            x=alt.X('날짜', sort=dates),
            y=alt.Y('수'),
            color='불량 유형',
            tooltip=['날짜', '불량 유형', '수']
        ).properties(height=400, title='일자별 불량 건수 추이')
        
        st.altair_chart(line_chart, use_container_width=True)
            
    except KeyError as ke:
        st.error(f"대시보드를 생성하는 중 오류가 발생했습니다: '지표' 컬럼을 찾을 수 없습니다. 파일의 첫 번째 컬럼명이 '지표'인지 확인해주세요.")
        st.dataframe(df)
        
    except Exception as e:
        st.error(f"대시보드를 생성하는 중 오류가 발생했습니다: {e}")
        st.dataframe(df)

def main():
    st.set_page_config(
        page_title="가성불량 현황 대시보드",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="auto"
    )

    st.title("CSV 파일 업로드 대시보드")
    st.write("대시보드를 생성하려면 아래에 CSV 파일을 업로드하세요.")
    
    uploaded_file = st.file_uploader("파일 업로드", type=["csv"])
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, low_memory=False)
            
            st.write("### 업로드된 데이터 미리보기")
            st.dataframe(df.head())
            
            if st.button("분석 시작"):
                st.session_state['show_dashboard'] = True
            
        except Exception as e:
            st.error(f"파일을 처리하는 중 오류가 발생했습니다: {e}")

        if 'show_dashboard' in st.session_state and 'df' in st.session_state:
            show_dashboard(st.session_state['df'])

if __name__ == "__main__":
    if 'show_dashboard' not in st.session_state:
        st.session_state['show_dashboard'] = False
        st.session_state['df'] = None
    
    uploaded_file = st.file_uploader("파일 업로드", type=["csv"])
    if uploaded_file is not None:
        try:
            st.session_state['df'] = pd.read_csv(uploaded_file, low_memory=False, skiprows=1)
            st.write("### 업로드된 데이터 미리보기")
            st.dataframe(st.session_state['df'].head())
            if st.button("분석 시작"):
                st.session_state['show_dashboard'] = True
        except Exception as e:
            st.error(f"파일을 처리하는 중 오류가 발생했습니다: {e}")

    if st.session_state['show_dashboard'] and st.session_state['df'] is not None:
        show_dashboard(st.session_state['df'])
