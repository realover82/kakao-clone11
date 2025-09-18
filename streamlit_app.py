import streamlit as st
import pandas as pd
import altair as alt
import sqlite3

# SQLite 연결 함수
@st.cache_resource
def get_connection():
    try:
        # secrets.toml을 사용하지 않고 파일 경로를 직접 지정합니다.
        db_path = "db/SJ_TM2360E.sqlite3"
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결에 실패했습니다: {e}")
        return None

def show_dashboard(df):
    """
    데이터베이스에서 불러온 데이터를 기반으로 대시보드를 생성
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

    st.title("SQLite DB 데이터 대시보드")
    
    conn = get_connection()
    if conn:
        st.success("SQLite 데이터베이스에 성공적으로 연결되었습니다!")

        try:
            # SQL 쿼리를 실행하여 데이터를 가져옵니다.
            # SQLite DB의 테이블 구조에 맞게 쿼리를 수정해야 합니다.
            # 여기서는 'test_results'라는 테이블이 있다고 가정합니다.
            query = "SELECT * FROM test_results;"
            df = pd.read_sql_query(query, conn)
            
            show_dashboard(df)

        except Exception as e:
            st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
