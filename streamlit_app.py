import streamlit as st
import pandas as pd
import sqlite3

# SQLite 연결 함수
@st.cache_resource
def get_connection():
    try:
        # secrets.toml에서 파일 경로 불러오기
        db_path = st.secrets["db_credentials"]["DB_FILE"]
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결에 실패했습니다: {e}")
        return None

# 앱 실행
def main():
    st.set_page_config(
        page_title="SNumber 조회",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="auto"
    )

    st.title("SQLite DB 데이터 조회")
    
    conn = get_connection()
    if conn:
        st.success("SQLite 데이터베이스에 성공적으로 연결되었습니다!")

        try:
            # SQL 쿼리를 실행하여 'historyinspection' 테이블에서 'SNumber' 컬럼만 가져옵니다.
            query = "SELECT SNumber FROM historyinspection;"
            df = pd.read_sql_query(query, conn)
            
            st.write("### SNumber 목록")
            st.dataframe(df)

        except pd.io.sql.DatabaseError as e:
            st.error(f"SQL 쿼리 오류: {e}")
        except Exception as e:
            st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")
        finally:
            conn.close()
    else:
        st.error("데이터베이스 연결에 실패하여 앱을 실행할 수 없습니다.")

if __name__ == "__main__":
    main()
