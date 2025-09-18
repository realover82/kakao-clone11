import streamlit as st
import pandas as pd
import sqlite3

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

def main():
    st.set_page_config(
        page_title="단일 데이터 조회",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="auto"
    )

    st.title("SQLite DB 데이터 조회")
    
    conn = get_connection()
    if conn:
        st.success("SQLite 데이터베이스에 성공적으로 연결되었습니다!")
        try:
            # SQL 쿼리를 실행하여 'SNumber' 필드 1개만 가져옵니다.
            query = "SELECT SNumber FROM historyinspection LIMIT 1;"
            df = pd.read_sql_query(query, conn)
            
            st.write("### SNumber 필드 첫 번째 행")
            if not df.empty:
                st.dataframe(df)
            else:
                st.write("데이터를 찾을 수 없습니다.")

        except pd.io.sql.DatabaseError as e:
            st.error(f"SQL 쿼리 오류: {e}")
        except Exception as e:
            st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")
    else:
        st.error("데이터베이스 연결에 실패하여 앱을 실행할 수 없습니다.")

if __name__ == "__main__":
    main()
