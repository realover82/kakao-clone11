import streamlit as st
import pymysql

def connect_to_db():
    conn = pymysql.connect(
        host=st.secrets["db_credentials"]["DB_HOST"],
        port=st.secrets["db_credentials"]["DB_PORT"],
        user=st.secrets["db_credentials"]["DB_USER"],
        password=st.secrets["db_credentials"]["DB_PASSWORD"]
    )
    return conn

try:
    conn = connect_to_db()
    st.success("데이터베이스에 성공적으로 연결되었습니다!")
    # ... 여기에 데이터프레임을 처리하는 로직을 추가합니다. ...
except Exception as e:
    st.error(f"연결 실패: {e}")
