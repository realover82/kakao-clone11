import streamlit as st
import sqlite3

def connect_to_db():
    # secrets.toml에서 파일 경로 불러오기
    db_path = st.secrets["db_credentials"]["DB_FILE"]
    conn = sqlite3.connect(db_path)
    return conn

# 앱 실행
try:
    conn = connect_to_db()
    st.success("SQLite 데이터베이스에 성공적으로 연결되었습니다!")
    # ... 데이터프레임 처리 및 대시보드 표시 ...
except Exception as e:
    st.error(f"SQLite 연결에 실패했습니다: {e}")
