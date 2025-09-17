import streamlit as st
import pymysql # 또는 사용하는 데이터베이스에 맞는 라이브러리 (psycopg2, sqlalchemy 등)

# secrets.toml에 저장된 정보 불러오기
DB_HOST = st.secrets["db_credentials"]["DB_HOST"]
DB_PORT = st.secrets["db_credentials"]["DB_PORT"]
DB_USER = st.secrets["db_credentials"]["DB_USER"]
DB_PASSWORD = st.secrets["db_credentials"]["DB_PASSWORD"]

st.write("데이터베이스 연결 정보 불러오기 완료!")

try:
    # 데이터베이스 연결
    connection = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        cursorclass=pymysql.cursors.DictCursor
    )

    st.success("데이터베이스에 성공적으로 연결되었습니다.")

    # 연결 테스트 (예시: 쿼리 실행)
    # with connection.cursor() as cursor:
    #    sql = "SELECT 'Hello, World!' as message"
    #    cursor.execute(sql)
    #    result = cursor.fetchone()
    #    st.write(result['message'])

except Exception as e:
    st.error(f"데이터베이스 연결에 실패했습니다: {e}")

finally:
    if 'connection' in locals() and connection.open:
        connection.close()
        st.info("데이터베이스 연결이 종료되었습니다.")
