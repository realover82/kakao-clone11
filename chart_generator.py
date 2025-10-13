import pandas as pd #Add new feature for user authentication
import streamlit as st
from typing import Optional, List, Dict

# [주의]: 이 파일은 Streamlit의 기본 차트 위젯을 사용합니다.

def create_simple_bar_chart(df: pd.DataFrame, title_suffix: str, group_by_col: str):
    """
    Streamlit의 st.bar_chart를 사용하여 QC 결과를 출력합니다.
    group_by_col: X축의 그룹화 기준이 되는 컬럼 ('Date_Jig_Test', 'Date', 'Jig', 'Test')
    """
    if df.empty:
        st.error("차트 생성 실패: 입력 데이터가 비어 있습니다.")
        return

    # 1. 차트 데이터 준비: 불량 항목만 포함하여 복사
    df_chart_base = df[['미달 (Under)', '초과 (Over)', 'Failure']].copy()

    # 2. X축 레이블 생성 및 데이터 그룹화
    x_axis_col_name = 'X_Axis_Label'
    x_axis_title = ""
    
    # 2. X축 레이블 생성 및 데이터 그룹화
    # [수정] group_by_col 인수를 직접 사용하여 그룹화 로직을 분기합니다.
    if group_by_col == 'Date_Jig_Test':
        # 일별/Jig별/Test 항목별 분리 (가장 상세)
        # df_chart_base['X_Axis'] = (
        #     df['Date'].astype(str) + " / " + df['Jig'].astype(str) + " / " + df['Test']
        # )
        # x_axis_label = '날짜 / Jig / 테스트 항목'
        # df_chart = df_chart_base.set_index('X_Axis')

        # [핵심 수정]: X축 레이블을 Test 항목 이름만 남깁니다. (글자 수 줄임)
        # 툴팁에 모든 정보가 포함되므로 데이터 손실은 없습니다.
        df_long[x_axis_col_name] = df_long['Test'].astype(str) 
        
        # 기존 복합 레이블은 별도 컬럼으로 저장 (필요 시 사용)
        df_long['Full_ID'] = (df_long['Date'].astype(str) + " / " + df_long['Jig'].astype(str) + " / " + df_long['Test'])
        
        x_axis_title = '테스트 항목 (전체 상세)'

    elif group_by_col == 'Date':
        # 날짜별 합산
        df_chart_base['X_Axis'] = df['Date'].astype(str)
        df_chart = df_chart_base.groupby('X_Axis')[['미달 (Under)', '초과 (Over)', 'Failure']].sum()
        x_axis_label = '날짜별 합산'
        
    elif group_by_col == 'Jig':
        # Jig별 합산
        df_chart_base['X_Axis'] = df['Jig'].astype(str)
        df_chart = df_chart_base.groupby('X_Axis')[['미달 (Under)', '초과 (Over)', 'Failure']].sum()
        x_axis_label = 'Jig별 합산'

    elif group_by_col == 'Test':
        # Test 항목별 합산
        df_chart_base['X_Axis'] = df['Test'].astype(str)
        df_chart = df_chart_base.groupby('X_Axis')[['미달 (Under)', '초과 (Over)', 'Failure']].sum()
        x_axis_label = '테스트 항목별 합산'
    
    else:
        st.error("잘못된 그룹화 기준입니다.")
        return

    # 3. Streamlit의 기본 막대 차트 위젯을 사용하여 출력
    st.subheader(f"차트: {title_suffix} - {x_axis_label}") 
    st.bar_chart(df_chart) 
    st.caption(f"X축: {x_axis_label}")