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
    # [핵심 수정]: '미달 (Under)' 컬럼이 없으므로, 세부 컬럼을 합산하여 새 컬럼을 만듭니다.
    
    # 1-A. 필요한 세부 컬럼이 존재하는지 확인하고, 없다면 0으로 채웁니다.
    # required_parts = ['가성불량_미달', '진성불량_미달', '가성불량_초과', '진성불량_초과', '가성불량_제외', '진성불량_제외', 'Failure']
    # for col in required_parts:
    #     if col not in df.columns:
    #         df[col] = 0

    # 1. 차트 데이터 준비: 불량 항목만 포함하여 복사
    # df_chart_base = df[['미달 (Under)', '초과 (Over)', 'Failure']].copy()
    # df_chart_base = df.copy()
    df_chart_base = df[['미달 (Under)', '초과 (Over)', 'Failure']].copy()

    # 가성불량의 세부 원인 (미달/초과/제외)을 합쳐서 새로운 컬럼으로 만듭니다.
    # df_chart_base['Total_미달'] = df_chart_base['가성불량_미달'] + df_chart_base['진성불량_미달']
    # df_chart_base['Total_초과'] = df_chart_base['가성불량_초과'] + df_chart_base['진성불량_초과']
    # df_chart_base['Total_제외'] = df_chart_base['가성불량_제외'] + df_chart_base['진성불량_제외']
    
    # 차트에 표시할 컬럼 목록
    # chart_cols = ['Total_미달', 'Total_초과', 'Total_제외', 'Failure']

    # 2. X축 레이블 생성 및 데이터 그룹화
    x_axis_label = ""
    
    if group_by_col == 'Date_Jig_Test':
        df_chart_base['X_Axis'] = (
            df_chart_base['Date'].astype(str) + " / " + df_chart_base['Jig'].astype(str) + " / " + df_chart_base['Test']
        )
        x_axis_label = '날짜 / Jig / 테스트 항목'
        df_chart = df_chart_base.set_index('X_Axis')

    elif group_by_col == 'Date':
        # 날짜별 합산
        df_chart_base['X_Axis'] = df_chart_base['Date'].astype(str)
        df_chart = df_chart_base.groupby('X_Axis')[chart_cols].sum()
        x_axis_label = '날짜별 합산'
        
    elif group_by_col == 'Jig':
        # Jig별 합산
        df_chart_base['X_Axis'] = df_chart_base['Jig'].astype(str)
        df_chart = df_chart_base.groupby('X_Axis')[chart_cols].sum()
        x_axis_label = 'Jig별 합산'

    elif group_by_col == 'Test':
        # Test 항목별 합산
        df_chart_base['X_Axis'] = df_chart_base['Test'].astype(str)
        df_chart = df_chart_base.groupby('X_Axis')[chart_cols].sum()
        x_axis_label = '테스트 항목별 합산'
    
    else:
        st.error("잘못된 그룹화 기준입니다.")
        return

    # 3. Streamlit의 기본 막대 차트 위젯을 사용하여 출력
    # st.subheader(f"차트: {title_suffix}") 
    
    # 3. Streamlit의 기본 막대 차트 위젯을 사용하여 출력
    st.subheader(f"차트: {title_suffix} - {x_axis_label}") # title_suffix 사용
    st.bar_chart(df_chart[['미달 (Under)', '초과 (Over)', 'Failure']]) 
    st.caption(f"X축: {x_axis_label}")

    # # [수정] 차트에 표시할 컬럼명을 사용자 친화적으로 변경
    # df_chart = df_chart.rename(columns={
    #     'Total_미달': '미달 (Under)',
    #     'Total_초과': '초과 (Over)',
    #     'Total_제외': '제외 (Excluded)'
    # })
    
    # st.bar_chart(df_chart[['미달 (Under)', '초과 (Over)', '제외 (Excluded)']]) 
    # st.caption(f"X축: {x_axis_label}")

    # # 2. X축 레이블 생성 및 데이터 그룹화
    # # [수정] group_by_col 인수를 직접 사용하여 그룹화 로직을 분기합니다.
    # if group_by_col == 'Date_Jig_Test':
    #     # 일별/Jig별/Test 항목별 분리 (가장 상세)
    #     df_chart_base['X_Axis'] = (
    #         df['Date'].astype(str) + " / " + df['Jig'].astype(str) + " / " + df['Test']
    #     )
    #     x_axis_label = '날짜 / Jig / 테스트 항목'
    #     df_chart = df_chart_base.set_index('X_Axis')

    # elif group_by_col == 'Date':
    #     # 날짜별 합산
    #     df_chart_base['X_Axis'] = df['Date'].astype(str)
    #     df_chart = df_chart_base.groupby('X_Axis')[['미달 (Under)', '초과 (Over)', 'Failure']].sum()
    #     x_axis_label = '날짜별 합산'
        
    # elif group_by_col == 'Jig':
    #     # Jig별 합산
    #     df_chart_base['X_Axis'] = df['Jig'].astype(str)
    #     df_chart = df_chart_base.groupby('X_Axis')[['미달 (Under)', '초과 (Over)', 'Failure']].sum()
    #     x_axis_label = 'Jig별 합산'

    # elif group_by_col == 'Test':
    #     # Test 항목별 합산
    #     df_chart_base['X_Axis'] = df['Test'].astype(str)
    #     df_chart = df_chart_base.groupby('X_Axis')[['미달 (Under)', '초과 (Over)', 'Failure']].sum()
    #     x_axis_label = '테스트 항목별 합산'
    
    # else:
    #     st.error("잘못된 그룹화 기준입니다.")
    #     return

    # # 3. Streamlit의 기본 막대 차트 위젯을 사용하여 출력
    # st.subheader(f"차트: {title_suffix} - {x_axis_label}") 
    # st.bar_chart(df_chart) 
    # st.caption(f"X축: {x_axis_label}")