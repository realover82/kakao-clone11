import streamlit as st
import pandas as pd
from typing import Dict, Any

# 분할된 모듈 임포트
from analysis_utils import check_initial_validity, setup_filtering_ui
from aggregation_logic import aggregate_and_display_summary
from detail_display import display_detail_section
from df_search import display_df_search

def display_analysis_result(analysis_key: str, file_name: str, props: Dict[str, str]):
    """
    세션 상태에 저장된 분석 결과를 Streamlit에 표시하는 메인 함수입니다.
    이 함수는 UI 필터링 결과를 받아 세부 분석 모듈에 전달합니다.
    """
    
    # 1. 초기 유효성 검사 및 데이터 추출
    validation_result = check_initial_validity(analysis_key, props)
    if validation_result:
        st.error(validation_result)
        return

    summary_data = st.session_state.analysis_data[analysis_key][0]
    all_dates = st.session_state.analysis_data[analysis_key][1]
    df_raw = st.session_state.analysis_results[analysis_key]
    
    st.markdown(f"### '{file_name}' 분석 리포트")
    
    # 2. 필터링 UI 설정 및 필터링된 DF 획득 (analysis_utils.py에서 실제 필터링 수행)
    selected_jig, start_date, end_date, df_filtered = setup_filtering_ui(analysis_key, df_raw, all_dates, props)
    
    # Jig 필터링을 위한 리스트 (aggregation_logic과 detail_display에서 사용)
    jigs_to_display = [selected_jig] if selected_jig != "전체" else sorted(df_raw[props['jig_col']].dropna().unique().tolist())
    
    # 세션 상태에 필터링된 DF 저장 (테이블/차트 생성을 위해 사용)
    st.session_state[f'filtered_df_{analysis_key}'] = df_filtered.copy()
    
    # --- 최종 상태 확인 및 테이블 생성 플래그 설정 ---
    if df_filtered.empty:
        st.warning("선택된 필터 조건에 해당하는 데이터가 없습니다.")
        st.session_state['show_summary_table'] = False 
        st.session_state['show_chart'] = False
        return # 데이터가 없으면 여기서 종료
    
    st.write(f"**분석 시간**: {st.session_state.analysis_time[analysis_key]}")
    st.markdown("---")

    # 3. 데이터 집계 및 기간 요약 테이블 표시
    # df_filtered를 넘겨서 집계 로직이 필터링된 행 수로만 작동하도록 변경
    aggregate_and_display_summary(summary_data, all_dates, jigs_to_display, start_date, end_date, analysis_key, df_filtered)

    st.markdown("---")
    
    # 4. 상세 내역 표시
    # df_filtered를 넘겨서 상세 내역이 필터링된 날짜와 Jig만 표시하도록 변경
    display_detail_section(analysis_key, df_filtered, summary_data, all_dates, jigs_to_display)

    st.markdown("---")
    
    # 5. DF 검색 기능 표시
    display_df_search(analysis_key, df_filtered, props)