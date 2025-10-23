import streamlit as st
import pandas as pd
from datetime import datetime, date
from typing import Dict, Any, Tuple, List, Optional

def check_initial_validity(analysis_key: str, props: Dict[str, str]) -> Optional[str]:
    """분석 시작 전 세션 상태의 필수 데이터 존재 여부를 확인합니다."""
    if st.session_state.analysis_results.get(analysis_key) is None:
        return "데이터 로드에 실패했습니다. 파일 형식을 확인해주세요."
    
    if st.session_state.analysis_data.get(analysis_key) is None:
        return "데이터 분석에 실패했습니다. 분석 함수를 확인해주세요."
        
    df_raw = st.session_state.analysis_results[analysis_key]
    all_dates = st.session_state.analysis_data[analysis_key][1]

    if all_dates is None:
        return "데이터 분석에 실패했습니다. 날짜 관련 컬럼 형식을 확인해주세요."
        
    required_columns = [props['jig_col'], props['timestamp_col']]
    missing_columns = [col for col in required_columns if col not in df_raw.columns]
    
    if missing_columns:
        return f"데이터에 필수 컬럼이 없습니다: {', '.join(missing_columns)}. 파일을 다시 확인해주세요."
        
    if df_raw.empty:
        return "원본 데이터가 비어있습니다."
        
    return None

def setup_filtering_ui(analysis_key: str, df_raw: pd.DataFrame, all_dates: List[date], props: Dict[str, str]) -> Tuple[str, date, date, pd.DataFrame]:
    """
    기본 필터링 UI를 설정하고 사용자의 선택 및 필터링된 DF를 반환합니다.
    """
    st.subheader("기본 필터링") 
    
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        jig_list = sorted(df_raw[props['jig_col']].dropna().unique().tolist()) if props['jig_col'] in df_raw.columns else []
        selected_jig = st.selectbox("PC(Jig) 선택", ["전체"] + jig_list, key=f"jig_select_{analysis_key}") 
    
    min_date = min(all_dates)
    max_date = max(all_dates)

    with filter_col2:
        start_date = st.date_input("시작 날짜", min_value=min_date, max_value=max_date, value=min_date, key=f"start_date_{analysis_key}")
    with filter_col3:
        end_date = st.date_input("종료 날짜", min_value=min_date, max_value=max_date, value=max_date, key=f"end_date_{analysis_key}")

    if start_date > end_date:
        st.error("시작 날짜는 종료 날짜보다 이전이어야 합니다.")
        # 오류 발생 시 빈 DataFrame 반환
        return selected_jig, min_date, max_date, pd.DataFrame()
        
    # === 핵심 로직: 실제 필터링 재활성화 및 안전한 날짜 변환 ===
    df_filtered = df_raw.copy()
    timestamp_col = props['timestamp_col']
    
    try:
        # 날짜 컬럼을 강제 변환 후 필터링을 시도합니다.
        df_temp = df_filtered.copy()
        df_temp['__DATE_TEMP__'] = pd.to_datetime(df_temp[timestamp_col], errors='coerce').dt.date
        df_temp = df_temp.dropna(subset=['__DATE_TEMP__']) 

        # 1. 날짜 필터링
        df_filtered = df_temp[
            (df_temp['__DATE_TEMP__'] >= start_date) & 
            (df_temp['__DATE_TEMP__'] <= end_date)
        ].copy()
        
        # 2. Jig 필터링
        if selected_jig != "전체" and not df_filtered.empty:
            df_filtered = df_filtered[df_filtered[props['jig_col']] == selected_jig].copy()
            
        # 임시 컬럼 제거 및 최종 반환
        if '__DATE_TEMP__' in df_filtered.columns:
            df_filtered = df_filtered.drop(columns=['__DATE_TEMP__'])

    except Exception as e:
        st.error(f"DEBUG ERROR: 필터링 중 심각한 오류 발생. 분석 함수 확인 필요. ({e})")
        df_filtered = pd.DataFrame() # 오류 시 빈 DF 반환

    return selected_jig, start_date, end_date, df_filtered