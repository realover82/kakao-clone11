import streamlit as st
import pandas as pd
from typing import Dict, Any

def display_df_search(analysis_key: str, df_filtered: pd.DataFrame, props: Dict[str, str]):
    """
    SNumber 검색 및 컬럼 선택을 포함한 DataFrame 조회 기능을 표시합니다.
    """
    st.subheader("DF 조회")
    search_col1, search_col2, search_col3 = st.columns([1, 2, 1])
    
    filter_state_key = f'applied_filters_{analysis_key}'

    # 초기 상태 설정 (없으면 초기화)
    if filter_state_key not in st.session_state:
        st.session_state[filter_state_key] = {'snumber': '', 'columns': []}
        
    applied_filters = st.session_state[filter_state_key]
    
    with search_col1:
        snumber_query = st.text_input("SNumber 검색", key=f"snumber_search_{analysis_key}", value=applied_filters['snumber'])
    with search_col2:
        all_columns = df_filtered.columns.tolist()
        qc_cols_default = [col for col in all_columns if col.endswith('_QC')]
        default_cols_for_search = ['SNumber', props['timestamp_col'], 'PassStatusNorm'] + qc_cols_default
        
        # 적용된 필터 컬럼이 없다면 기본 컬럼 사용
        default_cols_value = applied_filters['columns'] if applied_filters['columns'] else [col for col in all_columns if col in default_cols_for_search]
        
        selected_columns = st.multiselect(
            "표시할 필드(열) 선택", 
            all_columns, 
            key=f"col_select_{analysis_key}",
            default=default_cols_value
        )
    with search_col3:
        st.write("") 
        st.write("") 
        apply_button = st.button("필터 적용", key=f"apply_filter_{analysis_key}")

    if apply_button:
        st.session_state[filter_state_key] = {
            'snumber': snumber_query,
            'columns': selected_columns
        }
        applied_filters = st.session_state[filter_state_key] # 필터 즉시 반영
    
    with st.expander("DF 조회"):
        df_display = df_filtered.copy() # df_raw 대신 필터링된 df_filtered 사용
        
        has_snumber_query = False
        
        if applied_filters['snumber']:
            query = applied_filters['snumber']
            has_snumber_query = True
            
            if 'SNumber' in df_display.columns and pd.api.types.is_string_dtype(df_display['SNumber']):
                df_display = df_display[df_display['SNumber'].str.contains(query, na=False, case=False)]
            else:
                try:
                    df_display = df_display[df_display.apply(lambda row: query.lower() in str(row.values).lower(), axis=1)]
                except Exception:
                    st.warning("SNumber 검색을 지원하지 않는 데이터 형식입니다.")
                    pass 

        if applied_filters['columns']:
            existing_cols = [col for col in applied_filters['columns'] if col in df_display.columns]
            df_display = df_display[existing_cols]
        
        # DF 조회 결과 출력
        if df_display.empty:
            if has_snumber_query:
                st.info(f"선택된 필터 조건 ('{applied_filters['snumber']}')에 해당하는 결과가 없습니다. 검색어를 확인하거나 필터를 해제해 주세요.")
            else:
                st.info("데이터프레임에 표시할 행이 없습니다. 분석 데이터(df_raw)를 확인해주세요.")
        else:
            st.dataframe(df_display)