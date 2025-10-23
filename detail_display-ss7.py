import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Union
from datetime import date

def display_detail_section(analysis_key: str, df_filtered: pd.DataFrame, summary_data: Dict, all_dates: List[date], jigs_to_display: List[str]):
    """
    상세 내역 섹션의 UI 및 데이터 표시를 담당합니다.
    df_filtered: analysis_utils에서 날짜/Jig 필터링을 거친 실제 데이터프레임입니다.
    """
    st.subheader("상세 내역 (일별)")
    
    # 1. 상세 내역 필드 선택 기능 추가
    all_raw_columns = df_filtered.columns.tolist()
    
    snumber_col = next((col for col in all_raw_columns if col.lower() == 'snumber'), 'SNumber')
    qc_cols_found = [col for col in all_raw_columns if col.endswith('_QC')]
    initial_default = list(set([snumber_col] + qc_cols_found)) 
    
    # === 핵심 수정: 선택된 필드 목록을 세션 상태에 저장 ===
    selected_detail_fields = st.multiselect(
        "상세 내역에 표시할 필드 선택",
        all_raw_columns,
        default=initial_default,
        key=f"detail_fields_select_{analysis_key}"
    )
    # 멀티셀렉트 결과가 변경될 때마다 세션 상태에 저장합니다.
    st.session_state[f'detail_fields_{analysis_key}'] = selected_detail_fields 
    # ========================================================

    # 2. 상세 내역 보기 제어용 세션 상태 변수 초기화 및 버튼
    if f'show_details_{analysis_key}' not in st.session_state:
        st.session_state[f'show_details_{analysis_key}'] = False

    if st.button("상세 내역 조회", key=f"show_details_btn_{analysis_key}"):
        st.session_state[f'show_details_{analysis_key}'] = True
        st.session_state[f'detail_mode_{analysis_key}'] = 'defects'
        st.session_state[f'qc_filter_mode_{analysis_key}'] = 'None' # 필터 모드 초기화


    if st.session_state[f'show_details_{analysis_key}']:
        if f'detail_mode_{analysis_key}' not in st.session_state:
            st.session_state[f'detail_mode_{analysis_key}'] = 'all'

        detail_col1, detail_col2, detail_col3, detail_col4, detail_col5 = st.columns(5) # 5개의 컬럼으로 확장
        
        with detail_col1:
            if st.button("전체 보기", key=f"detail_all_{analysis_key}"):
                st.session_state[f'detail_mode_{analysis_key}'] = 'all'
                st.session_state[f'qc_filter_mode_{analysis_key}'] = 'None' #추가
        with detail_col2:
            if st.button("불량만 보기", key=f"detail_defects_{analysis_key}"):
                st.session_state[f'detail_mode_{analysis_key}'] = 'defects'
                st.session_state[f'qc_filter_mode_{analysis_key}'] = 'None' #추가
        with detail_col3:
            if st.button("PASS만 보기", key=f"detail_pass_{analysis_key}"):
                st.session_state[f'detail_mode_{analysis_key}'] = 'pass'
                st.session_state[f'qc_filter_mode_{analysis_key}'] = 'None' #추가
                
        # === 새로운 필터 버튼 추가 ===
        with detail_col4:
            if st.button("불량(초과,미달만)", key=f"filter_qc_fail_{analysis_key}"):
                st.session_state[f'qc_filter_mode_{analysis_key}'] = 'FailOnly'
                st.session_state[f'detail_mode_{analysis_key}'] = 'all' # 전체 모드에서 필터링
        with detail_col5:
            if st.button("PASS(초과,미달만)", key=f"filter_qc_pass_{analysis_key}"):
                st.session_state[f'qc_filter_mode_{analysis_key}'] = 'PassOnly'
                st.session_state[f'detail_mode_{analysis_key}'] = 'all' # 전체 모드에서 필터링
        # =============================     
        
        current_mode = st.session_state[f'detail_mode_{analysis_key}']
        qc_filter_mode = st.session_state[f'qc_filter_mode_{analysis_key}'] #추가
        
        date_range_for_display = st.session_state.get(f'agg_dates_{analysis_key}', all_dates)
        
        for date_obj in date_range_for_display: # 변경된 필터링된 날짜 사용
            st.markdown(f"**{date_obj.strftime('%Y-%m-%d')}**")
            
            for jig in jigs_to_display:
                data_point = summary_data.get(jig, {}).get(date_obj.strftime('%Y-%m-%d'))
                if not data_point or data_point.get('total_test', 0) == 0:
                    continue

                st.markdown(f"**PC(Jig): {jig}**")
                
                # === 1. 카테고리 결정 로직 ===
                if qc_filter_mode == 'FailOnly':
                    categories = ['false_defect', 'true_defect']
                    labels = ['가성불량', '진성불량']
                elif qc_filter_mode == 'PassOnly':
                    categories = ['pass'] 
                    labels = ['PASS']
                elif current_mode == 'defects':
                    categories = ['false_defect', 'true_defect']
                    labels = ['가성불량', '진성불량']
                elif current_mode == 'pass':
                    categories = ['pass']
                    labels = ['PASS']
                else: 
                    categories = ['pass', 'false_defect', 'true_defect', 'fail']
                    labels = ['PASS', '가성불량', '진성불량', 'FAIL']

                # ============================================================

                for cat, label in zip(categories, labels):
                    full_data_list = data_point.get(f'{cat}_data', [])
                    
                    if not full_data_list:
                        continue
                    
                    # === 2. QC 필터링 로직 (각 카테고리별로 필터링) ===
                    is_qc_filtering_active = qc_filter_mode in ['FailOnly', 'PassOnly']

                    if is_qc_filtering_active:
                        selected_qc_cols_for_filter = [col for col in selected_detail_fields if col.endswith('_QC')]
                        
                        if selected_qc_cols_for_filter:
                            filtered_list = []
                            target_statuses = ['미달', '초과']
                            
                            for record in full_data_list:
                                qc_value_found = False
                                for qc_col in selected_qc_cols_for_filter:
                                    qc_value = record.get(qc_col)
                                    
                                    # 조건 1: '불량(초과,미달만)' 버튼을 눌렀을 경우
                                    if qc_filter_mode == 'FailOnly' and qc_value in target_statuses:
                                        qc_value_found = True
                                        break
                                    # 조건 2: 'PASS(초과,미달만)' 버튼을 눌렀을 경우
                                    elif qc_filter_mode == 'PassOnly' and cat == 'pass' and qc_value in target_statuses:
                                        # PASS 카테고리 AND QC 미달/초과인 경우만 필터링
                                        qc_value_found = True
                                        break
                                    
                                if qc_value_found:
                                    filtered_list.append(record)
                            
                            full_data_list = filtered_list # 필터링된 리스트로 교체

                    if not full_data_list:
                        continue
                    # ======================================================

                    count = len(full_data_list)
                    unique_count = len(set(d.get('SNumber', 'N/A') for d in full_data_list))

                    qc_summary_parts_html = [] 
                    qc_summary_parts_plain = [] 
                    fields_to_check = selected_detail_fields
                    selected_qc_cols = [col for col in fields_to_check if col.endswith('_QC')]
                    
                    for qc_col in selected_qc_cols:
                        qc_statuses = [record.get(qc_col) for record in full_data_list if record.get(qc_col) is not None]
                        
                        if not qc_statuses: continue
                            
                        qc_counts = pd.Series(qc_statuses).value_counts().to_dict()
                        parts_html = []
                        parts_plain = []
                        
                        # --- HTML 및 Plain Text 구성 로직 ---
                        if qc_counts.get('Pass', 0) > 0: parts_html.append(f"Pass {qc_counts['Pass']}건"); parts_plain.append(f"Pass {qc_counts['Pass']}건")
                        if qc_counts.get('제외', 0) > 0: parts_html.append(f"제외 {qc_counts['제외']}건"); parts_plain.append(f"제외 {qc_counts['제외']}건")
                        if qc_counts.get('데이터 부족', 0) > 0: parts_html.append(f"데이터 부족 {qc_counts['데이터 부족']}건"); parts_plain.append(f"데이터 부족 {qc_counts['데이터 부족']}건")
                            
                        # 미달/초과 (빨간색 적용 / Plain Text)
                        if qc_counts.get('미달', 0) > 0: parts_html.append(f"<span style='color:red;'>미달 {qc_counts['미달']}건</span>"); parts_plain.append(f"미달 {qc_counts['미달']}건")
                        if qc_counts.get('초과', 0) > 0: parts_html.append(f"<span style='color:red;'>초과 {qc_counts['초과']}건</span>"); parts_plain.append(f"초과 {qc_counts['초과']}건")
                        
                        if parts_plain: # 순수 텍스트가 있어야만 집계함
                            qc_summary_parts_html.append(f"**{qc_col.replace('_QC', '')}**: {', '.join(parts_html)}")
                            qc_summary_parts_plain.append(f"{qc_col.replace('_QC', '')}: {', '.join(parts_plain)}")
                        # --- HTML 및 Plain Text 구성 로직 끝 ---

                    # 1. 제목에 들어갈 순수한 텍스트 QC 요약 구성
                    qc_summary_plain_text = ""
                    if qc_summary_parts_plain:
                        qc_summary_plain_text = f" [QC: {', '.join(qc_summary_parts_plain)}]" # 제목에 들어갈 내용
                        
                    # 2. Expander 제목 구성 (순수 텍스트)
                    expander_title_base = f"{label} - {count}건 (중복값제거 SN: {unique_count}건){qc_summary_plain_text}"
                    
                    with st.expander(expander_title_base, expanded=False):
                        
                        # 3. 제목 아래에 색상이 적용된 QC 요약 정보 출력 (HTML)
                        if qc_summary_parts_html:
                            qc_summary_html_output = f" [<span style='color:black;'>QC: {', '.join(qc_summary_parts_html)}</span>]"
                            qc_html = f"<div>{qc_summary_html_output.replace('QC:', 'QC:')}</div>"
                            st.markdown(qc_html, unsafe_allow_html=True)
                            st.markdown("---")
                        
                        fields_to_display = selected_detail_fields 
                        
                        if not fields_to_display:
                            st.info("표시할 필드가 선택되지 않았습니다.")
                            continue

                        # 4. 상세 내역 개별 항목 출력 (미달/초과 빨간색 적용)
                        for item in full_data_list:
                            formatted_fields = []
                            for field in fields_to_display:
                                value = item.get(field, 'N/A')
                                
                                # === 개별 항목 빨간색 적용 로직 ===
                                if field.endswith('_QC') and value in ['미달', '초과']:
                                    # QC 결과가 '미달' 또는 '초과'일 때 빨간색으로 감쌉니다.
                                    formatted_fields.append(f"{field}: <span style='color:red;'>{value}</span>")
                                else:
                                    formatted_fields.append(f"{field}: {value}")
                                # ===================================
                                
                                # st.markdown을 사용하여 HTML이 렌더링되도록 합니다.
                            st.markdown(", ".join(formatted_fields), unsafe_allow_html=True)

            st.markdown("---")