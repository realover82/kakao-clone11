import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Dict, Optional

# 1. 기능별 분할된 모듈 임포트
from config import ANALYSIS_KEYS, TAB_PROPS_MAP
from analysis_main import display_analysis_result 
# from chart_generator import create_simple_bar_chart 

# 2. 각 CSV 분석 모듈 임포트 (기존 코드 유지)
from csv2 import read_csv_with_dynamic_header, analyze_data
from csv_Fw import read_csv_with_dynamic_header_for_Fw, analyze_Fw_data
from csv_RfTx import read_csv_with_dynamic_header_for_RfTx, analyze_RfTx_data
from csv_Semi import read_csv_with_dynamic_header_for_Semi, analyze_Semi_data
from csv_Batadc import read_csv_with_dynamic_header_for_Batadc, analyze_Batadc_data

# ==============================
# 콜백 함수 정의
# ==============================
def set_show_table_true():
    """테이블 표시 플래그를 True로 설정"""
    st.session_state.show_summary_table = True
    
def set_show_table_false():
    """테이블 표시 플래그를 False로 설정"""
    st.session_state.show_summary_table = False
    
# def set_show_chart_only_true():
#     """차트 표시 플래그를 True로 설정"""
#     st.session_state.show_chart = True

# def set_show_chart_false():
#     """차트 표시 플래그를 False로 설정"""
#     st.session_state.show_chart = False
    
def set_hide_all():
    """모두 숨기기"""
    st.session_state.show_summary_table = False
    # st.session_state.show_chart = False

# ==============================
# 동적 요약 테이블 생성 함수 (가성불량/진성불량 포함 및 세분화)
# ==============================
def generate_dynamic_summary_table(df: pd.DataFrame, selected_fields: list, props: Dict[str, str]) -> Optional[pd.DataFrame]:
    """
    필터링된 DataFrame과 선택된 필드를 사용하여 테스트 항목별 QC 결과 요약 DataFrame을 반환합니다.
    [수정]: '가성불량' 및 '진성불량' 세부 원인 카운트를 summary_data에서 추출하여 테이블을 구성합니다.
    """
    if df.empty:
        st.warning("필터링된 데이터가 없어 요약 테이블을 생성할 수 없습니다.")
        return None

    # # 1. Raw Data (df_raw)와 Summary Data 추출
    # analysis_key = 'Pcb'
    # summary_data = st.session_state.analysis_data[analysis_key][0]
    
    # # 2. 필수 컬럼 및 상태 맵핑 (로직 유지)
    qc_columns = [col for col in selected_fields if col.endswith('_QC') and col in df.columns and df[col].dtype == object]
    
    if not qc_columns:
        # st.warning("테이블 생성 불가: '상세 내역'에서 _QC로 끝나는 품질 관리 컬럼을 1개 이상 선택해 주세요.")
        # st.session_state['summary_df_for_chart'] = None
        st.error("테이블 생성 불가: 데이터에 _QC 컬럼이 존재하지 않습니다.")
        return None

    # analysis_key = 'Pcb'
    # summary_data = st.session_state.analysis_data[analysis_key][0]
    
     # 2. 상태 매핑 및 데이터프레임 준비 (생략)
    # status_map = {
    #     'Pass': 'Pass', '미달': '미달 (Under)', '초과': '초과 (Over)', 
    #     '제외': '제외 (Excluded)', '데이터 부족': '제외 (Excluded)' 
    # }


    # # 1. QC 컬럼 식별
    # # [핵심 수정]: df_raw 대신 df를 사용합니다.
    # qc_columns = [col for col in selected_fields if col.endswith('_QC') and col in df.columns and df[col].dtype == object]
    
    # if not qc_columns:
    #     # 선택된 QC 컬럼이 없지만, DF에 QC 컬럼이 존재하면 모두 포함
    #     qc_columns = [col for col in df.columns if col.endswith('_QC') and df[col].dtype == object]
    
    # if not qc_columns:
    #     st.error("테이블 생성 불가: 데이터에 _QC 컬럼이 존재하지 않습니다.")
    #     st.session_state['summary_df_for_chart'] = None
    #     return None

    # # 2. 상태 매핑 및 데이터프레임 준비 (생략)
    # status_map = {
    #     'Pass': 'Pass', '미달': '미달 (Under)', '초과': '초과 (Over)', 
    #     '제외': '제외 (Excluded)', '데이터 부족': '제외 (Excluded)' 
    # }
    

    JIG_COL = props.get('jig_col')
    TIMESTAMP_COL = props.get('timestamp_col')

    analysis_key = 'Pcb'
    summary_data = st.session_state.analysis_data[analysis_key][0]
    
    
    # 3. 데이터프레임 준비 및 Date/Jig 추출
    try:
        df_temp = df.copy()
        df_temp['Date'] = pd.to_datetime(df_temp[TIMESTAMP_COL], errors='coerce').dt.date
    except Exception:
        st.error(f"테이블 생성 실패: 날짜 컬럼({TIMESTAMP_COL}) 변환 오류.")
        # st.session_state['summary_df_for_chart'] = None
        return None
        
    df_temp['Jig'] = df_temp[JIG_COL] # 이미 analysis_utils에서 생성됨
    # df_temp['Test'] = df_temp['QC_Test_Col'].str.replace('_QC', '') if 'QC_Test_Col' in df_temp.columns else (df_temp[qc_columns[0]].apply(lambda x: qc_columns[0].replace('_QC', '')) if qc_columns else pd.NA)
    
    # 4. Summary Data를 기반으로 최종 테이블 데이터 재구성
    final_table_data = []

    jig_date_combinations = df_temp[[JIG_COL, 'Date']].drop_duplicates().itertuples(index=False)

    for row in jig_date_combinations:
        current_jig = getattr(row, JIG_COL.replace(' ', '_'))
        current_date = getattr(row, 'Date')
        current_date_iso = current_date.strftime("%Y-%m-%d")
        
        day_summary = summary_data.get(current_jig, {}).get(current_date_iso, {})
        
        if day_summary:
            row_data = {
                'Date': current_date,
                'Jig': current_jig,
                
                'Pass': day_summary.get('pass', 0),
                
                # # ⭐ [핵심 수정]: 가성불량 총합은 가성불량 세부 항목만 합산합니다.
                # '가성불량_미달': day_summary.get('false_defect_미달', 0),
                # '가성불량_초과': day_summary.get('false_defect_초과', 0),
                # '가성불량_제외': day_summary.get('false_defect_제외', 0),
                # '가성불량': day_summary.get('false_defect_미달', 0) + day_summary.get('false_defect_초과', 0) + day_summary.get('false_defect_제외', 0),
                
                # # ⭐ [핵심 수정]: 진성불량 총합은 진성불량 세부 항목만 합산합니다.
                # '진성불량_미달': day_summary.get('true_defect_미달', 0),
                # '진성불량_초과': day_summary.get('true_defect_초과', 0),
                # '진성불량_제외': day_summary.get('true_defect_제외', 0),
                # '진성불량': day_summary.get('true_defect_미달', 0) + day_summary.get('true_defect_초과', 0) + day_summary.get('true_defect_제외', 0),
                
                # 'Failure': day_summary.get('fail', 0),
                # 'Total': day_summary.get('total_test', 0),
                # 'Failure Rate (%)': day_summary.get('pass_rate', '0.0%') 
                # [수정] Pass/Fail 및 가성/진성 총합은 day_summary에서 직접 가져옵니다.
                'Pass': day_summary.get('pass', 0),
                
                '가성불량': day_summary.get('false_defect', 0),
                '가성불량_미달': day_summary.get('false_defect_미달', 0),
                '가성불량_초과': day_summary.get('false_defect_초과', 0),
                '가성불량_제외': day_summary.get('false_defect_제외', 0),
                
                '진성불량': day_summary.get('true_defect', 0),
                '진성불량_미달': day_summary.get('true_defect_미달', 0),
                '진성불량_초과': day_summary.get('true_defect_초과', 0),
                '진성불량_제외': day_summary.get('true_defect_제외', 0),
                
                'Failure': day_summary.get('fail', 0),
                'Total': day_summary.get('total_test', 0),
                'Failure Rate (%)': day_summary.get('pass_rate', '0.0%')
            }
            final_table_data.append(row_data)

    if not final_table_data:
        st.warning("Summary Data에서 일치하는 데이터 포인트를 찾을 수 없습니다. (필터 조건 확인 필요)")
        return None

    summary_df = pd.DataFrame(final_table_data)
    # [수정] Final_cols 정의는 로직을 따르도록 재구성
    # [수정] Failure Rate를 Total Failure를 기반으로 다시 계산
    # Failure는 이미 day_summary에서 계산되어 들어왔으므로, 최종 Failure Rate를 계산합니다.
    summary_df['Total'] = summary_df['Pass'] + summary_df['Failure']
    summary_df['Failure Rate (%)'] = (summary_df['Failure'] / summary_df['Total'] * 100).apply(lambda x: f"{x:.1f}%" if x == x else "0.0%")

    #
    # 4. 최종 컬럼 순서 및 정리
    final_cols = [
        'Date', 'Jig', 'Pass', 
        '가성불량', '가성불량_미달', '가성불량_초과', '가성불량_제외', 
        '진성불량', '진성불량_미달', '진성불량_초과', '진성불량_제외', 
        'Failure', 'Total', 'Failure Rate (%)'
    ]
    
    final_cols_filtered = [col for col in final_cols if col in summary_df.columns]

    summary_df = summary_df[final_cols_filtered].sort_values(by=['Date', 'Jig']).reset_index(drop=True)
    
    return summary_df 
    # # DF Melt, Group, Pivot: 모든 세부 상태별 카운트 획득
    # df_melted = df_temp.melt(id_vars=['Date', 'Jig', 'Test'], value_vars=qc_columns, var_name='QC_Test_Col', value_name='Status')
    # df_melted = df_melted.dropna(subset=['Status'])
    # df_melted['Mapped_Status'] = df_melted['Status'].apply(lambda x: status_map.get(x, '기타'))
    
    # df_grouped = df_melted.groupby(['Date', 'Jig', 'Test', 'Mapped_Status'], dropna=False).size().reset_index(name='Count')
    
    # df_pivot = df_grouped.pivot_table(index=['Date', 'Jig', 'Test'], columns='Mapped_Status', values='Count', fill_value=0).reset_index()

    # required_cols_detailed = ['Pass', '미달 (Under)', '초과 (Over)', '제외 (Excluded)']
    # for col in required_cols_detailed:
    #     if col not in df_pivot.columns: df_pivot[col] = 0
        
    # # [수정] 가성/진성 불량 세부 원인 및 총합 계산 로직 (유지)
    # df_pivot['가성불량_미달'] = df_pivot['미달 (Under)']
    # df_pivot['가성불량_초과'] = df_pivot['초과 (Over)']
    # df_pivot['가성불량_제외'] = df_pivot['제외 (Excluded)']
    # df_pivot['진성불량_미달'] = df_pivot['미달 (Under)']
    # df_pivot['진성불량_초과'] = df_pivot['초과 (Over)']
    # df_pivot['진성불량_제외'] = df_pivot['제외 (Excluded)']
    
    # total_non_pass = df_pivot['미달 (Under)'] + df_pivot['초과 (Over)'] + df_pivot['제외 (Excluded)']
    
    # df_pivot['가성불량'] = total_non_pass
    # df_pivot['진성불량'] = total_non_pass

    # df_pivot['Failure'] = total_non_pass
    # df_pivot['Total'] = df_pivot['Pass'] + df_pivot['Failure']
    # df_pivot['Failure Rate (%)'] = (df_pivot['Failure'] / df_pivot['Total'] * 100).apply(lambda x: f"{x:.1f}%" if x == x else "0.0%")
    
    # final_cols = [
    #     'Date', 'Jig', 'Test', 'Pass', '가성불량', '가성불량_미달', '가성불량_초과', '가성불량_제외', 
    #     '진성불량', '진성불량_미달', '진성불량_초과', '진성불량_제외', 'Failure', 'Total', 'Failure Rate (%)'
    # ]
    
    # final_cols_filtered = [col for col in final_cols if col in df_pivot.columns]

    # summary_df = df_pivot[final_cols_filtered].sort_values(by=['Date', 'Jig']).reset_index(drop=True)
    
    # # st.session_state['summary_df_for_chart'] = summary_df # <-- [수정] 차트 데이터 저장 주석 처리
    # return summary_df 

    # # # [수정] row 객체의 속성에 접근할 수 있도록 .itertuples(index=False) 사용 가정
    # # analysis_key = 'Pcb'
    # # summary_data = st.session_state.analysis_data[analysis_key][0]
    
    # # df_temp['Jig'] = df_temp[JIG_COL]
    # # df_temp['Test'] = df_temp['QC_Test_Col'].str.replace('_QC', '') if 'QC_Test_Col' in df_temp.columns else (df_temp[qc_columns[0]].apply(lambda x: qc_columns[0].replace('_QC', '')) if qc_columns else pd.NA)

    # # 4. Summary Data를 기반으로 최종 테이블 데이터 재구성
    # # final_table_data = []
    
    # # df_filtered에서 추출된 고유한 Date/Jig 조합 사용 (df는 이미 필터링된 상태)
    # # [수정] row 객체의 속성에 접근할 수 있도록 .itertuples(index=False) 사용 가정
    # jig_date_combinations = df_temp[[JIG_COL, 'Date']].drop_duplicates().itertuples(index=False)

    # for row in jig_date_combinations:
    #     # [수정] 속성 접근 (namedtuple)으로 변경
    #     current_jig = getattr(row, JIG_COL.replace(' ', '_')) 
    #     current_date = getattr(row, 'Date')
    #     current_date_iso = current_date.strftime("%Y-%m-%d")
        
    #     day_summary = summary_data.get(current_jig, {}).get(current_date_iso, {})
        
    #     if day_summary:
    #         # 이 루프는 summary_data의 일자별/Jig별 총합을 가져옵니다.
            
    #         # [임시]: Test 항목은 summary_data에 저장된 모든 QC 항목에 대해 분리하여 행을 생성합니다.
    #         # (Test 항목별 분리가 필요하므로, 각 Test 항목에 대해 행을 복제해야 함)
            
    #         # [재시도]: 모든 QC 항목에 대해 행을 분리하여 생성합니다.
    #         # for qc_col_name in [col for col in df_raw.columns if col.endswith('_QC')]:
    #         #     test_name = qc_col_name.replace('_QC', '')
                
    #         row_data = {
    #             'Date': current_date,
    #             'Jig': current_jig,
    #             # 'Test': test_name, 
                
    #             'Pass': day_summary.get('pass', 0),
                
    #             # 가성불량 (false_defect)
    #             '가성불량': day_summary.get('false_defect', 0),
    #             '가성불량_미달': day_summary.get('false_defect_미달', 0),
    #             '가성불량_초과': day_summary.get('false_defect_초과', 0),
    #             '가성불량_제외': day_summary.get('false_defect_제외', 0),
                
    #             # 진성불량 (true_defect)
    #             '진성불량': day_summary.get('true_defect', 0),
    #             '진성불량_미달': day_summary.get('true_defect_미달', 0),
    #             '진성불량_초과': day_summary.get('true_defect_초과', 0),
    #             '진성불량_제외': day_summary.get('true_defect_제외', 0),
                
    #             'Failure': day_summary.get('fail', 0),
    #             'Total': day_summary.get('total_test', 0),
    #             'Failure Rate (%)': day_summary.get('pass_rate', '0.0%') 
    #         }
    #         final_table_data.append(row_data)


    # if not final_table_data:
    #     st.warning("summary_data에서 일치하는 데이터 포인트를 찾을 수 없습니다. (필터 조건 확인 필요)")
    #     st.session_state['summary_df_for_chart'] = None
    #     return None

    # summary_df = pd.DataFrame(final_table_data)
    # # [추가] Test 항목별로 분리되지 않은 데이터를 위해 Test 컬럼을 삭제합니다.
    # # summary_df = summary_df.drop(columns=['Test'], errors='ignore')
    
    # # 최종 컬럼 순서 재정의 
    # final_cols = [
    #     'Date', 'Jig', 'Test', 
    #     'Pass', 
    #     '가성불량', '가성불량_미달', '가성불량_초과', '가성불량_제외', 
    #     '진성불량', '진성불량_미달', '진성불량_초과', '진성불량_제외', 
    #     'Failure', 
    #     'Total', 'Failure Rate (%)'
    # ]
    
    # final_cols_filtered = [col for col in final_cols if col in summary_df.columns]

    # summary_df = summary_df[final_cols_filtered].sort_values(by=['Date', 'Jig', 'Test']).reset_index(drop=True)
    
    # # 세션 상태에 저장 및 반환
    # st.session_state['summary_df_for_chart'] = summary_df 
    # return summary_df 


# ==============================
# 메인 실행 함수 (나머지 로직 유지)
# ==============================
def main():
    st.set_page_config(layout="wide")
    
    # --------------------------
    # HEADER 영역 시작 
    # --------------------------
    st.title("리모컨 생산 데이터 분석 툴") 
    st.markdown("---")

    # 세션 상태 초기화 (생략)
    ANALYSIS_KEYS = ['Pcb', 'Fw', 'RfTx', 'Semi', 'Batadc']
    for state_key in ['analysis_results', 'uploaded_files', 'analysis_data', 'analysis_time']:
        if state_key not in st.session_state:
            st.session_state[state_key] = {k: None for k in ANALYSIS_KEYS}
            
    if 'field_mapping' not in st.session_state: st.session_state.field_mapping = {}
    if 'sidebar_columns' not in st.session_state: st.session_state.sidebar_columns = {}
    for key in ANALYSIS_KEYS:
        if f'qc_filter_mode_{key}' not in st.session_state: st.session_state[f'qc_filter_mode_{key}'] = 'None'
    
    if 'show_summary_table' not in st.session_state: st.session_state.show_summary_table = False
    if 'show_chart' not in st.session_state: st.session_state.show_chart = False
    
    tab_map = {
        key: {
            **TAB_PROPS_MAP[key], 
            'reader': globals()[f"read_csv_with_dynamic_header_for_{key}"] if key != 'Pcb' else read_csv_with_dynamic_header,
            'analyzer': globals()[f"analyze_{key}_data"] if key != 'Pcb' else analyze_data
        }
        for key in ANALYSIS_KEYS
    }
    
    # (중략: 사이드바 로직)
    st.sidebar.title("분석 항목 선택")
    
    analysis_options = {key: f"파일 {key} 분석" for key in ANALYSIS_KEYS}
    default_key = st.session_state.get('last_selected_analysis_key', 'Pcb')
    
    selected_analysis_label = st.sidebar.radio(
        "분석할 데이터 선택", 
        list(analysis_options.values()),
        index=list(analysis_options.keys()).index(default_key),
        key='analysis_radio'
    )
    
    selected_key = next(key for key, label in analysis_options.items() if label == selected_analysis_label)
    st.session_state.last_selected_analysis_key = selected_key
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("현재 데이터 컬럼")
    if selected_key in st.session_state.sidebar_columns and st.session_state.sidebar_columns[selected_key]:
        st.sidebar.expander(f"**{selected_key.upper()} 컬럼 목록**").code(st.session_state.sidebar_columns[selected_key])
    else:
        st.sidebar.info("분석 실행 후 컬럼 목록이 표시됩니다.")

    # ====================================================
    # 2. 사이드바: 테이블/차트 표시 버튼 배치 (차트 부분 주석 처리)
    # ====================================================

    st.sidebar.markdown("---")
    st.sidebar.subheader("QC 결과 시각화")

    df_pcb = st.session_state.analysis_results.get('Pcb')
    
    if df_pcb is None or df_pcb.empty:
        st.sidebar.warning("'파일 Pcb 분석' 실행 후 버튼이 나타납니다.")
    else:
        if st.session_state.show_summary_table:
            st.sidebar.button("테이블 숨기기", on_click=set_show_table_false, key='hide_pcb_table')
        else:
            st.sidebar.button("PCB 요약 테이블 보기", on_click=set_show_table_true, key='show_pcb_table_btn')
            
        # if st.session_state.show_chart:
        #     st.sidebar.button("차트 숨기기", on_click=set_show_chart_false, key='hide_pcb_chart')
        # else:
        #     st.sidebar.button("PCB 요약 차트 보기", on_click=set_show_chart_only_true, key='show_pcb_chart_btn')
            
        if st.session_state.show_summary_table or st.session_state.show_chart:
            st.sidebar.button("모두 숨기기", on_click=set_hide_all, key='hide_all_results')
        
        if selected_key != 'Pcb': set_hide_all()
    
    # --------------------------
    # MAIN 영역 시작
    # --------------------------
    
    # --- 1. 상단 영역 (QC 요약 테이블 및 차트) ---
    st.header("QC 요약 결과")
    
    df_pcb_filtered = st.session_state.get('filtered_df_Pcb')
    selected_fields_for_table = st.session_state.get(f'detail_fields_select_Pcb', [])
    
    summary_df_display = None
    
    # 테이블 데이터 생성 로직을 먼저 실행하고, 성공 시 결과를 summary_df_display에 저장합니다.
    if df_pcb_filtered is not None and not df_pcb_filtered.empty:
        summary_df_temp = generate_dynamic_summary_table(df_pcb_filtered, selected_fields_for_table, TAB_PROPS_MAP['Pcb'])
        if summary_df_temp is not None:
            summary_df_display = summary_df_temp
        
    # A) 테이블 출력 로직
    if st.session_state.show_summary_table and summary_df_display is not None:
        st.subheader("PCB 테스트 항목별 QC 결과 요약 테이블 (일별/Jig별)")
        # st.dataframe(summary_df_display.set_index(['Date', 'Jig', 'Test']))
        st.dataframe(summary_df_display.set_index(['Date', 'Jig'])) # <-- [핵심 수정]: 'Test' 컬럼 제거
        st.markdown("---")
            
    # # B) 차트 출력 로직 (st.bar_chart 사용)
    # if st.session_state.show_chart:
    #     summary_df = st.session_state.get('summary_df_for_chart') 
        
    #     if summary_df is not None and not summary_df.empty:
    #         st.subheader("QC 결과 막대 그래프 (Jig별 분리)")
    #         try:
    #             # chart_generator의 함수 호출
    #             create_simple_bar_chart(summary_df, 'PCB', jig_separated=True) 
    #         except Exception as e:
    #             st.error(f"그래프 렌더링 중 오류 발생: {e}")
    #     else:
    #          st.warning("차트를 생성할 요약 데이터가 없습니다. 먼저 테이블을 확인하거나 필터를 해제해 주세요.")
    
    # [데이터 유효성 검사 및 안내]
    if (st.session_state.show_summary_table or st.session_state.show_chart) and (df_pcb_filtered is None or df_pcb_filtered.empty):
        st.error("결과 생성 실패: PCB 분석 데이터가 없거나 필터링 결과 0건입니다.")
        set_hide_all()
            
    st.markdown("---") 
    
    # --- 2. 하단 영역 (분석 실행 및 상세 내역) ---
    key = selected_key
    props = tab_map[key]
    
    st.header(f"분석 대상: {key.upper()} 데이터 분석")
    
    st.session_state.uploaded_files[key] = st.file_uploader(f"{key.upper()} 파일을 선택하세요", type=["csv"], key=f"uploader_{key}")
    
    if st.session_state.uploaded_files[key]:
        if st.button(f"{key.upper()} 분석 실행", key=f"analyze_{key}"):
            try:
                df = props['reader'](st.session_state.uploaded_files[key])
                
                if df is None or df.empty:
                    st.error(f"{key.upper()} 데이터 파일을 읽을 수 없거나 내용이 비어 있습니다. 파일 형식을 확인해주세요.")
                    st.session_state.analysis_results[key] = None
                else:
                    if props['jig_col'] not in df.columns or props['timestamp_col'] not in df.columns:
                        st.error(f"데이터에 필수 컬럼 ('{props['jig_col']}', '{props['timestamp_col']}')이 없습니다. 파일을 다시 확인해주세요.")
                        st.session_state.analysis_results[key] = None
                    else:
                        with st.spinner("데이터 분석 및 저장 중..."):
                            summary_data, all_dates = props['analyzer'](df)
                            st.session_state.analysis_data[key] = (summary_data, all_dates)
                            st.session_state.analysis_results[key] = df.copy() 
                            st.session_state.analysis_time[key] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            final_df = st.session_state.analysis_results[key]
                            final_cols = final_df.columns.tolist()
                            st.session_state.sidebar_columns[key] = final_cols
                            st.session_state.field_mapping[key] = final_cols
                            
                            set_hide_all() 
                            st.success("분석 완료! 결과가 저장되었습니다.")
                        
            except Exception as e:
                print(f"Error during {key} analysis: {e}") 
                st.error(f"분석 중 오류 발생: {e}")
                st.session_state.analysis_results[key] = None

        if st.session_state.analysis_results.get(key) is not None:
            # 상세 분석 결과
            display_analysis_result(key, st.session_state.uploaded_files[key].name, TAB_PROPS_MAP[key])
            
    # --------------------------
    # FOOTER 영역 시작 
    # --------------------------
    st.markdown("---")
    st.markdown("데이터 분석 툴 v1.0 | Google Gemini 기반 분석") 
    st.markdown("---")


if __name__ == "__main__":
    main()