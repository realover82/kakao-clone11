import streamlit as st #Add new feature for user authentication
import pandas as pd
from datetime import datetime
# import altair as alt
from typing import Dict 

# 1. 기능별 분할된 모듈 임포트
from config import ANALYSIS_KEYS, TAB_PROPS_MAP
from analysis_main import display_analysis_result 
# from chart_generator import create_stacked_bar_chart 
from chart_generator import create_simple_bar_chart

# 2. 각 CSV 분석 모듈 임포트 (기존 코드 유지)
from csv2 import read_csv_with_dynamic_header, analyze_data
from csv_Fw import read_csv_with_dynamic_header_for_Fw, analyze_Fw_data
from csv_RfTx import read_csv_with_dynamic_header_for_RfTx, analyze_RfTx_data
from csv_Semi import read_csv_with_dynamic_header_for_Semi, analyze_Semi_data
from csv_Batadc import read_csv_with_dynamic_header_for_Batadc, analyze_Batadc_data

# ==============================
# 콜백 함수 정의 (생략)
# ==============================
def set_show_table_true():
    st.session_state.show_summary_table = True
    
def set_show_table_false():
    st.session_state.show_summary_table = False
    
def set_show_chart_only_true():
    st.session_state.show_chart = True

def set_show_chart_false():
    st.session_state.show_chart = False
    
def set_hide_all():
    st.session_state.show_summary_table = False
    st.session_state.show_chart = False

# ==============================
# 동적 요약 테이블 생성 함수 (생략, 변경 없음)
# ==============================
def generate_dynamic_summary_table(df: pd.DataFrame, selected_fields: list, props: Dict[str, str]):
    # ... (기존 테이블 생성 로직)
    if df.empty:
        st.warning("필터링된 데이터가 없어 요약 테이블을 생성할 수 없습니다.")
        return

    qc_columns = [col for col in selected_fields if col.endswith('_QC') and col in df.columns and df[col].dtype == object]
    
    if not qc_columns:
        st.warning("테이블 생성 불가: '상세 내역'에서 _QC로 끝나는 품질 관리 컬럼을 1개 이상 선택해 주세요.")
        st.session_state['summary_df_for_chart'] = None
        return

    status_map = {'Pass': 'Pass', '미달': '미달 (Under)', '초과': '초과 (Over)', '제외': '제외 (Excluded)', '데이터 부족': '제외 (Excluded)' }
    JIG_COL = props['jig_col']
    TIMESTAMP_COL = props['timestamp_col']
    
    # (중략: 데이터프레임 준비 및 그룹핑 로직)
    try:
        df['Date'] = pd.to_datetime(df[TIMESTAMP_COL], errors='coerce').dt.date
    except Exception:
        st.error(f"테이블 생성 실패: 날짜 컬럼({TIMESTAMP_COL}) 변환 오류.")
        st.session_state['summary_df_for_chart'] = None
        return None
        
    df['Jig'] = df[JIG_COL]
    df_melted = df.melt(id_vars=['Date', 'Jig'], value_vars=qc_columns, var_name='QC_Test_Col', value_name='Status')
    df_melted = df_melted.dropna(subset=['Status'])
    df_melted['Mapped_Status'] = df_melted['Status'].apply(lambda x: status_map.get(x, '제외 (Excluded)'))
    df_grouped = df_melted.groupby(['Date', 'Jig', 'QC_Test_Col', 'Mapped_Status'], dropna=False).size().reset_index(name='Count')
    df_grouped['Test'] = df_grouped['QC_Test_Col'].str.replace('_QC', '')
    df_pivot = df_grouped.pivot_table(index=['Date', 'Jig', 'Test'], columns='Mapped_Status', values='Count', fill_value=0).reset_index()

    required_cols = ['Pass', '미달 (Under)', '초과 (Over)', '제외 (Excluded)']
    for col in required_cols:
        if col not in df_pivot.columns: df_pivot[col] = 0

    df_pivot['Total'] = df_pivot[required_cols].sum(axis=1)
    df_pivot['Failure'] = df_pivot['미달 (Under)'] + df_pivot['초과 (Over)']
    df_pivot['Failure Rate (%)'] = (df_pivot['Failure'] / df_pivot['Total'] * 100).apply(lambda x: f"{x:.1f}%" if x == x else "0.0%")
    final_cols = ['Date', 'Jig', 'Test', 'Pass', '미달 (Under)', '초과 (Over)', '제외 (Excluded)', 'Total', 'Failure', 'Failure Rate (%)']
    summary_df = df_pivot[final_cols].sort_values(by=['Date', 'Jig', 'Test']).reset_index(drop=True)
    
    # # 5. Streamlit에 출력 및 차트 데이터 저장
    # st.subheader("PCB 테스트 항목별 QC 결과 요약 테이블 (일별/Jig별)")
    # st.dataframe(summary_df.set_index(['Date', 'Jig', 'Test']))
    # st.markdown("---")
    
    # 차트 생성을 위해 summary_df를 세션 상태에 저장합니다.
    st.session_state['summary_df_for_chart'] = summary_df
    return summary_df # DataFrame 반환


# ==============================
# 메인 실행 함수
# ==============================
def main():
    st.set_page_config(layout="wide")
    
    # --------------------------
    # HEADER 영역 시작 
    # --------------------------
    st.title("리모컨 생산 데이터 분석 툴") 
    st.markdown("---")

    # 세션 상태 초기화 (생략)
    # ...
    ANALYSIS_KEYS = ['Pcb', 'Fw', 'RfTx', 'Semi', 'Batadc']
    for state_key in ['analysis_results', 'uploaded_files', 'analysis_data', 'analysis_time']:
        if state_key not in st.session_state:
            st.session_state[state_key] = {k: None for k in ANALYSIS_KEYS}
            
    if 'field_mapping' not in st.session_state:
        st.session_state.field_mapping = {}
    if 'sidebar_columns' not in st.session_state:
        st.session_state.sidebar_columns = {}
        
    for key in ANALYSIS_KEYS:
        if f'qc_filter_mode_{key}' not in st.session_state:
            st.session_state[f'qc_filter_mode_{key}'] = 'None'
    
    if 'show_summary_table' not in st.session_state:
        st.session_state.show_summary_table = False
    
    if 'show_chart' not in st.session_state: 
        st.session_state.show_chart = False
    
    tab_map = {
        key: {
            **TAB_PROPS_MAP[key], 
            'reader': globals()[f"read_csv_with_dynamic_header_for_{key}"] if key != 'Pcb' else read_csv_with_dynamic_header,
            'analyzer': globals()[f"analyze_{key}_data"] if key != 'Pcb' else analyze_data
        }
        for key in ANALYSIS_KEYS
    }
    
    # ====================================================
    # 1. 사이드바: 분석 항목 선택 라디오 버튼 및 설정 (생략)
    # ====================================================
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
    # 2. 사이드바: 테이블/차트 표시 버튼 배치 
    # ====================================================
    st.sidebar.markdown("---")
    st.sidebar.subheader("QC 결과 시각화")

    df_pcb = st.session_state.analysis_results.get('Pcb')
    
    if df_pcb is None or df_pcb.empty:
        st.sidebar.warning("'파일 Pcb 분석' 실행 후 버튼이 나타납니다.")
    else:
        # --- 테이블 버튼 ---
        if st.session_state.show_summary_table:
            st.sidebar.button("테이블 숨기기", on_click=set_show_table_false, key='hide_pcb_table')
        else:
            st.sidebar.button("PCB 요약 테이블 보기", on_click=set_show_table_true, key='show_pcb_table_btn')
            
        # --- 차트 버튼 ---
        if st.session_state.show_chart:
            st.sidebar.button("차트 숨기기", on_click=set_show_chart_false, key='hide_pcb_chart')
        else:
            st.sidebar.button("PCB 요약 차트 보기", on_click=set_show_chart_only_true, key='show_pcb_chart_btn')
            
        # --- 모두 숨기기 버튼 ---
        if st.session_state.show_summary_table or st.session_state.show_chart:
            st.sidebar.button("모두 숨기기", on_click=set_hide_all, key='hide_all_results')
        
        # 다른 분석 항목을 선택하면 테이블/차트 플래그 초기화
        if selected_key != 'Pcb':
             set_hide_all()
    
    # --------------------------
    # MAIN 영역 시작
    # --------------------------
    
    # --- 1. 상단 영역 (QC 요약 테이블 및 차트) ---
    st.header("QC 요약 결과")
    
    df_pcb_filtered = st.session_state.get('filtered_df_Pcb')
    selected_fields_for_table = st.session_state.get(f'detail_fields_select_Pcb', [])
    
    summary_df_display = None

    # [수정] 차트 로드 직전, 테이블 함수를 강제 호출하여 summary_df_for_chart를 최신화
    if df_pcb_filtered is not None and not df_pcb_filtered.empty:
        # 테이블 보기 상태가 아니어도 차트 생성을 위해 데이터는 미리 생성합니다.
        # 이 부분이 summary_df_for_chart를 최신 데이터로 업데이트합니다.
        summary_df_temp = generate_dynamic_summary_table(df_pcb_filtered, selected_fields_for_table, TAB_PROPS_MAP['Pcb'])
        if summary_df_temp is not None:
            summary_df_display = summary_df_temp # 성공적으로 DF가 반환된 경우에만 저장
            st.session_state['summary_df_for_chart'] = summary_df_temp # 차트용 세션 데이터도 갱신
        

        # try:
        #      generate_dynamic_summary_table(df_pcb_filtered, selected_fields_for_table, TAB_PROPS_MAP['Pcb'])
        # except Exception as e:
        #      st.error(f"테이블 데이터 생성 중 치명적인 오류 발생: {e}")
    # A) 테이블 출력 로직
    if st.session_state.show_summary_table and summary_df_display is not None:
        st.subheader("PCB 테스트 항목별 QC 결과 요약 테이블 (일별/Jig별)")
        # 인덱스를 설정하고 출력합니다.
        st.dataframe(summary_df_display.set_index(['Date', 'Jig', 'Test']))
        st.markdown("---")    

    # # A) 테이블 출력 로직
    # if st.session_state.show_summary_table:
    #     # 이미 위에서 데이터가 생성되었으므로, 세션에서 최종 DF를 가져와 표시합니다.
    #     summary_df_display = st.session_state.get('summary_df_for_chart')
    #     if summary_df_display is not None and not summary_df_display.empty:
    #         st.subheader("PCB 테스트 항목별 QC 결과 요약 테이블 (일별/Jig별)")
    #         st.dataframe(summary_df_display.set_index(['Date', 'Jig', 'Test']))
    #         st.markdown("---")
    #     else:
    #         st.error("테이블 생성 실패: 필터링된 PCB 데이터가 없거나 필터링 결과 0건입니다.")
    #         st.session_state.show_summary_table = False 
            
    # # B) 차트 출력 로직 (테이블 아래에 생성)
    # if st.session_state.show_chart:
    #     summary_df = st.session_state.get('summary_df_for_chart') 
        
    #     # [수정] 디버깅 메시지 추가
    #     if summary_df is not None and not summary_df.empty:
    #         st.subheader("QC 결과 누적 막대 그래프")
    #         try:
    #             chart_figure = create_stacked_bar_chart(summary_df, 'PCB')
    #             if chart_figure:
    #                 st.altair_chart(chart_figure, use_container_width=True)
    #             else:
    #                 st.error("그래프 생성 중 오류가 발생했습니다. (차트 함수가 None 반환)")
    #         except Exception as e:
    #             st.error(f"그래프 렌더링 중 오류 발생: {e}")
    #     else:
    #          st.warning("차트를 생성할 요약 데이터가 없습니다. 먼저 분석을 실행하거나 필터를 해제해 주세요.")

    # # B) 차트 출력 로직 (Altair 제거, st.bar_chart 사용)
    # if st.session_state.show_chart:
    #     # 테이블 생성에 성공한 DF를 사용합니다.
    #     if summary_df_display is not None and not summary_df_display.empty:
    #         st.subheader("QC 결과 막대 그래프 (기본 Streamlit 차트)")
            
    #         # [핵심 수정]: st.bar_chart를 사용하여 Altair 충돌을 회피합니다.
    #         # 'Test'와 'Date'를 인덱스로 설정하여 막대가 분리되도록 준비합니다.
            
    #         # 1. 데이터 준비 (불량 건수와 Test 항목만 남김)
    #         df_chart_base = summary_df_display.copy()
    #         df_chart_base['Test_ID'] = df_chart_base['Date'].astype(str) + " / " + df_chart_base['Jig'].astype(str) + " / " + df_chart_base['Test']
            
    #         # 2. X축을 'Test_ID'로, Y축을 불량 항목으로 설정
    #         df_chart = df_chart_base.set_index('Test_ID')[['미달 (Under)', '초과 (Over)', 'Failure']]

    #         # 3. Streamlit의 기본 막대 차트 위젯을 사용하여 출력
    #         st.bar_chart(df_chart) 
            
    #     else:
    #          st.warning("차트를 생성할 요약 데이터가 없습니다. 먼저 테이블을 확인하거나 필터를 해제해 주세요.")

    # # B) 차트 출력 로직 (st.bar_chart 사용)
    # if st.session_state.show_chart:
    #     summary_df = st.session_state.get('summary_df_for_chart') 
        
    #     if summary_df is not None and not summary_df.empty:
    #         st.subheader("QC 결과 막대 그래프 (Jig별 분리)")
    #         try:
    #             # [수정] chart_generator의 함수 호출 (jig_separated=True)
    #             create_simple_bar_chart(summary_df, 'PCB', jig_separated=True) 
    #         except Exception as e:
    #             st.error(f"그래프 렌더링 중 오류 발생: {e}")
    #     else:
    #          st.warning("차트를 생성할 요약 데이터가 없습니다. 먼저 테이블을 확인하거나 필터를 해제해 주세요.")
    
    # # B) 차트 출력 로직 (3개 차트 분리)
    if st.session_state.show_chart:
        summary_df = st.session_state.get('summary_df_for_chart') 
        
        if summary_df is not None and not summary_df.empty:
            st.subheader("QC 결과 분할 차트")
            
            # 메인 화면을 3개의 컬럼으로 분할
            # chart_col1, chart_col2, chart_col3 = st.columns(3)

        # 1. 상세 분리 차트 (Date/Jig/Test)
        # with chart_col1:
            # st.info("차트 1: 상세 분리 (날짜/Jig/Test)")
            create_simple_bar_chart(summary_df, "상세 분리", 'Date_Jig_Test')

            st.markdown("---") # 세로 구분을 위한 구분선

        # 2. 날짜별 합산 차트
        # with chart_col2:
            # st.info("차트 2: 날짜별 합산")
            create_simple_bar_chart(summary_df, "날짜별 합산", 'Date')
            st.markdown("---") # 세로 구분을 위한 구분선

        # 3. Test 항목별 합산 차트
        # with chart_col3:
            # st.info("차트 3: 테스트 항목별 합산")
            create_simple_bar_chart(summary_df, "테스트 항목별 합산", 'Test')
            st.markdown("---") # 세로 구분을 위한 구분선

        # 4. jig 항목별 합산 차트
        # with chart_col4:
            # st.info("차트 4: jig별 합산")
            create_simple_bar_chart(summary_df, "jig별 합산", 'Jig')
            st.markdown("---") # 세로 구분을 위한 구분선    
            
        else:
             st.warning("차트를 생성할 요약 데이터가 없습니다. 먼저 테이블을 확인하거나 필터를 해제해 주세요.")
    
    # [데이터 유효성 검사 및 안내]
    if (st.session_state.show_summary_table or st.session_state.show_chart) and (df_pcb_filtered is None or df_pcb_filtered.empty):
        st.error("결과 생성 실패: PCB 분석 데이터가 없거나 필터링 결과 0건입니다.")
        set_hide_all()
            
    st.markdown("---") 

    # # [차트/테이블 출력 끝]
    # if st.session_state.show_summary_table or st.session_state.show_chart:
    #     pass # 정상 표시
    # elif df_pcb_filtered is not None and not df_pcb_filtered.empty:
    #     pass # 데이터는 있으나 숨김
    # else:
    #     # 데이터가 없는데 표시 시도 중이면 오류 초기화
    #     st.warning("분석 실행 후, 'PCB 요약 테이블 보기' 버튼을 눌러 결과를 확인하세요.")


    # st.markdown("---") 
    
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
                            
                            set_hide_all() # 분석 재실행 시 차트/테이블 상태 초기화
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
#Add new feature for user authentication    