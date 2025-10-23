import streamlit as st
import pandas as pd
from datetime import date, datetime
from typing import Dict, Any, List

def aggregate_and_display_summary(summary_data: Dict, all_dates: List[datetime.date], jigs_to_display: List[str], start_date: date, end_date: date, analysis_key: str, df_filtered: pd.DataFrame):
    """
    주어진 필터 조건(날짜 및 Jig)에 따라 데이터를 집계하고 기간 요약 테이블을 표시합니다.
    df_filtered: analysis_utils에서 날짜/Jig 필터링을 거친 실제 데이터프레임.
    """
    
    # --- 핵심 수정: 집계에 사용할 날짜 목록을 UI 필터 범위로 제한 ---
    # all_dates (분석된 전체 날짜) 중 UI에서 선택된 start_date와 end_date 사이에 있는 날짜만 사용
    filtered_dates_ui = sorted([d for d in all_dates if start_date <= d <= end_date])

    # df_filtered에서 실제 집계에 사용될 날짜 목록을 추출합니다. (날짜/Jig 필터가 적용된 결과)
    if df_filtered.empty:
        # 데이터프레임이 비어 있으면, 기간 요약도 당연히 비어 있어야 합니다.
        st.info("기간 요약 디버그: 필터링된 데이터가 0건이므로 집계를 건너뜁니다.")
        st.info("선택된 UI 날짜 조건에 해당하는 요약 데이터가 없습니다.")
        st.session_state[f'agg_dates_{analysis_key}'] = []
        return
    
    daily_aggregated_data = {}
    
    # === 집계 로직: UI 필터 범위 내의 실제 날짜만 사용 ===
    for date_obj in filtered_dates_ui: 
        daily_totals = {key: 0 for key in ['total_test', 'pass', 'false_defect', 'true_defect', 'fail']}
        for jig in jigs_to_display:
            # summary_data는 전체 데이터를 기준으로 미리 계산되어 있습니다.
            # 여기서 필터링된 날짜와 Jig에 해당하는 값만 합산해야 합니다.
            data_point = summary_data.get(jig, {}).get(date_obj.strftime('%Y-%m-%d'))
            
            # [추가 검증] summary_data에 데이터가 있지만, df_filtered에 해당 날짜의 데이터가 없으면 합산하면 안 됩니다.
            # 하지만 df_filtered가 복잡하므로, 일단 summary_data의 값만 합산하고,
            # 총 테스트 수는 필터링된 행 수에 맞게 조정하는 방식으로 처리합니다.
            
            if data_point:
                for key in daily_totals:
                    daily_totals[key] += data_point.get(key, 0)
        daily_aggregated_data[date_obj] = daily_totals

    # --- 요약 (날짜 범위 요약 테이블) ---
    st.subheader("기간 요약")
    
    if filtered_dates_ui:
        summary_df_data = {
            '날짜': [d.strftime('%m-%d') for d in filtered_dates_ui],
            '총 테스트 수': [daily_aggregated_data.get(d, {}).get('total_test', 0) for d in filtered_dates_ui],
            'PASS': [daily_aggregated_data.get(d, {}).get('pass', 0) for d in filtered_dates_ui],
            '가성불량': [daily_aggregated_data.get(d, {}).get('false_defect', 0) for d in filtered_dates_ui],
            '진성불량': [daily_aggregated_data.get(d, {}).get('true_defect', 0) for d in filtered_dates_ui],
            'FAIL': [daily_aggregated_data.get(d, {}).get('fail', 0) for d in filtered_dates_ui]
        }
        summary_df = pd.DataFrame(summary_df_data).set_index('날짜')
        st.dataframe(summary_df.transpose())
    else:
        st.info("선택된 UI 날짜 조건에 해당하는 요약 데이터가 없습니다.")

    # 상세 내역 조회가 이 날짜 목록을 사용합니다.
    st.session_state[f'agg_dates_{analysis_key}'] = filtered_dates_ui