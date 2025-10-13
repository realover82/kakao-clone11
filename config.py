# analysis_keys: 전체 분석 모듈의 고유 키
ANALYSIS_KEYS = ['Pcb', 'Fw', 'RfTx', 'Semi', 'Batadc']

# TAB_MAP: 각 분석 모듈에 대한 설정 정보
# (이 파일은 외부 분석 모듈을 import 하지 않습니다. 해당 정보는 app.py에서 정의됩니다.)
TAB_PROPS_MAP = {
    'Pcb': {'jig_col': 'PcbMaxIrPwr', 'timestamp_col': 'PcbStartTime'},
    'Fw': {'jig_col': 'FwPC', 'timestamp_col': 'FwStamp'},
    'RfTx': {'jig_col': 'RfTxPC', 'timestamp_col': 'RfTxStamp'},
    'Semi': {'jig_col': 'SemiAssyMaxSolarVolt', 'timestamp_col': 'SemiAssyStartTime'},
    'Batadc': {'jig_col': 'BatadcPC', 'timestamp_col': 'BatadcStamp'}
}
