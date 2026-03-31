"""
직방 전국 전 유형 매물 수집 스크립트 (v8 - 브이월드 없는 전국판)
대상: 아파트 / 원룸 / 빌라·투룸+ / 오피스텔 / 상가·사무실
저장: CSV (UTF-8 BOM, 엑셀 호환)
실행: pip install requests pygeohash

[아파트]  시군구 코드 → /apt/locals/{코드}/item-catalogs
[기타]    시군구 BBOX → geohash 분할 → 각 유형 API
[공통]    done_emd.txt 캐시로 중단 후 재시작 가능
"""

import requests
import time
import random
import csv
import threading
import pygeohash as pgh
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from requests import Request, Session

# =============================================
# 설정
# =============================================
BASE_DIR = Path("data")
BASE_DIR.mkdir(parents=True, exist_ok=True)

MAX_WORKERS    = 8
BATCH_SIZE     = 50
GH_PRECISION   = 5
GEOHASH_STEP   = 0.04
SLEEP_EVERY_N  = 10
DELAY_MIN      = 0.3
DELAY_MAX      = 0.6
APT_PAGE_LIMIT = 20

TARGET_SIDO = None  # None=전국 / 예: ["서울특별시", "경기도"]

timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE   = BASE_DIR / f"zigbang_전국_{timestamp}_log.txt"
DONE_LOG   = BASE_DIR / "done_emd.txt"
MASTER_CSV = BASE_DIR / f"zigbang_전국_전체_{timestamp}.csv"

# ── API 엔드포인트 ──────────────────────────────────────────
APT_CATALOG_URL  = "https://apis.zigbang.com/apt/locals/{local_code}/item-catalogs"
ONEROOM_URL      = "https://apis.zigbang.com/house/property/v1/items/onerooms"
VILLA_URL        = "https://apis.zigbang.com/house/property/v1/items/villas"
OFFICETL_URL     = "https://apis.zigbang.com/house/property/v1/items/officetels"
DETAIL_URL       = "https://apis.zigbang.com/house/property/v1/items/list"
STORE_LIST_URL   = "https://apis.zigbang.com/v2/store/article/stores"
STORE_DETAIL_URL = "https://apis.zigbang.com/v2/store/article/stores/list"

# ── 매핑 ────────────────────────────────────────────────────
TRAN_KR = {
    "trade": "매매", "charter": "전세", "rental": "월세",
    "매매": "매매", "전세": "전세", "월세": "월세",
}
URL_PATH = {
    "원룸": "oneroom", "투룸_빌라": "villa",
    "오피스텔": "officetel", "아파트": "apt", "상가사무실": "store",
}
DIR_KR = {
    "e": "동향", "w": "서향", "s": "남향", "n": "북향",
    "se": "남동향", "sw": "남서향", "ne": "북동향", "nw": "북서향",
}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ko-KR,ko;q=0.9",
    "content-type": "application/json",
    "origin": "https://www.zigbang.com",
    "referer": "https://www.zigbang.com/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36"
    ),
    "x-zigbang-platform": "www",
}

NON_APT_LABELS = ["원룸", "투룸_빌라", "오피스텔", "상가사무실"]

COLUMNS = [
    "매물구분", "시도", "시군구", "수집시간", "매물ID", "제목",
    "거래유형", "거래유형_영문",
    "매매가(만원)", "보증금(만원)", "월세(만원)", "가격(억)",
    "단지명", "주소", "층", "건물층수", "방향",
    "면적_m2", "면적_평", "공급면적_m2", "전용면적_m2",
    "서비스유형", "관리비", "등록일", "신규여부",
    "권리금(만원)", "상가업종",
    "매물URL", "위도", "경도",
]

# =============================================
# 전국 시군구 데이터 (코드 + BBOX)
# sido, sgg, code(5자리), min_lat, max_lat, min_lng, max_lng
# =============================================
SIGUNGU_DATA = [
    # ── 서울 ──────────────────────────────────────────────
    ("서울특별시", "종로구",   "11110", 37.572, 37.606, 126.952, 127.005),
    ("서울특별시", "중구",     "11140", 37.549, 37.575, 126.971, 127.009),
    ("서울특별시", "용산구",   "11170", 37.512, 37.556, 126.961, 127.003),
    ("서울특별시", "성동구",   "11200", 37.540, 37.580, 127.019, 127.070),
    ("서울특별시", "광진구",   "11215", 37.534, 37.566, 127.057, 127.102),
    ("서울특별시", "동대문구", "11230", 37.566, 37.606, 127.020, 127.068),
    ("서울특별시", "중랑구",   "11260", 37.580, 37.627, 127.063, 127.107),
    ("서울특별시", "성북구",   "11290", 37.589, 37.637, 127.001, 127.055),
    ("서울특별시", "강북구",   "11305", 37.624, 37.662, 127.009, 127.054),
    ("서울특별시", "도봉구",   "11320", 37.643, 37.690, 127.008, 127.059),
    ("서울특별시", "노원구",   "11350", 37.625, 37.678, 127.055, 127.103),
    ("서울특별시", "은평구",   "11380", 37.600, 37.654, 126.899, 126.956),
    ("서울특별시", "서대문구", "11410", 37.556, 37.605, 126.916, 126.970),
    ("서울특별시", "마포구",   "11440", 37.535, 37.580, 126.897, 126.959),
    ("서울특별시", "양천구",   "11470", 37.510, 37.547, 126.856, 126.905),
    ("서울특별시", "강서구",   "11500", 37.532, 37.577, 126.805, 126.876),
    ("서울특별시", "구로구",   "11530", 37.481, 37.530, 126.844, 126.910),
    ("서울특별시", "금천구",   "11545", 37.450, 37.490, 126.882, 126.924),
    ("서울특별시", "영등포구", "11560", 37.496, 37.540, 126.886, 126.942),
    ("서울특별시", "동작구",   "11590", 37.487, 37.532, 126.930, 126.993),
    ("서울특별시", "관악구",   "11620", 37.455, 37.499, 126.924, 126.987),
    ("서울특별시", "서초구",   "11650", 37.464, 37.517, 126.991, 127.060),
    ("서울특별시", "강남구",   "11680", 37.494, 37.545, 127.021, 127.100),
    ("서울특별시", "송파구",   "11710", 37.490, 37.532, 127.087, 127.148),
    ("서울특별시", "강동구",   "11740", 37.524, 37.571, 127.119, 127.184),
    # ── 부산 ──────────────────────────────────────────────
    ("부산광역시", "중구",     "26110", 35.094, 35.115, 129.013, 129.052),
    ("부산광역시", "서구",     "26140", 35.083, 35.120, 128.987, 129.022),
    ("부산광역시", "동구",     "26170", 35.110, 35.143, 129.033, 129.067),
    ("부산광역시", "영도구",   "26200", 35.065, 35.104, 128.998, 129.047),
    ("부산광역시", "부산진구", "26230", 35.148, 35.196, 129.017, 129.068),
    ("부산광역시", "동래구",   "26260", 35.185, 35.230, 129.060, 129.106),
    ("부산광역시", "남구",     "26290", 35.126, 35.167, 129.064, 129.112),
    ("부산광역시", "북구",     "26320", 35.184, 35.240, 128.985, 129.042),
    ("부산광역시", "해운대구", "26350", 35.152, 35.210, 129.115, 129.193),
    ("부산광역시", "사하구",   "26380", 35.063, 35.127, 128.930, 128.998),
    ("부산광역시", "금정구",   "26410", 35.215, 35.285, 129.058, 129.115),
    ("부산광역시", "강서구",   "26440", 35.050, 35.145, 128.820, 128.960),
    ("부산광역시", "연제구",   "26470", 35.170, 35.206, 129.058, 129.096),
    ("부산광역시", "수영구",   "26500", 35.136, 35.170, 129.096, 129.135),
    ("부산광역시", "사상구",   "26530", 35.130, 35.185, 128.960, 129.010),
    ("부산광역시", "기장군",   "26710", 35.170, 35.360, 129.110, 129.290),
    # ── 대구 ──────────────────────────────────────────────
    ("대구광역시", "중구",     "27110", 35.858, 35.884, 128.574, 128.620),
    ("대구광역시", "동구",     "27140", 35.855, 35.990, 128.630, 128.770),
    ("대구광역시", "서구",     "27170", 35.856, 35.910, 128.540, 128.590),
    ("대구광역시", "남구",     "27200", 35.834, 35.865, 128.580, 128.630),
    ("대구광역시", "북구",     "27230", 35.883, 35.980, 128.545, 128.640),
    ("대구광역시", "수성구",   "27260", 35.814, 35.875, 128.620, 128.720),
    ("대구광역시", "달서구",   "27290", 35.800, 35.880, 128.480, 128.580),
    ("대구광역시", "달성군",   "27710", 35.650, 35.850, 128.340, 128.580),
    ("대구광역시", "군위군",   "27720", 36.130, 36.320, 128.530, 128.780),
    # ── 인천 ──────────────────────────────────────────────
    ("인천광역시", "중구",     "28110", 37.390, 37.490, 126.390, 126.640),
    ("인천광역시", "동구",     "28140", 37.467, 37.493, 126.608, 126.650),
    ("인천광역시", "미추홀구", "28177", 37.443, 37.482, 126.630, 126.680),
    ("인천광역시", "연수구",   "28185", 37.386, 37.440, 126.630, 126.710),
    ("인천광역시", "남동구",   "28200", 37.422, 37.470, 126.700, 126.773),
    ("인천광역시", "부평구",   "28237", 37.479, 37.525, 126.697, 126.748),
    ("인천광역시", "계양구",   "28245", 37.518, 37.565, 126.693, 126.760),
    ("인천광역시", "서구",     "28260", 37.510, 37.610, 126.600, 126.720),
    ("인천광역시", "강화군",   "28710", 37.590, 37.790, 126.320, 126.620),
    ("인천광역시", "옹진군",   "28720", 36.980, 37.590, 125.760, 126.390),
    # ── 광주 ──────────────────────────────────────────────
    ("광주광역시", "동구",     "29110", 35.126, 35.175, 126.900, 126.960),
    ("광주광역시", "서구",     "29140", 35.121, 35.175, 126.836, 126.910),
    ("광주광역시", "남구",     "29155", 35.084, 35.140, 126.868, 126.940),
    ("광주광역시", "북구",     "29170", 35.150, 35.260, 126.850, 126.960),
    ("광주광역시", "광산구",   "29200", 35.100, 35.200, 126.770, 126.880),
    # ── 대전 ──────────────────────────────────────────────
    ("대전광역시", "동구",     "30110", 36.300, 36.380, 127.400, 127.490),
    ("대전광역시", "중구",     "30140", 36.306, 36.360, 127.380, 127.440),
    ("대전광역시", "서구",     "30170", 36.310, 36.400, 127.330, 127.410),
    ("대전광역시", "유성구",   "30200", 36.340, 36.440, 127.290, 127.400),
    ("대전광역시", "대덕구",   "30230", 36.340, 36.420, 127.390, 127.490),
    # ── 울산 ──────────────────────────────────────────────
    ("울산광역시", "중구",     "31110", 35.545, 35.590, 129.280, 129.340),
    ("울산광역시", "남구",     "31140", 35.510, 35.560, 129.290, 129.380),
    ("울산광역시", "동구",     "31170", 35.490, 35.545, 129.370, 129.440),
    ("울산광역시", "북구",     "31200", 35.550, 35.640, 129.290, 129.390),
    ("울산광역시", "울주군",   "31710", 35.360, 35.600, 129.000, 129.360),
    # ── 세종 ──────────────────────────────────────────────
    ("세종특별자치시", "세종시", "36110", 36.430, 36.650, 127.160, 127.360),
    # ── 경기 ──────────────────────────────────────────────
    ("경기도", "수원시 장안구", "41111", 37.278, 37.320, 126.974, 127.025),
    ("경기도", "수원시 권선구", "41113", 37.233, 37.285, 126.961, 127.020),
    ("경기도", "수원시 팔달구", "41115", 37.270, 37.295, 127.005, 127.040),
    ("경기도", "수원시 영통구", "41117", 37.238, 37.282, 127.030, 127.080),
    ("경기도", "성남시 수정구", "41131", 37.424, 37.470, 127.112, 127.170),
    ("경기도", "성남시 중원구", "41133", 37.420, 37.464, 127.138, 127.185),
    ("경기도", "성남시 분당구", "41135", 37.340, 37.415, 127.090, 127.160),
    ("경기도", "의정부시",     "41150", 37.698, 37.770, 127.010, 127.080),
    ("경기도", "안양시 만안구", "41171", 37.385, 37.432, 126.903, 126.955),
    ("경기도", "안양시 동안구", "41173", 37.375, 37.420, 126.940, 126.995),
    ("경기도", "부천시",       "41190", 37.470, 37.530, 126.745, 126.830),
    ("경기도", "광명시",       "41210", 37.453, 37.496, 126.845, 126.895),
    ("경기도", "평택시",       "41220", 36.950, 37.120, 126.940, 127.160),
    ("경기도", "동두천시",     "41250", 37.870, 37.950, 127.020, 127.110),
    ("경기도", "안산시 상록구", "41271", 37.290, 37.340, 126.810, 126.880),
    ("경기도", "안산시 단원구", "41273", 37.290, 37.340, 126.700, 126.820),  # 수정: 실제 범위에 맞게
    ("경기도", "고양시 덕양구", "41281", 37.620, 37.710, 126.790, 126.890),
    ("경기도", "고양시 일산동구","41285", 37.630, 37.700, 126.840, 126.940),
    ("경기도", "고양시 일산서구","41287", 37.630, 37.710, 126.730, 126.850),
    ("경기도", "과천시",       "41290", 37.420, 37.460, 126.972, 127.010),
    ("경기도", "구리시",       "41310", 37.578, 37.610, 127.118, 127.165),
    ("경기도", "남양주시",     "41360", 37.560, 37.720, 127.100, 127.300),
    ("경기도", "오산시",       "41370", 37.130, 37.190, 127.030, 127.090),
    ("경기도", "시흥시",       "41390", 37.310, 37.420, 126.720, 126.840),
    ("경기도", "군포시",       "41410", 37.340, 37.380, 126.920, 126.980),
    ("경기도", "의왕시",       "41430", 37.340, 37.400, 126.950, 127.010),
    ("경기도", "하남시",       "41450", 37.508, 37.580, 127.160, 127.260),
    ("경기도", "용인시 처인구", "41461", 37.080, 37.300, 127.100, 127.320),
    ("경기도", "용인시 기흥구", "41463", 37.240, 37.320, 127.050, 127.150),
    ("경기도", "용인시 수지구", "41465", 37.290, 37.360, 127.030, 127.100),
    ("경기도", "파주시",       "41480", 37.680, 37.950, 126.680, 126.920),
    ("경기도", "이천시",       "41500", 37.200, 37.360, 127.340, 127.540),
    ("경기도", "안성시",       "41550", 36.960, 37.170, 127.150, 127.420),
    ("경기도", "김포시",       "41570", 37.550, 37.720, 126.520, 126.760),
    ("경기도", "화성시",       "41590", 37.040, 37.300, 126.720, 127.110),
    ("경기도", "광주시",       "41610", 37.330, 37.520, 127.180, 127.400),
    ("경기도", "양주시",       "41630", 37.740, 37.890, 126.940, 127.120),
    ("경기도", "포천시",       "41650", 37.800, 38.060, 127.060, 127.360),
    ("경기도", "여주시",       "41670", 37.230, 37.430, 127.490, 127.720),
    ("경기도", "연천군",       "41800", 37.930, 38.170, 126.950, 127.210),
    ("경기도", "가평군",       "41820", 37.680, 37.940, 127.330, 127.680),
    ("경기도", "양평군",       "41830", 37.330, 37.680, 127.380, 127.740),
    # ── 충북 ──────────────────────────────────────────────
    ("충청북도", "청주시 상당구", "43111", 36.560, 36.720, 127.440, 127.680),
    ("충청북도", "청주시 서원구", "43112", 36.570, 36.660, 127.380, 127.480),
    ("충청북도", "청주시 흥덕구", "43113", 36.580, 36.680, 127.340, 127.470),
    ("충청북도", "청주시 청원구", "43114", 36.630, 36.800, 127.430, 127.620),
    ("충청북도", "충주시",      "43130", 36.900, 37.100, 127.780, 128.050),
    ("충청북도", "제천시",      "43150", 37.060, 37.240, 128.050, 128.310),
    ("충청북도", "보은군",      "43720", 36.450, 36.650, 127.580, 127.820),
    ("충청북도", "옥천군",      "43730", 36.240, 36.440, 127.520, 127.730),
    ("충청북도", "영동군",      "43740", 36.000, 36.250, 127.680, 127.940),
    ("충청북도", "증평군",      "43745", 36.740, 36.870, 127.490, 127.610),
    ("충청북도", "진천군",      "43750", 36.780, 36.960, 127.350, 127.580),
    ("충청북도", "괴산군",      "43760", 36.680, 36.950, 127.640, 127.930),
    ("충청북도", "음성군",      "43770", 36.870, 37.060, 127.380, 127.640),
    ("충청북도", "단양군",      "43800", 36.890, 37.140, 128.180, 128.480),
    # ── 충남 ──────────────────────────────────────────────
    ("충청남도", "천안시 동남구", "44131", 36.730, 36.870, 127.100, 127.300),
    ("충청남도", "천안시 서북구", "44133", 36.800, 36.940, 126.980, 127.160),
    ("충청남도", "공주시",      "44150", 36.360, 36.580, 126.980, 127.260),
    ("충청남도", "보령시",      "44180", 36.190, 36.430, 126.450, 126.720),
    ("충청남도", "아산시",      "44200", 36.680, 36.890, 126.850, 127.090),
    ("충청남도", "서산시",      "44210", 36.720, 36.940, 126.340, 126.620),
    ("충청남도", "논산시",      "44230", 36.100, 36.290, 127.040, 127.280),
    ("충청남도", "계룡시",      "44250", 36.230, 36.310, 127.190, 127.280),
    ("충청남도", "당진시",      "44270", 36.820, 37.010, 126.520, 126.780),
    ("충청남도", "금산군",      "44710", 36.020, 36.200, 127.380, 127.620),
    ("충청남도", "부여군",      "44760", 36.160, 36.380, 126.820, 127.080),
    ("충청남도", "서천군",      "44770", 36.000, 36.200, 126.560, 126.820),
    ("충청남도", "청양군",      "44790", 36.340, 36.560, 126.720, 126.980),
    ("충청남도", "홍성군",      "44800", 36.450, 36.660, 126.540, 126.820),
    ("충청남도", "예산군",      "44810", 36.540, 36.780, 126.680, 126.960),
    ("충청남도", "태안군",      "44825", 36.580, 36.880, 126.100, 126.480),
    # ── 전북 (45xxx - 2023 전북특별자치도 개편) ────────────
    ("전북특별자치도", "전주시 완산구", "45111", 35.780, 35.840, 127.090, 127.160),
    ("전북특별자치도", "전주시 덕진구", "45113", 35.820, 35.890, 127.090, 127.170),
    ("전북특별자치도", "군산시",       "45130", 35.900, 36.060, 126.560, 126.800),
    ("전북특별자치도", "익산시",       "45140", 35.900, 36.080, 126.880, 127.100),
    ("전북특별자치도", "정읍시",       "45180", 35.460, 35.700, 126.720, 126.980),
    ("전북특별자치도", "남원시",       "45190", 35.300, 35.530, 127.220, 127.540),
    ("전북특별자치도", "김제시",       "45210", 35.680, 35.890, 126.720, 126.980),
    ("전북특별자치도", "완주군",       "45710", 35.780, 36.000, 127.040, 127.340),
    ("전북특별자치도", "진안군",       "45720", 35.650, 35.860, 127.320, 127.640),
    ("전북특별자치도", "무주군",       "45730", 35.820, 36.050, 127.560, 127.900),
    ("전북특별자치도", "장수군",       "45740", 35.500, 35.750, 127.440, 127.720),
    ("전북특별자치도", "임실군",       "45750", 35.520, 35.760, 127.130, 127.420),
    ("전북특별자치도", "순창군",       "45770", 35.270, 35.530, 126.940, 127.220),
    ("전북특별자치도", "고창군",       "45790", 35.300, 35.560, 126.480, 126.780),
    ("전북특별자치도", "부안군",       "45800", 35.560, 35.820, 126.480, 126.780),
    # ── 전남 ──────────────────────────────────────────────
    ("전라남도", "목포시",  "46110", 34.750, 34.850, 126.350, 126.470),
    ("전라남도", "여수시",  "46130", 34.640, 34.850, 127.560, 127.820),
    ("전라남도", "순천시",  "46150", 34.880, 35.060, 127.380, 127.620),
    ("전라남도", "나주시",  "46170", 34.900, 35.100, 126.660, 126.920),
    ("전라남도", "광양시",  "46230", 34.880, 35.100, 127.520, 127.780),
    ("전라남도", "담양군",  "46710", 35.180, 35.420, 126.820, 127.080),
    ("전라남도", "곡성군",  "46720", 35.100, 35.360, 127.100, 127.380),
    ("전라남도", "구례군",  "46730", 35.100, 35.300, 127.380, 127.660),
    ("전라남도", "고흥군",  "46770", 34.460, 34.780, 127.160, 127.540),
    ("전라남도", "보성군",  "46780", 34.620, 34.900, 126.940, 127.260),
    ("전라남도", "화순군",  "46790", 34.860, 35.100, 126.780, 127.080),
    ("전라남도", "장흥군",  "46800", 34.520, 34.800, 126.820, 127.100),
    ("전라남도", "강진군",  "46810", 34.480, 34.720, 126.620, 126.900),
    ("전라남도", "해남군",  "46820", 34.260, 34.620, 126.340, 126.720),
    ("전라남도", "영암군",  "46830", 34.680, 34.940, 126.540, 126.820),
    ("전라남도", "무안군",  "46840", 34.820, 35.040, 126.360, 126.620),
    ("전라남도", "함평군",  "46860", 35.000, 35.180, 126.420, 126.700),
    ("전라남도", "영광군",  "46870", 35.180, 35.460, 126.380, 126.620),
    ("전라남도", "장성군",  "46880", 35.220, 35.460, 126.680, 126.960),
    ("전라남도", "완도군",  "46890", 34.200, 34.520, 126.540, 127.040),
    ("전라남도", "진도군",  "46900", 34.280, 34.600, 126.080, 126.480),
    ("전라남도", "신안군",  "46910", 34.600, 35.040, 125.700, 126.460),
    # ── 경북 ──────────────────────────────────────────────
    ("경상북도", "포항시 남구", "47111", 35.930, 36.100, 129.280, 129.480),
    ("경상북도", "포항시 북구", "47113", 36.000, 36.270, 129.220, 129.460),
    ("경상북도", "경주시",     "47130", 35.700, 36.080, 129.020, 129.360),
    ("경상북도", "김천시",     "47150", 36.020, 36.280, 127.940, 128.260),
    ("경상북도", "안동시",     "47170", 36.420, 36.780, 128.500, 128.960),
    ("경상북도", "구미시",     "47190", 36.060, 36.340, 128.200, 128.480),
    ("경상북도", "영주시",     "47210", 36.700, 36.980, 128.380, 128.720),
    ("경상북도", "영천시",     "47230", 35.900, 36.100, 128.820, 129.080),
    ("경상북도", "상주시",     "47250", 36.280, 36.620, 127.940, 128.320),
    ("경상북도", "문경시",     "47280", 36.560, 36.860, 127.980, 128.320),
    ("경상북도", "경산시",     "47290", 35.780, 35.960, 128.680, 128.900),
    ("경상북도", "의성군",     "47730", 36.280, 36.620, 128.540, 128.900),
    ("경상북도", "청송군",     "47750", 36.200, 36.540, 129.000, 129.260),
    ("경상북도", "영양군",     "47760", 36.540, 36.840, 129.000, 129.280),
    ("경상북도", "영덕군",     "47770", 36.260, 36.540, 129.240, 129.480),
    ("경상북도", "청도군",     "47820", 35.580, 35.820, 128.680, 128.980),
    ("경상북도", "고령군",     "47830", 35.660, 35.840, 128.220, 128.480),
    ("경상북도", "성주군",     "47840", 35.780, 36.020, 128.080, 128.400),
    ("경상북도", "칠곡군",     "47850", 35.900, 36.100, 128.300, 128.560),
    ("경상북도", "예천군",     "47900", 36.520, 36.760, 128.280, 128.620),
    ("경상북도", "봉화군",     "47920", 36.760, 37.060, 128.600, 129.000),
    ("경상북도", "울진군",     "47930", 36.700, 37.120, 129.120, 129.480),
    ("경상북도", "울릉군",     "47940", 37.440, 37.560, 130.780, 130.960),
    # ── 경남 ──────────────────────────────────────────────
    ("경상남도", "창원시 의창구",   "48121", 35.200, 35.340, 128.560, 128.700),
    ("경상남도", "창원시 성산구",   "48123", 35.180, 35.280, 128.620, 128.760),
    ("경상남도", "창원시 마산합포구","48125", 35.140, 35.240, 128.480, 128.620),
    ("경상남도", "창원시 마산회원구","48127", 35.170, 35.280, 128.540, 128.660),
    ("경상남도", "창원시 진해구",   "48129", 35.100, 35.200, 128.660, 128.820),
    ("경상남도", "진주시",         "48170", 35.100, 35.300, 127.980, 128.240),
    ("경상남도", "통영시",         "48220", 34.780, 34.980, 128.300, 128.560),
    ("경상남도", "사천시",         "48240", 34.900, 35.100, 127.980, 128.240),
    ("경상남도", "김해시",         "48250", 35.180, 35.380, 128.700, 128.960),
    ("경상남도", "밀양시",         "48270", 35.340, 35.620, 128.700, 129.000),
    ("경상남도", "거제시",         "48310", 34.780, 35.040, 128.460, 128.760),
    ("경상남도", "양산시",         "48330", 35.280, 35.520, 128.940, 129.180),
    ("경상남도", "의령군",         "48720", 35.220, 35.440, 128.100, 128.380),
    ("경상남도", "함안군",         "48730", 35.200, 35.380, 128.320, 128.560),
    ("경상남도", "창녕군",         "48740", 35.400, 35.660, 128.340, 128.660),
    ("경상남도", "고성군",         "48820", 34.880, 35.120, 128.060, 128.380),
    ("경상남도", "남해군",         "48840", 34.740, 34.980, 127.780, 128.080),
    ("경상남도", "하동군",         "48850", 34.980, 35.220, 127.580, 127.900),
    ("경상남도", "산청군",         "48860", 35.200, 35.500, 127.720, 128.060),
    ("경상남도", "함양군",         "48870", 35.400, 35.700, 127.540, 127.900),
    ("경상남도", "거창군",         "48880", 35.580, 35.840, 127.780, 128.120),
    ("경상남도", "합천군",         "48890", 35.440, 35.720, 128.000, 128.380),
    # ── 제주 ──────────────────────────────────────────────
    ("제주특별자치도", "제주시",   "50110", 33.380, 33.560, 126.300, 126.760),
    ("제주특별자치도", "서귀포시", "50130", 33.200, 33.440, 126.180, 126.800),
    # ── 강원 ──────────────────────────────────────────────
    ("강원특별자치도", "춘천시", "42110", 37.740, 37.960, 127.540, 127.820),
    ("강원특별자치도", "원주시", "42130", 37.260, 37.480, 127.820, 128.100),
    ("강원특별자치도", "강릉시", "42150", 37.620, 37.860, 128.760, 129.060),
    ("강원특별자치도", "동해시", "42170", 37.460, 37.620, 129.000, 129.200),
    ("강원특별자치도", "태백시", "42190", 37.100, 37.280, 128.900, 129.120),
    ("강원특별자치도", "속초시", "42210", 38.100, 38.280, 128.520, 128.720),
    ("강원특별자치도", "삼척시", "42230", 37.180, 37.500, 129.020, 129.300),
    ("강원특별자치도", "홍천군", "42720", 37.540, 37.900, 127.700, 128.280),
    ("강원특별자치도", "횡성군", "42730", 37.340, 37.580, 127.940, 128.260),
    ("강원특별자치도", "영월군", "42750", 37.060, 37.360, 128.280, 128.700),
    ("강원특별자치도", "평창군", "42760", 37.320, 37.660, 128.280, 128.740),
    ("강원특별자치도", "정선군", "42770", 37.180, 37.520, 128.600, 129.020),
    ("강원특별자치도", "철원군", "42780", 38.020, 38.320, 127.060, 127.480),
    ("강원특별자치도", "화천군", "42790", 38.020, 38.280, 127.480, 127.840),
    ("강원특별자치도", "양구군", "42800", 38.040, 38.340, 127.800, 128.160),
    ("강원특별자치도", "인제군", "42810", 37.860, 38.320, 128.060, 128.560),
    ("강원특별자치도", "고성군", "42820", 38.200, 38.620, 128.360, 128.700),
    ("강원특별자치도", "양양군", "42830", 37.900, 38.180, 128.520, 128.820),
]

# =============================================
# 스레드 안전 전역 변수
# =============================================
log_lock     = Lock()
done_lock    = Lock()
csv_lock     = Lock()
master_lock  = Lock()
written_lock = Lock()

_done_set        = set()
_master_csv_init = False
_written_ids     = set()
_http_session    = Session()

# =============================================
# 유틸
# =============================================
def log(msg):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line  = f"[{stamp}] {msg}"
    print(line)
    with log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

def sleep_rand():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

def get_m2(val):
    if isinstance(val, dict):
        return val.get("m2", "")
    return val or ""

def _empty_row():
    return {col: "" for col in COLUMNS}

def _load_done_set():
    global _done_set
    if DONE_LOG.exists():
        _done_set = set(DONE_LOG.read_text(encoding="utf-8").splitlines())
    log(f"✅ 완료 캐시 로드: {len(_done_set)}개")

def already_done(key):
    return key in _done_set

def mark_done(key):
    _done_set.add(key)
    with done_lock:
        with open(DONE_LOG, "a", encoding="utf-8") as f:
            f.write(key + "\n")

def _check_and_mark_written(rows):
    new_rows = []
    with written_lock:
        for r in rows:
            uid = f"{r.get('매물구분','')}_{r.get('매물ID','')}"
            if uid not in _written_ids:
                _written_ids.add(uid)
                new_rows.append(r)
    return new_rows

def append_master_csv(rows):
    global _master_csv_init
    new_rows = _check_and_mark_written(rows)
    if not new_rows:
        return 0
    with master_lock:
        mode = "w" if not _master_csv_init else "a"
        with open(MASTER_CSV, mode, newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            if not _master_csv_init:
                writer.writeheader()
                _master_csv_init = True
            writer.writerows(new_rows)
    return len(new_rows)

def save_local_csv(rows, sido_nm, sgg_nm, suffix=""):
    if not rows:
        return
    label   = f"_{suffix}" if suffix else ""
    out_csv = BASE_DIR / sido_nm / f"zigbang_{sgg_nm}{label}_{timestamp}.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with csv_lock:
        with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    log(f"  💾 {out_csv.name} ({len(rows):,}개)")

# =============================================
# HTTP
# =============================================
def rget(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 503):
                time.sleep(30 * (i + 1))
            elif r.status_code == 400:
                return None
        except Exception as e:
            if i == retries - 1:
                log(f"  ❌ GET 오류: {e}")
        time.sleep(2 * (i + 1))
    return None

def rget_raw(url, params_str, retries=3):
    for i in range(retries):
        try:
            req     = Request("GET", url, headers=HEADERS)
            prepped = req.prepare()
            prepped.url = url + "?" + params_str
            r = _http_session.send(prepped, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 503):
                time.sleep(30 * (i + 1))
            elif r.status_code == 400:
                return None
        except Exception as e:
            if i == retries - 1:
                log(f"  ❌ RAW GET 오류: {e}")
        time.sleep(2 * (i + 1))
    return None

def rpost(url, body, retries=3):
    for i in range(retries):
        try:
            r = requests.post(url, headers=HEADERS, json=body, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 503):
                time.sleep(30 * (i + 1))
            elif r.status_code == 400:
                break
        except Exception as e:
            if i == retries - 1:
                log(f"  ❌ POST 오류: {e}")
        time.sleep(2 * (i + 1))
    return None

# =============================================
# Phase 1: 아파트 (시군구 코드 기반)
# =============================================
def _apt_params(offset, limit):
    return (
        f"tranTypeIn[0]=trade&tranTypeIn[1]=charter&tranTypeIn[2]=rental"
        f"&includeOfferItem=true&offset={offset}&limit={limit}"
    )

def fetch_apt_by_local(sgg_cd):
    url       = APT_CATALOG_URL.format(local_code=sgg_cd)
    all_items = []
    offset    = 0
    total     = None
    while True:
        data = rget_raw(url, _apt_params(offset, APT_PAGE_LIMIT))
        if not data:
            break
        if total is None:
            total = data.get("count", 0)
            if total == 0:
                break
        items = data.get("list") or []
        if not items:
            break
        all_items.extend(items)
        offset += len(items)
        if offset >= total:
            break
        sleep_rand()
    return all_items, total or 0

def parse_apt(item, sido_nm, sgg_nm):
    now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tran     = item.get("tranType", "")
    dep      = item.get("depositMin") or 0
    rent     = item.get("rentMin") or 0
    size_ex  = item.get("sizeM2") or 0
    size_con = item.get("sizeContractM2") or 0
    iid      = ""
    id_list  = item.get("itemIdList") or []
    if id_list and isinstance(id_list[0], dict):
        iid = str(id_list[0].get("itemId", ""))
    elif id_list:
        iid = str(id_list[0])
    price = dep if tran == "trade" else 0
    row = _empty_row()
    row.update({
        "매물구분": "아파트", "시도": sido_nm, "시군구": sgg_nm,
        "수집시간": now, "매물ID": iid,
        "제목": item.get("itemTitle", ""),
        "단지명": item.get("areaDanjiName", ""),
        "거래유형": TRAN_KR.get(tran, tran), "거래유형_영문": tran,
        "매매가(만원)": price, "보증금(만원)": dep,
        "월세(만원)": rent if tran == "rental" else 0,
        "가격(억)": round(price/10000, 2) if price else round(dep/10000, 2),
        "층": item.get("floor", ""),
        "방향": DIR_KR.get(item.get("direction", ""), ""),
        "면적_m2": size_ex,
        "면적_평": round(float(size_ex)/3.3058, 2) if size_ex else "",
        "공급면적_m2": size_con, "전용면적_m2": size_ex,
        "서비스유형": "아파트",
        "매물URL": f"https://www.zigbang.com/home/apt/items/{iid}" if iid else "",
    })
    return row

def collect_apt(sgg_cd, sido_nm, sgg_nm):
    dk = f"APT_{sgg_cd}"
    if already_done(dk):
        return 0
    try:
        items, total = fetch_apt_by_local(sgg_cd)
        if not items:
            mark_done(dk)
            return 0
        rows = [parse_apt(i, sido_nm, sgg_nm) for i in items]
        rows = [r for r in rows if r.get("매물ID")]
        if rows:
            log(f"  ✅ {sgg_nm} [아파트] {len(rows):,}개 (전체:{total:,})")
            save_local_csv(rows, sido_nm, sgg_nm, "아파트")
            append_master_csv(rows)
        mark_done(dk)
        return len(rows)
    except Exception as e:
        log(f"  ❌ {sgg_nm} [아파트] 오류: {e}")
        return 0

# =============================================
# Phase 2: 원룸/빌라/오피스텔/상가 (geohash)
# =============================================
def bbox_to_geohashes(min_lat, max_lat, min_lng, max_lng):
    hashes = set()
    step   = GEOHASH_STEP
    if (max_lat - min_lat) < step or (max_lng - min_lng) < step:
        step = 0.01
    lat = min_lat
    while lat <= max_lat + step:
        lng = min_lng
        while lng <= max_lng + step:
            hashes.add(pgh.encode(lat, lng, precision=GH_PRECISION))
            lng += step
        lat += step
    return list(hashes)

def fetch_house_ids(geohashes, list_url, extra=None):
    all_ids = set()
    for idx, gh in enumerate(geohashes):
        params = {"geohash": gh, "depositMin": 0, "rentMin": 0, "salesPriceMin": 0}
        if extra:
            params.update(extra)
        data = rget(list_url, params=params)
        if data:
            items_raw = data.get("items") or []
            if items_raw and isinstance(items_raw[0], dict):
                all_ids.update(i["id"] for i in items_raw if "id" in i)
            else:
                all_ids.update(items_raw)
        if idx % SLEEP_EVERY_N == (SLEEP_EVERY_N - 1):
            sleep_rand()
    return all_ids

def fetch_house_details(item_ids):
    results = []
    ids = list(item_ids)
    for i in range(0, len(ids), BATCH_SIZE):
        chunk = [int(x) for x in ids[i:i+BATCH_SIZE]]
        data  = rpost(DETAIL_URL, {"itemIds": chunk})
        if data:
            results.extend(data.get("items") or [])
        sleep_rand()
    return results

def fetch_store_ids(geohashes):
    all_ids = set()
    for idx, gh in enumerate(geohashes):
        body = {"domain": "zigbang", "geohash": gh, "shuffle": False,
                "sales_type": "전체", "first_floor": False, "업종": []}
        data = rpost(STORE_LIST_URL, body)
        if data and isinstance(data, list):
            for section in data:
                for loc in (section.get("item_locations") or []):
                    if loc.get("item_id"):
                        all_ids.add(loc["item_id"])
        if idx % SLEEP_EVERY_N == (SLEEP_EVERY_N - 1):
            sleep_rand()
    return all_ids

def fetch_store_details(item_ids):
    results  = []
    ids_conv = []
    for x in item_ids:
        try:
            ids_conv.append(int(x))
        except (ValueError, TypeError):
            ids_conv.append(x)
    for i in range(0, len(ids_conv), BATCH_SIZE):
        data = rpost(STORE_DETAIL_URL, {"item_ids": ids_conv[i:i+BATCH_SIZE]})
        if data and isinstance(data, list):
            results.extend(data)
        sleep_rand()
    return results

def parse_house(item, category, sido_nm, sgg_nm):
    now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tran  = item.get("sales_type", "")
    size  = item.get("size_m2") or 0
    dep   = item.get("deposit") or 0
    rent  = item.get("rent") or 0
    price = item.get("sales_price") or 0
    iid   = item.get("item_id", "")
    loc   = item.get("location") or item.get("random_location") or {}
    row   = _empty_row()
    row.update({
        "매물구분": category, "시도": sido_nm, "시군구": sgg_nm,
        "수집시간": now, "매물ID": str(iid),
        "제목": item.get("title", ""),
        "거래유형": TRAN_KR.get(tran, tran), "거래유형_영문": tran,
        "매매가(만원)": price, "보증금(만원)": dep, "월세(만원)": rent,
        "가격(억)": round(price/10000,2) if price else round(dep/10000,2),
        "주소": item.get("address1",""),
        "층": item.get("floor_string") or str(item.get("floor","")),
        "건물층수": item.get("building_floor",""),
        "방향": DIR_KR.get(item.get("direction",""),""),
        "면적_m2": size,
        "면적_평": round(float(size)/3.3058,2) if size else "",
        "공급면적_m2": get_m2(item.get("공급면적")),
        "전용면적_m2": get_m2(item.get("전용면적")),
        "서비스유형": item.get("service_type",""),
        "관리비": item.get("manage_cost",""),
        "등록일": item.get("reg_date",""),
        "신규여부": item.get("is_new",""),
        "매물URL": (
            f"https://www.zigbang.com/home/"
            f"{URL_PATH.get(category,'villa')}/items/{iid}"
        ),
        "위도": loc.get("lat","") if isinstance(loc,dict) else "",
        "경도": loc.get("lng","") if isinstance(loc,dict) else "",
    })
    return row

def parse_store(item, sido_nm, sgg_nm):
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st   = item.get("sales_type","")
    size = item.get("size_m2") or 0
    iid  = item.get("item_id","")
    dep  = item.get("보증금액") or 0
    rent = item.get("월세금액") or 0
    sell = item.get("매매금액") or 0
    key  = item.get("권리금액") or 0
    row  = _empty_row()
    row.update({
        "매물구분": "상가사무실", "시도": sido_nm, "시군구": sgg_nm,
        "수집시간": now, "매물ID": str(iid),
        "제목": item.get("title",""), "상가업종": item.get("업종",""),
        "권리금(만원)": key,
        "거래유형": TRAN_KR.get(st,st), "거래유형_영문": st,
        "매매가(만원)": sell, "보증금(만원)": dep, "월세(만원)": rent,
        "가격(억)": round(sell/10000,2) if sell else round(dep/10000,2),
        "주소": item.get("address1",""),
        "층": str(item.get("floor","")),
        "면적_m2": size,
        "면적_평": round(float(size)/3.3058,2) if size else "",
        "서비스유형": "상가사무실",
        "관리비": item.get("manage_cost",""),
        "등록일": item.get("reg_date",""),
        "매물URL": f"https://www.zigbang.com/home/store/items/{iid}",
        "위도": item.get("lat",""), "경도": item.get("lng",""),
    })
    return row

def collect_non_apt(sgg_cd, sido_nm, sgg_nm, min_lat, max_lat, min_lng, max_lng):
    all_rows = []
    geohashes = bbox_to_geohashes(min_lat, max_lat, min_lng, max_lng)
    if not geohashes:
        for lb in NON_APT_LABELS:
            mark_done(f"{sgg_cd}_{lb}")
        return 0

    # 원룸
    dk = f"{sgg_cd}_원룸"
    if not already_done(dk):
        try:
            ids = fetch_house_ids(geohashes, ONEROOM_URL)
            if ids:
                items = fetch_house_details(ids)
                rows  = [r for r in [parse_house(i,"원룸",sido_nm,sgg_nm) for i in items]
                         if r.get("서비스유형") == "원룸"]
                all_rows.extend(rows)
                if rows:
                    log(f"  ✅ {sgg_nm} [원룸] {len(rows):,}개")
            mark_done(dk)
        except Exception as e:
            log(f"  ❌ {sgg_nm} [원룸] 오류: {e}")

    # 투룸·빌라
    dk = f"{sgg_cd}_투룸_빌라"
    if not already_done(dk):
        try:
            ids = fetch_house_ids(geohashes, VILLA_URL)
            if ids:
                items = fetch_house_details(ids)
                rows  = [r for r in [parse_house(i,"투룸_빌라",sido_nm,sgg_nm) for i in items]
                         if r.get("서비스유형") == "빌라"]
                all_rows.extend(rows)
                if rows:
                    log(f"  ✅ {sgg_nm} [투룸_빌라] {len(rows):,}개")
            mark_done(dk)
        except Exception as e:
            log(f"  ❌ {sgg_nm} [투룸_빌라] 오류: {e}")

    # 오피스텔
    dk = f"{sgg_cd}_오피스텔"
    if not already_done(dk):
        try:
            ids = fetch_house_ids(geohashes, OFFICETL_URL, {"withBuildings": "true"})
            if ids:
                items = fetch_house_details(ids)
                rows  = [r for r in [parse_house(i,"오피스텔",sido_nm,sgg_nm) for i in items]
                         if r.get("서비스유형") == "오피스텔"]
                all_rows.extend(rows)
                if rows:
                    log(f"  ✅ {sgg_nm} [오피스텔] {len(rows):,}개")
            mark_done(dk)
        except Exception as e:
            log(f"  ❌ {sgg_nm} [오피스텔] 오류: {e}")

    # 상가·사무실
    dk = f"{sgg_cd}_상가사무실"
    if not already_done(dk):
        try:
            ids = fetch_store_ids(geohashes)
            if ids:
                items = fetch_store_details(ids)
                rows  = [parse_store(i, sido_nm, sgg_nm) for i in items]
                all_rows.extend(rows)
                if rows:
                    log(f"  ✅ {sgg_nm} [상가사무실] {len(rows):,}개")
            mark_done(dk)
        except Exception as e:
            log(f"  ❌ {sgg_nm} [상가사무실] 오류: {e}")

    if all_rows:
        save_local_csv(all_rows, sido_nm, sgg_nm, "기타유형")
        append_master_csv(all_rows)

    return len(all_rows)

# =============================================
# 메인
# =============================================
def main():
    _load_done_set()

    # TARGET_SIDO 필터 적용
    targets = [
        row for row in SIGUNGU_DATA
        if TARGET_SIDO is None or row[0] in TARGET_SIDO
    ]

    log("=" * 60)
    log("직방 전국 전 유형 매물 수집 시작 (v8)")
    log("대상: 아파트 · 원룸 · 투룸빌라 · 오피스텔 · 상가사무실")
    log(f"저장경로: {BASE_DIR.resolve()}")
    log(f"병렬: {MAX_WORKERS}개 | 아파트 limit={APT_PAGE_LIMIT}")
    log(f"수집 시군구: {len(targets)}개")
    log("=" * 60)

    # ── Phase 1: 아파트 ──────────────────────────────────
    log("\n📌 Phase 1: 아파트 수집")
    apt_targets = [r for r in targets if not already_done(f"APT_{r[2]}")]
    log(f"  대상: {len(apt_targets)}개 시군구")

    apt_total = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(collect_apt, r[2], r[0], r[1]): r[1]
            for r in apt_targets
        }
        for i, future in enumerate(as_completed(futures), 1):
            try:
                apt_total += future.result()
                if i % 20 == 0 or i == len(apt_targets):
                    log(f"  📊 아파트 {i}/{len(apt_targets)} | 누적: {apt_total:,}개")
            except Exception as e:
                log(f"  ❌ 아파트 오류: {e}")
    log(f"  ✅ 아파트 완료: {apt_total:,}개")

    # ── Phase 2: 원룸/빌라/오피스텔/상가 ────────────────
    log("\n📌 Phase 2: 원룸·빌라·오피스텔·상가 수집")
    non_apt_targets = [
        r for r in targets
        if not all(already_done(f"{r[2]}_{lb}") for lb in NON_APT_LABELS)
    ]
    log(f"  대상: {len(non_apt_targets)}개 시군구")

    non_apt_total = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(collect_non_apt, r[2], r[0], r[1], r[3], r[4], r[5], r[6]): r[1]
            for r in non_apt_targets
        }
        for i, future in enumerate(as_completed(futures), 1):
            try:
                non_apt_total += future.result()
                if i % 20 == 0 or i == len(non_apt_targets):
                    log(f"  📊 기타 {i}/{len(non_apt_targets)} | 누적: {non_apt_total:,}개")
            except Exception as e:
                log(f"  ❌ 기타유형 오류: {e}")
    log(f"  ✅ 기타유형 완료: {non_apt_total:,}개")

    # ── 최종 요약 ──────────────────────────────────────
    log("\n" + "=" * 60)
    log("🎉 전국 전 유형 수집 완료!")
    log(f"   아파트:                   {apt_total:,}개")
    log(f"   원룸/빌라/오피스텔/상가:  {non_apt_total:,}개")
    log(f"   합계:                     {apt_total+non_apt_total:,}개")
    log(f"   통합 CSV (중복제거):      {len(_written_ids):,}개")
    log(f"📁 {MASTER_CSV.name}")
    log("=" * 60)


if __name__ == "__main__":
    main()
