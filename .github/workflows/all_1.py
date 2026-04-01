"""
직방 전국 전 유형 매물 수집 스크립트 (final - 읍면동 + 적응형 세분화)
======================================================================
대상: 아파트 / 원룸 / 빌라·투룸+ / 오피스텔 / 상가·사무실
저장: CSV (UTF-8 BOM, 엑셀 호환)
실행: pip install requests pygeohash

[v9 + 성공코드 합산 개선]
  아파트
    - 행안부 regcodes API로 읍면동 코드 취득 (성공코드 방식, 검증됨)
    - 읍면동별 /apt/locals/{code}/item-catalogs 호출 → 읍면동 컬럼 기록
  원룸/빌라/오피스텔/상가
    - pygeohash 사용 (C 확장, 타일 경계 오차 없음)
    - 적응형 세분화: precision=5 시작 → 매물 ≥ SUBDIVIDE_THRESHOLD 이면
      precision=7까지 자동 재귀 분할 (밀집 지역만 세분화)
    - 주소 파싱으로 읍면동 컬럼 자동 보완
  공통
    - done_emd.txt 캐시로 중단 후 재시작 가능
    - 원자적 CSV 저장 (임시 파일 → rename)
    - race condition 수정 (fetch 성공 후에만 seen 등록)
    - 400/429/503 스마트 재시도
"""

import requests
import pygeohash as pgh
import time
import random
import csv
import shutil
import sys
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ─────────────────────────────────────────────────────────
#  설정
# ─────────────────────────────────────────────────────────
MAX_WORKERS         = 8
BATCH_SIZE          = 100
DELAY_MIN           = 0.08
DELAY_MAX           = 0.25

# 적응형 세분화
START_PRECISION     = 5       # 시작 precision (≈4.9km 격자)
MAX_PRECISION       = 7       # 최대 precision (≈150m 격자)
SUBDIVIDE_THRESHOLD = 400     # 이 수 이상이면 자식 타일로 재귀 분할

# 저장 경로
if len(sys.argv) > 1:
    OUTPUT_ROOT = Path(sys.argv[1])
else:
    OUTPUT_ROOT = Path(__file__).resolve().parent / "zigbang_data"

BASE_DIR = OUTPUT_ROOT / datetime.now().strftime("%Y%m%d")
BASE_DIR.mkdir(parents=True, exist_ok=True)

ts         = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_APT    = BASE_DIR / f"직방_아파트_{ts}.csv"
CSV_HOUSE  = BASE_DIR / f"직방_원룸빌라오피스텔_{ts}.csv"
CSV_STORE  = BASE_DIR / f"직방_상가사무실_{ts}.csv"
LOG_FILE   = BASE_DIR / f"직방_수집_{ts}_log.txt"
DONE_FILE  = BASE_DIR / "done_cache.txt"   # 중단 재시작용 캐시

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ko-KR,ko;q=0.9",
    "content-type": "application/json",
    "origin": "https://www.zigbang.com",
    "referer": "https://www.zigbang.com/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
    ),
    "x-zigbang-platform": "www",
}

TRAN_KR = {
    "trade": "매매", "charter": "전세", "rental": "월세",
    "매매": "매매", "전세": "전세", "월세": "월세", "전체": "전체",
}
DIR_KR = {
    "e": "동향", "w": "서향", "s": "남향", "n": "북향",
    "se": "남동향", "sw": "남서향", "ne": "북동향", "nw": "북서향",
}

# ─────────────────────────────────────────────────────────
#  시군구 코드 (행안부 기준)
# ─────────────────────────────────────────────────────────
SIGUNGU_CODES = {
    "서울 종로구":"11110","서울 중구":"11140","서울 용산구":"11170","서울 성동구":"11200","서울 광진구":"11215",
    "서울 동대문구":"11230","서울 중랑구":"11260","서울 성북구":"11290","서울 강북구":"11305","서울 도봉구":"11320",
    "서울 노원구":"11350","서울 은평구":"11380","서울 서대문구":"11410","서울 마포구":"11440","서울 양천구":"11470",
    "서울 강서구":"11500","서울 구로구":"11530","서울 금천구":"11545","서울 영등포구":"11560","서울 동작구":"11590",
    "서울 관악구":"11620","서울 서초구":"11650","서울 강남구":"11680","서울 송파구":"11710","서울 강동구":"11740",
    "부산 중구":"26110","부산 서구":"26140","부산 동구":"26170","부산 영도구":"26200","부산 부산진구":"26230",
    "부산 동래구":"26260","부산 남구":"26290","부산 북구":"26320","부산 해운대구":"26350","부산 사하구":"26380",
    "부산 금정구":"26410","부산 강서구":"26440","부산 연제구":"26470","부산 수영구":"26500","부산 사상구":"26530",
    "부산 기장군":"26710","대구 중구":"27110","대구 동구":"27140","대구 서구":"27170","대구 남구":"27200",
    "대구 북구":"27230","대구 수성구":"27260","대구 달서구":"27290","대구 달성군":"27710","대구 군위군":"27720",
    "인천 중구":"28110","인천 동구":"28140","인천 미추홀구":"28177","인천 연수구":"28185","인천 남동구":"28200",
    "인천 부평구":"28237","인천 계양구":"28245","인천 서구":"28260","인천 강화군":"28710","인천 옹진군":"28720",
    "광주 동구":"29110","광주 서구":"29140","광주 남구":"29155","광주 북구":"29170","광주 광산구":"29200",
    "대전 동구":"30110","대전 중구":"30140","대전 서구":"30170","대전 유성구":"30200","대전 대덕구":"30230",
    "울산 중구":"31110","울산 남구":"31140","울산 동구":"31170","울산 북구":"31200","울산 울주군":"31710",
    "세종특별자치시":"36110",
    "경기 수원시 장안구":"41111","경기 수원시 권선구":"41113","경기 수원시 팔달구":"41115","경기 수원시 영통구":"41117",
    "경기 성남시 수정구":"41131","경기 성남시 중원구":"41133","경기 성남시 분당구":"41135","경기 의정부시":"41150",
    "경기 안양시 만안구":"41171","경기 안양시 동안구":"41173","경기 부천시":"41190","경기 광명시":"41210",
    "경기 평택시":"41220","경기 동두천시":"41250","경기 안산시 상록구":"41271","경기 안산시 단원구":"41273",
    "경기 고양시 덕양구":"41281","경기 고양시 일산동구":"41285","경기 고양시 일산서구":"41287","경기 과천시":"41290",
    "경기 구리시":"41310","경기 남양주시":"41360","경기 오산시":"41370","경기 시흥시":"41390","경기 군포시":"41410",
    "경기 의왕시":"41430","경기 하남시":"41450","경기 용인시 처인구":"41461","경기 용인시 기흥구":"41463",
    "경기 용인시 수지구":"41465","경기 파주시":"41480","경기 이천시":"41500","경기 안성시":"41550",
    "경기 김포시":"41570","경기 화성시":"41590","경기 광주시":"41610","경기 양주시":"41630","경기 포천시":"41650",
    "경기 여주시":"41670","경기 연천군":"41800","경기 가평군":"41820","경기 양평군":"41830",
    "충북 청주시 상당구":"43111","충북 청주시 서원구":"43112","충북 청주시 흥덕구":"43113","충북 청주시 청원구":"43114",
    "충북 충주시":"43130","충북 제천시":"43150","충북 보은군":"43720","충북 옥천군":"43730","충북 영동군":"43740",
    "충북 증평군":"43745","충북 진천군":"43750","충북 괴산군":"43760","충북 음성군":"43770","충북 단양군":"43800",
    "충남 천안시 동남구":"44131","충남 천안시 서북구":"44133","충남 공주시":"44150","충남 보령시":"44180",
    "충남 아산시":"44200","충남 서산시":"44210","충남 논산시":"44230","충남 계룡시":"44250","충남 당진시":"44270",
    "충남 금산군":"44710","충남 부여군":"44760","충남 서천군":"44770","충남 청양군":"44790","충남 홍성군":"44800",
    "충남 예산군":"44810","충남 태안군":"44825",
    "전남 목포시":"46110","전남 여수시":"46130","전남 순천시":"46150","전남 나주시":"46170","전남 광양시":"46230",
    "전남 담양군":"46710","전남 곡성군":"46720","전남 구례군":"46730","전남 고흥군":"46770","전남 보성군":"46780",
    "전남 화순군":"46790","전남 장흥군":"46800","전남 강진군":"46810","전남 해남군":"46820","전남 영암군":"46830",
    "전남 무안군":"46840","전남 함평군":"46860","전남 영광군":"46870","전남 장성군":"46880","전남 완도군":"46890",
    "전남 진도군":"46900","전남 신안군":"46910",
    "경북 포항시 남구":"47111","경북 포항시 북구":"47113","경북 경주시":"47130","경북 김천시":"47150",
    "경북 안동시":"47170","경북 구미시":"47190","경북 영주시":"47210","경북 영천시":"47230","경북 상주시":"47250",
    "경북 문경시":"47280","경북 경산시":"47290","경북 의성군":"47730","경북 청송군":"47750","경북 영양군":"47760",
    "경북 영덕군":"47770","경북 청도군":"47820","경북 고령군":"47830","경북 성주군":"47840","경북 칠곡군":"47850",
    "경북 예천군":"47900","경북 봉화군":"47920","경북 울진군":"47930","경북 울릉군":"47940",
    "경남 창원시 의창구":"48121","경남 창원시 성산구":"48123","경남 창원시 마산합포구":"48125",
    "경남 창원시 마산회원구":"48127","경남 창원시 진해구":"48129","경남 진주시":"48170","경남 통영시":"48220",
    "경남 사천시":"48240","경남 김해시":"48250","경남 밀양시":"48270","경남 거제시":"48310","경남 양산시":"48330",
    "경남 의령군":"48720","경남 함안군":"48730","경남 창녕군":"48740","경남 고성군":"48820","경남 남해군":"48840",
    "경남 하동군":"48850","경남 산청군":"48860","경남 함양군":"48870","경남 거창군":"48880","경남 합천군":"48890",
    "제주 제주시":"50110","제주 서귀포시":"50130",
    "강원 춘천시":"51110","강원 원주시":"51130","강원 강릉시":"51150","강원 동해시":"51170","강원 태백시":"51190",
    "강원 속초시":"51210","강원 삼척시":"51230","강원 홍천군":"51720","강원 횡성군":"51730","강원 영월군":"51750",
    "강원 평창군":"51760","강원 정선군":"51770","강원 철원군":"51780","강원 화천군":"51790","강원 양구군":"51800",
    "강원 인제군":"51810","강원 고성군":"51820","강원 양양군":"51830",
    "전북 전주시 완산구":"52111","전북 전주시 덕진구":"52113","전북 군산시":"52130","전북 익산시":"52140",
    "전북 정읍시":"52180","전북 남원시":"52190","전북 김제시":"52210","전북 완주군":"52710","전북 진안군":"52720",
    "전북 무주군":"52730","전북 장수군":"52740","전북 임실군":"52750","전북 순창군":"52770","전북 고창군":"52790",
    "전북 부안군":"52800",
}

# ─────────────────────────────────────────────────────────
#  CSV 컬럼 정의
# ─────────────────────────────────────────────────────────
APT_FIELDS = [
    "수집시간", "매물유형", "매물ID", "거래유형",
    "단지명", "시도", "구군", "읍면동",
    "보증금(만원)", "월세(만원)", "매매가(만원)",
    "면적(m2)", "평", "층", "방향", "URL",
]

HOUSE_FIELDS = [
    "수집시간", "매물유형", "매물ID", "거래유형", "방유형",
    "시도", "구군", "읍면동",
    "보증금(만원)", "월세(만원)", "매매가(만원)",
    "면적(m2)", "평", "층", "제목", "URL",
]

STORE_FIELDS = [
    "수집시간", "매물유형", "매물ID", "거래유형", "상가업종",
    "시도", "구군", "읍면동",
    "보증금(만원)", "월세(만원)", "매매가(만원)", "권리금(만원)",
    "면적(m2)", "평", "층", "제목", "URL",
]

# ─────────────────────────────────────────────────────────
#  지오해시 유틸 (pygeohash 사용 - C 확장, 경계 오차 없음)
# ─────────────────────────────────────────────────────────
_B32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def subdivide_geohash(gh: str) -> list:
    """한 단계 세분화 → 32개 자식 반환"""
    return [gh + c for c in _B32]


def is_in_korea(gh: str) -> bool:
    """타일 중심점이 남한 영역 안에 있는지 확인"""
    lat, lng, _, _ = pgh.decode_exactly(gh)[:4]
    return (32.5 <= lat <= 39.0) and (124.0 <= lng <= 131.0)


def korea_geohashes(precision: int = 5) -> list:
    """남한 전체를 커버하는 지오해시 타일 목록"""
    LAT_MIN, LAT_MAX = 33.0, 38.8
    LNG_MIN, LNG_MAX = 124.5, 130.1
    step_map = {
        4: (0.13, 0.22), 5: (0.02, 0.04),
        6: (0.004, 0.006), 7: (0.0005, 0.0008),
    }
    slat, slng = step_map.get(precision, (0.02, 0.04))
    seen = set()
    lat = LAT_MIN
    while lat <= LAT_MAX:
        lng = LNG_MIN
        while lng <= LNG_MAX:
            seen.add(pgh.encode(lat, lng, precision))
            lng += slng
        lat += slat
    return sorted(seen)



# ─────────────────────────────────────────────────────────
#  로그 & done 캐시
# ─────────────────────────────────────────────────────────
_log_lock  = Lock()
_done_lock = Lock()
_done_set  = set()


def _load_done():
    global _done_set
    if DONE_FILE.exists():
        _done_set = set(DONE_FILE.read_text(encoding="utf-8").splitlines())
    log(f"캐시 로드: {len(_done_set)}개 완료 항목")


def already_done(key: str) -> bool:
    return key in _done_set


def mark_done(key: str):
    _done_set.add(key)
    with _done_lock:
        with open(DONE_FILE, "a", encoding="utf-8") as f:
            f.write(key + "\n")


def log(msg: str):
    stamp = datetime.now().strftime("%H:%M:%S")
    line  = f"[{stamp}] {msg}"
    with _log_lock:
        print(line)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


# ─────────────────────────────────────────────────────────
#  HTTP 헬퍼
# ─────────────────────────────────────────────────────────
def rget(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 400:
                return None
            if r.status_code in (429, 503):
                wait = 2 ** i + random.uniform(0, 1)
                log(f"  ⏳ {r.status_code} → {wait:.1f}s 대기")
                time.sleep(wait)
                continue
        except requests.exceptions.Timeout:
            time.sleep(1)
        except Exception as e:
            if i == retries - 1:
                log(f"  ❌ GET 실패: {e}")
            time.sleep(1)
    return None


def rpost(url, body, retries=3):
    for i in range(retries):
        try:
            r = requests.post(url, headers=HEADERS, json=body, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 400:
                return None
            if r.status_code in (429, 503):
                wait = 2 ** i + random.uniform(0, 1)
                log(f"  ⏳ {r.status_code} → {wait:.1f}s 대기")
                time.sleep(wait)
                continue
        except requests.exceptions.Timeout:
            time.sleep(1)
        except Exception as e:
            if i == retries - 1:
                log(f"  ❌ POST 실패: {e}")
            time.sleep(1)
    return None


# ─────────────────────────────────────────────────────────
#  원자적 CSV 저장
# ─────────────────────────────────────────────────────────
def save_csv(rows: list, path: Path, fields: list):
    """임시 파일에 쓴 후 rename → 중간 실패해도 기존 파일 보존"""
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        shutil.move(str(tmp), str(path))
        log(f"💾 저장: {path.name} ({len(rows):,}건)")
    except Exception as e:
        log(f"❌ CSV 저장 실패: {path.name} - {e}")


def append_csv(rows: list, path: Path, fields: list):
    """기존 파일에 이어쓰기 (초기 생성 또는 append)"""
    mode   = "a" if path.exists() else "w"
    write_header = not path.exists()
    try:
        with open(path, mode, newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if write_header:
                w.writeheader()
            w.writerows(rows)
    except Exception as e:
        log(f"❌ CSV append 실패: {path.name} - {e}")


# ─────────────────────────────────────────────────────────
#  ① 아파트 - 행안부 regcodes API로 읍면동 취득
# ─────────────────────────────────────────────────────────
def get_dong_codes(sigungu_5: str) -> list:
    """
    시군구 5자리 코드 → [(dong_code_8, dong_name), ...]
    행안부 regcodes API 사용 (성공코드 방식, 검증됨)
    """
    try:
        res = rget(
            "https://grpc-proxy-server-mkvo6j4wsq-du.a.run.app/v1/regcodes",
            params={"regcode_pattern": f"{sigungu_5}*", "is_ignore_zero": "true"},
        )
        if res:
            return [(c["code"][:8], c["name"]) for c in res.get("regcodes", [])]
    except Exception as e:
        log(f"  ⚠ 동코드 조회 실패 ({sigungu_5}): {e}")
    return []


def fetch_apt_by_dong(dong_code: str) -> list:
    """읍면동 코드(8자리)로 아파트 매물 전수 조회 (페이지네이션)"""
    url = f"https://apis.zigbang.com/apt/locals/{dong_code}/item-catalogs"
    params_base = [
        ("tranTypeIn[]", "trade"),
        ("tranTypeIn[]", "charter"),
        ("tranTypeIn[]", "rental"),
        ("includeOfferItem", "true"),
        ("limit", "20"),
    ]
    items, offset = [], 0
    while True:
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        data = rget(url, params=params_base + [("offset", str(offset))])
        if not data:
            break
        chunk = data.get("list") or []
        if not chunk:
            break
        items.extend(chunk)
        if len(chunk) < 20:
            break
        offset += 20
    return items


def parse_apt(item: dict, dong_name: str, now: str) -> dict:
    s   = item.get("sizeM2") or 0
    d   = item.get("depositMin", 0) or 0
    il  = item.get("itemIdList") or [{}]
    iid = il[0].get("itemId", "") if il and isinstance(il[0], dict) else ""
    return {
        "수집시간":    now,
        "매물유형":    "아파트",
        "매물ID":      iid,
        "거래유형":    TRAN_KR.get(item.get("tranType", ""), ""),
        "단지명":      item.get("areaDanjiName", ""),
        "시도":        item.get("local1", ""),
        "구군":        item.get("local2", ""),
        "읍면동":      dong_name,
        "보증금(만원)": d,
        "월세(만원)":  item.get("rentMin", 0) or 0,
        "매매가(만원)": d if item.get("tranType") == "trade" else 0,
        "면적(m2)":   round(s, 2) if s else "",
        "평":          round(s / 3.3058, 2) if s else "",
        "층":          item.get("floor", ""),
        "방향":        DIR_KR.get(item.get("direction", ""), ""),
        "URL":         f"https://www.zigbang.com/home/apt/items/{iid}" if iid else "",
    }


# ─────────────────────────────────────────────────────────
#  ② 원룸 / 빌라투룸+ / 오피스텔 - 적응형 지오해시
# ─────────────────────────────────────────────────────────
HOUSE_TYPES = {
    "원룸":    "https://apis.zigbang.com/house/property/v1/items/onerooms",
    "빌라투룸": "https://apis.zigbang.com/house/property/v1/items/villas",
    "오피스텔": "https://apis.zigbang.com/house/property/v1/items/officetels",
}


def _house_ids_single(gh: str, prop_type: str) -> list:
    """단일 geohash → ID 목록"""
    url    = HOUSE_TYPES[prop_type]
    params = {"geohash": gh, "depositMin": 0, "rentMin": 0, "salesPriceMin": 0}
    if prop_type == "오피스텔":
        params["withBuildings"] = "true"
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    data = rget(url, params=params)
    if not data:
        return []
    items = data.get("items") or []
    return [i["id"] for i in items if isinstance(i, dict) and i.get("id")]


def fetch_house_ids_adaptive(gh: str, prop_type: str) -> list:
    """
    적응형 세분화:
    반환된 ID ≥ SUBDIVIDE_THRESHOLD 이고 precision < MAX_PRECISION 이면
    32개 자식 타일로 분할하여 재귀 호출.
    밀집 지역(강남, 송파 등)만 세분화되고 한적한 지역은 큰 타일 유지.
    """
    ids = _house_ids_single(gh, prop_type)
    if len(ids) >= SUBDIVIDE_THRESHOLD and len(gh) < MAX_PRECISION:
        log(f"    🔍 세분화: {prop_type}/{gh} → {len(ids)}건 "
            f"(p{len(gh)}→{len(gh)+1})")
        all_ids = []
        for child in subdivide_geohash(gh):
            if is_in_korea(child):
                all_ids.extend(fetch_house_ids_adaptive(child, prop_type))
        return all_ids
    return ids


def fetch_house_details(item_ids: list) -> list:
    if not item_ids:
        return []
    valid = [i for i in item_ids if i]
    url   = "https://apis.zigbang.com/house/property/v1/items/list"
    result = []
    for i in range(0, len(valid), BATCH_SIZE):
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        data = rpost(url, {"item_ids": valid[i:i+BATCH_SIZE]})
        if data:
            result.extend(data.get("items") or [])
    return result


def _emd_from_address(addr: str) -> str:
    """주소 문자열에서 읍면동 파싱 (동/읍/면/리/가 로 끝나는 토큰)"""
    for tok in addr.split():
        if len(tok) >= 2 and any(tok.endswith(s) for s in ("동","읍","면","리","가")):
            return tok
    return ""


def parse_house(item: dict, prop_type: str, now: str) -> dict:
    s      = item.get("size_m2") or 0
    deposit= item.get("deposit") or 0
    rent   = item.get("rent") or 0
    stype  = item.get("sales_type") or ""
    iid    = item.get("item_id", "")
    addr   = item.get("addressOrigin") or {}
    addr3  = item.get("address3") or addr.get("local3", "")
    # address3가 없으면 전체 주소에서 파싱 시도
    if not addr3:
        full_addr = (item.get("address1") or "")
        addr3 = _emd_from_address(full_addr)
    return {
        "수집시간":    now,
        "매물유형":    prop_type,
        "매물ID":      str(iid),
        "거래유형":    TRAN_KR.get(stype, stype),
        "방유형":      item.get("room_type_title") or item.get("room_type") or "",
        "시도":        item.get("address1") or addr.get("local1", ""),
        "구군":        item.get("address2") or addr.get("local2", ""),
        "읍면동":      addr3,
        "보증금(만원)": deposit,
        "월세(만원)":  rent,
        "매매가(만원)": deposit if stype in ("매매", "trade") else 0,
        "면적(m2)":   round(s, 2) if s else "",
        "평":          round(s / 3.3058, 2) if s else "",
        "층":          item.get("floor", ""),
        "제목":        item.get("title", ""),
        "URL":         f"https://www.zigbang.com/home/apt/items/{iid}" if iid else "",
    }


# ─────────────────────────────────────────────────────────
#  ③ 상가·사무실 - 적응형 지오해시
# ─────────────────────────────────────────────────────────
def _store_ids_single(gh: str) -> list:
    url  = "https://apis.zigbang.com/v2/store/article/stores"
    body = {
        "domain": "zigbang", "geohash": gh, "shuffle": False,
        "sales_type": "전체", "first_floor": False, "업종": [],
    }
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    data = rpost(url, body)
    if not data or not isinstance(data, list):
        return []
    ids = []
    for section in data:
        for loc in section.get("item_locations") or []:
            if loc.get("item_id"):
                ids.append(loc["item_id"])
    return ids


def fetch_store_ids_adaptive(gh: str) -> list:
    ids = _store_ids_single(gh)
    if len(ids) >= SUBDIVIDE_THRESHOLD and len(gh) < MAX_PRECISION:
        log(f"    🔍 세분화: store/{gh} → {len(ids)}건 "
            f"(p{len(gh)}→{len(gh)+1})")
        all_ids = []
        for child in subdivide_geohash(gh):
            if is_in_korea(child):
                all_ids.extend(fetch_store_ids_adaptive(child))
        return all_ids
    return ids


def fetch_store_details(item_ids: list) -> list:
    if not item_ids:
        return []
    valid = [i for i in item_ids if i]
    url   = "https://apis.zigbang.com/v2/store/article/stores/list"
    result = []
    for i in range(0, len(valid), BATCH_SIZE):
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        data = rpost(url, {"item_ids": valid[i:i+BATCH_SIZE]})
        if data and isinstance(data, list):
            result.extend(data)
    return result


def parse_store(item: dict, now: str) -> dict:
    s   = item.get("size_m2") or 0
    iid = item.get("item_id", "")
    addr3 = item.get("local3", "")
    if not addr3:
        addr3 = _emd_from_address(item.get("address1") or "")
    return {
        "수집시간":    now,
        "매물유형":    "상가사무실",
        "매물ID":      str(iid),
        "거래유형":    TRAN_KR.get(item.get("sales_type", ""), item.get("sales_type", "")),
        "상가업종":    item.get("업종", ""),
        "시도":        item.get("local1", ""),
        "구군":        item.get("local2", ""),
        "읍면동":      addr3,
        "보증금(만원)": item.get("보증금액") or 0,
        "월세(만원)":  item.get("월세금액") or 0,
        "매매가(만원)": item.get("매매금액") or 0,
        "권리금(만원)": item.get("권리금액") or 0,
        "면적(m2)":   round(s, 2) if s else "",
        "평":          round(s / 3.3058, 2) if s else "",
        "층":          item.get("floor", ""),
        "제목":        item.get("title", ""),
        "URL":         f"https://www.zigbang.com/home/store/items/{iid}" if iid else "",
    }


# ─────────────────────────────────────────────────────────
#  메인
# ─────────────────────────────────────────────────────────
def main():
    start = time.time()
    _load_done()

    log("=" * 62)
    log("  직방 전국 전 유형 매물 수집 (읍면동 + 적응형 세분화)")
    log(f"  precision: {START_PRECISION} → {MAX_PRECISION} | 세분화 임계: {SUBDIVIDE_THRESHOLD}건")
    log(f"  저장: {BASE_DIR}")
    log("=" * 62)

    geohashes = korea_geohashes(START_PRECISION)
    log(f"지오해시 타일: {len(geohashes)}개 (precision={START_PRECISION})")

    # ──────────────────────────────────────────────────────
    #  ① 아파트: 행안부 regcodes → 읍면동 코드 → 매물
    # ──────────────────────────────────────────────────────
    log("\n[1/3] 아파트 수집 ─────────────────────────────────")

    # 읍면동 코드 병렬 취득
    all_dongs: list[tuple[str, str, str]] = []  # (dong_code, dong_name, sgg_name)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(get_dong_codes, v): k for k, v in SIGUNGU_CODES.items()}
        for fut in as_completed(futs):
            sgg_name = futs[fut]
            for dong_code, dong_name in (fut.result() or []):
                all_dongs.append((dong_code, dong_name, sgg_name))
    log(f"  읍면동 {len(all_dongs)}개 확인")

    apt_seen  = set()
    apt_rows  = []
    apt_lock  = Lock()
    done_n    = [0]

    def process_dong(dong_code: str, dong_name: str, sgg_name: str) -> list:
        dk = f"APT_{dong_code}"
        if already_done(dk):
            return []
        rows = []
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            for item in fetch_apt_by_dong(dong_code):
                r = parse_apt(item, dong_name, now)
                if r["매물ID"]:
                    rows.append(r)
        except Exception as e:
            log(f"  ⚠ {sgg_name} {dong_name}: {e}")
        mark_done(dk)
        return rows

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {
            ex.submit(process_dong, dc, dn, sg): (dc, dn)
            for dc, dn, sg in all_dongs
        }
        for fut in as_completed(futs):
            results = fut.result()
            with apt_lock:
                for r in results:
                    if r["매물ID"] not in apt_seen:
                        apt_seen.add(r["매물ID"])
                        apt_rows.append(r)
                done_n[0] += 1
                if done_n[0] % 100 == 0 or done_n[0] == len(all_dongs):
                    log(f"  {done_n[0]}/{len(all_dongs)}동 | "
                        f"{len(apt_rows):,}건 | {time.time()-start:.0f}s")

    save_csv(apt_rows, CSV_APT, APT_FIELDS)

    # ──────────────────────────────────────────────────────
    #  ② 원룸 / 빌라투룸 / 오피스텔
    # ──────────────────────────────────────────────────────
    log("\n[2/3] 원룸·빌라투룸·오피스텔 수집 (적응형) ──────────")
    house_seen = set()
    house_rows = []
    house_lock = Lock()
    done_n[0]  = 0
    tasks_h    = [(gh, pt) for pt in HOUSE_TYPES for gh in geohashes]
    log(f"  작업: {len(tasks_h)}개 (타일 × 유형)")

    def process_house(gh: str, prop_type: str) -> list:
        dk = f"HOUSE_{prop_type}_{gh}"
        if already_done(dk):
            return []
        rows = []
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            ids = fetch_house_ids_adaptive(gh, prop_type)
            if not ids:
                mark_done(dk)
                return []
            with house_lock:
                new_ids = [i for i in ids if f"{prop_type}_{i}" not in house_seen]
            if not new_ids:
                mark_done(dk)
                return []
            details = fetch_house_details(new_ids)
            parsed  = [parse_house(item, prop_type, now) for item in details]
            with house_lock:
                for p in parsed:
                    key = f"{prop_type}_{p['매물ID']}"
                    if key not in house_seen:
                        house_seen.add(key)
                        rows.append(p)
        except Exception as e:
            log(f"  ⚠ {prop_type}/{gh}: {e}")
        mark_done(dk)
        return rows

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process_house, gh, pt): (gh, pt) for gh, pt in tasks_h}
        for fut in as_completed(futs):
            results = fut.result()
            with house_lock:
                house_rows.extend(results)
                done_n[0] += 1
                if done_n[0] % 100 == 0 or done_n[0] == len(tasks_h):
                    log(f"  {done_n[0]}/{len(tasks_h)} 타일 | "
                        f"{len(house_rows):,}건 | {time.time()-start:.0f}s")

    save_csv(house_rows, CSV_HOUSE, HOUSE_FIELDS)

    # ──────────────────────────────────────────────────────
    #  ③ 상가·사무실
    # ──────────────────────────────────────────────────────
    log("\n[3/3] 상가·사무실 수집 (적응형) ───────────────────")
    store_seen = set()
    store_rows = []
    store_lock = Lock()
    done_n[0]  = 0

    def process_store(gh: str) -> list:
        dk = f"STORE_{gh}"
        if already_done(dk):
            return []
        rows = []
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            ids = fetch_store_ids_adaptive(gh)
            if not ids:
                mark_done(dk)
                return []
            with store_lock:
                new_ids = [i for i in ids if i not in store_seen]
            if not new_ids:
                mark_done(dk)
                return []
            details = fetch_store_details(new_ids)
            parsed  = [parse_store(item, now) for item in details]
            with store_lock:
                for p in parsed:
                    mid = p["매물ID"]
                    if mid not in store_seen:
                        store_seen.add(mid)
                        rows.append(p)
        except Exception as e:
            log(f"  ⚠ store/{gh}: {e}")
        mark_done(dk)
        return rows

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process_store, gh): gh for gh in geohashes}
        for fut in as_completed(futs):
            results = fut.result()
            with store_lock:
                store_rows.extend(results)
                done_n[0] += 1
                if done_n[0] % 100 == 0 or done_n[0] == len(geohashes):
                    log(f"  {done_n[0]}/{len(geohashes)} 타일 | "
                        f"{len(store_rows):,}건 | {time.time()-start:.0f}s")

    save_csv(store_rows, CSV_STORE, STORE_FIELDS)

    # ──────────────────────────────────────────────────────
    #  완료 요약
    # ──────────────────────────────────────────────────────
    elapsed = time.time() - start
    total   = len(apt_rows) + len(house_rows) + len(store_rows)
    log("\n" + "=" * 62)
    log("  수집 완료!")
    log(f"  아파트         : {len(apt_rows):>8,}건  →  {CSV_APT.name}")
    log(f"  원룸/빌라/오피스: {len(house_rows):>8,}건  →  {CSV_HOUSE.name}")
    log(f"  상가/사무실    : {len(store_rows):>8,}건  →  {CSV_STORE.name}")
    log(f"  합계           : {total:>8,}건  |  소요 {elapsed:.0f}초")
    log("=" * 62)


if __name__ == "__main__":
    main()
