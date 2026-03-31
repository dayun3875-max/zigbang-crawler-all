"""
직방 전국 전 유형 매물 수집 스크립트 (v7 - 전국판)
대상: 아파트 / 원룸 / 빌라·투룸+ / 오피스텔 / 상가·사무실
저장: CSV (UTF-8 BOM, 엑셀 호환)
실행: pip install requests pygeohash

[아파트]
 - /apt/locals/{시군구코드}/item-catalogs (limit=20, offset 페이지네이션)
 - 브이월드 API로 전국 읍면동 BBOX + 시군구 코드 자동 추출
 - tranTypeIn[] 파라미터 인코딩 방지 (Request.prepare)
[원룸/빌라/오피스텔/상가]
 - geohash + BBOX 방식
[공통]
 - 읍면동 단위 CSV 저장
 - master CSV 중복 제거 (카테고리+매물ID)
 - done_emd.txt 캐시로 중단 후 재시작 가능
 - mark_done은 성공 시에만 (오류 시 재수집)
 - 전북 코드: 45xxx (2023년 전북특별자치도 개편 반영)
"""

import requests
import time
import random
import csv
import json
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
VWORLD_KEY  = "YOUR_VWORLD_KEY_HERE"   # 브이월드 API 키 입력
BASE_DIR    = Path("data")
BASE_DIR.mkdir(parents=True, exist_ok=True)

MAX_WORKERS       = 8
BATCH_SIZE        = 50
GH_PRECISION      = 5
GEOHASH_STEP      = 0.04
SLEEP_EVERY_N     = 10
DELAY_MIN         = 0.3
DELAY_MAX         = 0.6
APT_PAGE_LIMIT    = 20      # 25 이상 400 오류 → 20 고정

TARGET_SIDO = None  # None=전국 / 특정 시도만 수집 시: ["서울특별시", "경기도"]

timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE   = BASE_DIR / f"zigbang_전국_{timestamp}_log.txt"
DONE_LOG   = BASE_DIR / "done_emd.txt"
EMD_CACHE  = BASE_DIR / "emd_bbox_cache.json"
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
SIDO_CODE_MAP = {
    "11": "서울특별시",   "26": "부산광역시",   "27": "대구광역시",
    "28": "인천광역시",   "29": "광주광역시",   "30": "대전광역시",
    "31": "울산광역시",   "36": "세종특별자치시", "41": "경기도",
    "42": "강원특별자치도", "43": "충청북도",   "44": "충청남도",
    "45": "전북특별자치도", "46": "전라남도",   "47": "경상북도",
    "48": "경상남도",     "50": "제주특별자치도",
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
    "매물구분", "시도", "시군구", "읍면동", "수집시간", "매물ID", "제목",
    "거래유형", "거래유형_영문",
    "매매가(만원)", "보증금(만원)", "월세(만원)", "가격(억)",
    "단지명", "주소", "층", "건물층수", "방향",
    "면적_m2", "면적_평", "공급면적_m2", "전용면적_m2",
    "서비스유형", "관리비", "등록일", "신규여부",
    "권리금(만원)", "상가업종",
    "매물URL", "위도", "경도",
]

# =============================================
# 스레드 안전 전역 변수
# =============================================
log_lock      = Lock()
done_lock     = Lock()
csv_lock      = Lock()
master_lock   = Lock()
written_lock  = Lock()

_done_set         = set()
_master_csv_init  = False
_written_ids      = set()
_http_session     = Session()

# =============================================
# 유틸 함수
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

def get_sido_from_code(code2):
    return SIDO_CODE_MAP.get(code2, "기타")

def _empty_row():
    return {col: "" for col in COLUMNS}

# ── done 캐시 ────────────────────────────────────────────────
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

# ── master CSV 중복 제거 ─────────────────────────────────────
def _check_and_mark_written(rows):
    new_rows = []
    with written_lock:
        for r in rows:
            uid = f"{r.get('매물구분', '')}_{r.get('매물ID', '')}"
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

def save_local_csv(rows, sido_nm, sgg_nm, emd_nm, category_suffix=""):
    if not rows:
        return
    suffix  = f"_{category_suffix}" if category_suffix else ""
    out_csv = (
        BASE_DIR / sido_nm / sgg_nm
        / f"zigbang_{sgg_nm}_{emd_nm}{suffix}_{timestamp}.csv"
    )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with csv_lock:
        with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    log(f"  💾 {out_csv.name} ({len(rows):,}개)")

# =============================================
# HTTP 유틸
# =============================================
def rget(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 503):
                wait = 30 * (i + 1)
                log(f"  ⚠️ {r.status_code} → {wait}초 대기")
                time.sleep(wait)
            elif r.status_code == 400:
                return None
        except Exception as e:
            if i == retries - 1:
                log(f"  ❌ GET 오류: {e}")
        time.sleep(2 * (i + 1))
    return None

def rget_raw(url, params_str, retries=3):
    """tranTypeIn[0] 등 대괄호 인코딩 방지용 raw GET"""
    for i in range(retries):
        try:
            req    = Request("GET", url, headers=HEADERS)
            prepped = req.prepare()
            prepped.url = url + "?" + params_str
            r = _http_session.send(prepped, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 503):
                wait = 30 * (i + 1)
                log(f"  ⚠️ {r.status_code} → {wait}초 대기")
                time.sleep(wait)
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
                wait = 30 * (i + 1)
                log(f"  ⚠️ {r.status_code} → {wait}초 대기")
                time.sleep(wait)
            elif r.status_code == 400:
                break
        except Exception as e:
            if i == retries - 1:
                log(f"  ❌ POST 오류: {e}")
        time.sleep(2 * (i + 1))
    return None

# =============================================
# [1단계] 브이월드 → 전국 읍면동 BBOX + 시군구 코드 추출
# =============================================
def fetch_emd_bbox_from_vworld():
    if EMD_CACHE.exists():
        with open(EMD_CACHE, encoding="utf-8") as f:
            cached = json.load(f)
        if cached:
            log(f"✅ 읍면동 캐시 로드: {len(cached)}개")
            return cached
        EMD_CACHE.unlink()

    log("🌐 브이월드 API 전국 읍면동 수집 시작...")
    emd_dict    = {}
    page        = 1
    total_pages = None

    while True:
        params = {
            "service": "data", "request": "GetFeature",
            "data": "LT_C_ADEMD_INFO", "key": VWORLD_KEY,
            "domain": "localhost", "format": "json",
            "size": "1000", "page": str(page),
            "geometry": "true", "attribute": "true",
            "geomFilter": "BOX(124.0,33.0,132.0,39.0)",
            "crs": "EPSG:4326",
        }
        features = []
        for attempt in range(3):
            try:
                res  = requests.get(
                    "https://api.vworld.kr/req/data",
                    params=params, timeout=30
                )
                data = res.json()
                resp = data.get("response", {})
                if resp.get("status") != "OK":
                    log(f"  ❌ 브이월드 상태 오류: {resp.get('status')}")
                    break
                if total_pages is None:
                    total_pages = int(resp.get("page", {}).get("total", 1))
                    total_cnt   = resp.get("record", {}).get("total", "?")
                    log(f"  전체 읍면동: {total_cnt}개 | 전체 페이지: {total_pages}개")
                features = (
                    resp.get("result", {})
                        .get("featureCollection", {})
                        .get("features", [])
                )
                for feat in features:
                    props   = feat.get("properties", {})
                    geom    = feat.get("geometry", {})
                    emd_cd  = props.get("emd_cd", "")
                    emd_nm  = props.get("emd_kor_nm", "")
                    sgg_nm  = props.get("sig_kor_nm", "")
                    sido_nm = props.get("ctp_kor_nm", "") or \
                              get_sido_from_code(emd_cd[:2] if emd_cd else "")
                    coords = []
                    if geom.get("type") == "Polygon":
                        coords = geom["coordinates"][0]
                    elif geom.get("type") == "MultiPolygon":
                        for poly in geom["coordinates"]:
                            coords.extend(poly[0])
                    if not coords:
                        continue
                    lngs = [c[0] for c in coords]
                    lats = [c[1] for c in coords]
                    key  = f"{sgg_nm}_{emd_nm}_{emd_cd}"
                    emd_dict[key] = {
                        "sido_nm": sido_nm,
                        "sgg_nm":  sgg_nm,
                        "emd_nm":  emd_nm,
                        "emd_cd":  emd_cd,
                        "sgg_cd":  emd_cd[:5],
                        "min_lat": min(lats), "max_lat": max(lats),
                        "min_lng": min(lngs), "max_lng": max(lngs),
                    }
                break
            except Exception as e:
                log(f"  ❌ 브이월드 오류 ({attempt+1}/3): {e}")
                time.sleep(3)

        if page % 50 == 0:
            log(f"  페이지 {page}/{total_pages} | 누적: {len(emd_dict)}개")
        if not features or (total_pages and page >= total_pages):
            break
        page += 1
        time.sleep(0.3)

    with open(EMD_CACHE, "w", encoding="utf-8") as f:
        json.dump(emd_dict, f, ensure_ascii=False, indent=2)
    log(f"✅ 읍면동 수집 완료: {len(emd_dict)}개 → 캐시 저장")
    return emd_dict

# =============================================
# [Phase 1] 아파트: 시군구 코드 기반 수집
# =============================================
def _apt_catalog_params(offset, limit):
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
        params_str = _apt_catalog_params(offset, APT_PAGE_LIMIT)
        data = rget_raw(url, params_str)
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

def parse_apt_catalog(item, sido_nm, sgg_nm):
    now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tran     = item.get("tranType", "")
    dep      = item.get("depositMin") or 0
    rent     = item.get("rentMin") or 0
    size_ex  = item.get("sizeM2") or 0
    size_con = item.get("sizeContractM2") or 0
    emd_nm   = item.get("local3", "")

    iid = ""
    id_list = item.get("itemIdList") or []
    if id_list and isinstance(id_list[0], dict):
        iid = str(id_list[0].get("itemId", ""))
    elif id_list:
        iid = str(id_list[0])

    price = dep if tran == "trade" else 0

    row = _empty_row()
    row.update({
        "매물구분":      "아파트",
        "시도":          sido_nm,
        "시군구":        sgg_nm,
        "읍면동":        emd_nm,
        "수집시간":      now,
        "매물ID":        iid,
        "제목":          item.get("itemTitle", ""),
        "단지명":        item.get("areaDanjiName", ""),
        "거래유형":      TRAN_KR.get(tran, tran),
        "거래유형_영문": tran,
        "매매가(만원)":  price,
        "보증금(만원)":  dep,
        "월세(만원)":    rent if tran == "rental" else 0,
        "가격(억)":      round(price / 10000, 2) if price else round(dep / 10000, 2),
        "층":            item.get("floor", ""),
        "방향":          DIR_KR.get(item.get("direction", ""), ""),
        "면적_m2":       size_ex,
        "면적_평":       round(float(size_ex) / 3.3058, 2) if size_ex else "",
        "공급면적_m2":   size_con,
        "전용면적_m2":   size_ex,
        "서비스유형":    "아파트",
        "매물URL":       f"https://www.zigbang.com/home/apt/items/{iid}" if iid else "",
    })
    return row

def collect_apt_for_sgg(sgg_cd, sido_nm, sgg_nm):
    done_key = f"APT_SGG_{sgg_cd}"
    if already_done(done_key):
        return 0
    try:
        items, total = fetch_apt_by_local(sgg_cd)
        if not items:
            mark_done(done_key)
            return 0
        rows = [parse_apt_catalog(i, sido_nm, sgg_nm) for i in items]
        rows = [r for r in rows if r.get("매물ID")]

        if rows:
            log(f"  ✅ {sgg_nm} [아파트] {len(rows):,}개 (전체: {total:,})")
            by_emd = defaultdict(list)
            for r in rows:
                by_emd[r.get("읍면동", "기타")].append(r)
            for emd_nm, emd_rows in by_emd.items():
                save_local_csv(emd_rows, sido_nm, sgg_nm, emd_nm, "아파트")
            append_master_csv(rows)

        mark_done(done_key)
        return len(rows)
    except Exception as e:
        log(f"  ❌ {sgg_nm} [아파트] 오류: {e}")
        return 0

# =============================================
# [Phase 2] 원룸/빌라/오피스텔/상가: geohash 기반
# =============================================
def bbox_to_geohashes(bbox, precision=GH_PRECISION):
    hashes = set()
    step   = GEOHASH_STEP
    lat_range = bbox["max_lat"] - bbox["min_lat"]
    lng_range = bbox["max_lng"] - bbox["min_lng"]
    if lat_range < step or lng_range < step:
        step = 0.01
    lat = bbox["min_lat"]
    while lat <= bbox["max_lat"] + step:
        lng = bbox["min_lng"]
        while lng <= bbox["max_lng"] + step:
            hashes.add(pgh.encode(lat, lng, precision=precision))
            lng += step
        lat += step
    return list(hashes)

def fetch_house_ids_batch(geohashes, list_url, extra_params=None):
    all_ids = set()
    for idx, gh in enumerate(geohashes):
        params = {"geohash": gh, "depositMin": 0, "rentMin": 0, "salesPriceMin": 0}
        if extra_params:
            params.update(extra_params)
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
    for i in range(0, len(item_ids), BATCH_SIZE):
        chunk = [int(x) for x in list(item_ids)[i:i + BATCH_SIZE]]
        data  = rpost(DETAIL_URL, {"itemIds": chunk})
        if data:
            results.extend(data.get("items") or [])
        sleep_rand()
    return results

def fetch_store_ids_batch(geohashes):
    all_ids = set()
    for idx, gh in enumerate(geohashes):
        body = {
            "domain": "zigbang", "geohash": gh,
            "shuffle": False, "sales_type": "전체",
            "first_floor": False, "업종": [],
        }
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
        chunk = ids_conv[i:i + BATCH_SIZE]
        data  = rpost(STORE_DETAIL_URL, {"item_ids": chunk})
        if data and isinstance(data, list):
            results.extend(data)
        sleep_rand()
    return results

# ── 파싱: 원룸/빌라/오피스텔 ──────────────────────────────
def parse_house(item, category, emd_info):
    now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tran  = item.get("sales_type", "")
    size  = item.get("size_m2") or 0
    dep   = item.get("deposit") or 0
    rent  = item.get("rent") or 0
    price = item.get("sales_price") or 0
    iid   = item.get("item_id", "")
    loc   = item.get("location") or item.get("random_location") or {}

    row = _empty_row()
    row.update({
        "매물구분":      category,
        "시도":          emd_info["sido_nm"],
        "시군구":        emd_info["sgg_nm"],
        "읍면동":        emd_info["emd_nm"],
        "수집시간":      now,
        "매물ID":        str(iid),
        "제목":          item.get("title", ""),
        "거래유형":      TRAN_KR.get(tran, tran),
        "거래유형_영문": tran,
        "매매가(만원)":  price,
        "보증금(만원)":  dep,
        "월세(만원)":    rent,
        "가격(억)":      round(price/10000, 2) if price else round(dep/10000, 2),
        "주소":          item.get("address1", ""),
        "층":            item.get("floor_string") or str(item.get("floor", "")),
        "건물층수":      item.get("building_floor", ""),
        "방향":          DIR_KR.get(item.get("direction", ""), ""),
        "면적_m2":       size,
        "면적_평":       round(float(size)/3.3058, 2) if size else "",
        "공급면적_m2":   get_m2(item.get("공급면적")),
        "전용면적_m2":   get_m2(item.get("전용면적")),
        "서비스유형":    item.get("service_type", ""),
        "관리비":        item.get("manage_cost", ""),
        "등록일":        item.get("reg_date", ""),
        "신규여부":      item.get("is_new", ""),
        "매물URL":       (
            f"https://www.zigbang.com/home/"
            f"{URL_PATH.get(category, 'villa')}/items/{iid}"
        ),
        "위도":  loc.get("lat", "") if isinstance(loc, dict) else "",
        "경도":  loc.get("lng", "") if isinstance(loc, dict) else "",
    })
    return row

# ── 파싱: 상가 ───────────────────────────────────────────
def parse_store(item, emd_info):
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st   = item.get("sales_type", "")
    size = item.get("size_m2") or 0
    iid  = item.get("item_id", "")
    dep  = item.get("보증금액") or 0
    rent = item.get("월세금액") or 0
    sell = item.get("매매금액") or 0
    key  = item.get("권리금액") or 0

    row = _empty_row()
    row.update({
        "매물구분":      "상가사무실",
        "시도":          emd_info["sido_nm"],
        "시군구":        emd_info["sgg_nm"],
        "읍면동":        emd_info["emd_nm"],
        "수집시간":      now,
        "매물ID":        str(iid),
        "제목":          item.get("title", ""),
        "상가업종":      item.get("업종", ""),
        "권리금(만원)":  key,
        "거래유형":      TRAN_KR.get(st, st),
        "거래유형_영문": st,
        "매매가(만원)":  sell,
        "보증금(만원)":  dep,
        "월세(만원)":    rent,
        "가격(억)":      round(sell/10000, 2) if sell else round(dep/10000, 2),
        "주소":          item.get("address1", ""),
        "층":            str(item.get("floor", "")),
        "면적_m2":       size,
        "면적_평":       round(float(size)/3.3058, 2) if size else "",
        "서비스유형":    "상가사무실",
        "관리비":        item.get("manage_cost", ""),
        "등록일":        item.get("reg_date", ""),
        "매물URL":       f"https://www.zigbang.com/home/store/items/{iid}",
        "위도":          item.get("lat", ""),
        "경도":          item.get("lng", ""),
    })
    return row

# ── 읍면동 단위 수집 (원룸/빌라/오피스텔/상가) ───────────
def collect_emd_non_apt(emd_key, emd_info):
    sgg_nm  = emd_info["sgg_nm"]
    emd_nm  = emd_info["emd_nm"]
    sido_nm = emd_info["sido_nm"]
    bbox    = {k: emd_info[k] for k in ["min_lat", "max_lat", "min_lng", "max_lng"]}
    all_rows = []

    geohashes = bbox_to_geohashes(bbox)
    if not geohashes:
        for lb in NON_APT_LABELS:
            mark_done(f"{emd_key}_{lb}")
        return emd_key, []

    # ── 원룸 ──
    dk = f"{emd_key}_원룸"
    if not already_done(dk):
        try:
            ids = fetch_house_ids_batch(geohashes, ONEROOM_URL)
            if ids:
                items = fetch_house_details(list(ids))
                rows  = [
                    r for r in [parse_house(i, "원룸", emd_info) for i in items]
                    if r.get("서비스유형") == "원룸"
                ]
                all_rows.extend(rows)
                if rows:
                    log(f"  ✅ {sgg_nm} {emd_nm} [원룸] {len(rows):,}개")
            mark_done(dk)
        except Exception as e:
            log(f"  ❌ {sgg_nm} {emd_nm} [원룸] 오류: {e}")

    # ── 투룸·빌라 ──
    dk = f"{emd_key}_투룸_빌라"
    if not already_done(dk):
        try:
            ids = fetch_house_ids_batch(geohashes, VILLA_URL)
            if ids:
                items = fetch_house_details(list(ids))
                rows  = [
                    r for r in [parse_house(i, "투룸_빌라", emd_info) for i in items]
                    if r.get("서비스유형") == "빌라"
                ]
                all_rows.extend(rows)
                if rows:
                    log(f"  ✅ {sgg_nm} {emd_nm} [투룸_빌라] {len(rows):,}개")
            mark_done(dk)
        except Exception as e:
            log(f"  ❌ {sgg_nm} {emd_nm} [투룸_빌라] 오류: {e}")

    # ── 오피스텔 ──
    dk = f"{emd_key}_오피스텔"
    if not already_done(dk):
        try:
            ids = fetch_house_ids_batch(geohashes, OFFICETL_URL, {"withBuildings": "true"})
            if ids:
                items = fetch_house_details(list(ids))
                rows  = [
                    r for r in [parse_house(i, "오피스텔", emd_info) for i in items]
                    if r.get("서비스유형") == "오피스텔"
                ]
                all_rows.extend(rows)
                if rows:
                    log(f"  ✅ {sgg_nm} {emd_nm} [오피스텔] {len(rows):,}개")
            mark_done(dk)
        except Exception as e:
            log(f"  ❌ {sgg_nm} {emd_nm} [오피스텔] 오류: {e}")

    # ── 상가·사무실 ──
    dk = f"{emd_key}_상가사무실"
    if not already_done(dk):
        try:
            ids = fetch_store_ids_batch(geohashes)
            if ids:
                items = fetch_store_details(list(ids))
                rows  = [parse_store(i, emd_info) for i in items]
                all_rows.extend(rows)
                if rows:
                    log(f"  ✅ {sgg_nm} {emd_nm} [상가사무실] {len(rows):,}개")
            mark_done(dk)
        except Exception as e:
            log(f"  ❌ {sgg_nm} {emd_nm} [상가사무실] 오류: {e}")

    # ── 저장 ──
    if all_rows:
        save_local_csv(all_rows, sido_nm, sgg_nm, emd_nm)
        append_master_csv(all_rows)

    return emd_key, all_rows

# =============================================
# 메인
# =============================================
def main():
    _load_done_set()

    log("=" * 60)
    log("직방 전국 전 유형 매물 수집 시작 (v7 전국판)")
    log("대상: 아파트 · 원룸 · 투룸빌라 · 오피스텔 · 상가사무실")
    log(f"저장경로: {BASE_DIR.resolve()}")
    log(f"병렬: {MAX_WORKERS}개 | 아파트 limit={APT_PAGE_LIMIT}")
    log(f"기타: geohash step={GEOHASH_STEP} | chunk={BATCH_SIZE}개")
    if TARGET_SIDO:
        log(f"수집 시도: {TARGET_SIDO}")
    else:
        log("수집 시도: 전국")
    log("=" * 60)

    # 브이월드로 전국 읍면동 정보 수집 (캐시 있으면 재사용)
    emd_dict = fetch_emd_bbox_from_vworld()
    if not emd_dict:
        log("❌ 읍면동 목록 수집 실패 → 종료")
        return

    # 시군구 코드 → 시도/시군구명 매핑 추출
    sgg_map = {}
    for key, info in emd_dict.items():
        sgg_cd = info.get("sgg_cd") or info.get("emd_cd", "")[:5]
        if sgg_cd and sgg_cd not in sgg_map:
            sgg_map[sgg_cd] = {
                "sido_nm": info["sido_nm"],
                "sgg_nm":  info["sgg_nm"],
            }

    # ══════════════════════════════════════════
    # Phase 1: 아파트 수집 (시군구 코드 기반)
    # ══════════════════════════════════════════
    log("\n" + "=" * 60)
    log("📌 Phase 1: 아파트 수집 (시군구 코드 기반)")
    log("=" * 60)

    apt_sggs = [
        (sgg_cd, info)
        for sgg_cd, info in sgg_map.items()
        if (not TARGET_SIDO or info["sido_nm"] in TARGET_SIDO)
        and not already_done(f"APT_SGG_{sgg_cd}")
    ]
    log(f"  수집 대상: {len(apt_sggs)}개 시군구")

    apt_total = 0
    if apt_sggs:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    collect_apt_for_sgg, sgg_cd,
                    info["sido_nm"], info["sgg_nm"]
                ): sgg_cd
                for sgg_cd, info in apt_sggs
            }
            for i, future in enumerate(as_completed(futures), 1):
                sgg_cd = futures[future]
                try:
                    cnt = future.result()
                    apt_total += cnt
                    if i % 20 == 0 or i == len(apt_sggs):
                        log(f"  📊 아파트 {i}/{len(apt_sggs)} | 누적: {apt_total:,}개")
                except Exception as e:
                    log(f"  ❌ 아파트 {sgg_cd} 오류: {e}")

    log(f"  ✅ 아파트 수집 완료: {apt_total:,}개")

    # ══════════════════════════════════════════
    # Phase 2: 원룸/빌라/오피스텔/상가 (geohash)
    # ══════════════════════════════════════════
    log("\n" + "=" * 60)
    log("📌 Phase 2: 원룸·빌라·오피스텔·상가 수집 (geohash 기반)")
    log("=" * 60)

    targets = [
        (key, info)
        for key, info in emd_dict.items()
        if (not TARGET_SIDO or info["sido_nm"] in TARGET_SIDO)
        and not all(already_done(f"{key}_{lb}") for lb in NON_APT_LABELS)
    ]

    sido_counts = defaultdict(int)
    for _, info in targets:
        sido_counts[info["sido_nm"]] += 1
    for sido, cnt in sorted(sido_counts.items()):
        log(f"  {sido}: {cnt}개 읍면동")
    log(f"  총 수집 대상: {len(targets)}개 읍면동")

    non_apt_total = 0
    total = len(targets)

    if targets:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(collect_emd_non_apt, key, info): key
                for key, info in targets
            }
            for i, future in enumerate(as_completed(futures), 1):
                emd_key = futures[future]
                try:
                    _, rows = future.result()
                    non_apt_total += len(rows)
                    if i % 100 == 0 or i == total:
                        log(
                            f"  📊 {i}/{total} ({i/total*100:.1f}%)"
                            f" | 누적: {non_apt_total:,}개"
                        )
                except Exception as e:
                    log(f"  ❌ {emd_key} 오류: {e}")

    # ── 최종 요약 ──────────────────────────────────────────
    grand_total = apt_total + non_apt_total
    log("\n" + "=" * 60)
    log("🎉 전국 전 유형 수집 완료!")
    log(f"   아파트:                    {apt_total:,}개")
    log(f"   원룸/빌라/오피스텔/상가:  {non_apt_total:,}개")
    log(f"   합계 (수집):               {grand_total:,}개")
    log(f"   통합 CSV (중복제거):       {len(_written_ids):,}개")
    log(f"📁 {MASTER_CSV.name}")
    log("=" * 60)


if __name__ == "__main__":
    main()
