import requests
import csv
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# =========================
# 📁 파일 설정
# =========================
BASE_DIR = Path("data")
BASE_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
TOTAL_CSV = BASE_DIR / f"전국전체_{timestamp}.csv"
LOG_FILE  = BASE_DIR / f"전국전체_{timestamp}_log.txt"

# =========================
# ⚡ 병렬 처리 설정
# =========================
MAX_WORKERS_SIGUNGU  = 10   # 시군구 병렬 수
MAX_WORKERS_DONG     = 5    # 동 병렬 수 (시군구당)
REQUEST_SEMAPHORE    = threading.Semaphore(20)  # 동시 요청 상한
MIN_SLEEP            = 0.1  # 최소 딜레이 (초)

# =========================
# 🔤 매핑
# =========================
TRAN_TYPE_KR = {"trade": "매매", "charter": "전세", "rental": "월세"}
DIRECTION_KR = {
    "e": "동향", "w": "서향", "s": "남향", "n": "북향",
    "se": "남동향", "sw": "남서향", "ne": "북동향", "nw": "북서향",
}

ZIGBANG_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ko-KR,ko;q=0.9",
    "origin": "https://www.zigbang.com",
    "referer": "https://www.zigbang.com/",
    "sdk-version": "0.112.0",
    "user-agent": "Mozilla/5.0",
    "x-zigbang-platform": "www",
}

PAGE_LIMIT = 20

# =========================
# 전국 시군구 코드 (5자리)
# =========================
SIGUNGU_CODES = {
    "서울 종로구": "11110", "서울 중구": "11140", "서울 용산구": "11170",
    "서울 성동구": "11200", "서울 광진구": "11215", "서울 동대문구": "11230",
    "서울 중랑구": "11260", "서울 성북구": "11290", "서울 강북구": "11305",
    "서울 도봉구": "11320", "서울 노원구": "11350", "서울 은평구": "11380",
    "서울 서대문구": "11410", "서울 마포구": "11440", "서울 양천구": "11470",
    "서울 강서구": "11500", "서울 구로구": "11530", "서울 금천구": "11545",
    "서울 영등포구": "11560", "서울 동작구": "11590", "서울 관악구": "11620",
    "서울 서초구": "11650", "서울 강남구": "11680", "서울 송파구": "11710",
    "서울 강동구": "11740",
    "부산 중구": "26110", "부산 서구": "26140", "부산 동구": "26170",
    "부산 영도구": "26200", "부산 부산진구": "26230", "부산 동래구": "26260",
    "부산 남구": "26290", "부산 북구": "26320", "부산 해운대구": "26350",
    "부산 사하구": "26380", "부산 금정구": "26410", "부산 강서구": "26440",
    "부산 연제구": "26470", "부산 수영구": "26500", "부산 사상구": "26530",
    "부산 기장군": "26710",
    "대구 중구": "27110", "대구 동구": "27140", "대구 서구": "27170",
    "대구 남구": "27200", "대구 북구": "27230", "대구 수성구": "27260",
    "대구 달서구": "27290", "대구 달성군": "27710", "대구 군위군": "27720",
    "인천 중구": "28110", "인천 동구": "28140", "인천 미추홀구": "28177",
    "인천 연수구": "28185", "인천 남동구": "28200", "인천 부평구": "28237",
    "인천 계양구": "28245", "인천 서구": "28260", "인천 강화군": "28710",
    "인천 옹진군": "28720",
    "광주 동구": "29110", "광주 서구": "29140", "광주 남구": "29155",
    "광주 북구": "29170", "광주 광산구": "29200",
    "대전 동구": "30110", "대전 중구": "30140", "대전 서구": "30170",
    "대전 유성구": "30200", "대전 대덕구": "30230",
    "울산 중구": "31110", "울산 남구": "31140", "울산 동구": "31170",
    "울산 북구": "31200", "울산 울주군": "31710",
    "세종특별자치시": "36110",
    "경기 수원시 장안구": "41111", "경기 수원시 권선구": "41113",
    "경기 수원시 팔달구": "41115", "경기 수원시 영통구": "41117",
    "경기 성남시 수정구": "41131", "경기 성남시 중원구": "41133",
    "경기 성남시 분당구": "41135",
    "경기 의정부시": "41150",
    "경기 안양시 만안구": "41171", "경기 안양시 동안구": "41173",
    "경기 부천시": "41190",
    "경기 광명시": "41210", "경기 평택시": "41220", "경기 동두천시": "41250",
    "경기 안산시 상록구": "41271", "경기 안산시 단원구": "41273",
    "경기 고양시 덕양구": "41281", "경기 고양시 일산동구": "41285",
    "경기 고양시 일산서구": "41287",
    "경기 과천시": "41290", "경기 구리시": "41310", "경기 남양주시": "41360",
    "경기 오산시": "41370", "경기 시흥시": "41390", "경기 군포시": "41410",
    "경기 의왕시": "41430", "경기 하남시": "41450",
    "경기 용인시 처인구": "41461", "경기 용인시 기흥구": "41463",
    "경기 용인시 수지구": "41465",
    "경기 파주시": "41480", "경기 이천시": "41500", "경기 안성시": "41550",
    "경기 김포시": "41570",
    "경기 화성시": "41590",
    "경기 광주시": "41610", "경기 양주시": "41630", "경기 포천시": "41650",
    "경기 여주시": "41670", "경기 연천군": "41800", "경기 가평군": "41820",
    "경기 양평군": "41830",
    "충북 청주시 상당구": "43111", "충북 청주시 서원구": "43112",
    "충북 청주시 흥덕구": "43113", "충북 청주시 청원구": "43114",
    "충북 충주시": "43130", "충북 제천시": "43150", "충북 보은군": "43720",
    "충북 옥천군": "43730", "충북 영동군": "43740", "충북 증평군": "43745",
    "충북 진천군": "43750", "충북 괴산군": "43760", "충북 음성군": "43770",
    "충북 단양군": "43800",
    "충남 천안시 동남구": "44131", "충남 천안시 서북구": "44133",
    "충남 공주시": "44150", "충남 보령시": "44180", "충남 아산시": "44200",
    "충남 서산시": "44210", "충남 논산시": "44230", "충남 계룡시": "44250",
    "충남 당진시": "44270", "충남 금산군": "44710", "충남 부여군": "44760",
    "충남 서천군": "44770", "충남 청양군": "44790", "충남 홍성군": "44800",
    "충남 예산군": "44810", "충남 태안군": "44825",
    "전남 목포시": "46110", "전남 여수시": "46130", "전남 순천시": "46150",
    "전남 나주시": "46170", "전남 광양시": "46230", "전남 담양군": "46710",
    "전남 곡성군": "46720", "전남 구례군": "46730", "전남 고흥군": "46770",
    "전남 보성군": "46780", "전남 화순군": "46790", "전남 장흥군": "46800",
    "전남 강진군": "46810", "전남 해남군": "46820", "전남 영암군": "46830",
    "전남 무안군": "46840", "전남 함평군": "46860", "전남 영광군": "46870",
    "전남 장성군": "46880", "전남 완도군": "46890", "전남 진도군": "46900",
    "전남 신안군": "46910",
    "경북 포항시 남구": "47111", "경북 포항시 북구": "47113",
    "경북 경주시": "47130", "경북 김천시": "47150", "경북 안동시": "47170",
    "경북 구미시": "47190", "경북 영주시": "47210", "경북 영천시": "47230",
    "경북 상주시": "47250", "경북 문경시": "47280", "경북 경산시": "47290",
    "경북 의성군": "47730", "경북 청송군": "47750", "경북 영양군": "47760",
    "경북 영덕군": "47770", "경북 청도군": "47820", "경북 고령군": "47830",
    "경북 성주군": "47840", "경북 칠곡군": "47850", "경북 예천군": "47900",
    "경북 봉화군": "47920", "경북 울진군": "47930", "경북 울릉군": "47940",
    "경남 창원시 의창구": "48121", "경남 창원시 성산구": "48123",
    "경남 창원시 마산합포구": "48125", "경남 창원시 마산회원구": "48127",
    "경남 창원시 진해구": "48129",
    "경남 진주시": "48170", "경남 통영시": "48220", "경남 사천시": "48240",
    "경남 김해시": "48250", "경남 밀양시": "48270", "경남 거제시": "48310",
    "경남 양산시": "48330", "경남 의령군": "48720", "경남 함안군": "48730",
    "경남 창녕군": "48740", "경남 고성군": "48820", "경남 남해군": "48840",
    "경남 하동군": "48850", "경남 산청군": "48860", "경남 함양군": "48870",
    "경남 거창군": "48880", "경남 합천군": "48890",
    "제주 제주시": "50110", "제주 서귀포시": "50130",
    "강원 춘천시": "51110", "강원 원주시": "51130", "강원 강릉시": "51150",
    "강원 동해시": "51170", "강원 태백시": "51190", "강원 속초시": "51210",
    "강원 삼척시": "51230", "강원 홍천군": "51720", "강원 횡성군": "51730",
    "강원 영월군": "51750", "강원 평창군": "51760", "강원 정선군": "51770",
    "강원 철원군": "51780", "강원 화천군": "51790", "강원 양구군": "51800",
    "강원 인제군": "51810", "강원 고성군": "51820", "강원 양양군": "51830",
    "전북 전주시 완산구": "52111", "전북 전주시 덕진구": "52113",
    "전북 군산시": "52130", "전북 익산시": "52140", "전북 정읍시": "52180",
    "전북 남원시": "52190", "전북 김제시": "52210", "전북 완주군": "52710",
    "전북 진안군": "52720", "전북 무주군": "52730", "전북 장수군": "52740",
    "전북 임실군": "52750", "전북 순창군": "52770", "전북 고창군": "52790",
    "전북 부안군": "52800",
}

# =========================
# 로그 (thread-safe)
# =========================
_log_lock = threading.Lock()

def log(msg):
    stamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{stamp}] {msg}"
    with _log_lock:
        print(line)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

# =========================
# 행안부 API → 동 코드 목록
# =========================
def get_dong_codes(sigungu_5):
    url = "https://grpc-proxy-server-mkvo6j4wsq-du.a.run.app/v1/regcodes"
    params = {"regcode_pattern": f"{sigungu_5}*", "is_ignore_zero": "true"}
    try:
        with REQUEST_SEMAPHORE:
            res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            codes = res.json().get("regcodes", [])
            return [(c["code"][:8], c["name"]) for c in codes]
    except Exception as e:
        log(f"❌ 행안부 API 오류 ({sigungu_5}): {e}")
    return []

# =========================
# 직방 API (동 단위) - 재시도 포함
# =========================
def fetch_by_dong(dong_code, retries=2):
    url = f"https://apis.zigbang.com/apt/locals/{dong_code}/item-catalogs"
    all_items = []
    offset = 0

    while True:
        params = [
            ("tranTypeIn[]", "trade"),
            ("tranTypeIn[]", "charter"),
            ("tranTypeIn[]", "rental"),
            ("includeOfferItem", "true"),
            ("offset", offset),
            ("limit", PAGE_LIMIT),
        ]
        for attempt in range(retries + 1):
            try:
                with REQUEST_SEMAPHORE:
                    res = requests.get(url, headers=ZIGBANG_HEADERS, params=params, timeout=15)
                break
            except Exception:
                if attempt == retries:
                    return all_items
                time.sleep(0.5)

        if res.status_code == 429:
            # Rate limit → 잠시 대기 후 재시도
            time.sleep(3 + random.uniform(0, 1))
            continue
        if res.status_code != 200:
            break

        data = res.json()
        if data.get("count", 0) == 0 and offset == 0:
            break

        items = data.get("list") or []
        if not items:
            break

        all_items.extend(items)
        if len(items) < PAGE_LIMIT:
            break

        offset += PAGE_LIMIT
        time.sleep(MIN_SLEEP + random.uniform(0, 0.1))  # 최소 딜레이

    return all_items

# =========================
# 데이터 정제
# =========================
def parse_row(item):
    size_m2 = item.get("sizeM2") or 0
    deposit = item.get("depositMin", 0) or 0
    rent = item.get("rentMin", 0) or 0
    rt = item.get("roomTypeTitle") or {}
    tran = item.get("tranType", "")
    item_id = ""
    item_list = item.get("itemIdList") or [{}]
    if item_list and isinstance(item_list[0], dict):
        item_id = item_list[0].get("itemId", "")

    return {
        "수집시간": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "단지명": item.get("areaDanjiName"),
        "시도": item.get("local1"),
        "구": item.get("local2"),
        "동": item.get("local3"),
        "거래유형": TRAN_TYPE_KR.get(tran, ""),
        "층": item.get("floor"),
        "면적(m2)": size_m2,
        "평": round(size_m2 / 3.3058, 2) if size_m2 else "",
        "보증금": deposit,
        "월세": rent,
        "가격(억)": round(deposit / 10000, 2) if deposit else 0,
        "타입": rt.get("p") if isinstance(rt, dict) else "",
        "방향": DIRECTION_KR.get(item.get("direction", "")),
        "매물ID": item_id,
        "URL": f"https://www.zigbang.com/home/apt/items/{item_id}"
    }

# =========================
# 시군구 단위 작업 (병렬 실행 단위)
# =========================
def process_sigungu(district_name, sigungu_5):
    dong_list = get_dong_codes(sigungu_5)
    if not dong_list:
        return district_name, []

    local_items = []

    # 동 단위 병렬 호출
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DONG) as dong_exec:
        futures = {dong_exec.submit(fetch_by_dong, dong_code): dong_name
                   for dong_code, dong_name in dong_list}
        for future in as_completed(futures):
            items = future.result()
            for item in items:
                local_items.append(parse_row(item))

    return district_name, local_items

# =========================
# CSV 저장
# =========================
def save_csv(rows, path):
    rows = sorted(rows, key=lambda r: (
        r.get("시도") or "", r.get("구") or "",
        r.get("동") or "", r.get("단지명") or "", r.get("가격(억)") or 0
    ))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log(f"💾 저장 완료: {path} ({len(rows)}개)")

# =========================
# 메인
# =========================
def main():
    log("==== 전국 매물 수집 시작 (병렬 최적화) ====")
    log(f"⚙ 시군구 병렬: {MAX_WORKERS_SIGUNGU} / 동 병렬: {MAX_WORKERS_DONG} / 동시 요청 상한: {REQUEST_SEMAPHORE._value}")

    all_items = []
    seen = set()
    seen_lock = threading.Lock()

    total = len(SIGUNGU_CODES)
    completed = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_SIGUNGU) as executor:
        futures = {
            executor.submit(process_sigungu, name, code): name
            for name, code in SIGUNGU_CODES.items()
        }

        for future in as_completed(futures):
            district_name, rows = future.result()
            completed += 1

            new_count = 0
            with seen_lock:
                for r in rows:
                    uid = r.get("매물ID")
                    if uid and uid not in seen:
                        seen.add(uid)
                        all_items.append(r)
                        new_count += 1

            elapsed = time.time() - start_time
            eta = (elapsed / completed) * (total - completed) if completed else 0
            status = f"✅ {new_count}개" if new_count else "⚠ 매물없음"
            log(f"[{completed}/{total}] {district_name}: {status} | "
                f"경과 {elapsed/60:.1f}분 / 예상잔여 {eta/60:.1f}분")

    if not all_items:
        log("❌ 전체 데이터 없음")
        return

    save_csv(all_items, TOTAL_CSV)
    total_time = (time.time() - start_time) / 60
    log(f"\n🎉 완료! 총 매물: {len(all_items)}개 | 소요시간: {total_time:.1f}분")

if __name__ == "__main__":
    main()
