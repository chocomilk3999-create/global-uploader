import os, json, random, requests
from urllib.parse import urlparse
from typing import List, Optional
from fastapi import FastAPI
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright, Page

app = FastAPI(title="Global Auto Uploader (No Amazon)")

# --- [설정: 디렉토리] ---
STATE_DIR = "state"
DEBUG_DIR = "debug"
TMP_IMG_DIR = "tmp_images"

os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)
os.makedirs(TMP_IMG_DIR, exist_ok=True)

# --- [설정: 글로벌 마켓 정보] ---
# 형님의 제국 확장을 위한 국가별 기본 정보 (Make에서 market="UK"라고 보내면 알아서 바뀜)
EBAY_MARKETS = {
    "US": {"base": "https://www.ebay.com", "locale": "en-US", "tz": "America/Los_Angeles", "currency": "USD"},
    "UK": {"base": "https://www.ebay.co.uk", "locale": "en-GB", "tz": "Europe/London", "currency": "GBP"},
    "DE": {"base": "https://www.ebay.de", "locale": "de-DE", "tz": "Europe/Berlin", "currency": "EUR"},
    "AU": {"base": "https://www.ebay.com.au", "locale": "en-AU", "tz": "Australia/Sydney", "currency": "AUD"},
}

# --- [설정: eBay 고정 셀렉터 (형님이 주신 ID)] ---
EBAY_IDS = {
    "title": "s0-1-0-24-6-@TITLE-5-33-6-4-se-textbox",         # 형님이 찾은 ID
    "price": "s0-1-0-24-6-@PRICE-1-33-2-14-3-2-se-textbox",    # 형님이 찾은 ID
    "qty":   "s0-1-0-24-6-@PRICE-1-33-2-21-2-se-textbox",      # 형님이 찾은 ID
    "rte_iframe": "se-rte-frame__summary",                     # 설명창 iframe ID
}

# --- [데이터 모델] ---
class Policy(BaseModel):
    brand_mode: str = "generic"
    shipping_policy: str = "DEFAULT"
    return_policy: str = "DEFAULT"

class UploadTask(BaseModel):
    id: str
    sku: str
    title: str
    price_usd: float
    quantity: int = 1
    images: List[str] = Field(default_factory=list)
    description_html: str
    bullet_points: List[str] = Field(default_factory=list)
    category_hint: Optional[str] = None
    policy: Policy = Policy()
    
    # [글로벌 확장용 필드]
    targets: List[str] = Field(default_factory=lambda: ["ebay"]) # 기본은 eBay만
    market: str = "US"               # US, UK, DE, AU...
    currency: str = "USD"

class UploadResult(BaseModel):
    success: bool
    retryable: bool
    ebay_listing_url: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    notes: Optional[str] = None

# --- [헬퍼 함수: 이미지 다운로드] ---
def download_image(url: str, prefix: str) -> Optional[str]:
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        path = urlparse(url).path
        ext = os.path.splitext(path)[1]
        if not ext: ext = ".jpg"
        filename = f"{prefix}{ext}"
        filepath = os.path.join(TMP_IMG_DIR, filename)
        with open(filepath, "wb") as f:
            f.write(r.content)
        return filepath
    except Exception as e:
        print(f"Image download failed: {url} -> {e}")
        return None

# --- [헬퍼 함수: 에러 분류] ---
def classify_retryable(msg: str) -> bool:
    m = msg.lower()
    return any(k in m for k in ["timeout", "net::", "network", "login", "2fa", "captcha", "verification", "load"])

async def save_debug(page, prefix: str):
    try:
        await page.screenshot(path=os.path.join(DEBUG_DIR, f"{prefix}.png"), full_page=True)
        html = await page.content()
        with open(os.path.join(DEBUG_DIR, f"{prefix}.html"), "w", encoding="utf-8") as f:
            f.write(html)
    except:
        pass

async def ensure_not_login(page, market_context: str):
    url = page.url.lower()
    if any(s in url for s in ["signin", "login", "two-step", "captcha"]):
        raise RuntimeError(f"{market_context.upper()}_LOGIN_REQUIRED")

def get_ebay_state_path(market: str) -> str:
    return os.path.join(STATE_DIR, f"ebay_{market.upper()}_state.json")

# --- [eBay 업로드 로직 (핵심)] ---
async def ebay_fill_form(page: Page, task: UploadTask):
    # 1. 제목 입력
    await page.locator(f"#{EBAY_IDS['title']}").fill(task.title[:80])
    
    # 2. 가격 입력
    await page.locator(f"#{EBAY_IDS['price']}").fill(f"{task.price_usd:.2f}")
    
    # 3. 수량 입력
    await page.locator(f"#{EBAY_IDS['qty']}").fill(str(task.quantity))

    # 4. 상세 설명 (iframe 내부 주입)
    # iframe이 로드될 때까지 약간 대기
    iframe_selector = f"iframe#{EBAY_IDS['rte_iframe']}"
    await page.wait_for_selector(iframe_selector, timeout=10000)
    
    frame = page.frame_locator(iframe_selector)
    # body 클릭 후 내용 입력
    await frame.locator("body").click()
    await page.keyboard.press("Control+A") # 전체 선택
    await page.keyboard.press("Backspace") # 지우기
    # HTML이 너무 길면 잘릴 수 있으므로, JS로 주입하는게 가장 확실함
    safe_html = task.description_html.replace("`", "\`")
    await frame.locator("body").evaluate(f"el => el.innerHTML = `{safe_html}`")

async def ebay_upload_images_logic(page: Page, image_urls: list[str], task_id: str):
    if not image_urls: return
    
    # 1) 이미지 다운로드
    local_files = []
    for i, u in enumerate(image_urls[:8]): # 최대 8장까지만 (안전하게)
        fp = download_image(u, f"{task_id}_{i}")
        if fp: local_files.append(fp)
    
    if not local_files: return

    # 2) 파일 인풋 찾아서 업로드
    # 보통 type='file' 인풋이 숨겨져 있음.
    file_input = page.locator('input[type="file"]')
    if await file_input.count() > 0:
        await file_input.first.set_input_files(local_files)
        # 업로드 로딩 대기 (eBay가 이미지 처리하는 시간)
        await page.wait_for_timeout(5000 + (len(local_files) * 1000))

async def upload_ebay_ui(task: UploadTask) -> str:
    async with async_playwright() as p:
        # 마켓 설정 가져오기 (기본 US)
        market = (task.market or "US").upper()
        conf = EBAY_MARKETS.get(market, EBAY_MARKETS["US"])
        state_path = get_ebay_state_path(market)

        # 브라우저 실행
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(
            storage_state=state_path if os.path.exists(state_path) else None,
            locale=conf["locale"],
            timezone_id=conf["tz"],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # 1. 판매 페이지 접속
            target_url = f"{conf['base']}/sl/sell"
            await page.goto(target_url, timeout=60000)
            await ensure_not_login(page, f"ebay_{market}")
            await page.wait_for_timeout(random.randint(3000, 6000))

            # 2. 폼 작성 (제목, 가격, 수량, 설명)
            await ebay_fill_form(page, task)
            
            # 3. 이미지 업로드
            await ebay_upload_images_logic(page, task.images, task.id)

            # 4. [중요] 게시(Publish) 버튼 클릭 로직
            # 아직은 테스트 단계이므로 "클릭" 코드는 주석 처리해둠.
            # 형님이 "잘 입력된다!" 확인하면 그때 주석 풀면 됨.
            
            # publish_candidates = ["List it", "Publish", "List item", "Submit listing", "등록"]
            # for txt in publish_candidates:
            #     btn = page.get_by_role("button", name=txt)
            #     if await btn.count() > 0:
            #         await btn.first.click()
            #         break
            
            # 테스트용: 잠시 대기 후 종료
            await page.wait_for_timeout(3000)

            # 세션 저장 (로그인 유지용)
            await context.storage_state(path=state_path)
            
            return page.url

        except Exception as e:
            await save_debug(page, f"{task.id}_ebay_{market}")
            raise RuntimeError(str(e))
        finally:
            await browser.close()

# --- [메인 API 엔드포인트] ---
@app.post("/upload-global", response_model=UploadResult)
async def upload_global(task: UploadTask):
    ebay_url = None
    errors = []

    try:
        # 타겟에 ebay가 있으면 실행
        if "ebay" in [t.lower() for t in task.targets]:
            ebay_url = await upload_ebay_ui(task)
        
        # Shopify, Walmart 등은 나중에 API 모드로 여기에 추가
        
        return UploadResult(success=True, retryable=False, ebay_listing_url=ebay_url, notes="Listed on enabled targets")

    except RuntimeError as e:
        msg = str(e)
        return UploadResult(
            success=False,
            retryable=classify_retryable(msg),
            error_type="UPLOAD_ERROR",
            error_message=msg
        )
