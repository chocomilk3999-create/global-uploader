import os, json, random, requests
from urllib.parse import urlparse
from typing import List, Optional
from fastapi import FastAPI
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright, Page

app = FastAPI(title="Global Auto Uploader (Smart Selector)")

# --- [설정: 디렉토리] ---
STATE_DIR = "state"
DEBUG_DIR = "debug"
TMP_IMG_DIR = "tmp_images"

os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)
os.makedirs(TMP_IMG_DIR, exist_ok=True)

# --- [설정: 글로벌 마켓] ---
EBAY_MARKETS = {
    "US": {"base": "https://www.ebay.com", "locale": "en-US", "tz": "America/Los_Angeles", "currency": "USD"},
    "UK": {"base": "https://www.ebay.co.uk", "locale": "en-GB", "tz": "Europe/London", "currency": "GBP"},
    "DE": {"base": "https://www.ebay.de", "locale": "de-DE", "tz": "Europe/Berlin", "currency": "EUR"},
    "AU": {"base": "https://www.ebay.com.au", "locale": "en-AU", "tz": "Australia/Sydney", "currency": "AUD"},
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
    targets: List[str] = Field(default_factory=lambda: ["ebay"])
    market: str = "US"
    currency: str = "USD"

class UploadResult(BaseModel):
    success: bool
    retryable: bool
    ebay_listing_url: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None

# --- [헬퍼 함수] ---
def download_image(url: str, prefix: str) -> Optional[str]:
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        ext = os.path.splitext(urlparse(url).path)[1] or ".jpg"
        filepath = os.path.join(TMP_IMG_DIR, f"{prefix}{ext}")
        with open(filepath, "wb") as f:
            f.write(r.content)
        return filepath
    except:
        return None

def classify_retryable(msg: str) -> bool:
    return any(k in msg.lower() for k in ["timeout", "net::", "login", "captcha", "load"])

async def ensure_not_login(page):
    # 로그인 페이지에 갇혔는지 확인 (가장 중요)
    if "signin" in page.url or "login" in page.url:
        raise RuntimeError("EBAY_LOGIN_BLOCK: Bot is stuck at Login Screen.")

def get_ebay_state_path(market: str) -> str:
    return os.path.join(STATE_DIR, f"ebay_{market.upper()}_state.json")

# --- [핵심: 스마트 셀렉터 로직] ---
async def ebay_fill_form(page: Page, task: UploadTask):
    # 1. 제목 (Title) - ID 대신 '라벨'이나 'placeholder'로 찾기
    # eBay 화면마다 달라서 여러 방법으로 시도
    try:
        await page.get_by_label("Title").first.fill(task.title[:80])
    except:
        # 실패하면 'Title'이라는 글자 근처의 입력칸 찾기
        await page.locator('input[name*="title"], input[aria-label*="Title"]').first.fill(task.title[:80])
    
    # 2. 가격 (Price)
    try:
        await page.get_by_label("Price").first.fill(f"{task.price_usd:.2f}")
    except:
        await page.locator('input[name*="price"], input[aria-label*="Price"]').first.fill(f"{task.price_usd:.2f}")
    
    # 3. 수량 (Quantity)
    try:
        await page.get_by_label("Quantity").first.fill(str(task.quantity))
    except:
         await page.locator('input[name*="quantity"], input[aria-label*="Quantity"]').first.fill(str(task.quantity))

    # 4. 설명 (Description) - iframe 찾기
    # ID가 바뀌어도 'iframe' 태그는 변하지 않음
    try:
        iframe = page.locator("iframe").first # 첫번째 iframe이 보통 에디터임
        await iframe.wait_for(timeout=5000)
        frame_ctx = iframe.content_frame
        if frame_ctx:
            await frame_ctx.locator("body").click()
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            safe_html = task.description_html.replace("`", "\`")
            await frame_ctx.locator("body").evaluate(f"el => el.innerHTML = `{safe_html}`")
    except:
        print("Description iframe skipping (Test mode)")

async def ebay_upload_images_logic(page: Page, image_urls: list[str], task_id: str):
    if not image_urls: return
    local_files = []
    for i, u in enumerate(image_urls[:8]):
        fp = download_image(u, f"{task_id}_{i}")
        if fp: local_files.append(fp)
    
    if local_files:
        # 파일 업로드 버튼도 'type=file'로 찾음 (ID 불필요)
        await page.locator('input[type="file"]').first.set_input_files(local_files)
        await page.wait_for_timeout(5000)

async def upload_ebay_ui(task: UploadTask) -> str:
    async with async_playwright() as p:
        market = (task.market or "US").upper()
        conf = EBAY_MARKETS.get(market, EBAY_MARKETS["US"])
        state_path = get_ebay_state_path(market)

        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            storage_state=state_path if os.path.exists(state_path) else None,
            locale=conf["locale"],
            timezone_id=conf["tz"]
        )
        page = await context.new_page()

        try:
            await page.goto(f"{conf['base']}/sl/sell", timeout=60000)
            await ensure_not_login(page) # 로그인 체크
            await page.wait_for_timeout(4000)

            # 스마트 입력 시작
            await ebay_fill_form(page, task)
            await ebay_upload_images_logic(page, task.images, task.id)
            
            # 테스트 완료 후 저장
            await context.storage_state(path=state_path)
            return page.url

        except Exception as e:
            # 디버그용 스크린샷 저장
            await save_debug(page, f"{task.id}_error")
            raise RuntimeError(str(e))
        finally:
            await browser.close()

@app.post("/upload-global", response_model=UploadResult)
async def upload_global(task: UploadTask):
    try:
        ebay_url = None
        if "ebay" in [t.lower() for t in task.targets]:
            ebay_url = await upload_ebay_ui(task)
        return UploadResult(success=True, retryable=False, ebay_listing_url=ebay_url)
    except RuntimeError as e:
        return UploadResult(success=False, retryable=classify_retryable(str(e)), error_message=str(e))
