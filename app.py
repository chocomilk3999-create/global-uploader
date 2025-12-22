import os, json, random, requests
from urllib.parse import urlparse
from typing import List, Optional
from fastapi import FastAPI
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright, Page

app = FastAPI(title="Global Auto Uploader (Fixed Debug)")

# --- [설정: 디렉토리] ---
STATE_DIR = "state"
DEBUG_DIR = "debug"
TMP_IMG_DIR = "tmp_images"

# 폴더 없으면 만들기
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

# --- [중요: 헬퍼 함수를 맨 위로 올림] ---
async def save_debug(page, prefix: str):
    """에러 났을 때 화면 스크린샷 찍는 함수"""
    try:
        # 파일명에 특수문자 제거
        safe_prefix = "".join(x for x in prefix if x.isalnum() or x in "_-")
        await page.screenshot(path=os.path.join(DEBUG_DIR, f"{safe_prefix}.png"), full_page=True)
        # HTML도 저장 (선택)
        # html = await page.content()
        # with open(os.path.join(DEBUG_DIR, f"{safe_prefix}.html"), "w", encoding="utf-8") as f:
        #     f.write(html)
        print(f"Saved debug screenshot: {safe_prefix}.png")
    except Exception as e:
        print(f"Failed to save debug info: {e}")

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
    return any(k in msg.lower() for k in ["timeout", "net::", "login", "captcha", "load", "block"])

# --- [핵심 로직] ---
async def ebay_fill_form(page: Page, task: UploadTask):
    print(f"Starting to fill form for {task.title}")
    
    # [1] 로그인 화면인지 먼저 체크 (가장 중요)
    title_text = await page.title()
    if "Sign in" in title_text or "Login" in page.url:
        raise RuntimeError(f"EBAY_LOGIN_BLOCK: Redirected to login page. (Title: {title_text})")

    # [2] 제목 입력 (여러가지 방법으로 시도)
    try:
        # 방법 A: Label로 찾기
        await page.get_by_label("Title").first.fill(task.title[:80])
    except:
        try:
            # 방법 B: ID로 찾기 (형님이 찾은 ID의 일부)
            await page.locator("input[id*='TITLE']").first.fill(task.title[:80])
        except:
            # 방법 C: 범용 Selector
            await page.locator('input[name*="title"], input[aria-label*="Title"]').first.fill(task.title[:80])
    
    # [3] 가격 입력
    try:
        await page.get_by_label("Price").first.fill(f"{task.price_usd:.2f}")
    except:
        await page.locator('input[name*="price"], input[aria-label*="Price"], input[id*="PRICE"]').first.fill(f"{task.price_usd:.2f}")
    
    # [4] 수량 입력
    try:
        await page.get_by_label("Quantity").first.fill(str(task.quantity))
    except:
         await page.locator('input[name*="quantity"], input[aria-label*="Quantity"]').first.fill(str(task.quantity))

    # [5] 설명 (iframe)
    try:
        iframe = page.locator("iframe").first
        if await iframe.count() > 0:
            frame_ctx = iframe.content_frame
            if frame_ctx:
                await frame_ctx.locator("body").click()
                await page.keyboard.press("Control+A")
                await page.keyboard.press("Backspace")
                safe_html = task.description_html.replace("`", "\`")
                await frame_ctx.locator("body").evaluate(f"el => el.innerHTML = `{safe_html}`")
    except Exception as e:
        print(f"Description skip warning: {e}")

async def ebay_upload_images_logic(page: Page, image_urls: list[str], task_id: str):
    if not image_urls: return
    local_files = []
    for i, u in enumerate(image_urls[:8]):
        fp = download_image(u, f"{task_id}_{i}")
        if fp: local_files.append(fp)
    
    if local_files:
        try:
            # 파일 업로드 버튼 찾기
            await page.locator('input[type="file"]').first.set_input_files(local_files)
            # 업로드 대기
            await page.wait_for_timeout(5000)
        except:
            print("Image upload failed (selector not found)")

async def upload_ebay_ui(task: UploadTask) -> str:
    async with async_playwright() as p:
        market = (task.market or "US").upper()
        conf = EBAY_MARKETS.get(market, EBAY_MARKETS["US"])
        # state 파일 경로 (쿠키 저장소)
        state_path = os.path.join(STATE_DIR, f"ebay_{market}_state.json")

        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            storage_state=state_path if os.path.exists(state_path) else None,
            locale=conf["locale"],
            timezone_id=conf["tz"],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            print(f"Navigating to eBay {market}...")
            await page.goto(f"{conf['base']}/sl/sell", timeout=60000)
            
            # 페이지 로딩 대기
            await page.wait_for_timeout(5000)

            # 폼 작성 시작
            await ebay_fill_form(page, task)
            await ebay_upload_images_logic(page, task.images, task.id)
            
            # 성공 시 세션 저장
            await context.storage_state(path=state_path)
            return page.url

        except Exception as e:
            # 에러 발생 시 여기서 사진을 찍음
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
        msg = str(e)
        return UploadResult(success=False, retryable=classify_retryable(msg), error_message=msg)
