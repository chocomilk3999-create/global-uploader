import os, json, random, requests
from urllib.parse import urlparse
from typing import List, Optional, Dict, Any
from fastapi import FastAPI
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright, Page

app = FastAPI(title="Global Auto Uploader (Cookie Receiver)")

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
class UploadTask(BaseModel):
    id: str
    sku: str
    title: str
    price_usd: float
    quantity: int = 1
    images: List[str] = Field(default_factory=list)
    description_html: str
    targets: List[str] = Field(default_factory=lambda: ["ebay"])
    market: str = "US"
    currency: str = "USD"

class UploadResult(BaseModel):
    success: bool
    retryable: bool
    ebay_listing_url: Optional[str] = None
    error_message: Optional[str] = None

# ✅ [신규 추가] 쿠키(State) 업데이트용 모델
class StateUpdate(BaseModel):
    market: str = "US"
    state_json: Dict[str, Any] # Playwright 전체 state 데이터

# --- [헬퍼 함수] ---
async def save_debug(page, prefix: str):
    try:
        safe_prefix = "".join(x for x in prefix if x.isalnum() or x in "_-")
        await page.screenshot(path=os.path.join(DEBUG_DIR, f"{safe_prefix}.png"), full_page=True)
        print(f"Saved debug screenshot: {safe_prefix}.png")
    except: pass

def download_image(url: str, prefix: str) -> Optional[str]:
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        ext = os.path.splitext(urlparse(url).path)[1] or ".jpg"
        filepath = os.path.join(TMP_IMG_DIR, f"{prefix}{ext}")
        with open(filepath, "wb") as f:
            f.write(r.content)
        return filepath
    except: return None

async def ensure_not_login(page, market_context: str):
    url = page.url.lower()
    if "signin" in url or "login" in url:
        raise RuntimeError(f"{market_context.upper()}_LOGIN_BLOCK")

# --- [핵심 로직] ---
async def ebay_fill_form(page: Page, task: UploadTask):
    # 1. 제목
    try:
        await page.get_by_label("Title").first.fill(task.title[:80])
    except:
        await page.locator('input[name*="title"], input[aria-label*="Title"]').first.fill(task.title[:80])
    
    # 2. 가격
    try:
        await page.get_by_label("Price").first.fill(f"{task.price_usd:.2f}")
    except:
        await page.locator('input[name*="price"], input[aria-label*="Price"]').first.fill(f"{task.price_usd:.2f}")
    
    # 3. 수량
    try:
        await page.get_by_label("Quantity").first.fill(str(task.quantity))
    except:
         await page.locator('input[name*="quantity"]').first.fill(str(task.quantity))

    # 4. 설명 (iframe)
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
    except: pass

async def ebay_upload_images_logic(page: Page, image_urls: list[str], task_id: str):
    if not image_urls: return
    local_files = []
    for i, u in enumerate(image_urls[:8]):
        fp = download_image(u, f"{task_id}_{i}")
        if fp: local_files.append(fp)
    if local_files:
        try:
            await page.locator('input[type="file"]').first.set_input_files(local_files)
            await page.wait_for_timeout(5000)
        except: pass

async def upload_ebay_ui(task: UploadTask) -> str:
    async with async_playwright() as p:
        market = (task.market or "US").upper()
        conf = EBAY_MARKETS.get(market, EBAY_MARKETS["US"])
        state_path = os.path.join(STATE_DIR, f"ebay_{market}_state.json")

        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        
        # 저장된 쿠키(State)가 있으면 불러오기
        context = await browser.new_context(
            storage_state=state_path if os.path.exists(state_path) else None,
            locale=conf["locale"],
            timezone_id=conf["tz"],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            await page.goto(f"{conf['base']}/sl/sell", timeout=60000)
            await page.wait_for_timeout(5000)
            await ensure_not_login(page, f"ebay_{market}")

            await ebay_fill_form(page, task)
            await ebay_upload_images_logic(page, task.images, task.id)
            
            # 성공하면 최신 쿠키 저장
            await context.storage_state(path=state_path)
            return page.url

        except Exception as e:
            await save_debug(page, f"{task.id}_error")
            raise RuntimeError(str(e))
        finally:
            await browser.close()

# ✅ [신규 API] 외부에서 쿠키 넣어주는 곳
@app.post("/update-state")
async def update_state(payload: StateUpdate):
    market = payload.market.upper()
    path = os.path.join(STATE_DIR, f"ebay_{market}_state.json")
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload.state_json, f)
        
    return {"status": "success", "message": f"Cookie injected for {market}"}

@app.post("/upload-global", response_model=UploadResult)
async def upload_global(task: UploadTask):
    try:
        ebay_url = None
        if "ebay" in [t.lower() for t in task.targets]:
            ebay_url = await upload_ebay_ui(task)
        return UploadResult(success=True, retryable=False, ebay_listing_url=ebay_url)
    except RuntimeError as e:
        return UploadResult(success=False, retryable=True, error_message=str(e))
