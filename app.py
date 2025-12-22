import os, json, random, requests, asyncio
from urllib.parse import urlparse
from typing import List, Optional, Dict, Any
from fastapi import FastAPI
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright, Page

app = FastAPI(title="Global Auto Uploader (Final + Ping)")

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

class StateUpdate(BaseModel):
    market: str = "US"
    state_json: Dict[str, Any]

# --- [헬퍼 함수] ---
async def save_debug(page, prefix: str):
    try:
        safe_prefix = "".join(x for x in prefix if x.isalnum() or x in "_-")
        path = os.path.join(DEBUG_DIR, f"{safe_prefix}.png")
        await page.screenshot(path=path, full_page=True)
        print(f"Saved debug screenshot: {path}")
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
    # 로그인 페이지 감지
    url = page.url.lower()
    title = await page.title()
    if "signin" in url or "login" in url or "Sign in" in title:
        raise RuntimeError(f"{market_context.upper()}_LOGIN_BLOCK")

# --- [핵심 로직] ---
async def ebay_fill_form(page: Page, task: UploadTask):
    # 제목 입력
    try:
        await page.get_by_label("Title").first.fill(task.title[:80])
    except:
        await page.locator('input[name*="title"], input[aria-label*="Title"]').first.fill(task.title[:80])
    
    # 가격 입력
    try:
        await page.get_by_label("Price").first.fill(f"{task.price_usd:.2f}")
    except:
        await page.locator('input[name*="price"], input[aria-label*="Price"]').first.fill(f"{task.price_usd:.2f}")
    
    # 수량 입력
    try:
        await page.get_by_label("Quantity").first.fill(str(task.quantity))
    except:
         await page.locator('input[name*="quantity"]').first.fill(str(task.quantity))

    # 설명 (iframe)
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
        
        # 쿠키 로드
        context = await browser.new_context(
            storage_state=state_path if os.path.exists(state_path) else None,
            locale=conf["locale"],
            timezone_id=conf["tz"],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # 판매 페이지 접속
            await page.goto(f"{conf['base']}/sl/sell", timeout=60000)
            await page.wait_for_timeout(5000)
            
            # 로그인 체크
            await ensure_not_login(page, f"ebay_{market}")

            await ebay_fill_form(page, task)
            await ebay_upload_images_logic(page, task.images, task.id)
            
            # 성공 시 최신 쿠키 저장
            await context.storage_state(path=state_path)
            return page.url

        except Exception as e:
            await save_debug(page, f"{task.id}_error")
            raise RuntimeError(str(e))
        finally:
            await browser.close()

# --- [API 엔드포인트] ---

# 1. 쿠키 업데이트 (기존 유지)
@app.post("/update-state")
async def update_state(payload: StateUpdate):
    market = payload.market.upper()
    path = os.path.join(STATE_DIR, f"ebay_{market}_state.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload.state_json, f)
    print(f"✅ Saved state for market={market}")
    return {"status": "success", "message": f"Cookie injected for {market}"}

# 2. 업로드 (기존 유지)
@app.post("/upload-global", response_model=UploadResult)
async def upload_global(task: UploadTask):
    try:
        ebay_url = None
        if "ebay" in [t.lower() for t in task.targets]:
            ebay_url = await upload_ebay_ui(task)
        return UploadResult(success=True, retryable=False, ebay_listing_url=ebay_url)
    except RuntimeError as e:
        return UploadResult(success=False, retryable=True, error_message=str(e))

# 3. 🔥 [신규] 로그인 상태 확인 (Ping Test)
@app.get("/ping-ebay")
async def ping_ebay(market: str = "US"):
    market = market.upper()
    conf = EBAY_MARKETS.get(market, EBAY_MARKETS["US"])
    state_path = os.path.join(STATE_DIR, f"ebay_{market}_state.json")
    
    if not os.path.exists(state_path):
        return {"status": "error", "message": "No cookie file found. Run cookie_shooter first."}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            storage_state=state_path,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        try:
            # 셀러 허브 메인으로 접속해봄
            target_url = "https://www.ebay.com/sh/ovw"
            print(f"Pinging {target_url}...")
            await page.goto(target_url, timeout=30000)
            await page.wait_for_timeout(3000)
            
            # 제목에 'Sign in'이나 URL에 'login'이 있는지 체크
            title = await page.title()
            url = page.url
            
            if "signin" in url.lower() or "login" in url.lower() or "Sign in" in title:
                # 스크린샷 저장
                await save_debug(page, "ping_failed")
                return {"status": "failed", "message": "Login required (Cookie expired or invalid)", "current_url": url}
            
            return {"status": "authenticated", "message": "Login successful!", "title": title}
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            await browser.close()
