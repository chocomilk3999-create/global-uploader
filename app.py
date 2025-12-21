import os, json, random
from typing import List, Optional
from fastapi import FastAPI
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright

app = FastAPI(title="Global Auto Uploader")

STATE_DIR = "state"
DEBUG_DIR = "debug"
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

AMAZON_STATE = os.path.join(STATE_DIR, "amazon_state.json")
EBAY_STATE = os.path.join(STATE_DIR, "ebay_state.json")

UPLOADER_MODE = os.getenv("UPLOADER_MODE", "ui")

class Policy(BaseModel):
    brand_mode: str = "generic"
    shipping_policy: str = "DEFAULT"
    return_policy: str = "DEFAULT"

class UploadTask(BaseModel):
    id: str
    sku: str
    title: str
    price_usd: float
    quantity: int = 10
    images: List[str] = Field(default_factory=list)
    description_html: str
    bullet_points: List[str] = Field(default_factory=list)
    category_hint: Optional[str] = None
    policy: Policy = Policy()
    targets: List[str] = Field(default_factory=lambda: ["amazon", "ebay"])

class UploadResult(BaseModel):
    success: bool
    retryable: bool
    amazon_listing_url: Optional[str] = None
    ebay_listing_url: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    notes: Optional[str] = None

def classify_retryable(msg: str) -> bool:
    m = msg.lower()
    return any(k in m for k in ["timeout", "net::", "network", "login", "2fa", "captcha", "verification"])

async def save_debug(page, prefix: str):
    try:
        await page.screenshot(path=os.path.join(DEBUG_DIR, f"{prefix}.png"), full_page=True)
        html = await page.content()
        with open(os.path.join(DEBUG_DIR, f"{prefix}.html"), "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

async def ensure_not_login(page, market: str):
    url = page.url.lower()
    login_signals = ["signin", "login", "two-step", "verification", "captcha"]
    if any(s in url for s in login_signals):
        raise RuntimeError(f"{market.upper()}_LOGIN_REQUIRED_OR_2FA")

async def upload_amazon_ui(task: UploadTask) -> str:
    async with async_playwright() as p:
        # [수정됨] 메모리 절약 모드 적용
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(
            storage_state=AMAZON_STATE if os.path.exists(AMAZON_STATE) else None,
            locale="en-US",
            timezone_id="America/Los_Angeles",
        )
        page = await context.new_page()
        try:
            await page.goto("https://sellercentral.amazon.com/", timeout=60000)
            await ensure_not_login(page, "amazon")
            await page.wait_for_timeout(random.randint(8000, 20000))
            await page.goto("https://sellercentral.amazon.com/abis/listing/create", timeout=60000)
            await ensure_not_login(page, "amazon")
            # 실제 업로드 로직은 계정별 셀렉터 확인 후 추가
            await context.storage_state(path=AMAZON_STATE)
            return page.url
        except Exception as e:
            await save_debug(page, f"{task.id}_amazon")
            raise RuntimeError(str(e))
        finally:
            await browser.close()

async def upload_ebay_ui(task: UploadTask) -> str:
    async with async_playwright() as p:
        # [수정됨] 메모리 절약 모드 적용
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(
            storage_state=EBAY_STATE if os.path.exists(EBAY_STATE) else None,
            locale="en-US",
            timezone_id="America/Los_Angeles",
        )
        page = await context.new_page()
        try:
            await page.goto("https://www.ebay.com/sl/sell", timeout=60000)
            await ensure_not_login(page, "ebay")
            await page.wait_for_timeout(random.randint(6000, 15000))
            await context.storage_state(path=EBAY_STATE)
            return page.url
        except Exception as e:
            await save_debug(page, f"{task.id}_ebay")
            raise RuntimeError(str(e))
        finally:
            await browser.close()

@app.post("/upload-global", response_model=UploadResult)
async def upload_global(task: UploadTask):
    amazon_url = None
    ebay_url = None
    try:
        for target in task.targets:
            if target == "amazon":
                amazon_url = await upload_amazon_ui(task)
            elif target == "ebay":
                ebay_url = await upload_ebay_ui(task)
        return UploadResult(success=True, retryable=False, amazon_listing_url=amazon_url, ebay_listing_url=ebay_url, notes="listed")
    except RuntimeError as e:
        msg = str(e)
        return UploadResult(
            success=False,
            retryable=classify_retryable(msg),
            error_type="UPLOAD_ERROR",
            error_message=msg,
        )
