from phantomwright.sync_api import sync_playwright
from phantomwright.stealth import Stealth
from phantomwright.user_simulator import SyncUserSimulator
import json
import time
import subprocess
import math
import ctypes
from inference_sdk import InferenceHTTPClient
import pkg_resources
import requests
from PIL import Image
import os
import warnings

warnings.simplefilter("ignore")

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(PACKAGE_DIR)
COOKIES_DIR = os.path.join(PROJECT_ROOT, "cookies")
TEMP_COOKIE_EXPORT_PATH = os.path.join(PROJECT_ROOT, "TK_cookies.json")


def _emit_log(message, suppressprint=False, log_callback=None):
    if not suppressprint:
        print(message)
    if log_callback:
        try:
            log_callback(message)
        except Exception:
            pass


class TikTokUploadError(RuntimeError):
    """Raised when a TikTok upload fails or cannot be confirmed.

    Replaces sys.exit() so callers can catch upload failures without
    SystemExit propagating to the host process (critical in async
    and multi-threaded environments).
    """
    pass

UPLOAD_URL = "https://www.tiktok.com/tiktokstudio/upload?from=upload&lang=en"
DRAFT_URL = "https://www.tiktok.com/tiktokstudio/content?tab=draft"
CONTENT_URL = "https://www.tiktok.com/tiktokstudio/content"

CAPTCHA_QUESTION_SELECTOR = "div.VerifyBar___StyledDiv-sc-12zaxoy-0.hRJhHT"
CAPTCHA_IMAGE_SELECTOR = "img#captcha-verify-image"
CAPTCHA_REFRESH_SELECTOR = "span.secsdk_captcha_refresh--text"
CAPTCHA_SUCCESS_SELECTOR = "div.captcha_verify_message.captcha_verify_message-pass"
CAPTCHA_FAIL_SELECTOR = "div.captcha_verify_message.captcha_verify_message-fail"
CAPTCHA_SUBMIT_SELECTOR = "div.verify-captcha-submit-button"

SCHEDULE_DAY_ICON_SELECTOR = 'div.TUXTextInputCore-leadingIconWrapper:has(svg > path[d="M15 3a1 1 0 0 0-1 1v3h-1.4c-3.36 0-5.04 0-6.32.65a6 6 0 0 0-2.63 2.63C3 11.56 3 13.24 3 16.6v16.8c0 3.36 0 5.04.65 6.32a6 6 0 0 0 2.63 2.63c1.28.65 2.96.65 6.32.65h22.8c3.36 0 5.04 0 6.32-.65a6 6 0 0 0 2.63-2.63c.65-1.28.65-2.96.65-6.32V16.6c0-3.36 0-5.04-.65-6.32a6 6 0 0 0-2.63-2.63C40.44 7 38.76 7 35.4 7H34V4a1 1 0 0 0-1-1h-2a1 1 0 0 0-1 1v3H18V4a1 1 0 0 0-1-1h-2Zm-2.4 8H14v3a1 1 0 0 0 1 1h2a1 1 0 0 0 1-1v-3h12v3a1 1 0 0 0 1 1h2a1 1 0 0 0 1-1v-3h1.4c1.75 0 2.82 0 3.62.07a5.11 5.11 0 0 1 .86.14h.03a2 2 0 0 1 .88.91 5.11 5.11 0 0 1 .14.86c.07.8.07 1.87.07 3.62v1.9H7v-1.9c0-1.75 0-2.82.07-3.62a5.12 5.12 0 0 1 .14-.86v-.03a2 2 0 0 1 .88-.87l.03-.01a5.11 5.11 0 0 1 .86-.14c.8-.07 1.87-.07 3.62-.07ZM7 22.5h34v10.9c0 1.75 0 2.82-.07 3.62a5.11 5.11 0 0 1-.14.86v.03a2 2 0 0 1-.88.87l-.03.01a5.11 5.11 0 0 1-.86.14c-.8.07-1.87.07-3.62.07H12.6c-1.75 0-2.82 0-3.62-.07a5.11 5.11 0 0 1-.89-.15 2 2 0 0 1-.87-.87l-.01-.03a5.12 5.12 0 0 1-.14-.86C7 36.22 7 35.15 7 33.4V22.5Z"])'
SCHEDULE_TIME_ICON_SELECTOR = 'div.TUXTextInputCore-leadingIconWrapper:has(svg > path[d="M24 2a22 22 0 1 0 0 44 22 22 0 0 0 0-44ZM6 24a18 18 0 1 1 36 0 18 18 0 0 1-36 0Z"])'

SOUND_VOLUME_ICON_WAIT_SELECTOR = 'img[src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjEiIGhlaWdodD0iMjAiIHZpZXdCb3g9IjAgMCAyMSAyMCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTAgNy41MDE2QzAgNi42NzMxNyAwLjY3MTU3MyA2LjAwMTYgMS41IDYuMDAxNkgzLjU3NzA5QzMuODY4MDUgNi4wMDE2IDQuMTQ0NTggNS44NzQ4OCA0LjMzNDU1IDUuNjU0NDlMOC43NDI1NSAwLjU0MDUyQzkuMzQ3OCAtMC4xNjE2NjggMTAuNSAwLjI2NjM3NCAxMC41IDEuMTkzNDFWMTguOTY3MkMxMC41IDE5Ljg3NDUgOS4zODg5NCAyMC4zMTI5IDguNzY5NDIgMTkuNjVMNC4zMzE3OSAxNC45MDIxQzQuMTQyNjkgMTQuNjk5OCAzLjg3ODE2IDE0LjU4NDkgMy42MDEyMiAxNC41ODQ5SDEuNUMwLjY3MTU3MyAxNC41ODQ5IDAgMTMuOTEzNCAwIDEzLjA4NDlWNy41MDE2Wk01Ljg0OTQ1IDYuOTYwMjdDNS4yNzk1NiA3LjYyMTQzIDQuNDQ5OTcgOC4wMDE2IDMuNTc3MDkgOC4wMDE2SDJWMTIuNTg0OUgzLjYwMTIyQzQuNDMyMDMgMTIuNTg0OSA1LjIyNTY0IDEyLjkyOTUgNS43OTI5NSAxMy41MzY0TDguNSAxNi40MzI4VjMuODg1MjJMNS44NDk0NSA2Ljk2MDI3WiIgZmlsbD0iIzE2MTgyMyIgZmlsbC1vcGFjaXR5PSIwLjYiLz4KPHBhdGggZD0iTTEzLjUxNSA3LjE5MTE5QzEzLjM0MjQgNi45NzU1OSAxMy4zMzk5IDYuNjYwNTYgMTMuNTM1MiA2LjQ2NTNMMTQuMjQyMyA1Ljc1ODE5QzE0LjQzNzYgNS41NjI5MyAxNC43NTU4IDUuNTYxNzUgMTQuOTM1NiA1Ljc3MTM2QzE2Ljk5NTkgOC4xNzM2MiAxNi45OTU5IDExLjgyOCAxNC45MzU2IDE0LjIzMDNDMTQuNzU1OCAxNC40Mzk5IDE0LjQzNzYgMTQuNDM4NyAxNC4yNDIzIDE0LjI0MzVMMTMuNTM1MiAxMy41MzY0QzEzLjMzOTkgMTMuMzQxMSAxMy4zNDI0IDEzLjAyNjEgMTMuNTE1IDEyLjgxMDVDMTQuODEzIDExLjE4ODUgMTQuODEzIDguODEzMTIgMTMuNTE1IDcuMTkxMTlaIiBmaWxsPSIjMTYxODIzIiBmaWxsLW9wYWNpdHk9IjAuNiIvPgo8cGF0aCBkPSJNMTYuNzE3MiAxNi43MTgzQzE2LjUyMTkgMTYuNTIzMSAxNi41MjMxIDE2LjIwNzQgMTYuNzA3MiAxNi4wMDE3QzE5LjcyNTcgMTIuNjMgMTkuNzI1NyA3LjM3MTY4IDE2LjcwNzIgNC4wMDAwMUMxNi41MjMxIDMuNzk0MjcgMTYuNTIxOSAzLjQ3ODU4IDE2LjcxNzIgMy4yODMzMkwxNy40MjQzIDIuNTc2MjFDMTcuNjE5NSAyLjM4MDk1IDE3LjkzNyAyLjM4MDIgMTguMTIzMyAyLjU4NDA4QzIxLjkwOTkgNi43MjkyNiAyMS45MDk5IDEzLjI3MjQgMTguMTIzMyAxNy40MTc2QzE3LjkzNyAxNy42MjE1IDE3LjYxOTUgMTcuNjIwNyAxNy40MjQzIDE3LjQyNTVMMTYuNzE3MiAxNi43MTgzWiIgZmlsbD0iIzE2MTgyMyIgZmlsbC1vcGFjaXR5PSIwLjYiLz4KPC9zdmc+Cg=="]'

SOUND_VOLUME_ICON_CLICK_UPLOAD_SELECTOR = 'img[src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjEiIGhlaWdodD0iMjAiIHZpZXdCb3g9IjAgMCAyMSAyMCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTAgNy41MDE2QzAgNi42NzMxNyAwLjY3MTU3MyA2LjAwMTYgMS41IDYuMDAxNkgzLjU3NzA5QzMuODY4MDUgNi4wMDE2IDQuMTQ0NTggNS44NzQ4OCA0LjMzNDU1IDUuNjU0NDlMOC43NDI1NSAwLjU0MDUyQzkuMzQ3OCAtMC4xNjE2NjggMTAuNSAwLjI2NjM3NCAxMC41IDEuMTkzNDFWMTguOTY3MkMxMC41IDE5Ljg3NDUgOS4zODg5NCAyMC4zMTI5IDguNzY5NDIgMTkuNjVMNC4zMzE3OSAxNC45MDIxQzQuMTQyNjkgMTQuNjk5OCAzLjg3ODE2IDE0LjU4NDkgMy42MDEyMiAxNC41ODQ5SDEuNUMwLjY3MTU3MyAxNC41ODQ5IDAgMTMuOTEzNCAwIDEzLjA4NDlWNy41MDE2Wk01Ljg0OTQ1IDYuOTYwMjdDNS4yNzk1NiA3LjYyMTQzIDQuNDQ5OTcgOC4wMDE2IDMuNTc3MDkgOC4wMDE2SDJWMTIuNTg0OUgzLjYwMTIyQzQuNDMyMDMgMTIuNTg0OSA1LjIyNTY0IDEyLjkyOTUgNS43OTI5NSAxMy41MzY0TDguNSAxNi4wMDAxIDEzLjI3MjQgMTguMTIzMyAxNy40MTc2QzE3LjkzNyAxNy42MjE1IDE3LjYxOTUgMTcuNjIwNyAxNy40MjQzIDE3LjQyNTVMMTYuNzE3MiAxNi43MTgzWiIgZmlsbD0iIzE2MTgyMyIgZmlsbC1vcGFjaXR5PSIwLjYiLz4KPC9zdmc+Cg=="]'

SOUND_VOLUME_ICON_CLICK_DRAFT_SELECTOR = 'img[src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjEiIGhlaWdodD0iMjAiIHZpZXdCb3g9IjAgMCAyMSAyMCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTAgNy41MDE2QzAgNi42NzMxNyAwLjY3MTU3MyA2LjAwMTYgMS41IDYuMDAxNkgzLjU3NzA5QzMuODY4MDUgNi4wMDE2IDQuMTQ0NTggNS44NzQ4OCA0LjMzNDU1IDUuNjU0NDlMOC43NDI1NSAwLjU0MDUyQzkuMzQ3OCAtMC4xNjE2NjggMTAuNSAwLjI2NjM3NCAxMC41IDEuMTkzNDFWMTguOTY3MkMxMC41IDE5Ljg3NDUgOS4zODg5NCAyMC4zMTI5IDguNzY5NDIgMTkuNjVMNC4zMzE3OSAxNC45MDIxQzQuMTQyNjkgMTQuNjk5OCAzLjg3ODE2IDE0LjU4NDkgMy42MDEyMiAxNC41ODQ5SDEuNUMwLjY3MTU3MyAxNC41ODQ5IDAgMTMuOTEzNCAwIDEzLjA4NDlWNy41MDE2Wk01Ljk0OTQ1IDYuOTYwMjdDNS4yNzk1NiA3LjYyMTQzIDQuNDQ5OTcgOC4wMDE2IDMuNTc3MDkgOC4wMDE2SDJWMTIuNTg0OUgzLjYwMTIyQzQuNDMyMDMgMTIuNTg0OSA1LjIyNTY0IDEyLjkyOTUgNS43OTI5NSAxMy41MzY0TDguNSAxNi40MzI4VjMuODg1MjJMNS44NDk0NSA2Ljk2MDI3WiIgZmlsbD0iIzE2MTgyMyIgZmlsbC1vcGFjaXR5PSIwLjYiLz4KPC9zdmc+Cg=="]'

DRAFT_EDIT_ICON_SELECTOR = "path[d='M37.37 4.85a4.01 4.01 0 0 0-.99-.79 3 3 0 0 0-2.72 0c-.45.23-.81.6-1 .79a9 9 0 0 1-.04.05l-19.3 19.3c-1.64 1.63-2.53 2.52-3.35 3.47a36 36 0 0 0-4.32 6.16c-.6 1.1-1.14 2.24-2.11 4.33l-.3.6c-.4.75-.84 1.61-.8 2.43a2.5 2.5 0 0 0 2.37 2.36c.82.05 1.68-.4 2.44-.79l.59-.3c2.09-.97 3.23-1.5 4.33-2.11a36 36 0 0 0 6.16-4.32c.95-.82 1.84-1.71 3.47-3.34l19.3-19.3.05-.06a3 3 0 0 0 .78-3.71c-.22-.45-.6-.81-.78-1l-.02-.02-.03-.03-3.67-3.67a8.7 8.7 0 0 1-.06-.05ZM16.2 26.97 35.02 8.15l2.83 2.83L19.03 29.8c-1.7 1.7-2.5 2.5-3.33 3.21a32 32 0 0 1-7.65 4.93 32 32 0 0 1 4.93-7.65c.73-.82 1.51-1.61 3.22-3.32Z']"


def _dismiss_overlays(page):
    """Attempt to dismiss tutorial overlays and pop-ups that block interactions."""
    try:
        # Check for "Sure you want to cancel your upload?" and click "No"
        cancel_modal = page.locator('div:has-text("Sure you want to cancel your upload?")')
        if cancel_modal.is_visible(timeout=500):
            no_btn = page.locator('button:has-text("No")')
            if no_btn.is_visible():
                no_btn.click(force=True)

        # Common tutorial dismissal buttons. Avoid generic "Cancel" here because
        # TikTok's real upload card has a visible Cancel button while the file is
        # still transferring, and clicking it aborts the upload.
        selectors = [
            "button:has-text('Got it')",
            "button:has-text('Skip')",
            "button:has-text('Dismiss')",
            "button[aria-label='Close tutorial']",
            "button[aria-label='Close overlay']",
            "div.react-joyride__overlay",
            "div[data-test-id='overlay']",
            "div.TUXModal-overlay"
        ]
        
        for selector in selectors:
            loc = page.locator(selector)
            if loc.is_visible(timeout=500):
                try:
                    # Use force=True to click even if obscured, or just click if it's the blocking element
                    loc.first.click(timeout=1000, force=True)
                except:
                    pass
        
        # Also try to remove them via script if they persist
        page.evaluate('''() => {
            const overlays = document.querySelectorAll('.react-joyride__overlay, [data-test-id="overlay"], .common-modal-mask, .TUXModal-overlay');
            // Only remove if it's a tutorial, don't remove critical modals like "Post now" unless we've handled them
            overlays.forEach(el => {
                if (el.innerText.includes('tutorial') || el.innerText.includes('Skip') || el.classList.contains('react-joyride__overlay')) {
                    el.remove();
                }
            });
            const joyridePopper = document.querySelector('.react-joyride__tooltip');
            if (joyridePopper) joyridePopper.remove();
        }''')
    except:
        pass


def check_for_updates():

    current_version = pkg_resources.get_distribution("tiktokautouploader").version
    response = requests.get("https://pypi.org/pypi/tiktokautouploader/json")

    if response.status_code == 200:
        latest_version = response.json()["info"]["version"]
        if current_version != latest_version:
            print(
                f"WARNING: You are using version {current_version} of tiktokautouploader, "
                f"PLEASE UPDATE TO LATEST VERSION {latest_version} FOR BEST EXPERIENCE."
            )


def login_warning(accountname):
    print(f"NO COOKIES FILE FOUND FOR ACCOUNT {accountname}, PLEASE LOG-IN TO {accountname} WHEN PROMPTED")


def save_cookies(cookies):
    with open(TEMP_COOKIE_EXPORT_PATH, "w") as file:
        json.dump(cookies, file, indent=4)


def check_expiry(accountname, cookies_path=None):
    if cookies_path is None:
        cookies_path = _cookie_file(accountname)

    with open(cookies_path, "r") as file:
        cookies = json.load(file)

    current_time = int(time.time())
    cookies_expire = []
    expired = False
    for cookie in cookies:
        if cookie["name"] in ["sessionid", "sid_tt", "sessionid_ss", "passport_auth_status"]:
            expiry = cookie.get("expires")
            if not expiry:
                expiry = cookie.get("expirationDate")
            cookies_expire.append(expiry < current_time)

    if all(cookies_expire):
        expired = True

    return expired


def run_javascript(proxy_data=None):
    js_file_path = pkg_resources.resource_filename(__name__, "Js_assets/login.js")
    proxy_argument = str(proxy_data) if proxy_data is not None else str({})
    try:
        result = subprocess.run(
            ["node", js_file_path, "--proxy", proxy_argument],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
    except Exception as e:
        raise TikTokUploadError(f"Error while running the JavaScript file, when trying to parse cookies: {e}")
    return result


def install_js_dependencies():
    js_dir = pkg_resources.resource_filename(__name__, "Js_assets")
    node_modules_path = os.path.join(js_dir, "node_modules")

    if not os.path.exists(node_modules_path):
        print("JavaScript dependencies not found. Installing...")
        try:
            subprocess.run(["npm", "install", "--silent"], cwd=js_dir, check=True)
        except Exception as e:
            print("An error occurred during npm installation.")
            print(f"Error details: {e}")
            print("Trying to install JavaScript dependencies with shell...")
            try:
                subprocess.run(["npm", "install", "--silent"], cwd=js_dir, check=True, shell=True)
            except Exception as e:
                print("An error occurred during shell npm installation.")
                print(f"Error details: {e}")
    else:
        time.sleep(0.1)


def read_cookies(cookies_path):
    cookie_read = False
    try:
        with open(cookies_path, "r") as cookiefile:
            cookies = json.load(cookiefile)

        for cookie in cookies:
            if cookie.get("sameSite") not in ["Strict", "Lax", "None"]:
                cookie["sameSite"] = "Lax"

        cookie_read = True
    except Exception:
        raise TikTokUploadError("ERROR: CANT READ COOKIES FILE")

    return cookies, cookie_read


def detect_redirect(page):
    redirect_detected = False

    def on_response(response):
        nonlocal redirect_detected
        if response.request.redirected_from:
            redirect_detected = True

    page.on("response", on_response)

    return redirect_detected


def understood_Qs(question):
    understood_terms = {
        "touchdowns": "football",
        "orange and round": "basketball",
        "used in hoops": "basketball",
        "has strings": "guitar",
        "oval and inflatable": "football",
        "strumming": "guitar",
        "bounces": "basketball",
        "musical instrument": "guitar",
        "laces": "football",
        "bands": "guitar",
        "leather": "football",
        "leaves": "tree",
        "pages": "book",
        "throwing": "football",
        "tossed in a spiral": "football",
        "spiky crown": "pineapple",
        "pigskin": "football",
        "photography": "camera",
        "lens": "camera",
        "grow": "tree",
        "captures images": "camera",
        "keeps doctors": "apple",
        "crown": "pineapple",
        "driven": "car",
    }

    for key in understood_terms.keys():
        if key in question:
            item = understood_terms.get(key)
            return item

    return "N.A"


def get_image_src(page):
    image_url = page.get_attribute(CAPTCHA_IMAGE_SELECTOR, "src")
    return image_url


def download_image(image_url):
    response = requests.get(image_url)
    image_path = "captcha_image.jpg"
    with open(image_path, "wb") as f:
        f.write(response.content)
    return image_path


def run_inference_on_image_tougher(image_path, object):
    rk = "kyHFbAWkOWfGz8fSEw8O"
    client = InferenceHTTPClient(
        api_url="https://detect.roboflow.com",
        api_key=f"{rk}",
    )
    results = client.infer(image_path, model_id="captcha-2-6ehbe/2")

    class_names = []
    bounding_boxes = []
    for obj in results["predictions"]:
        class_names.append(obj["class"])
        bounding_boxes.append(
            {
                "x": obj["x"],
                "y": obj["y"],
                "width": obj["width"],
                "height": obj["height"],
            }
        )

    bounding_box = []
    class_to_click = object
    for i, classes in enumerate(class_names):
        if classes == class_to_click:
            bounding_box.append(bounding_boxes[i])

    return bounding_box


def run_inference_on_image(image_path):
    rk = "kyHFbAWkOWfGz8fSEw8O"
    client = InferenceHTTPClient(
        api_url="https://detect.roboflow.com",
        api_key=f"{rk}",
    )
    results = client.infer(image_path, model_id="tk-3nwi9/2")

    class_names = []
    bounding_boxes = []
    for obj in results["predictions"]:
        class_names.append(obj["class"])
        bounding_boxes.append(
            {
                "x": obj["x"],
                "y": obj["y"],
                "width": obj["width"],
                "height": obj["height"],
            }
        )

    already_written = []
    bounding_box = []
    class_to_click = []
    for i, detected_class in enumerate(class_names):
        if detected_class in already_written:
            class_to_click.append(detected_class)
            bounding_box.append(bounding_boxes[i])
            index = already_written.index(detected_class)
            bounding_box.append(bounding_boxes[index])
        already_written.append(detected_class)

    found = False
    if len(class_to_click) == 1:
        found = True

    return bounding_box, found


def convert_to_webpage_coordinates(
    bounding_boxes,
    image_x,
    image_y,
    image_height_web,
    image_width_web,
    image_height_real,
    image_width_real,
):
    webpage_coordinates = []
    for box in bounding_boxes:
        x_box = box["x"]
        y_box = box["y"]
        rel_x = (x_box * image_width_web) / image_width_real
        rel_y = (y_box * image_height_web) / image_height_real
        x_cord = image_x + rel_x
        y_cord = image_y + rel_y
        webpage_coordinates.append((x_cord, y_cord))
    return webpage_coordinates


def click_on_objects(page, object_coords):
    for (x, y) in object_coords:
        page.mouse.click(x, y)
        time.sleep(0.5)


def validate_proxy(proxy):
    if not proxy:
        return

    if not isinstance(proxy, dict):
        raise ValueError("Proxy must be a dictionary.")

    if "server" not in proxy or not isinstance(proxy["server"], str):
        raise ValueError("Proxy must contain a 'server' key with a string value.")

    try:
        proxies = {
            "http": f'http://{proxy["server"]}/',
            "https": f'https://{proxy["server"]}/',
        }
        if proxy.get("username"):
            proxies = {
                "http": f'http://{proxy.get("username")}:{proxy.get("password")}@{proxy["server"]}/',
                "https": f'https://{proxy.get("username")}:{proxy.get("password")}@{proxy["server"]}/',
            }

        response = requests.get("https://www.google.com", proxies=proxies)
        if response.status_code == 200:
            print("Proxy is valid!")
        else:
            raise ValueError(f"Proxy test failed with status code: {response.status_code}")
    except Exception as e:
        raise ValueError(f"Invalid proxy configuration when trying to simple request: {e}")


def _get_primary_screen_size():
    default_width, default_height = 1920, 1080

    try:
        if os.name == "nt":
            user32 = ctypes.windll.user32
            try:
                user32.SetProcessDPIAware()
            except Exception:
                pass

            width = int(user32.GetSystemMetrics(0))
            height = int(user32.GetSystemMetrics(1))
            if width > 0 and height > 0:
                return width, height
    except Exception:
        pass

    try:
        import tkinter as tk
        root = tk.Tk()
        root.withdraw()
        width = int(root.winfo_screenwidth())
        height = int(root.winfo_screenheight())
        root.destroy()
        if width > 0 and height > 0:
            return width, height
    except Exception:
        pass

    return default_width, default_height


def _compute_square_window_slot(window_index=None, window_count=None):
    count = max(1, int(window_count or 1))
    index = max(0, min(int(window_index or 0), count - 1))

    cols = max(1, math.ceil(math.sqrt(count)))
    rows = max(1, math.ceil(count / cols))

    screen_width, screen_height = _get_primary_screen_size()
    cell = max(220, int(math.floor(min(screen_width / cols, screen_height / rows))))

    grid_width = cell * cols
    grid_height = cell * rows

    margin_x = max(0, (screen_width - grid_width) // 2)
    margin_y = max(0, (screen_height - grid_height) // 2)

    row = index // cols
    col = index % cols

    x = margin_x + (col * cell)
    y = margin_y + (row * cell)

    return {
        "x": int(x),
        "y": int(y),
        "size": int(cell),
        "rows": int(rows),
        "cols": int(cols),
        "count": int(count),
        "index": int(index),
        "screen_width": int(screen_width),
        "screen_height": int(screen_height),
    }


def _make_stealth_context(
    p,
    headless,
    proxy,
    accountname=None,
    *,
    tile_windows=False,
    window_index=None,
    window_count=None,
    suppressprint=False,
    log_callback=None,
):
    stealth = Stealth(
        navigator_languages_override=("en-US", "en"),
    )

    args = [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-infobars",
        "--disable-dev-shm-usage",
    ]

    if tile_windows and not headless:
        slot = _compute_square_window_slot(window_index=window_index, window_count=window_count)
        args.extend(
            [
                f"--window-size={slot['size']},{slot['size']}",
                f"--window-position={slot['x']},{slot['y']}",
            ]
        )
        _emit_log(
            (
                f"Window slot {slot['index'] + 1}/{slot['count']}: "
                f"{slot['size']}x{slot['size']} at ({slot['x']},{slot['y']}) "
                f"on {slot['screen_width']}x{slot['screen_height']}"
            ),
            suppressprint,
            log_callback,
        )

    launch_args = {
        "headless": headless,
        "proxy": proxy,
        "args": args,
    }

    try:
        # Prefer real Chrome channel when available (less bot-signaturey than stock Chromium).
        browser = p.chromium.launch(channel="chrome", **launch_args)
    except Exception:
        browser = p.chromium.launch(**launch_args)
    
    storage_state = None
    if accountname:
        cookie_path = _cookie_file(accountname)
        if os.path.exists(cookie_path):
            storage_state = cookie_path
            print(f"Loading cookies for account: {accountname}")

    context_kwargs = dict(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        timezone_id="America/New_York",
        storage_state=storage_state,
    )
    if tile_windows and not headless:
        # Keep page dimensions tied to OS window size for clean square tiling.
        context_kwargs["viewport"] = None
    else:
        context_kwargs["viewport"] = {"width": 1280, "height": 900}

    context = browser.new_context(**context_kwargs)

    stealth.apply_stealth_sync(context)
    return browser, context


def select_sound_from_favorites(page, sound_name, sim=None, stealth=False, suppressprint=False):
    """
    Selects a sound from the favorites tab by searching through the list or picking randomly.
    """
    try:
        if stealth:
            time.sleep(1)
        
        # Open Favorites Tab
        try:
            page.click('button:has-text("Favorites")')
        except Exception:
            try:
                page.click("button#favourite")
            except Exception:
                page.click("div.TUXTabBar-item#favourite button")

        time.sleep(1.5)
        
        # Wait for music cards to load
        page.wait_for_selector('div[class*="MusicPanelMusicItem__content"]', timeout=30000)
        time.sleep(1.5)

        music_cards = page.locator('div[class*="MusicPanelMusicItem__content"]')
        card_count = music_cards.count()

        # RANDOM SELECTION LOGIC
        if not sound_name or str(sound_name).lower() in ["none", "random"]:
            if card_count == 0:
                if not suppressprint: print("No favorites found to pick from.")
                return False
            
            # Pick from top 10 or total count if less
            import random as py_random
            target_idx = py_random.randint(0, min(card_count, 10) - 1)
            card = music_cards.nth(target_idx)
            
            try:
                title = card.locator('div[class*="Title"], div[class*="name-text"], div[class*="music-info"]').first.inner_text()
            except: title = "Random Favorite"
            
            if not suppressprint:
                print(f"🎲 Blank or 'random' sound name detected. Randomly picked: '{title}' index {target_idx}")

            if sim:
                sim.prepare_for_interaction(card)
                time.sleep(0.3)
                sim.click(card)
            else:
                card.hover()
                time.sleep(0.2)
                card.click()
            
            # Click the 'Use' or 'Plus' button
            try:
                # Target the plus button specifically
                plus_btn = card.locator('button:has-text("+"), [class*="plus"], [class*="add"]').last
                plus_btn.click(timeout=2000, force=True)
            except:
                try:
                    card.locator("button").last.click(timeout=1000)
                except:
                    card.click(force=True) # Fallback click on card itself
            return True

        keywords = str(sound_name).split()
        keywords_lower = [kw.lower() for kw in keywords if kw.strip()]

        if not suppressprint and len(keywords_lower) > 1:
            print(f"Searching for sounds containing all keywords: {keywords_lower}")

        found = False
        for i in range(card_count):
            try:
                card = music_cards.nth(i)
                title_element = card.locator('div[class*="Title"], div[class*="name-text"], div[class*="music-info"]').first
                title_text = title_element.inner_text() if title_element.count() > 0 else ""
                other_element = card.locator('div[class*="MusicPanelMusicItem__infoBasicDesc"]')
                other_text = other_element.inner_text() if other_element.count() > 0 else ""

                combined_text = f"{title_text} {other_text}".strip().lower()
                all_keywords_match = all(kw in combined_text for kw in keywords_lower)

                if all_keywords_match and combined_text:
                    display_title = title_text if title_text else "Unknown"
                    if not suppressprint:
                        print(f"Found matching sound: '{display_title} {other_text}'")

                    if stealth:
                        time.sleep(0.5)

                    if sim:
                        sim.prepare_for_interaction(card)
                        time.sleep(0.3)
                        sim.click(card)
                    else:
                        card.hover()
                        time.sleep(0.3)
                        card.click()

                    card.locator("button").last.click()
                    if stealth:
                        time.sleep(1)

                    found = True
                    break
            except Exception:
                continue

        return found

    except Exception as e:
        if not suppressprint:
            print(f"Error in favorites search: {e}")
        return False


def select_sound_from_search(page, sound_name, sim=None, stealth=False):
    """
    Selects a sound using the search functionality (original behavior).
    Uses SyncUserSimulator for human-like typing when available.
    """
    search_box = page.get_by_placeholder("Search sounds")
    if sim:
        sim.click(search_box)
        sim.type(search_box, sound_name)
    else:
        search_box.click()
        page.keyboard.type(sound_name)

    time.sleep(0.2)
    if stealth:
        time.sleep(2)
    page.keyboard.press("Enter")
    try:
        page.wait_for_selector("div[class*='MusicPanelMusicItem__operation']")
        if stealth:
            time.sleep(0.5)
        page.locator("div[class*='MusicPanelMusicItem__operation']").first.click()
        if stealth:
            time.sleep(1)
        return True
    except Exception:
        return False


def _cookie_file(accountname):
    filename = f"TK_cookies_{accountname}.json"
    default_path = os.path.join(PROJECT_ROOT, filename)
    alt_path = os.path.join(COOKIES_DIR, filename)

    # Prefer dedicated cookies folder when available.
    if os.path.exists(alt_path):
        return alt_path
    if os.path.exists(default_path):
        return default_path

    # If none exists yet, create/use the cookies folder target by default.
    return alt_path


def _load_or_create_cookies(accountname, proxy):
    cookie_read = False
    cookies_path = _cookie_file(accountname)
    cookie_dir = os.path.dirname(cookies_path)
    if cookie_dir:
        os.makedirs(cookie_dir, exist_ok=True)

    if os.path.exists(cookies_path):
        cookies, cookie_read = read_cookies(cookies_path=cookies_path)
        expired = check_expiry(accountname=accountname, cookies_path=cookies_path)
        if expired:
            os.remove(cookies_path)
            print(f"COOKIES EXPIRED FOR ACCOUNT {accountname}, PLEASE LOG-IN AGAIN")
            cookie_read = False

    if not cookie_read:
        install_js_dependencies()
        login_warning(accountname=accountname)
        run_javascript(proxy_data=proxy)
        os.replace(TEMP_COOKIE_EXPORT_PATH, cookies_path)
        cookies, cookie_read = read_cookies(cookies_path)
        if not cookie_read:
            raise TikTokUploadError("ERROR READING COOKIES")

    return cookies


def _goto_with_retry(page, url):
    retries = 0
    while retries < 2:
        try:
            page.goto(url, timeout=30000)
        except Exception:
            retries += 1
            time.sleep(5)
            if retries == 2:
                raise TikTokUploadError("ERROR: TIK TOK PAGE FAILED TO LOAD, try again.")
        else:
            break


def _is_login_screen(page):
    try:
        url = (page.url or "").lower()
        if "/login" in url or "passport" in url:
            return True
    except Exception:
        pass

    markers = [
        'h2:has-text("Log in to TikTok")',
        'button:has-text("Use QR code")',
        'button:has-text("Use phone / email / username")',
    ]
    for selector in markers:
        try:
            if page.locator(selector).first.is_visible(timeout=250):
                return True
        except Exception:
            continue
    return False


def _upload_input_present(page):
    selectors = [
        'input[type="file"][accept*="video"]',
        'input[type="file"][accept*="mp4"]',
        'input[type="file"]',
    ]
    for selector in selectors:
        try:
            if page.locator(selector).count() > 0:
                return True
        except Exception:
            continue
    return False


def _wait_for_upload_or_captcha(page):
    deadline = time.time() + 90
    while time.time() < deadline:
        _dismiss_overlays(page)

        if _is_login_screen(page):
            raise TikTokUploadError(
                "ERROR: TIKTOK LOGIN REQUIRED. Session cookies are missing or expired for this account."
            )

        try:
            if page.locator(CAPTCHA_QUESTION_SELECTOR).is_visible(timeout=250):
                return True
        except Exception:
            pass

        try:
            if page.locator(".upload-text-container").is_visible(timeout=250):
                return False
        except Exception:
            pass

        if _upload_input_present(page):
            return False

        time.sleep(0.2)

    raise TikTokUploadError(
        f"ERROR: Upload page did not become ready in time. Current URL: {getattr(page, 'url', 'unknown')}"
    )


def _solve_captcha_if_needed(page, suppressprint, log_callback=None):
    image = get_image_src(page)
    if not image:
        return

    _emit_log("CAPTCHA DETECTED, Attempting to solve", suppressprint, log_callback)

    solved = False
    attempts = 0
    old_question = "N.A"
    question = page.locator(CAPTCHA_QUESTION_SELECTOR).text_content()

    while not solved:
        attempts += 1
        start_time = time.time()
        while question == old_question:
            question = page.locator(CAPTCHA_QUESTION_SELECTOR).text_content()
            if time.time() - start_time > 2:
                break

        if "Select 2 objects that are the same" in question or "Select two objects that are the same" in question:
            found = False
            while not found:
                page.click(CAPTCHA_REFRESH_SELECTOR)
                image = get_image_src(page)
                img_path = download_image(image)
                b_box, found = run_inference_on_image(image_path=img_path)

            with Image.open(img_path) as img:
                image_size = img.size

            imageweb = page.locator("#captcha-verify-image")
            imageweb.wait_for()
            box = imageweb.bounding_box()
            image_x = box["x"]
            image_y = box["y"]
            image_height_web = box["height"]
            image_width_web = box["width"]
            image_width_real, image_height_real = image_size

            webpage_coords = convert_to_webpage_coordinates(
                b_box,
                image_x,
                image_y,
                image_height_web,
                image_width_web,
                image_height_real,
                image_width_real,
            )
            if not webpage_coords:
                webpage_coords.append((image_x + 50, image_y + 50))

            click_on_objects(page, webpage_coords)
            page.click(CAPTCHA_SUBMIT_SELECTOR)
            time.sleep(0.5)

            if attempts > 5:
                raise TikTokUploadError("FAILED TO SOLVE CAPTCHA")

            showedup = False
            while not showedup:
                if page.locator(CAPTCHA_SUCCESS_SELECTOR).is_visible():
                    solved = True
                    showedup = True
                    os.remove("captcha_image.jpg")
                if page.locator(CAPTCHA_FAIL_SELECTOR).is_visible():
                    showedup = True
                    old_question = question
                    page.click(CAPTCHA_REFRESH_SELECTOR)
        else:
            objectclick = understood_Qs(question)
            while objectclick == "N.A":
                old_question = question
                page.click(CAPTCHA_REFRESH_SELECTOR)
                start_time = time.time()
                runs = 0
                while question == old_question:
                    runs += 1
                    question = page.locator(CAPTCHA_QUESTION_SELECTOR).text_content()
                    if runs > 1:
                        time.sleep(1)
                    if time.time() - start_time > 2:
                        break
                objectclick = understood_Qs(question)

            image = get_image_src(page)
            img_path = download_image(image)
            b_box = run_inference_on_image_tougher(image_path=img_path, object=objectclick)

            with Image.open(img_path) as img:
                image_size = img.size

            imageweb = page.locator("#captcha-verify-image")
            imageweb.wait_for()
            box = imageweb.bounding_box()
            image_x = box["x"]
            image_y = box["y"]
            image_height_web = box["height"]
            image_width_web = box["width"]
            image_width_real, image_height_real = image_size

            webpage_coords = convert_to_webpage_coordinates(
                b_box,
                image_x,
                image_y,
                image_height_web,
                image_width_web,
                image_height_real,
                image_width_real,
            )
            if not webpage_coords:
                webpage_coords.append((image_x + 50, image_y + 50))

            click_on_objects(page, webpage_coords)
            page.click(CAPTCHA_SUBMIT_SELECTOR)
            time.sleep(1)

            if attempts > 20:
                raise TikTokUploadError("FAILED TO SOLVE CAPTCHA")

            showedup = False
            while not showedup:
                if page.locator(CAPTCHA_SUCCESS_SELECTOR).is_visible():
                    solved = True
                    showedup = True
                    os.remove("captcha_image.jpg")
                    _emit_log("CAPTCHA SOLVED", suppressprint, log_callback)
                if page.locator(CAPTCHA_FAIL_SELECTOR).is_visible():
                    showedup = True
                    old_question = question
                    page.click(CAPTCHA_REFRESH_SELECTOR)


def _set_video_input(page, video):
    video_path = os.path.abspath(str(video))
    if not os.path.isfile(video_path):
        raise TikTokUploadError(f"ERROR: VIDEO FILE NOT FOUND: {video_path}")

    _dismiss_overlays(page)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
    except Exception:
        pass

    if _is_login_screen(page):
        raise TikTokUploadError(
            "ERROR: TIKTOK LOGIN REQUIRED before setting file input. Re-authenticate this account first."
        )

    candidates = [
        'input[type="file"][accept*="video"]',
        'input[type="file"][accept*="mp4"]',
        'input[type="file"]',
    ]
    last_error = None

    for selector in candidates:
        try:
            inputs = page.locator(selector)
            count = inputs.count()
        except Exception as e:
            last_error = e
            continue

        if count == 0:
            continue

        for idx in range(min(count, 6)):
            input_loc = inputs.nth(idx)
            try:
                accept_attr = (input_loc.get_attribute("accept") or "").lower()
                if selector == 'input[type="file"]' and accept_attr and "video" not in accept_attr:
                    continue
                input_loc.set_input_files(video_path)
                return
            except Exception as e:
                last_error = e
                continue

    try:
        page.locator('input[type="file"]').first.set_input_files(video_path)
        return
    except Exception as e:
        last_error = e

    if _is_login_screen(page):
        raise TikTokUploadError(
            "ERROR: SESSION REDIRECTED TO LOGIN while trying to upload file. Cookies likely expired."
        )

    raise TikTokUploadError(
        f"ERROR: FAILED TO INPUT FILE. File='{video_path}', URL='{getattr(page, 'url', 'unknown')}'. "
        "Possible causes: invalid session, TikTok DOM change, blocked upload UI, or bad file path."
    ) from last_error


def _add_description_and_hashtags(page, sim, video, description, hashtags, stealth, suppressprint, log_callback=None):
    _dismiss_overlays(page)
    page.wait_for_selector('div[data-contents="true"]')

    time.sleep(0.5)
    _dismiss_overlays(page)

    desc_box = page.locator('div[data-contents="true"]')
    if sim:
        sim.click(desc_box)
    else:
        desc_box.click()

    _emit_log(
        "Entered file, waiting for TikTok to load it onto their server. This may take a couple of minutes depending on video length.",
        suppressprint,
        log_callback,
    )

    time.sleep(0.5)
    if description is None:
        raise TikTokUploadError("ERROR: PLEASE INCLUDE A DESCRIPTION")

    for _ in range(len(video) + 2):
        page.keyboard.press("Backspace")
        page.keyboard.press("Delete")

    time.sleep(0.5)
    if sim:
        sim.type(desc_box, description)
    else:
        page.keyboard.type(description)

    if hashtags is not None:
        for hashtag in hashtags:
            if hashtag[0] != "#":
                hashtag = "#" + hashtag

            page.keyboard.type(hashtag)
            time.sleep(0.5)
            try:
                if stealth:
                    time.sleep(2)
                page.click(f'span.hash-tag-topic:has-text("{hashtag}")', timeout=1000)
            except Exception:
                try:
                    page.click("span.hash-tag-topic", timeout=1000)
                except Exception:
                    page.keyboard.press("Backspace")
                    try:
                        page.click("span.hash-tag-topic", timeout=1000)
                    except Exception:
                        _emit_log(f"Tik tok hashtag not working for {hashtag}, moving onto next", suppressprint, log_callback)
                        page.keyboard.type(f"{hashtag[-1]} ")

    _emit_log("Description and Hashtags added", suppressprint, log_callback)


def _wait_for_upload_ready(page):
    deadline = time.time() + (20 * 60)

    while time.time() < deadline:
        _dismiss_overlays(page)

        try:
            cancel_prompt = page.locator('div:has-text("Sure you want to cancel your upload?")')
            if cancel_prompt.first.is_visible(timeout=200):
                no_btn = page.locator('button:has-text("No")')
                if no_btn.first.is_visible(timeout=200):
                    no_btn.first.click(force=True)
        except Exception:
            pass

        if _is_login_screen(page):
            raise TikTokUploadError(
                "ERROR: SESSION EXPIRED DURING FILE PROCESSING. TikTok redirected to login."
            )

        ready_selectors = [
            'button:has-text("Post")[aria-disabled="false"]',
            'button:has-text("Post"):not([aria-disabled="true"])',
            'button:has-text("Post"):not([disabled])',
        ]
        for selector in ready_selectors:
            try:
                if page.locator(selector).first.is_visible(timeout=300):
                    return
            except Exception:
                continue

        time.sleep(0.5)

    raise TikTokUploadError(
        "ERROR: TIK TOK TOOK TOO LONG TO UPLOAD YOUR FILE (>20min). Try again, if issue persists then try a lower file size or different wifi connection"
    )


def _validate_schedule_request(schedule, day):
    if (schedule is None) and (day is not None):
        raise TikTokUploadError(
            "ERROR: CANT SCHEDULE FOR ANOTHER DAY USING 'day' WITHOUT ALSO INCLUDING TIME OF UPLOAD WITH 'schedule'; PLEASE ALSO INCLUDE TIME WITH 'schedule' PARAMETER"
        )


def _normalize_schedule_and_day(schedule, day):
    # Backward-compatible normalization for callers that pass day number via
    # `schedule` and time string via `day` (e.g. schedule=25, day="12:05").
    if isinstance(schedule, int) and isinstance(day, str) and ":" in day:
        return day, str(schedule)
    return schedule, day


def _apply_schedule(page, schedule, day, stealth, suppressprint, log_callback=None):
    if schedule is None:
        return

    try:
        hour = schedule[0:2]
        minute = schedule[3:]
        if (int(minute) % 5) != 0:
            raise TikTokUploadError(
                "MINUTE FORMAT ERROR: PLEASE MAKE SURE MINUTE YOU SCHEDULE AT IS A MULTIPLE OF 5 UNTIL 60 (i.e: 40), VIDEO SAVED AS DRAFT"
            )
    except Exception:
        raise TikTokUploadError(
            "SCHEDULE TIME ERROR: PLEASE MAKE SURE YOUR SCHEDULE TIME IS A STRING THAT FOLLOWS THE 24H FORMAT 'HH:MM', VIDEO SAVED AS DRAFT"
        )

    page.locator('label:has-text("Schedule")').click()
    if stealth:
        time.sleep(2)

    visible = False
    while not visible:
        if page.locator('button:has-text("Allow")').nth(0).is_visible():
            if stealth:
                time.sleep(1)
            page.locator('button:has-text("Allow")').nth(0).click()
            visible = True
            time.sleep(0.1)
        else:
            if page.locator("div.TUXTextInputCore-trailingIconWrapper").nth(1).is_visible():
                visible = True
                time.sleep(0.1)

    if day is not None:
        if stealth:
            time.sleep(1)
        page.locator(SCHEDULE_DAY_ICON_SELECTOR).click()
        time.sleep(0.2)
        try:
            if stealth:
                time.sleep(1)
            page.locator(f'span.day.valid:text-is("{day}")').click()
        except Exception:
            raise TikTokUploadError(
                "SCHEDULE DAY ERROR: ERROR WITH SCHEDULED DAY, read documentation for more information on format of day"
            )

    try:
        time.sleep(0.2)
        page.locator(SCHEDULE_TIME_ICON_SELECTOR).click()
        time.sleep(0.2)
        page.locator(
            f'.tiktok-timepicker-option-text.tiktok-timepicker-right:text-is("{minute}")'
        ).scroll_into_view_if_needed()
        time.sleep(0.2)
        if stealth:
            time.sleep(2)
        page.locator(
            f'.tiktok-timepicker-option-text.tiktok-timepicker-right:text-is("{minute}")'
        ).click()
        time.sleep(0.2)
        if page.locator("div.tiktok-timepicker-time-picker-container").is_visible():
            time.sleep(0.1)
        else:
            page.locator(SCHEDULE_TIME_ICON_SELECTOR).click()
        page.locator(
            f'.tiktok-timepicker-option-text.tiktok-timepicker-left:text-is("{hour}")'
        ).scroll_into_view_if_needed()
        if stealth:
            time.sleep(2)
        page.locator(
            f'.tiktok-timepicker-option-text.tiktok-timepicker-left:text-is("{hour}")'
        ).click()
        time.sleep(1)
        _emit_log("Done scheduling video", suppressprint, log_callback)
    except Exception:
        raise TikTokUploadError("SCHEDULING ERROR: VIDEO SAVED AS DRAFT")


def _adjust_sound_volume_upload(page, sound_aud_vol, stealth):
    page.wait_for_selector(SOUND_VOLUME_ICON_WAIT_SELECTOR)
    if stealth:
        time.sleep(1)

    page.click(SOUND_VOLUME_ICON_CLICK_UPLOAD_SELECTOR)
    time.sleep(0.5)
    sliders = page.locator("input.scaleInput")

    if sound_aud_vol == "background":
        slider2 = sliders.nth(1)
        bounding_box2 = slider2.bounding_box()
        if bounding_box2:
            x2 = bounding_box2["x"] + (bounding_box2["width"] * 0.07)
            y2 = bounding_box2["y"] + bounding_box2["height"] / 2
            if stealth:
                time.sleep(1)
            page.mouse.click(x2, y2)

    if sound_aud_vol == "main":
        slider1 = sliders.nth(0)
        bounding_box1 = slider1.bounding_box()
        if bounding_box1:
            x1 = bounding_box1["x"] + (bounding_box1["width"] * 0.07)
            y1 = bounding_box1["y"] + bounding_box1["height"] / 2
            if stealth:
                time.sleep(1)
            page.mouse.click(x1, y1)

    time.sleep(1)


def _pick_sound(page, sound_name, sim, stealth, suppressprint, search_mode):
    sound_found = False
    if search_mode == "favorites":
        sound_found = select_sound_from_favorites(
            page,
            sound_name,
            sim=sim,
            stealth=stealth,
            suppressprint=suppressprint,
        )
    else:
        sound_found = select_sound_from_search(page, sound_name, sim=sim, stealth=stealth)

    if not sound_found:
        raise TikTokUploadError(f"ERROR: SOUND '{sound_name}' NOT FOUND")


def _add_sound_from_upload_page(page, sound_name, sound_aud_vol, sim, stealth, suppressprint, search_mode, log_callback=None):
    sound_fail = False
    if sound_name is None:
        return sound_fail

    try:
        if stealth:
            time.sleep(2)
        sounds_btn = page.locator("button:has-text('Sounds')").last
        sim.click(sounds_btn)
    except Exception:
        sound_fail = True

    if sound_fail:
        return sound_fail

    time.sleep(1.5)
    _pick_sound(page, sound_name, sim, stealth, suppressprint, search_mode)

    if sound_aud_vol != "mix":
        try:
            _adjust_sound_volume_upload(page, sound_aud_vol, stealth)
        except Exception:
            raise TikTokUploadError("ERROR ADJUSTING SOUND VOLUME: please try again or use the default 'mix'.")

    page.wait_for_selector("button:has-text('Save')")
    if stealth:
        time.sleep(1)
    page.locator("button:has-text('Save')").first.click()

    _emit_log("Added sound", suppressprint, log_callback)

    return sound_fail


def _run_upload_copyright_check(page, stealth, suppressprint, log_callback=None):
    copy_check_counter = 0
    if stealth:
        time.sleep(1)

    page.locator('div[data-e2e="copyright_container"] span[data-part="thumb"]').click()
    while True:
        time.sleep(2)
        if page.get_by_text("No issues found.", exact=True).is_visible():
            _emit_log("Copyright check complete", suppressprint, log_callback)
            break
        if page.locator("span:has-text('Copyright issues detected')").is_visible():
            raise TikTokUploadError("COPYRIGHT CHECK FAILED: VIDEO SAVED AS DRAFT, COPYRIGHT AUDIO DETECTED FROM TIKTOK")

        copy_check_counter += 1
        if copy_check_counter > 10:
            _emit_log(
                "COPYRIGHT CHECK TIMEOUT: UNABLE TO CONFIRM IF VIDEO PASSED COPYRIGHT CHECK, CONTINUING TO UPLOAD IN 5 SECONDS.",
                suppressprint,
                log_callback,
            )
            break

def _submit_upload(page, schedule, stealth, suppressprint, post_success_wait, schedule_success_wait, log_callback=None):
    # Look for various success indicators
    success_indicators = [
        ':has-text("Leaving the page does not interrupt")',
        ':has-text("Manage posts")',
        ':has-text("Upload another video")',
        'button:has-text("Manage posts")',
        'div:has-text("Video has been uploaded")'
    ]
    try:
        if schedule is None:
            if stealth:
                time.sleep(1)
            try:
                page.click('button:has-text("Post")[data-e2e="post_video_button"]', timeout=3000)
            except Exception:
                try:
                    page.click('button:has-text("Post")[aria-disabled="false"]', timeout=3000)
                except Exception:
                    pass
            
            # Handle "Continue to post?" modal (Copyright check still in progress)
            try:
                post_now_btn = page.locator('button:has-text("Post now")')
                if post_now_btn.is_visible(timeout=2000):
                    _emit_log("Confirming upload via 'Post now' modal...", suppressprint, log_callback)
                    post_now_btn.click(timeout=2000, force=True)
            except Exception:
                pass

            try:
                page.wait_for_url(url=CONTENT_URL, timeout=2000)
            except Exception:
                pass

            uploaded = False
            checks = 0
            
            while not uploaded and checks < 50: # Increase check time
                for indicator in success_indicators:
                    if page.locator(indicator).first.is_visible():
                        uploaded = True
                        _emit_log(f"Success detected via: {indicator}", suppressprint, log_callback)
                        break
                if uploaded:
                    time.sleep(post_success_wait)
                    break
                time.sleep(0.5)
                checks += 1
        else:
            if stealth:
                time.sleep(1)
            page.click('button:has-text("Schedule")', timeout=10000)

            uploaded = False
            checks = 0
            while not uploaded and checks < 50:
                for indicator in success_indicators:
                    if page.locator(indicator).first.is_visible():
                        uploaded = True
                        break
                if uploaded:
                    time.sleep(schedule_success_wait)
                    break
                time.sleep(0.5)
                checks += 1

        _emit_log("Done uploading video, NOTE: it may take a minute or two to show on TikTok", suppressprint, log_callback)
    except Exception as e:
        _emit_log(f"Post submission error: {e}", suppressprint, log_callback)
        time.sleep(2)
        raise TikTokUploadError(
            "POSSIBLE ERROR UPLOADING: Cannot confirm if uploaded successfully, Please check account in a minute or two to confirm."
        )

    time.sleep(1)
    page.close()
    return None


def _select_cover_last_frame(page) -> bool:
    """
    Open TikTok Studio's cover editor and drag the frame slider to the last frame.

    The caller must ensure the desired cover image is already the last frame
    of the MP4 (baked in at encode time).

    Returns True on success, False on failure (upload proceeds without custom cover).
    """
    # Step 1: Open the cover editor modal
    try:
        edit_btn = page.locator('[data-e2e="cover_container"] div.edit-container')
        if not edit_btn.is_visible(timeout=5000):
            edit_btn = page.locator('div.edit-container:has-text("Edit cover")')
            if not edit_btn.is_visible(timeout=3000):
                return False
        edit_btn.click()
    except Exception:
        return False

    # Step 2: Wait for the frame slider
    try:
        page.wait_for_selector('div.drag-item', timeout=8000)
        time.sleep(1)
    except Exception:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return False

    # Step 3: Drag the slider to the far right (last frame)
    try:
        drag_item = page.locator('div.drag-item')
        container = drag_item.locator('..')
        container_box = container.bounding_box()
        drag_box = drag_item.bounding_box()

        if not container_box or not drag_box:
            return False

        target_x = container_box['x'] + container_box['width'] - 4
        current_x = drag_box['x'] + drag_box['width'] / 2
        current_y = drag_box['y'] + drag_box['height'] / 2

        page.mouse.move(current_x, current_y)
        page.mouse.down()
        # Move in steps — TikTok ignores instant jumps
        for i in range(1, 11):
            page.mouse.move(current_x + (target_x - current_x) * i / 10, current_y)
            time.sleep(0.05)
        page.mouse.up()
        time.sleep(1)
    except Exception:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return False

    # Step 4: Confirm
    try:
        confirm_btn = page.locator('button:has-text("Confirm")').first
        confirm_btn.scroll_into_view_if_needed()
        time.sleep(0.3)
        try:
            confirm_btn.click(timeout=5000)
        except Exception:
            confirm_btn.evaluate("el => el.click()")
        time.sleep(1.5)

        # Wait for modal to close
        try:
            page.wait_for_selector('div.drag-item', state="hidden", timeout=5000)
        except Exception:
            pass

        return True
    except Exception:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return False


def upload_tiktok(
    video: str,
    description: str,
    accountname: str,
    *,
    cover_image=None,
    hashtags=None,
    sound_name=None,
    sound_aud_vol: str = "mix",
    schedule=None,
    day=None,
    copyrightcheck: bool = False,
    suppressprint: bool = False,
    headless: bool = True,
    stealth: bool = False,
    proxy=None,
    search_mode: str = "search",
    log_callback=None,
    tile_windows: bool = False,
    window_index=None,
    window_count=None,
) -> str:
    """
    UPLOADS VIDEO TO TIKTOK (powered by Phantomwright for bot-detection evasion)
    --------------------------------------------------------------------------------
    video (str) -> path to video to upload
    description (str) -> description for video
    accountname (str) -> account to upload on
    cover_image (str or Path, optional) ->
        Path to a PNG/JPG to use as the video cover. The image must already be
        baked into the last frame of the video (see notes below). When provided,
        the upload flow will open TikTok's cover editor and drag the frame slider
        to the last frame before posting.

        Note: TikTok's "Upload cover" tab silently discards uploaded images
        server-side. This parameter instead uses the native cover editor to select
        the last frame of the video, which reliably sticks.
    hashtags (str)(array)(opt) -> hashtags for video
    sound_name (str)(opt) -> name of tik tok sound to use for video
    sound_aud_vol (str)(opt) -> volume of tik tok sound, 'main', 'mix' or 'background'
    schedule (str)(opt) -> format HH:MM, your local time to upload video
    day (int)(opt) -> day to schedule video for
    copyrightcheck (bool)(opt) -> include copyright check or not
    suppressprint (bool)(opt) -> True means function doesnt print anything
    headless (bool)(opt) -> run in headless mode or not
    stealth (bool)(opt) -> will wait second(s) before each operation
    proxy (dict)(opt) -> proxy server to run code on
    search_mode (str)(opt) -> 'search' or 'favorites'
    tile_windows (bool)(opt) -> place headed browser in auto square grid slots
    window_index (int)(opt) -> 0-based browser slot index for tiling
    window_count (int)(opt) -> total browser windows in this launch set
    """
    try:
        check_for_updates()
    except Exception:
        time.sleep(0.1)

    try:
        validate_proxy(proxy)
    except Exception as e:
        raise TikTokUploadError(f"Error validating proxy: {e}")

    if accountname is None:
        raise TikTokUploadError("PLEASE ENTER NAME OF ACCOUNT TO POST ON, READ DOCUMENTATION FOR MORE INFO")

    cookies = _load_or_create_cookies(accountname, proxy)

    with sync_playwright() as p:
        _, context = _make_stealth_context(
            p,
            headless=headless,
            proxy=proxy,
            accountname=accountname,
            tile_windows=tile_windows,
            window_index=window_index,
            window_count=window_count,
            suppressprint=suppressprint,
            log_callback=log_callback,
        )
        context.add_cookies(cookies)
        page = context.new_page()

        sim = SyncUserSimulator(page)

        _emit_log(f"Uploading to account '{accountname}'", suppressprint, log_callback)

        _goto_with_retry(page, UPLOAD_URL)
        sim.simulate_browsing(duration_ms=1500)

        captcha = _wait_for_upload_or_captcha(page)
        if captcha:
            _solve_captcha_if_needed(page, suppressprint, log_callback)

        _set_video_input(page, video)
        _add_description_and_hashtags(page, sim, video, description, hashtags, stealth, suppressprint, log_callback)
        _wait_for_upload_ready(page)

        time.sleep(0.2)
        _emit_log("Tik tok done loading file onto servers", suppressprint, log_callback)

        sim.simulate_browsing(duration_ms=1000)

        schedule, day = _normalize_schedule_and_day(schedule, day)
        _validate_schedule_request(schedule, day)
        _apply_schedule(page, schedule, day, stealth, suppressprint, log_callback)

        sound_fail = _add_sound_from_upload_page(
            page,
            sound_name,
            sound_aud_vol,
            sim,
            stealth,
            suppressprint,
            search_mode,
            log_callback,
        )

        if not sound_fail:
            page.wait_for_selector('div[data-contents="true"]')

            if copyrightcheck:
                _run_upload_copyright_check(page, stealth, suppressprint, log_callback)

            if cover_image:
                _select_cover_last_frame(page)
                time.sleep(0.5)

            result = _submit_upload(
                page,
                schedule,
                stealth,
                suppressprint,
                post_success_wait=0.1,
                schedule_success_wait=0.2,
                log_callback=log_callback,
            )
            if result == "Error":
                return "Error"
        else:
            try:
                if stealth:
                    time.sleep(1)
                page.click('button:has-text("Save draft")', timeout=10000)
                raise TikTokUploadError("ERROR ADDING SOUND: Video saved as draft, please try again or check documentation for more info")
                return "Error"
            except Exception:
                raise TikTokUploadError("ERROR ADDING SOUND; SAVE AS DRAFT BUTTON NOT FOUND SO VIDEO NOT ADDED AS DRAFT")
                return "Error"


    return "Completed"


import concurrent.futures

# Global stop signal
FORCE_STOP = False

def stop_all_uploads():
    global FORCE_STOP
    FORCE_STOP = True

def reset_stop_signal():
    global FORCE_STOP
    FORCE_STOP = False

def upload_tiktok_multi(
    video_list,
    accountname,
    hashtags=None,
    headless=False,
    stealth=False,
    suppressprint=False,
    schedule=None,
    day=None,
    sound_name=None,
    sound_aud_vol="mix",
    search_mode="favorites",
    proxy=None,
    copyrightcheck=True,
    cover_image=False,
    log_callback=None,
):
    """
    Uploads multiple videos sequentially using tabs in a SINGLE browser session.
    """
    from playwright.sync_api import sync_playwright

    results = []
    def _log(msg):
        if log_callback: log_callback(msg)
        if not suppressprint: print(msg)

    with sync_playwright() as p:
        _log("🔧 Initializing browser engine...")
        browser, context = _make_stealth_context(
            p,
            headless,
            proxy,
            accountname,
            suppressprint=suppressprint,
            log_callback=log_callback,
        )
        cookies = _load_or_create_cookies(accountname, proxy)
        context.add_cookies(cookies)
        _log(f"🍪 Cookies loaded for @{accountname} from {_cookie_file(accountname)}")
        
        for idx, video_item in enumerate(video_list):
            if FORCE_STOP:
                _log("⏹️ STOPPED: Batch interrupted by user.")
                results.append("⏹️ STOPPED: User interrupt.")
                break
            
            _log(f"📂 Tab {idx+1}/{len(video_list)}: Opening {os.path.basename(video_item['video'])}")
            page = context.new_page()
            try:
                if not suppressprint:
                    print(f"Tab {idx+1}: Processing {video_item['video']}")

                page.set_default_timeout(60000)
                page.goto(UPLOAD_URL)
                _dismiss_overlays(page)
                captcha = _wait_for_upload_or_captcha(page)
                if captcha:
                    _log(f"Tab {idx+1}: CAPTCHA detected, attempting solve...")
                    _solve_captcha_if_needed(page, suppressprint, log_callback)
                _set_video_input(page, video_item["video"])

                sim = None
                if stealth:
                    from phantomwright.user_simulator import SyncUserSimulator
                    sim = SyncUserSimulator(page)

                # Determine per-video settings, falling back to global defaults
                description_text = video_item.get("desc", "")
                hashtags_list = video_item.get("hashtags", hashtags)
                schedule_time = video_item.get("schedule", schedule)
                schedule_day = video_item.get("day", day)
                selected_sound = video_item.get("sound_name", sound_name)
                sound_volume = video_item.get("sound_aud_vol", sound_aud_vol)

                _log(f"✍️ Tab {idx+1}: Adding description & hashtags...")
                _add_description_and_hashtags(
                    page,
                    sim,
                    video_item["video"],
                    description_text,
                    hashtags_list,
                    stealth,
                    suppressprint,
                    log_callback,
                )
                _wait_for_upload_ready(page)
                
                if schedule_time and schedule_day:
                    _log(f"⏰ Tab {idx+1}: Setting schedule for Day {schedule_day} at {schedule_time}")
                    _apply_schedule(page, schedule_time, schedule_day, stealth, suppressprint)

                _log(f"🎵 Tab {idx+1}: Searching/Applying sound '{selected_sound}'")
                _add_sound_from_upload_page(
                    page,
                    selected_sound,
                    sound_volume,
                    sim,
                    stealth,
                    suppressprint,
                    search_mode,
                    log_callback,
                )
                
                if copyrightcheck:
                    _log(f"🛡️ Tab {idx+1}: Running copyright check...")
                    _run_upload_copyright_check(page, stealth, suppressprint, log_callback)

                _log(f"🚀 Tab {idx+1}: Finalizing upload for {os.path.basename(video_item['video'])}")
                result = _submit_upload(
                    page,
                    schedule_time if schedule_time and schedule_day else None,
                    stealth,
                    suppressprint,
                    0.1,
                    0.2,
                )
                
                success_msg = f"✅ SUCCESS: {os.path.basename(video_item['video'])} uploaded!"
                _log(success_msg)
                results.append(success_msg)
                page.close()
            except Exception as e:
                err_msg = f"❌ FAILED: Tab {idx+1} | {str(e)}"
                _log(err_msg)
                results.append(err_msg)
                try: page.close()
                except: pass
            
        browser.close()
    return results
