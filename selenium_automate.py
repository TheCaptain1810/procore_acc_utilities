from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def create_driver_options(debug_mode=False):
    """Create and configure Edge options"""
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    
    if debug_mode:
        options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    else:
        # Add additional security and performance options
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--ignore-certificate-errors")
        options.page_load_strategy = 'normal'
    
    return options

def connect_to_existing_edge(driver_path):
    """Connect to an existing Edge instance"""
    options = create_driver_options(debug_mode=True)
    service = Service(driver_path)
    return webdriver.Edge(service=service, options=options)

def upload_photo_to_acc(input_text, driver_path):
    """Upload and edit photo description in ACC"""
    options = create_driver_options()
    service = Service(driver_path)
    driver = webdriver.Edge(service=service, options=options)
    wait = WebDriverWait(driver, 10)  # 10 second timeout

    try:
        # Step 1: Open the URL
        url = "https://acc.autodesk.com/build/photos/projects/fe3cafee-bfb9-41a7-a199-045a926ed74c/photos"
        driver.get(url)

        # Step 2: Wait for and click the first MediaCard
        media_card = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-qa="Shared.MediaCard"]'))
        )
        media_card.click()

        # Step 3: Wait for and click the edit button
        edit_button = wait.until(
            EC.element_to_be_clickable((By.CLASS_NAME, "cPNuUt"))
        )
        edit_button.click()

        # Step 4: Type the input content
        input_field = driver.switch_to.active_element
        input_field.clear()
        input_field.send_keys(input_text)
        input_field.send_keys(Keys.ENTER)

        # Step 5: Wait for and click the close icon
        close_icon = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="navigator"]/div/div[1]/div/div/svg[2]')
            )
        )
        close_icon.click()

        print("✅ Photo upload process completed successfully.")

    except Exception as e:
        print(f"❌ Error during photo upload: {str(e)}")
        raise

    finally:
        driver.quit()

if __name__ == "__main__":
    DRIVER_PATH = "C:/Users/Vedant.desai/Downloads/edgedriver_win64/msedgedriver.exe"
    upload_photo_to_acc("TestPhotoDesc", DRIVER_PATH)