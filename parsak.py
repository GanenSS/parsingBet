import os
import time
import random
import traceback
import logging
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, WebDriverException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()])
logger = logging.getLogger("FonbetScraper")

# Изменим словарь на английские названия
SPORT_IDS = {
    "Football": 1, "Hockey": 2, "Tennis": 3, "Basketball": 4, "Volleyball": 5, "Table Tennis": 6,
    "Esports": 7, "MMA": 8, "Boxing": 9, "Handball": 10, "Squash": 11, "Water Polo": 12,
    "Futsal": 13, "Badminton": 14, "Beach Volleyball": 15, "Beach Football": 16, "Field Hockey": 17,
    "Rugby": 18, "Floorball": 19, "Sports": 20, "American Football": 21, "Baseball": 22, "Cricket": 23,
    "Lacrosse": 24, "Racing": 25, "Australian Football": 26, "Gaelic Football": 27, "Netball": 28,
    "Billiard": 29, "Softball": 30, "Curling": 31, "Darts": 33, "Cycling": 34, "Chess": 35
}

# Словарь для сопоставления URL-частей с английскими названиями спорта
SPORT_URL_TO_NAME = {
    "football": "Football",
    "hockey": "Hockey",
    "tennis": "Tennis",
    "basketball": "Basketball",
    "volleyball": "Volleyball",
    "table-tennis": "Table Tennis",
    "esports": "Esports",
    "mix-fights": "MMA",
    "boxing": "Boxing",
    "handball": "Handball",
    "squash": "Squash",
    "waterpolo": "Water Polo",
    "futsal": "Futsal",
    "badminton": "Badminton",
    "beach-volley": "Beach Volleyball",
    "beach-football": "Beach Football",
    "field-hockey": "Field Hockey",
    "rugby": "Rugby",
    "floorball": "Floorball",
    "sport-2": "Sports",
    "am-football": "American Football",
    "baseball": "Baseball",
    "cricket": "Cricket",
    "lacrosse": "Lacrosse",
    "racing": "Racing",
    "australian-football": "Australian Football",
    "gaelic-football": "Gaelic Football",
    "netball": "Netball",
    "billiard": "Billiard",
    "softball": "Softball",
    "curling": "Curling",
    "darts": "Darts",
    "cycling": "Cycling",
    "chess": "Chess"
}

# Словарь для перевода русских названий на английский (для полей данных)
RU_TO_EN_FIELDS = {
    "ФОРА 1": "HANDICAP 1",
    "ФОРА 2": "HANDICAP 2",
    "Тотал": "TOTAL",
    "Б": "OVER",
    "М": "UNDER"
}

class FonbetScraper:
    def __init__(self):
        self.base_url = "https://fon.bet/sports/"
        # Обновленные ссылки на виды спорта
        self.sports = [
            "football", "hockey", "tennis", "basketball", "volleyball",
            "table-tennis", "esports", "mix-fights", "boxing", "handball",
            "squash", "waterpolo", "futsal", "beach-football", "rugby",
            "floorball", "sport-2", "am-football", "baseball", "cricket",
            "lacrosse", "racing", "australian-football", "gaelic-football",
            "billiard", "softball", "darts"
        ]
        # Глобальные счетчики для уникальных ID
        self.global_championship_id = 10000  # Начинаем с большого числа для наглядности
        self.global_match_id = 100000  # Начинаем с большого числа для наглядности
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        try:
            logger.info("Setting up Chrome WebDriver...")
            options = Options()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-infobars")
            options.add_argument("--disable-notifications")
            options.add_argument("--disable-popup-blocking")
            options.add_argument("--disable-browser-side-navigation")
            options.add_argument("--dns-prefetch-disable")
            options.add_argument("--remote-debugging-port=9222")
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
            options.page_load_strategy = 'normal'
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(60)
            self.driver.set_script_timeout(60)
            self.wait = WebDriverWait(self.driver, 30)
            logger.info("Chrome WebDriver setup completed successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to setup Chrome WebDriver: {e}")
            logger.error(traceback.format_exc())
            return False

    def restart_driver(self):
        logger.info("Restarting driver...")
        try:
            if self.driver:
                try:
                    self.driver.close()
                    logger.info("Browser window closed")
                except Exception as close_e:
                    logger.warning(f"Error closing browser window: {close_e}")
                try:
                    self.driver.quit()
                    logger.info("WebDriver quit successfully")
                except Exception as quit_e:
                    logger.warning(f"Error quitting WebDriver: {quit_e}")
        except Exception as e:
            logger.error(f"Error during driver cleanup: {e}")
        finally:
            time.sleep(5)
            setup_attempts = 0
            max_setup_attempts = 3
            while setup_attempts < max_setup_attempts:
                logger.info(f"Setting up new WebDriver (attempt {setup_attempts+1}/{max_setup_attempts})")
                if self.setup_driver():
                    logger.info("WebDriver restarted successfully")
                    break
                setup_attempts += 1
                time.sleep(5)
            if setup_attempts >= max_setup_attempts:
                logger.error("Failed to restart WebDriver after multiple attempts")

    def is_driver_alive(self):
        try:
            current_url = self.driver.current_url
            return True
        except Exception:
            return False

    def safe_find_elements(self, by, value, max_retries=3):
        for attempt in range(max_retries):
            try:
                elements = self.driver.find_elements(by, value)
                return elements
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error finding elements (attempt {attempt+1}): {e}. Retrying...")
                    time.sleep(1)
                else:
                    logger.error(f"Failed to find elements after {max_retries} attempts: {e}")
                    if not self.is_driver_alive():
                        logger.error("WebDriver connection lost. Restarting driver...")
                        self.restart_driver()
                        try:
                            return self.driver.find_elements(by, value)
                        except Exception as final_e:
                            logger.error(f"Final attempt failed: {final_e}")
                    return []

    def load_page_with_retry(self, url, max_retries=5):
        for attempt in range(max_retries):
            try:
                logger.info(f"Loading {url}, attempt {attempt+1}/{max_retries}")
                if not self.is_driver_alive() and attempt > 0:
                    logger.warning("Driver connection lost. Restarting driver before loading page...")
                    self.restart_driver()
                self.driver.get(url)
                try:
                    WebDriverWait(self.driver, 60).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
                    )
                    logger.info("Page body loaded")
                except TimeoutException:
                    logger.warning("Timed out waiting for page body to load")

                # Дополнительное время для полной загрузки страницы
                time.sleep(random.uniform(3, 5))

                try:
                    championships = self.safe_find_elements(By.CSS_SELECTOR,
                        ".sport-competition--Xt2wb, [class*='sport-competition']")
                    if championships:
                        logger.info(f"Successfully loaded page with {len(championships)} championships")
                        # Дополнительная пауза после успешной загрузки
                        time.sleep(random.uniform(1, 2))
                        return True
                    else:
                        logger.warning("Page loaded but no championships found. Retrying...")
                except Exception as e:
                    logger.error(f"Error checking for championships: {e}")

                if attempt < max_retries - 1:
                    logger.info("Page didn't load properly. Restarting driver and retrying...")
                    self.restart_driver()
                    time.sleep(random.uniform(2, 5))
            except (TimeoutException, WebDriverException) as e:
                logger.error(f"Error loading page (attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    logger.info("Restarting driver and retrying...")
                    self.restart_driver()
                    time.sleep(random.uniform(2, 5))
                else:
                    logger.error(f"Failed to load page after {max_retries} attempts")
                    return False
        return False

    def get_main_scroll_container(self):
        """Находит основной контейнер для прокрутки"""
        try:
            # Пробуем найти основной контейнер - высокий контейнер с контентом
            scroll_selectors = [
                ".scroll-area__view-port__default--J1yYl",
                ".scroll-area__view-port--F1xJK",
                ".virtual-list--FMDYy._vertical--GsTT6",
                "[class*='scroll-area__view-port']"
            ]

            main_container = None
            max_height = 0

            for selector in scroll_selectors:
                try:
                    containers = self.safe_find_elements(By.CSS_SELECTOR, selector)
                    for container in containers:
                        try:
                            if container.is_displayed():
                                scroll_height = self.driver.execute_script("return arguments[0].scrollHeight", container)
                                client_height = self.driver.execute_script("return arguments[0].clientHeight", container)

                                # Ищем контейнер с наибольшей высотой содержимого
                                if scroll_height > max_height:
                                    max_height = scroll_height
                                    main_container = container
                                    logger.info(f"Found potential main container with selector: {selector}")
                                    logger.info(f"Scroll height: {scroll_height}, Visible height: {client_height}")
                        except Exception as e:
                            logger.error(f"Error checking container: {e}")
                            continue
                except Exception as e:
                    logger.error(f"Error finding containers with selector {selector}: {e}")
                    continue

            if main_container:
                logger.info(f"Selected main scroll container with height: {max_height}px")
                return main_container
            else:
                logger.warning("Could not find specific scroll container, will try document body")
                return self.driver.find_element(By.TAG_NAME, "body")

        except Exception as e:
            logger.error(f"Error identifying main scroll container: {e}")
            logger.error(traceback.format_exc())
            return self.driver.find_element(By.TAG_NAME, "body")

    def scroll_with_delay(self, container, scroll_amount=500):
        """Выполняет прокрутку контейнера с задержкой для загрузки содержимого"""
        try:
            # Текущая позиция прокрутки
            current_position = self.driver.execute_script("return arguments[0].scrollTop", container)

            # Новая позиция прокрутки
            new_position = current_position + scroll_amount

            # Прокрутка контейнера
            logger.info(f"Scrolling container by {scroll_amount}px from position {current_position}...")
            self.driver.execute_script(f"arguments[0].scrollTop = {new_position};", container)

            # Задержка для загрузки контента (1-3 секунды)
            loading_delay = random.uniform(1, 3)
            logger.info(f"Waiting {loading_delay:.2f} seconds for content to load...")
            time.sleep(loading_delay)

            # Проверяем новую позицию после прокрутки
            after_position = self.driver.execute_script("return arguments[0].scrollTop", container)

            # Проверка успешности прокрутки
            if abs(after_position - current_position) > 10:
                logger.info(f"Scroll successful: from {current_position} to {after_position} (delta: {after_position-current_position}px)")
                return True, after_position
            else:
                logger.info(f"Scroll unchanged: position still at {after_position}")
                return False, current_position
        except Exception as e:
            logger.error(f"Error during scroll operation: {e}")
            return False, 0

    def collect_data_after_scroll(self, sport_id):
        """Собирает данные после прокрутки"""
        championship_dict = {}
        processed_teams = set()

        try:
            # Собираем информацию о чемпионатах
            championship_elems = self.safe_find_elements(By.CSS_SELECTOR,
                ".sport-competition--Xt2wb, [class*='sport-competition']")

            logger.info(f"Processing {len(championship_elems)} championships...")

            # Обработка чемпионатов
            for champ_elem in championship_elems:
                try:
                    champ_name = "Unknown Championship"
                    name_found = False
                    selectors = [
                        ".table-component-text--Tjj3g:not([style*='width'])",
                        ".table-component-text--Tjj3g:not([class*='sport-competition__factor'])",
                        ".table-component-text--Tjj3g",
                        "[class*='table-component-text']:not([class*='sport-competition__factor'])"
                    ]
                    for selector in selectors:
                        try:
                            elements = champ_elem.find_elements(By.CSS_SELECTOR, selector)
                            for elem in elements:
                                text = elem.text.strip()
                                if text and len(text) > 1 and text not in ["1", "2", "X", "Х", "ФОРА 1", "ФОРА 2", "Тотал", "Б", "М"]:
                                    champ_name = text
                                    name_found = True
                                    break
                            if name_found:
                                break
                        except Exception:
                            continue
                    if champ_name == "Unknown Championship":
                        continue

                    champ_position = champ_elem.location['y'] if champ_elem.location else 0

                    # Генерируем уникальный ID для чемпионата, комбинируя спорт ID и счетчик
                    championship_id = self.global_championship_id
                    self.global_championship_id += 1

                    if champ_name not in championship_dict:
                        championship_dict[champ_name] = {
                            "championship_id": championship_id,
                            "championship_name": champ_name,
                            "position": champ_position,
                            "matches": []
                        }
                    championship_dict[champ_name]["position"] = champ_position
                except Exception as e:
                    logger.error(f"Error processing championship: {e}")

            # Обработка матчей
            match_elems = self.safe_find_elements(By.CSS_SELECTOR,
                ".sport-base-event--W4qkO, [class*='sport-base-event']")

            logger.info(f"Processing {len(match_elems)} potential matches...")

            for match_elem in match_elems:
                try:
                    match_position = match_elem.location['y'] if match_elem.location else 0

                    # Находим ближайший чемпионат
                    closest_champ = None
                    min_distance = float('inf')
                    for champ_name, champ_data in championship_dict.items():
                        champ_position = champ_data["position"]
                        if champ_position < match_position and match_position - champ_position < min_distance:
                            min_distance = match_position - champ_position
                            closest_champ = champ_name

                    if not closest_champ:
                        continue

                    try:
                        team1 = "Unknown"
                        team2 = "Unknown"

                        try:
                            event_selector = "a[data-testid='event'], [class*='sport-event__name'], [class*='sport-base-event__description']"
                            event_elem = match_elem.find_element(By.CSS_SELECTOR, event_selector)
                            match_name = event_elem.text.strip()

                            if " — " in match_name:
                                team1, team2 = match_name.split(" — ", 1)
                            elif " - " in match_name:
                                team1, team2 = match_name.split(" - ", 1)
                            elif "—" in match_name:
                                team1, team2 = match_name.split("—", 1)
                            elif "-" in match_name:
                                team1, team2 = match_name.split("-", 1)

                            team1 = team1.strip()
                            team2 = team2.strip()
                        except Exception:
                            pass

                        if team1 == "Unknown" or team2 == "Unknown":
                            continue

                        teams_fingerprint = f"{team1}_{team2}"
                        if teams_fingerprint in processed_teams:
                            continue

                        processed_teams.add(teams_fingerprint)

                        match_time = "Unknown"
                        try:
                            time_elem = match_elem.find_element(By.CSS_SELECTOR,
                                ".event-block-planned-time__time--RtMgQ, [class*='event-block-planned-time__time'], [class*='time']"
                            )
                            match_time = time_elem.text.strip()
                        except Exception:
                            pass

                        odds = {
                            "1": "-",
                            "X": "-",
                            "2": "-",
                            RU_TO_EN_FIELDS["ФОРА 1"]: "-",
                            RU_TO_EN_FIELDS["ФОРА 2"]: "-",
                            RU_TO_EN_FIELDS["Тотал"]: "-",
                            RU_TO_EN_FIELDS["Б"]: "-",
                            RU_TO_EN_FIELDS["М"]: "-"
                        }

                        try:
                            factor_values = match_elem.find_elements(By.CSS_SELECTOR, ".factor-value--zrkpK")
                            for idx, factor_value in enumerate(factor_values):
                                try:
                                    if idx == 0:
                                        value_elem = factor_value.find_element(By.CSS_SELECTOR, ".value--OUKql")
                                        value_text = value_elem.text.strip().replace(",", ".")
                                        if value_text and value_text != "-":
                                            try:
                                                odds["1"] = float(value_text)
                                            except ValueError:
                                                odds["1"] = value_text
                                        else:
                                            odds["1"] = "-"
                                    elif idx == 1:
                                        value_elem = factor_value.find_element(By.CSS_SELECTOR, ".value--OUKql")
                                        value_text = value_elem.text.strip().replace(",", ".")
                                        if value_text and value_text != "-":
                                            try:
                                                odds["X"] = float(value_text)
                                            except ValueError:
                                                odds["X"] = value_text
                                        else:
                                            odds["X"] = "-"
                                    elif idx == 2:
                                        value_elem = factor_value.find_element(By.CSS_SELECTOR, ".value--OUKql")
                                        value_text = value_elem.text.strip().replace(",", ".")
                                        if value_text and value_text != "-":
                                            try:
                                                odds["2"] = float(value_text)
                                            except ValueError:
                                                odds["2"] = value_text
                                        else:
                                            odds["2"] = "-"
                                    elif idx == 3:
                                        try:
                                            param_elem = factor_value.find_element(By.CSS_SELECTOR, ".param--qbIN_")
                                            value_elem = factor_value.find_element(By.CSS_SELECTOR, ".value--OUKql")
                                            param_text = param_elem.text.strip()
                                            value_text = value_elem.text.strip().replace(",", ".")
                                            if value_text and value_text != "-":
                                                odds[RU_TO_EN_FIELDS["ФОРА 1"]] = {
                                                    "value": float(value_text) if value_text.replace(".", "", 1).isdigit() else value_text,
                                                    "param": param_text
                                                }
                                            else:
                                                odds[RU_TO_EN_FIELDS["ФОРА 1"]] = "-"
                                        except Exception:
                                            odds[RU_TO_EN_FIELDS["ФОРА 1"]] = "-"
                                    elif idx == 4:
                                        try:
                                            param_elem = factor_value.find_element(By.CSS_SELECTOR, ".param--qbIN_")
                                            value_elem = factor_value.find_element(By.CSS_SELECTOR, ".value--OUKql")
                                            param_text = param_elem.text.strip()
                                            value_text = value_elem.text.strip().replace(",", ".")
                                            if value_text and value_text != "-":
                                                odds[RU_TO_EN_FIELDS["ФОРА 2"]] = {
                                                    "value": float(value_text) if value_text.replace(".", "", 1).isdigit() else value_text,
                                                    "param": param_text
                                                }
                                            else:
                                                odds[RU_TO_EN_FIELDS["ФОРА 2"]] = "-"
                                        except Exception:
                                            odds[RU_TO_EN_FIELDS["ФОРА 2"]] = "-"
                                    elif idx == 5:
                                        try:
                                            param_elem = factor_value.find_element(By.CSS_SELECTOR, ".param--qbIN_")
                                            total_text = param_elem.text.strip()
                                            odds[RU_TO_EN_FIELDS["Тотал"]] = total_text
                                        except Exception:
                                            odds[RU_TO_EN_FIELDS["Тотал"]] = "-"
                                    elif idx == 6:
                                        value_elem = factor_value.find_element(By.CSS_SELECTOR, ".value--OUKql")
                                        value_text = value_elem.text.strip().replace(",", ".")
                                        if value_text and value_text != "-":
                                            try:
                                                odds[RU_TO_EN_FIELDS["Б"]] = float(value_text)
                                            except ValueError:
                                                odds[RU_TO_EN_FIELDS["Б"]] = value_text
                                        else:
                                            odds[RU_TO_EN_FIELDS["Б"]] = "-"
                                    elif idx == 7:
                                        value_elem = factor_value.find_element(By.CSS_SELECTOR, ".value--OUKql")
                                        value_text = value_elem.text.strip().replace(",", ".")
                                        if value_text and value_text != "-":
                                            try:
                                                odds[RU_TO_EN_FIELDS["М"]] = float(value_text)
                                            except ValueError:
                                                odds[RU_TO_EN_FIELDS["М"]] = value_text
                                        else:
                                            odds[RU_TO_EN_FIELDS["М"]] = "-"
                                except Exception:
                                    continue
                        except Exception:
                            pass

                        # Генерируем уникальный ID для матча
                        match_id = self.global_match_id
                        self.global_match_id += 1

                        championship_dict[closest_champ]["matches"].append({
                            "match_id": match_id,
                            "team1": team1,
                            "team2": team2,
                            "time": match_time,
                            "total": odds[RU_TO_EN_FIELDS["Тотал"]],
                            "odds": odds,
                            "events": []
                        })
                    except Exception as e:
                        logger.error(f"Error extracting match details: {e}")
                except Exception as e:
                    logger.error(f"Error processing match: {e}")
        except Exception as e:
            logger.error(f"Error collecting data: {e}")
            logger.error(traceback.format_exc())

        return championship_dict

    def is_end_of_content(self, container):
        """Проверяет, достигнут ли конец контента"""
        try:
            scroll_top = self.driver.execute_script("return arguments[0].scrollTop", container)
            scroll_height = self.driver.execute_script("return arguments[0].scrollHeight", container)
            client_height = self.driver.execute_script("return arguments[0].clientHeight", container)

            # Если прокрутка близка к концу, считаем что достигнут конец контента
            if scroll_top + client_height >= scroll_height - 50:
                logger.info(f"End of content reached: scrollTop({scroll_top}) + clientHeight({client_height}) >= scrollHeight({scroll_height}) - 50")
                return True

            return False
        except Exception as e:
            logger.error(f"Error checking end of content: {e}")
            return False

    def scroll_and_collect_data(self, sport_id):
        """Прокрутка контейнера и сбор данных"""
        logger.info("Starting controlled scrolling and data collection...")

        # Проверка соединения с браузером
        if not self.is_driver_alive():
            logger.error("Browser connection lost before scrolling. Attempting to recover...")
            if not self.is_driver_alive():
                logger.error("Failed to recover browser connection. Aborting data collection.")
                return []

        # Получаем основной контейнер для прокрутки
        main_container = self.get_main_scroll_container()
        if not main_container:
            logger.error("Failed to find main scroll container")
            return []

        # Инициализация словаря для чемпионатов
        all_championship_data = {}

        # Количество прокруток до конца
        scroll_count = 0
        max_scrolls = 10000  # Увеличенное количество максимальных прокруток

        # Замеряем начальную высоту контейнера
        try:
            initial_scroll_height = self.driver.execute_script("return arguments[0].scrollHeight", main_container)
            logger.info(f"Initial container scroll height: {initial_scroll_height}px")
        except Exception as e:
            logger.error(f"Error getting initial scroll height: {e}")
            initial_scroll_height = 0

        # Счетчик одинаковых высот подряд для подтверждения конца контента
        same_height_count = 0
        last_scroll_height = initial_scroll_height

        # Счетчик неудачных прокруток подряд
        consecutive_failed_scrolls = 0

        # Собираем начальные данные перед прокруткой
        logger.info("Collecting initial data before scrolling...")
        initial_data = self.collect_data_after_scroll(sport_id)
        for champ_name, champ_data in initial_data.items():
            all_championship_data[champ_name] = champ_data

        # Начинаем процесс прокрутки с фиксированным шагом
        while scroll_count < max_scrolls:
            scroll_count += 1
            logger.info(f"Scroll iteration {scroll_count}/{max_scrolls}")

            # Проверяем соединение с браузером каждые 10 прокруток
            if scroll_count % 10 == 0:
                if not self.is_driver_alive():
                    logger.error("Browser connection lost during scrolling. Attempting to recover...")
                    if not self.is_driver_alive():
                        logger.error("Failed to recover browser connection. Returning collected data.")
                        break

            # Выполняем прокрутку с фиксированным шагом в 500 пикселей
            scroll_success, new_position = self.scroll_with_delay(main_container, 500)

            if not scroll_success:
                consecutive_failed_scrolls += 1
                logger.warning(f"Scroll operation failed or reached end of container (failed: {consecutive_failed_scrolls})")

                # Делаем еще одну проверку, действительно ли конец
                if self.is_end_of_content(main_container):
                    logger.info("Confirmed end of container content")
                    # Делаем последнюю попытку прокрутить еще немного, чтобы убедиться
                    final_scroll, _ = self.scroll_with_delay(main_container, 200)
                    if not final_scroll:
                        logger.info("Final scroll check confirms we've reached the end")
                        break
                    else:
                        logger.info("Final scroll was successful, continuing...")
                        consecutive_failed_scrolls = 0

                # Если слишком много неудачных прокруток подряд, вероятно мы достигли конца
                if consecutive_failed_scrolls >= 5:
                    logger.info(f"Multiple consecutive scroll failures ({consecutive_failed_scrolls}), likely at end of content")
                    break

                # Если текущая попытка скрола неудачна, но это еще не конец контейнера
                # (например, временная задержка загрузки), пытаемся еще раз после паузы
                time.sleep(3)  # Увеличенное ожидание для возможной подгрузки
                continue
            else:
                consecutive_failed_scrolls = 0  # Сбрасываем счетчик неудач при успешной прокрутке

            # После каждой успешной прокрутки, даем дополнительное время для полной загрузки
            logger.info("Waiting for full content loading...")
            data_collection_delay = random.uniform(1, 3)  # 1-3 секунды на загрузку
            logger.info(f"Allowing {data_collection_delay:.2f} seconds for content loading...")
            time.sleep(data_collection_delay)

            # Собираем данные после прокрутки
            new_data = self.collect_data_after_scroll(sport_id)

            # Обновляем общий словарь данными текущей прокрутки
            for champ_name, champ_data in new_data.items():
                if champ_name in all_championship_data:
                    # Для существующих чемпионатов добавляем только новые матчи
                    existing_team_pairs = {f"{m['team1']}_{m['team2']}" for m in all_championship_data[champ_name]["matches"]}
                    for match in champ_data["matches"]:
                        team_pair = f"{match['team1']}_{match['team2']}"
                        if team_pair not in existing_team_pairs:
                            all_championship_data[champ_name]["matches"].append(match)
                            existing_team_pairs.add(team_pair)
                else:
                    # Для новых чемпионатов добавляем все данные
                    all_championship_data[champ_name] = champ_data

            # Выводим статистику каждые 5 прокруток
            if scroll_count % 5 == 0:
                total_championships = len(all_championship_data)
                total_matches = sum(len(data["matches"]) for data in all_championship_data.values())
                logger.info(f"Current statistics: {total_championships} championships, {total_matches} matches")

            # Проверяем высоту контейнера
            try:
                current_scroll_height = self.driver.execute_script("return arguments[0].scrollHeight", main_container)
                logger.info(f"Current container scroll height: {current_scroll_height}px")

                if current_scroll_height == last_scroll_height:
                    same_height_count += 1
                    logger.info(f"Container height unchanged for {same_height_count} consecutive scrolls")

                    # Если высота не меняется 10 прокруток подряд и мы далеко внизу,
                    # проверяем достигли ли мы конца
                    if same_height_count >= 10:
                        # Получаем текущую позицию прокрутки
                        current_scroll_top = self.driver.execute_script("return arguments[0].scrollTop", main_container)
                        client_height = self.driver.execute_script("return arguments[0].clientHeight", main_container)

                        # Если мы близко к концу, вероятно достигли конца контента
                        if current_scroll_top + client_height >= current_scroll_height - 200:
                            logger.info("Container height stable at near-bottom position. End of content confirmed.")
                            break
                        else:
                            # Если мы не близко к концу, попробуем прокрутить дальше
                            logger.info("Container height stable but not at bottom. Trying to force-scroll...")
                            # Принудительная большая прокрутка для проверки
                            large_scroll_success, _ = self.scroll_with_delay(main_container, 2000)

                            if not large_scroll_success:
                                logger.info("Force-scroll failed. Likely at end of content.")
                                break
                            else:
                                logger.info("Force-scroll successful, continuing normal scroll")
                                same_height_count = 0  # Сбрасываем счетчик
                else:
                    same_height_count = 0  # Сбрасываем счетчик при изменении высоты

                last_scroll_height = current_scroll_height
            except Exception as e:
                logger.error(f"Error checking container height: {e}")

        logger.info(f"Scroll process completed after {scroll_count} iterations")

        # Формируем итоговый список чемпионатов
        championships = []
        for champ_data in all_championship_data.values():
            if champ_data["matches"]:  # Добавляем только чемпионаты с матчами
                championships.append({
                    "championship_id": champ_data["championship_id"],
                    "championship_name": champ_data["championship_name"],
                    "matches": champ_data["matches"]
                })

        logger.info(f"Data collection complete. Found {len(championships)} championships with {sum(len(c['matches']) for c in championships)} matches")
        return championships

    def get_sport_name(self, url):
        """Преобразует URL вида спорта в его английское название"""
        for url_part, name in SPORT_URL_TO_NAME.items():
            if url_part in url:
                return name
        return "Unknown Sport"

    def scrape_sport(self, sport_url):
        """Main method to scrape a specific sport"""
        url = f"{self.base_url}{sport_url}"
        logger.info(f"\n{'#'*80}\nScraping {url}\n{'#'*80}")

        try:
            if not self.load_page_with_retry(url):
                logger.error(f"Failed to load {url} after multiple attempts. Skipping this sport.")
                return None

            # Дополнительное время для стабилизации страницы после загрузки
            time.sleep(random.uniform(2, 4))

            if not self.is_driver_alive():
                logger.error("Browser session died after page load. Restarting driver...")
                self.restart_driver()
                if not self.load_page_with_retry(url):
                    logger.error(f"Failed to reload {url} after driver restart. Skipping this sport.")
                    return None

            # Получаем английское название спорта из URL
            sport_name = self.get_sport_name(url)
            sport_id = SPORT_IDS.get(sport_name, 99)

            # Прокручиваем содержимое и собираем данные более контролируемым способом
            championships = self.scroll_and_collect_data(sport_id)

            if not championships:
                logger.error(f"Failed to find any championships for {sport_url}. Skipping.")
                return None

            sport_data = {
                "sport_id": sport_id,
                "sport_name": sport_name,
                "sport_url": sport_url,
                "url": url,
                "championships": championships,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            return sport_data
        except Exception as e:
            logger.error(f"Critical error scraping {url}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def save_data(self, data, sport_url):
        if not data or not data.get("championships"):
            logger.warning(f"No data to save for {sport_url}")
            return

        # Используем часть URL (sport_url) и английское название спорта для формирования имени файла
        sport_name = data.get("sport_name", "unknown")
        sport_url_part = sport_url  # Сохраняем URL часть для уникальности файла

        os.makedirs("data", exist_ok=True)
        filename = f"data/{sport_url_part}.json"

        try:
            championships = data.get("championships", [])
            championship_count = len(championships)
            match_count = sum(len(c.get("matches", [])) for c in championships)

            logger.info(f"\nPreparing to save data:")
            logger.info(f"- Sport: {sport_name}")
            logger.info(f"- Sport URL: {sport_url}")
            logger.info(f"- Championships: {championship_count}")
            logger.info(f"- Matches: {match_count}")
            logger.info(f"- Output file: {filename}")

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"Data saved to {filename}")

            # Проверка сохраненного файла
            file_size = os.path.getsize(filename)
            if file_size > 0:
                logger.info(f"File saved successfully ({file_size} bytes)")
                try:
                    with open(filename, "r", encoding="utf-8") as check_file:
                        check_data = json.load(check_file)
                        champ_count = len(check_data.get('championships', []))
                        match_count = sum(len(c.get("matches", [])) for c in check_data.get('championships', []))
                        logger.info(f"File validation: {champ_count} championships with {match_count} matches")
                except Exception as e:
                    logger.error(f"Error validating saved file: {e}")
            else:
                logger.warning(f"WARNING: File is empty (0 bytes)")
        except Exception as e:
            logger.error(f"Error saving data to {filename}: {e}")
            logger.error(traceback.format_exc())

    def run(self):
        """Main method to run the scraper on all sports"""
        start_time = time.time()
        successful_sports = []
        failed_sports = []

        logger.info(f"\n{'='*80}")
        logger.info(f"Starting FonbetScraper at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Target sports: {len(self.sports)}")
        logger.info(f"{'='*80}\n")

        os.makedirs("data", exist_ok=True)

        try:
            for sport_idx, sport in enumerate(self.sports):
                try:
                    logger.info(f"\n{'='*80}")
                    logger.info(f"Starting to scrape {sport} ({sport_idx+1}/{len(self.sports)})")
                    logger.info(f"{'='*80}\n")

                    if sport_idx > 0:
                        delay = random.uniform(2, 5)
                        logger.info(f"Waiting {delay:.2f} seconds before next sport...")
                        time.sleep(delay)

                    sport_attempt = 0
                    max_sport_attempts = 2

                    while sport_attempt < max_sport_attempts:
                        try:
                            if sport_attempt > 0:
                                logger.info(f"Retry attempt {sport_attempt} for {sport}")

                            data = self.scrape_sport(sport)

                            if data and data.get("championships"):
                                self.save_data(data, sport)
                                logger.info(f"Successfully scraped {sport} with {len(data.get('championships', []))} championships")
                                successful_sports.append(sport)
                                break
                            else:
                                logger.warning(f"No data found for {sport} (attempt {sport_attempt+1})")
                                sport_attempt += 1
                                if sport_attempt >= max_sport_attempts:
                                    logger.error(f"Failed to scrape {sport} after {max_sport_attempts} attempts")
                                    failed_sports.append(sport)
                        except Exception as attempt_e:
                            logger.error(f"Error during attempt {sport_attempt+1} for {sport}: {attempt_e}")
                            logger.error(traceback.format_exc())
                            sport_attempt += 1
                            if sport_attempt >= max_sport_attempts:
                                failed_sports.append(sport)

                        self.restart_driver()

                except Exception as e:
                    logger.error(f"Unhandled error scraping {sport}: {str(e)}")
                    logger.error(traceback.format_exc())
                    failed_sports.append(sport)
                    self.restart_driver()

            if failed_sports:
                retry_sports = failed_sports.copy()
                failed_sports = []

                logger.info(f"\n{'='*80}")
                logger.info(f"Retrying {len(retry_sports)} failed sports")
                logger.info(f"{'='*80}\n")

                for retry_idx, sport in enumerate(retry_sports):
                    try:
                        logger.info(f"Retry {retry_idx+1}/{len(retry_sports)}: {sport}")

                        delay = random.uniform(2, 5)
                        logger.info(f"Waiting {delay:.2f} seconds before retry...")
                        time.sleep(delay)

                        data = self.scrape_sport(sport)

                        if data and data.get("championships"):
                            self.save_data(data, sport)
                            logger.info(f"Successfully scraped {sport} on retry")
                            successful_sports.append(sport)
                        else:
                            logger.warning(f"Failed to scrape {sport} even after retry")
                            failed_sports.append(sport)

                        self.restart_driver()
                    except Exception as e:
                        logger.error(f"Error retrying {sport}: {str(e)}")
                        logger.error(traceback.format_exc())
                        failed_sports.append(sport)
                        self.restart_driver()

            total_time = time.time() - start_time
            hours, remainder = divmod(total_time, 3600)
            minutes, seconds = divmod(remainder, 60)

            logger.info(f"\n{'='*80}")
            logger.info(f"FonbetScraper finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Total runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            logger.info(f"Successful sports: {len(successful_sports)}/{len(self.sports)}")
            logger.info(f"Failed sports: {len(failed_sports)}/{len(self.sports)}")

            if successful_sports:
                logger.info(f"\nSuccessful sports: {', '.join(successful_sports)}")
            if failed_sports:
                logger.info(f"\nFailed sports: {', '.join(failed_sports)}")

            logger.info(f"{'='*80}\n")

        except Exception as e:
            logger.error(f"Critical error in main run: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    logger.info("WebDriver closed successfully")
                except Exception as quit_e:
                    logger.error(f"Error closing WebDriver: {quit_e}")

if __name__ == "__main__":
    scraper = FonbetScraper()
    scraper.run()
