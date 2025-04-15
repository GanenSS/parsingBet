import os
import time
import logging
import json
import requests
from datetime import datetime
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()])
logger = logging.getLogger("LineBetScraper")

class LineBetScraper:
    def __init__(self):
        self.base_url = "https://line-lb21.bk6bba-resources.com/ma/events/listBase?lang=ru&scopeMarket=1600"
        self.global_championship_id = 10000
        self.global_match_id = 100000
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://line.bk6bba-resources.com",
            "Referer": "https://line.bk6bba-resources.com/"
        }

        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(script_dir, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        logger.info(f"Data directory created/confirmed at: {self.data_dir}")

    def process_odds_structure(self, match_odds_list):
        """Process odds into a structured format with consistent order"""
        odds_structure = {
            "1": None,
            "X": None,
            "2": None,
            "HANDICAP 1": None,
            "HANDICAP 2": None,
            "TOTAL": None,
            "OVER": None,
            "UNDER": None
        }

        for factor in match_odds_list:
            factor_id = factor.get("f")
            factor_value = factor.get("v", "-")
            factor_pt = factor.get("pt")

            if isinstance(factor_value, (int, float)) or (isinstance(factor_value, str) and factor_value.replace('.', '', 1).isdigit()):
                factor_value = float(factor_value)

            if factor_id == 921:
                odds_structure["1"] = factor_value
            elif factor_id == 922:
                odds_structure["X"] = factor_value
            elif factor_id == 923:
                odds_structure["2"] = factor_value
            elif factor_id == 927:
                odds_structure["HANDICAP 1"] = {
                    "value": factor_value,
                    "param": factor_pt if factor_pt else "0"
                }
            elif factor_id == 928:
                odds_structure["HANDICAP 2"] = {
                    "value": factor_value,
                    "param": factor_pt if factor_pt else "0"
                }
            elif factor_id == 930:
                odds_structure["TOTAL"] = factor_pt if factor_pt else "0"
                odds_structure["OVER"] = factor_value
            elif factor_id == 931:
                odds_structure["UNDER"] = factor_value

        return {k: v for k, v in odds_structure.items() if v is not None}

    def fetch_data(self):
        """Fetch data from the API"""
        logger.info(f"Fetching data from {self.base_url}")
        try:
            response = requests.get(
                self.base_url,
                headers=self.headers,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"Failed to fetch data: HTTP {response.status_code}")
                return None

            logger.info(f"Successfully fetched data: {len(response.content)} bytes")
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            return None

    def process_data(self, raw_data):
        """Process the raw JSON data into our desired format"""
        if not raw_data:
            logger.error("No data to process")
            return []

        try:
            logger.info("Starting to process raw data")

            sports_data = {}

            sports_list = raw_data.get("sports", [])
            if not sports_list:
                logger.error("No sports list found in raw data")
                return []

            logger.info(f"Processing sports list with {len(sports_list)} items")

            all_items_map = {}
            for item in sports_list:
                if isinstance(item, dict) and item.get("id"):
                    all_items_map[str(item.get("id"))] = item

            for item in sports_list:
                if isinstance(item, dict) and item.get("kind") == "sport":
                    sport_id = str(item.get("id"))
                    if not sport_id:
                        continue

                    sport_name = item.get("name", f"Sport {sport_id}")

                    sports_data[sport_id] = {
                        "sportId": sport_id,
                        "sportName": sport_name,
                        "championships": {}
                    }

            championships_data = {}
            championship_to_sport_map = {}

            for item in sports_list:
                if isinstance(item, dict) and item.get("kind") == "segment":
                    championship_id = str(item.get("id"))
                    if not championship_id:
                        continue

                    championship_name = item.get("name", f"Championship {championship_id}")
                    parent_sport_id = None

                    if item.get("parentId"):
                        parent_id = str(item.get("parentId"))
                        if parent_id in sports_data:
                            parent_sport_id = parent_id

                    if not parent_sport_id and item.get("parentIds"):
                        for potential_parent_id in item.get("parentIds", []):
                            potential_parent_id = str(potential_parent_id)
                            if potential_parent_id in sports_data:
                                parent_sport_id = potential_parent_id
                                break

                    if not parent_sport_id and item.get("sportCategoryId"):
                        sport_category_id = str(item.get("sportCategoryId"))
                        if sport_category_id in sports_data:
                            parent_sport_id = sport_category_id

                    if parent_sport_id:
                        championships_data[championship_id] = {
                            "championshipId": championship_id,
                            "championshipName": championship_name,
                            "matches": {}
                        }
                        sports_data[parent_sport_id]["championships"][championship_id] = championships_data[championship_id]
                        championship_to_sport_map[championship_id] = parent_sport_id

            events_list = raw_data.get("events", [])
            custom_factors_list = raw_data.get("customFactors", [])

            level1_events = {}
            level2_events = {}
            level3_events = {}

            for event in events_list:
                if not isinstance(event, dict):
                    continue

                event_id = str(event.get("id"))
                if not event_id:
                    continue

                event_level = event.get("level", 0)

                if event_level == 1:
                    level1_events[event_id] = event
                elif event_level == 2:
                    level2_events[event_id] = event
                elif event_level == 3:
                    level3_events[event_id] = event

            logger.info(f"Found {len(level1_events)} matches, {len(level2_events)} events, and {len(level3_events)} sub-events")

            match_odds = {}
            for custom_factor in custom_factors_list:
                event_id = str(custom_factor.get("e"))
                if event_id:
                    match_odds[event_id] = custom_factor.get("factors", [])

            for match_id, match in level1_events.items():
                team1 = match.get("team1", "Unknown")
                team2 = match.get("team2", "Unknown")

                if team1 == "Unknown" or team2 == "Unknown":
                    continue

                championship_id = str(match.get("sportId", ""))
                sport_id = championship_to_sport_map.get(championship_id)

                if not sport_id or sport_id not in sports_data:
                    continue

                start_time = match.get("startTime")
                formatted_time = ""
                if start_time:
                    try:
                        timestamp = int(start_time)
                        formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        formatted_time = str(start_time)

                odds_structure = self.process_odds_structure(match_odds.get(match_id, []))

                match_obj = {
                    "eventId": match_id,
                    "time": formatted_time,
                    "team1": team1,
                    "team2": team2,
                    "odds": odds_structure,
                    "events": []
                }

                for event_id, event in level2_events.items():
                    if str(event.get("parentId")) == match_id:
                        event_time = event.get("startTime")
                        event_formatted_time = ""
                        if event_time:
                            try:
                                timestamp = int(event_time)
                                event_formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                event_formatted_time = str(event_time)

                        event_odds = self.process_odds_structure(match_odds.get(event_id, []))

                        event_obj = {
                            "eventId": event_id,
                            "parentId": match_id,
                            "name": event.get("name", ""),
                            "time": event_formatted_time,
                            "description": event.get("info", ""),
                            "kind": event.get("kind", ""),
                            "odds": event_odds,
                            "subEvents": []
                        }

                        for subevent_id, subevent in level3_events.items():
                            if str(subevent.get("parentId")) == event_id:
                                subevent_time = subevent.get("startTime")
                                subevent_formatted_time = ""
                                if subevent_time:
                                    try:
                                        timestamp = int(subevent_time)
                                        subevent_formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                                    except:
                                        subevent_formatted_time = str(subevent_time)

                                subevent_odds = self.process_odds_structure(match_odds.get(subevent_id, []))

                                subevent_obj = {
                                    "eventId": subevent_id,
                                    "parentId": event_id,
                                    "name": subevent.get("name", ""),
                                    "time": subevent_formatted_time,
                                    "description": subevent.get("info", ""),
                                    "kind": subevent.get("kind", ""),
                                    "odds": subevent_odds
                                }

                                event_obj["subEvents"].append(subevent_obj)

                        match_obj["events"].append(event_obj)

                sports_data[sport_id]["championships"][championship_id]["matches"][match_id] = match_obj

            result = []
            for sport_id, sport_info in sports_data.items():
                championships_list = []
                for champ_id, champ_info in sport_info["championships"].items():
                    matches_list = list(champ_info["matches"].values())
                    if matches_list:
                        championships_list.append({
                            "championshipId": champ_info["championshipId"],
                            "championshipName": champ_info["championshipName"],
                            "matches": matches_list
                        })

                if championships_list:
                    result.append({
                        "sport": {
                            "sportId": sport_info["sportId"],
                            "sportName": sport_info["sportName"],
                            "championships": championships_list
                        }
                    })

            return result

        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def save_data(self, processed_data):
        if not processed_data:
            logger.warning("No processed data to save")
            return

        for sport_data in processed_data:
            try:
                sport_info = sport_data.get("sport", {})
                sport_name = sport_info.get("sportName", "unknown").lower().replace(" ", "_").replace("/", "_")
                filename = os.path.join(self.data_dir, f"{sport_name}.json")

                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(sport_data, f, ensure_ascii=False, indent=2)

            except Exception as e:
                sport_name = sport_info.get("sportName", "unknown") if 'sport_info' in locals() else "unknown"
                logger.error(f"Error saving data for sport {sport_name}: {str(e)}")

    def run(self):
        """Main execution method"""
        start_time = time.time()

        logger.info(f"Starting LineBetScraper at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            raw_data = self.fetch_data()
            if not raw_data:
                logger.error("Failed to fetch data, aborting")
                return False

            processed_data = self.process_data(raw_data)
            if not processed_data:
                logger.error("Failed to process data, aborting")
                return False

            self.save_data(processed_data)

            total_time = time.time() - start_time
            minutes, seconds = divmod(total_time, 60)
            logger.info(f"LineBetScraper finished in {int(minutes)}m {int(seconds)}s")

            return True

        except Exception as e:
            logger.error(f"Critical error in main run: {str(e)}")
            logger.error(traceback.format_exc())
            return False

if __name__ == "__main__":
    scraper = LineBetScraper()
    scraper.run()
