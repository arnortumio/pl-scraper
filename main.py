import cloudscraper
from bs4 import BeautifulSoup, Comment
import pandas as pd
import time
import random
import schedule
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import logging
import os
import json
import re
from io import StringIO
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading

class PremierLeagueScraper:
    def __init__(self):
        self.base_url = "https://fbref.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = cloudscraper.create_scraper()
        self.session.headers.update(self.headers)
        self._stats_page_html = None
        self._stats_page_soup = None
        self.setup_logging()
        self.setup_google_sheets()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def setup_google_sheets(self):
        try:
            self.logger.info("🔍 BYRJA Á GOOGLE SHEETS UPPSETNINGU...")
            creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
            if creds_json:
                creds_info = json.loads(creds_json)
                scope = [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"
                ]
                creds = Credentials.from_service_account_info(creds_info, scopes=scope)
                self.gc = gspread.authorize(creds)
                self.logger.info("✅ GOOGLE SHEETS TENGING TÓKST!")
                self.test_google_connection()
            else:
                self.logger.error("❌ ENGIN GOOGLE_CREDENTIALS_JSON fundin!")
                self.gc = None
        except Exception as e:
            self.logger.error(f"💥 Villa við Google Sheets uppsetningu: {e}")
            self.gc = None

    def test_google_connection(self):
        if not self.gc:
            self.logger.error("❌ Engin Google tenging til að prófa.")
            return False
        try:
            self.gc.list_spreadsheet_files()
            self.logger.info("✅ GOOGLE TENGING VIRKAR")
            return True
        except Exception as e:
            self.logger.error(f"💥 Villa við prófun: {e}")
            return False

    # ---------- NET / HTML HJÁLPARAR ----------

    def get_stats_page_soup(self, force_refresh=False):
        """
        Sækir /en/comps/9/stats/Premier-League-Stats einu sinni og geymir í cache.
        Notar exponential backoff með jitter og virðir Retry-After haus á 429 svörum.
        """
        if self._stats_page_soup is not None and not force_refresh:
            return self._stats_page_soup

        url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
        last_status = None
        max_attempts = 5
        base_backoff = 1

        for attempt in range(max_attempts):
            resp = self.session.get(url, timeout=30)
            last_status = resp.status_code
            self.logger.info(f"📡 HTTP Status: {resp.status_code} @ {url}")

            if resp.status_code == 200:
                self._stats_page_html = resp.text
                self._stats_page_soup = BeautifulSoup(resp.text, 'html.parser')
                return self._stats_page_soup

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = 0
                else:
                    wait = min(base_backoff * (2 ** attempt), 60) + random.uniform(0, 1)
                self.logger.warning(f"⚠️ 429 frá FBref, bíð í {wait:.2f}s og reyni aftur...")
                time.sleep(wait)
                continue

            break

        self.logger.error(f"❌ Gat ekki sótt stats-síðuna. síðasti status: {last_status}")
        return None

    def get_html_table(self, url=None, div_id=None, table_id=None, soup=None):
        """
        Nær í <table> með gefnu div_id/table_id.
        - Ef soup er gefið: notum það (engin ný nettenging).
        - Ef url vísar á stats-síðuna: notum cache (get_stats_page_soup).
        - Annars sækjum við url beint.
        - Ef tafla er í HTML comment innan div eða á heildarsíðunni, parse-um comment.
        - table_id má vera strengur eða regex mynstur.
        """
        try:
            if soup is None:
                stats_url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
                if url == stats_url:
                    soup = self.get_stats_page_soup()
                    if soup is None:
                        return None
                else:
                    if not url:
                        return None
                    response = self.session.get(url, timeout=30)
                    self.logger.info(f"📡 HTTP Status: {response.status_code} @ {url}")
                    if response.status_code != 200:
                        return None
                    soup = BeautifulSoup(response.text, 'html.parser')

            target = soup
            if div_id:
                div = soup.find('div', id=div_id)
                if not div:
                    return None
                comment = div.find(string=lambda text: isinstance(text, Comment))
                target = BeautifulSoup(comment, 'html.parser') if comment else div

            table = target.find('table', id=table_id) if table_id else target.find('table', {'class': 'stats_table'})

            if not table and table_id is not None and not div_id:
                for comment_block in soup.find_all(string=lambda text: isinstance(text, Comment)):
                    comment_soup = BeautifulSoup(comment_block, 'html.parser')
                    table = comment_soup.find('table', id=table_id)
                    if table:
                        break

            return table
        except Exception as e:
            self.logger.error(f"💥 Villa við að sækja töflu (div_id={div_id}, table_id={table_id}): {e}")
            return None

    # ---------- DFRAME HJÁLPARAR ----------

    def _flatten_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df.columns, pd.MultiIndex):
            new_cols = []
            for col in df.columns.values:
                parts = [c for c in col if c and not str(c).startswith("Unnamed")]
                name = "_".join(parts).strip() if parts else "col"
                new_cols.append(name)
            df.columns = new_cols
        return df

    def _clean_header_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        for dup in ("Squad", "Team", "Rk"):
            if dup in df.columns:
                df = df[df[dup] != dup]
        return df

    def _finalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._flatten_columns(df)
        df = self._clean_header_rows(df)
        df['Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return df

    # ---------- GÖGNASÖFNUN ----------

    def get_premier_league_table(self):
        self.logger.info("🏴 Sæki Premier League töflu...")
        soup = self.get_stats_page_soup()
        if soup is None:
            self.logger.error("❌ Gat ekki sótt stats-síðuna.")
            return None
        table = self.get_html_table(table_id=re.compile(r'^results'), soup=soup)
        if not table:
            self.logger.error("❌ Gat ekki fundið PL töflu.")
            return None
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except ValueError:
            self.logger.error("❌ pd.read_html tókst ekki á PL töflu.")
            return None
        df = self._finalize_df(df)
        self.logger.info(f"✅ PL tafla fundin: {len(df)} lið")
        return df

    def get_player_stats(self):
        self.logger.info("⚽ Sæki leikmannastatistík...")
        soup = self.get_stats_page_soup()
        if soup is None:
            self.logger.error("❌ Gat ekki sótt stats-síðuna.")
            return None
        table = self.get_html_table(div_id='all_stats_standard', table_id='stats_standard', soup=soup)
        if table:
            try:
                df = pd.read_html(StringIO(str(table)))[0]
            except ValueError:
                self.logger.error("❌ pd.read_html tókst ekki á player stats.")
                return None
            df = self._finalize_df(df)
            self.logger.info(f"✅ Leikmenn fundnir: {len(df)}")
            return df
        self.logger.error("❌ Gat ekki fundið leikmannatöflu.")
        return None

    def get_squad_table(self, base: str, direction: str):
        """Nær í squad-töflur með regex-leit sem tekur mið af "base" og "direction"."""
        soup = self.get_stats_page_soup()
        if soup is None:
            return None
        dir_part = f"_{direction}" if direction and direction != "for" else ""
        div_regex = re.compile(fr"^all_(stats_)?{base}{dir_part}$")
        div = soup.find('div', id=div_regex)
        if not div:
            for comment_block in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment_soup = BeautifulSoup(comment_block, 'html.parser')
                div = comment_soup.find('div', id=div_regex)
                if div:
                    break
        if not div:
            self.logger.error(f"❌ Gat ekki fundið div fyrir {base}{dir_part}.")
            return None
        comment = div.find(string=lambda text: isinstance(text, Comment))
        target = BeautifulSoup(comment, 'html.parser') if comment else div
        table_regex = re.compile(fr"^(stats_)?{base}{dir_part}$")
        table = target.find('table', id=table_regex) or target.find('table', {'class': 'stats_table'})
        if not table:
            self.logger.error(f"❌ Gat ekki fundið töflu {base}{dir_part}.")
            return None
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except ValueError:
            self.logger.error(f"❌ pd.read_html tókst ekki á {base}{dir_part}.")
            return None
        df = self._finalize_df(df)
        self.logger.info(f"✅ {base}{dir_part} fundin: {len(df)} línur")
        return df

    def get_fixtures_and_results(self):
        self.logger.info("📅 Sæki leikjaupplýsingar...")
        url = f"{self.base_url}/en/comps/9/schedule/Premier-League-Fixtures"
        response = self.session.get(url, timeout=30)
        self.logger.info(f"📡 HTTP Status: {response.status_code} @ {url}")
        if response.status_code != 200:
            self.logger.error("❌ Gat ekki sótt leikjadagskrá.")
            return None
        soup = BeautifulSoup(response.text, 'html.parser')
        table = self.get_html_table(table_id=re.compile(r'^sched'), soup=soup)
        if not table:
            self.logger.error("❌ Gat ekki fundið leikjatöflu.")
            return None
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except ValueError:
            self.logger.error("❌ pd.read_html tókst ekki á fixtures/results.")
            return None
        df = self._finalize_df(df)
        self.logger.info(f"✅ Leikir fundnir: {len(df)}")
        return df

    # ---------- SHEETS ----------

    def clean_data_for_sheets(self, data_list):
        """Skiptir út NaN í tóman streng fyrir Google Sheets."""
        cleaned = []
        for row in data_list:
            new_row = []
            for item in row:
                if pd.isna(item):
                    new_row.append("")
                else:
                    new_row.append(item)
            cleaned.append(new_row)
        return cleaned

    def update_google_sheet(self, sheet_name, data, worksheet_name):
        if self.gc is None:
            self.logger.error("❌ Engin Google Sheets tenging.")
            return
        try:
            try:
                sheet = self.gc.open(sheet_name)
            except gspread.SpreadsheetNotFound:
                sheet = self.gc.create(sheet_name)
                # Settu service account email ef þú vilt deila sjálfkrafa
                sheet.share('your-email@example.com', perm_type='user', role='writer')

            try:
                worksheet = sheet.worksheet(worksheet_name)
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=worksheet_name, rows="2000", cols="50")

            if data is not None and not data.empty:
                data_list = [data.columns.tolist()] + data.values.tolist()
                cleaned_data = self.clean_data_for_sheets(data_list)
                try:
                    # Nýja röðin: values fyrst, síðan range_name eða með nafngildum
                    worksheet.update(values=cleaned_data, range_name='A1')
                    self.logger.info(f"✅ Uppfærði {worksheet_name} með {len(data)} röðum.")
                except Exception as e:
                    self.logger.error(f"💥 Villa við uppfærslu á worksheet.update fyrir {worksheet_name}: {e}")
            else:
                self.logger.warning(f"⚠️ Engin gögn til að uppfæra í {worksheet_name}.")
        except Exception as e:
            self.logger.error(f"💥 Villa við að nálgast eða búa til sheet/worksheet: {e}")

    # ---------- KEYRSLUR ----------

    def full_update(self):
        self.logger.info("🚀 Byrja fulla uppfærslu...")
        if not self.test_google_connection():
            self.logger.error("❌ Engin virk Google tenging.")
            return

        # Try to fetch and cache the main stats page once. If this fails,
        # there's no point in continuing with the update since many
        # subsequent functions rely on these data. Avoid multiple failing
        # requests by exiting early.
        if self.get_stats_page_soup() is None:
            self.logger.error("❌ Stats-síða ekki tiltæk. Hætti við uppfærslu.")
            return

        sheet_name = "PL_Fantasy_Data"

        # Heimsíður / leikmenn / leikir eins og áður
        league = self.get_premier_league_table()
        players = self.get_player_stats()
        fixtures = self.get_fixtures_and_results()

        squad_specs = [
            ("Squad_Standard_For", "squads_standard", "for"),
            ("Squad_Standard_Against", "squads_standard", "against"),
            ("Squad_Shooting_For", "squads_shooting", "for"),
            ("Squad_Shooting_Against", "squads_shooting", "against"),
            ("Squad_Passing_For", "squads_passing", "for"),
            ("Squad_Passing_Against", "squads_passing", "against"),
            ("Squad_PassingTypes_For", "squads_passing_types", "for"),
            ("Squad_PassingTypes_Against", "squads_passing_types", "against"),
            ("Squad_GCA_For", "squads_gca", "for"),
            ("Squad_GCA_Against", "squads_gca", "against"),
            ("Squad_Defense_For", "squads_defense", "for"),
            ("Squad_Defense_Against", "squads_defense", "against"),
            ("Squad_Possession_For", "squads_possession", "for"),
            ("Squad_Possession_Against", "squads_possession", "against"),
            ("Squad_PlayingTime_For", "squads_playing_time", "for"),
            ("Squad_PlayingTime_Against", "squads_playing_time", "against"),
            ("Squad_Misc_For", "squads_misc", "for"),
            ("Squad_Misc_Against", "squads_misc", "against"),
            ("Squad_GK_For", "squads_keeper[s]?", "for"),
            ("Squad_GK_Against", "squads_keeper[s]?", "against"),
            ("Squad_GKAdv_For", "squads_keeper_adv[s]?", "for"),
            ("Squad_GKAdv_Against", "squads_keeper_adv[s]?", "against"),
        ]

        if league is not None:
            self.update_google_sheet(sheet_name, league, "League_Table")
        if players is not None:
            self.update_google_sheet(sheet_name, players, "Player_Stats")
        if fixtures is not None:
            self.update_google_sheet(sheet_name, fixtures, "Fixtures_Results")

        for worksheet_name, base, direction in squad_specs:
            df = self.get_squad_table(base, direction)
            if df is not None:
                self.update_google_sheet(sheet_name, df, worksheet_name)

        self.logger.info("✅ Full uppfærsla lokið!")

    def run_once(self):
        self.full_update()

    def start_scheduler(self):
        schedule.every(30).minutes.do(self.full_update)
        schedule.every().day.at("08:00").do(self.full_update)
        self.logger.info("⏰ Scheduler settur upp.")
        self.full_update()
        while True:
            schedule.run_pending()
            time.sleep(60)

# ---------- LÍTILL VEFÞJÓNN TIL STATUS ----------

def run_web_server():
    class Handler(SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            html = f"""
            <html>
            <head><title>PL Scraper</title></head>
            <body>
                <h1>✅ PL Scraper í gangi</h1>
                <p>Síðast keyrt: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </body>
            </html>
            """
            self.wfile.write(html.encode('utf-8'))

    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(('0.0.0.0', port), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    print(f"🌐 Web server keyrir á port {port}")

def main():
    print("🚀 Ræsi Premier League Scraper...")
    scraper = PremierLeagueScraper()
    if os.environ.get('RENDER') or os.environ.get('RAILWAY_ENVIRONMENT'):
        print("☁️ Production mode")
        run_web_server()
        scraper.start_scheduler()
    else:
        print("💻 Development mode")
        scraper.run_once()

if __name__ == "__main__":
    main()
