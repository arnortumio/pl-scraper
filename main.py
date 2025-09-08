import cloudscraper
from bs4 import BeautifulSoup, Comment
import pandas as pd
import time
import schedule
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import logging
import os
import json
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
        Notar einfalt exponential backoff ef 429 kemur.
        """
        if self._stats_page_soup is not None and not force_refresh:
            return self._stats_page_soup

        url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
        backoff = [0, 2, 4, 8]
        last_status = None
        for wait in backoff:
            if wait:
                time.sleep(wait)
            resp = self.session.get(url, timeout=30)
            last_status = resp.status_code
            self.logger.info(f"📡 HTTP Status: {resp.status_code} @ {url}")
            if resp.status_code == 200:
                self._stats_page_html = resp.text
                self._stats_page_soup = BeautifulSoup(resp.text, 'html.parser')
                return self._stats_page_soup
            if resp.status_code == 429:
                self.logger.warning("⚠️ 429 frá FBref, reyni aftur...")
                continue
            break

        self.logger.error(f"❌ Gat ekki sótt stats-síðuna. síðasti status: {last_status}")
        return None

    def get_html_table(self, url=None, div_id=None, table_id=None, soup=None):
        """
        Nær í <table> með gefnu div_id/table_id.
        - Ef soup er gefið: notum það (engin ný nettenging).
        - Annars sækjum við url (eins og áður).
        - Ef tafla er í HTML comment innan div, parse-um comment.
        """
        try:
            if soup is None:
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

            table = target.find('table', {'id': table_id}) if table_id else target.find('table', {'class': 'stats_table'})
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
        url = f"{self.base_url}/en/comps/9/Premier-League-Stats"
        table = self.get_html_table(url, div_id='all_results2024-2025_9_overall')
        if table:
            try:
                df = pd.read_html(StringIO(str(table)))[0]
            except ValueError:
                self.logger.error("❌ pd.read_html tókst ekki á PL töflu.")
                return None
            df = self._finalize_df(df)
            self.logger.info(f"✅ PL tafla fundin: {len(df)} lið")
            return df
        self.logger.error("❌ Gat ekki fundið PL töflu.")
        return None

    def get_player_stats(self):
        self.logger.info("⚽ Sæki leikmannastatistík...")
        url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
        table = self.get_html_table(url, div_id='all_stats_standard', table_id='stats_standard')
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

    def get_squad_standard_stats(self):
        self.logger.info("👥 Sæki Squad Standard Stats (lið, For)...")
        url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
        table = self.get_html_table(url, div_id='all_stats_squads_standard_for', table_id='stats_squads_standard_for')
        if table:
            try:
                df = pd.read_html(StringIO(str(table)))[0]
            except ValueError:
                self.logger.error("❌ pd.read_html tókst ekki á Squad Standard For.")
                return None
            df = self._finalize_df(df)
            self.logger.info(f"✅ Squad Standard For fundin: {len(df)} línur")
            return df
        self.logger.error("❌ Gat ekki fundið Squad Standard For.")
        return None

    def get_squad_table_generic(self, id_candidates):
        """
        Reynir marga mögulega (div_id, table_id) para fyrir SÖMU töflu úr stats-síðunni (úr cache).
        """
        soup = self.get_stats_page_soup()
        if soup is None:
            return None

        for div_id, table_id in id_candidates:
            self.logger.info(f"🔎 Reyni (div_id={div_id}, table_id={table_id})...")
            table = self.get_html_table(div_id=div_id, table_id=table_id, soup=soup)
            if not table:
                continue
            try:
                df = pd.read_html(StringIO(str(table)))[0]
            except ValueError:
                try:
                    df_list = pd.read_html(StringIO(str(table)))
                    df = df_list[0] if df_list else None
                except Exception:
                    df = None
            if df is not None and not df.empty:
                df = self._finalize_df(df)
                self.logger.info(f"✅ Tókst: {table_id} ({len(df)} línur)")
                return df

        self.logger.error(f"❌ Tókst ekki að ná í töfluna eftir {len(id_candidates)} tilraunum.")
        return None

    def get_fixtures_and_results(self):
        self.logger.info("📅 Sæki leikjaupplýsingar...")
        url = f"{self.base_url}/en/comps/9/schedule/Premier-League-Fixtures"
        table = self.get_html_table(url, div_id='all_sched_ks_3232_1')
        if table:
            try:
                df = pd.read_html(StringIO(str(table)))[0]
            except ValueError:
                self.logger.error("❌ pd.read_html tókst ekki á fixtures/results.")
                return None
            df = self._finalize_df(df)
            self.logger.info(f"✅ Leikir fundnir: {len(df)}")
            return df
        self.logger.error("❌ Gat ekki fundið leikjatöflu.")
        return None

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

        sheet_name = "PL_Fantasy_Data"

        # Heimsíður / leikmenn / leikir eins og áður
        league = self.get_premier_league_table()
        players = self.get_player_stats()
        fixtures = self.get_fixtures_and_results()
        squads_std_for = self.get_squad_standard_stats()  # For

        # ALLAR SQUAD TÖFLUR (For/Against) með víðari fallback
        squad_tables = {
            "Squad_Standard_Against": [
                ("all_stats_squads_standard_against", "stats_squads_standard_against"),
                ("all_squads_standard_against", "squads_standard_against"),
                ("all_stats_squads_standard", "stats_squads_standard"),
                ("all_squads_standard", "squads_standard"),
            ],
            "Squad_Shooting_For": [
                ("all_stats_squads_shooting_for", "stats_squads_shooting_for"),
                ("all_squads_shooting_for", "squads_shooting_for"),
                ("all_stats_squads_shooting", "stats_squads_shooting"),
                ("all_squads_shooting", "squads_shooting"),
            ],
            "Squad_Shooting_Against": [
                ("all_stats_squads_shooting_against", "stats_squads_shooting_against"),
                ("all_squads_shooting_against", "squads_shooting_against"),
                ("all_stats_squads_shooting", "stats_squads_shooting"),
                ("all_squads_shooting", "squads_shooting"),
            ],
            "Squad_Passing_For": [
                ("all_stats_squads_passing_for", "stats_squads_passing_for"),
                ("all_squads_passing_for", "squads_passing_for"),
                ("all_stats_squads_passing", "stats_squads_passing"),
                ("all_squads_passing", "squads_passing"),
            ],
            "Squad_Passing_Against": [
                ("all_stats_squads_passing_against", "stats_squads_passing_against"),
                ("all_squads_passing_against", "squads_passing_against"),
                ("all_stats_squads_passing", "stats_squads_passing"),
                ("all_squads_passing", "squads_passing"),
            ],
            "Squad_PassingTypes_For": [
                ("all_stats_squads_passing_types_for", "stats_squads_passing_types_for"),
                ("all_squads_passing_types_for", "squads_passing_types_for"),
                ("all_stats_squads_passing_types", "stats_squads_passing_types"),
                ("all_squads_passing_types", "squads_passing_types"),
            ],
            "Squad_PassingTypes_Against": [
                ("all_stats_squads_passing_types_against", "stats_squads_passing_types_against"),
                ("all_squads_passing_types_against", "squads_passing_types_against"),
                ("all_stats_squads_passing_types", "stats_squads_passing_types"),
                ("all_squads_passing_types", "squads_passing_types"),
            ],
            "Squad_GCA_For": [
                ("all_stats_squads_gca_for", "stats_squads_gca_for"),
                ("all_squads_gca_for", "squads_gca_for"),
                ("all_stats_squads_gca", "stats_squads_gca"),
                ("all_squads_gca", "squads_gca"),
            ],
            "Squad_GCA_Against": [
                ("all_stats_squads_gca_against", "stats_squads_gca_against"),
                ("all_squads_gca_against", "squads_gca_against"),
                ("all_stats_squads_gca", "stats_squads_gca"),
                ("all_squads_gca", "squads_gca"),
            ],
            "Squad_Defense_For": [
                ("all_stats_squads_defense_for", "stats_squads_defense_for"),
                ("all_squads_defense_for", "squads_defense_for"),
                ("all_stats_squads_defense", "stats_squads_defense"),
                ("all_squads_defense", "squads_defense"),
            ],
            "Squad_Defense_Against": [
                ("all_stats_squads_defense_against", "stats_squads_defense_against"),
                ("all_squads_defense_against", "squads_defense_against"),
                ("all_stats_squads_defense", "stats_squads_defense"),
                ("all_squads_defense", "squads_defense"),
            ],
            "Squad_Possession_For": [
                ("all_stats_squads_possession_for", "stats_squads_possession_for"),
                ("all_squads_possession_for", "squads_possession_for"),
                ("all_stats_squads_possession", "stats_squads_possession"),
                ("all_squads_possession", "squads_possession"),
            ],
            "Squad_Possession_Against": [
                ("all_stats_squads_possession_against", "stats_squads_possession_against"),
                ("all_squads_possession_against", "squads_possession_against"),
                ("all_stats_squads_possession", "stats_squads_possession"),
                ("all_squads_possession", "squads_possession"),
            ],
            "Squad_PlayingTime_For": [
                ("all_stats_squads_playing_time_for", "stats_squads_playing_time_for"),
                ("all_squads_playing_time_for", "squads_playing_time_for"),
                ("all_stats_squads_playing_time", "stats_squads_playing_time"),
                ("all_squads_playing_time", "squads_playing_time"),
            ],
            "Squad_PlayingTime_Against": [
                ("all_stats_squads_playing_time_against", "stats_squads_playing_time_against"),
                ("all_squads_playing_time_against", "squads_playing_time_against"),
                ("all_stats_squads_playing_time", "stats_squads_playing_time"),
                ("all_squads_playing_time", "squads_playing_time"),
            ],
            "Squad_Misc_For": [
                ("all_stats_squads_misc_for", "stats_squads_misc_for"),
                ("all_squads_misc_for", "squads_misc_for"),
                ("all_stats_squads_misc", "stats_squads_misc"),
                ("all_squads_misc", "squads_misc"),
            ],
            "Squad_Misc_Against": [
                ("all_stats_squads_misc_against", "stats_squads_misc_against"),
                ("all_squads_misc_against", "squads_misc_against"),
                ("all_stats_squads_misc", "stats_squads_misc"),
                ("all_squads_misc", "squads_misc"),
            ],
            "Squad_GK_For": [
                ("all_stats_squads_keeper_for", "stats_squads_keeper_for"),
                ("all_stats_squads_keepers_for", "stats_squads_keepers_for"),
                ("all_squads_keeper_for", "squads_keeper_for"),
                ("all_squads_keepers_for", "squads_keepers_for"),
                ("all_stats_squads_keeper", "stats_squads_keeper"),
                ("all_squads_keeper", "squads_keeper"),
            ],
            "Squad_GK_Against": [
                ("all_stats_squads_keeper_against", "stats_squads_keeper_against"),
                ("all_stats_squads_keepers_against", "stats_squads_keepers_against"),
                ("all_squads_keeper_against", "squads_keeper_against"),
                ("all_squads_keepers_against", "squads_keepers_against"),
                ("all_stats_squads_keeper", "stats_squads_keeper"),
                ("all_squads_keeper", "squads_keeper"),
            ],
            "Squad_GKAdv_For": [
                ("all_stats_squads_keeper_adv_for", "stats_squads_keeper_adv_for"),
                ("all_stats_squads_keepers_adv_for", "stats_squads_keepers_adv_for"),
                ("all_squads_keeper_adv_for", "squads_keeper_adv_for"),
                ("all_squads_keepers_adv_for", "squads_keepers_adv_for"),
                ("all_stats_squads_keeper_adv", "stats_squads_keeper_adv"),
                ("all_squads_keeper_adv", "squads_keeper_adv"),
            ],
            "Squad_GKAdv_Against": [
                ("all_stats_squads_keeper_adv_against", "stats_squads_keeper_adv_against"),
                ("all_stats_squads_keepers_adv_against", "stats_squads_keepers_adv_against"),
                ("all_squads_keeper_adv_against", "squads_keeper_adv_against"),
                ("all_squads_keepers_adv_against", "squads_keepers_adv_against"),
                ("all_stats_squads_keeper_adv", "stats_squads_keeper_adv"),
                ("all_squads_keeper_adv", "squads_keeper_adv"),
            ],
        }

        # Ýtum á Sheets
        if league is not None:
            self.update_google_sheet(sheet_name, league, "League_Table")
        if players is not None:
            self.update_google_sheet(sheet_name, players, "Player_Stats")
        if fixtures is not None:
            self.update_google_sheet(sheet_name, fixtures, "Fixtures_Results")
        if squads_std_for is not None:
            self.update_google_sheet(sheet_name, squads_std_for, "Squad_Standard_For")

        # Rest af squad töflum (úr cachaðri stats-síðu)
        for worksheet_name, id_candidates in squad_tables.items():
            df = self.get_squad_table_generic(id_candidates)
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
