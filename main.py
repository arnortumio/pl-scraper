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

    # --------------------------- FBref hjálparföll --------------------------- #
    def get_html_table(self, url, div_id=None, table_id=None):
        try:
            response = self.session.get(url, timeout=30)
            self.logger.info(f"📡 HTTP Status: {response.status_code} ({url})")
            if response.status_code != 200:
                return None
            soup = BeautifulSoup(response.text, 'html.parser')
            if div_id:
                div = soup.find('div', id=div_id)
                comment = div.find(string=lambda text: isinstance(text, Comment)) if div else None
                soup = BeautifulSoup(comment, 'html.parser') if comment else soup
            table = soup.find('table', {'id': table_id}) if table_id else soup.find('table', {'class': 'stats_table'})
            return table
        except Exception as e:
            self.logger.error(f"💥 Villa við að sækja töflu: {e}")
            return None

    def get_premier_league_table(self):
        self.logger.info("🏴 Sæki Premier League töflu...")
        url = f"{self.base_url}/en/comps/9/Premier-League-Stats"
        table = self.get_html_table(url, div_id='all_results2024-2025_9_overall')
        if table:
            df = pd.read_html(str(table))[0]
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(col).strip() for col in df.columns.values]
            df['Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"✅ PL tafla fundin: {len(df)} lið")
            return df
        self.logger.error("❌ Gat ekki fundið PL töflu.")
        return None

    def get_player_stats(self):
        self.logger.info("⚽ Sæki leikmannastatistík (FBref)...")
        url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
        table = self.get_html_table(url, div_id='all_stats_standard', table_id='stats_standard')
        if table:
            df = pd.read_html(str(table))[0]
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(col).strip() for col in df.columns.values]
            df['Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"✅ Leikmenn fundnir (FBref): {len(df)}")
            return df
        self.logger.error("❌ Gat ekki fundið leikmannatöflu (FBref).")
        return None

    def get_fixtures_and_results(self):
        self.logger.info("📅 Sæki leikjaupplýsingar (FBref)...")
        url = f"{self.base_url}/en/comps/9/schedule/Premier-League-Fixtures"
        table = self.get_html_table(url, div_id='all_sched_ks_3232_1')
        if table:
            df = pd.read_html(str(table))[0]
            df['Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"✅ Leikir fundnir (FBref): {len(df)}")
            return df
        self.logger.error("❌ Gat ekki fundið leikjatöflu (FBref).")
        return None

    # --------------------------- FPL hjálparföll ---------------------------- #
    def _json_get(self, url: str):
        """Örugg JSON beiðni með skýrri villumeðhöndlun."""
        try:
            r = self.session.get(url, timeout=30)
            self.logger.info(f"📡 HTTP Status: {r.status_code} ({url})")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            self.logger.error(f"💥 Villa við JSON beiðni á {url}: {e}")
            return None

    def get_fpl_data(self):
        """
        Sækir *öll* almenn FPL gögn (án innskráningar) og skilar sem dict af DataFrame-um.
        Notar:
          - https://fantasy.premierleague.com/api/bootstrap-static/
          - https://fantasy.premierleague.com/api/fixtures/
        """
        self.logger.info("🧩 Sæki FPL gögn (bootstrap-static, fixtures)...")

        bootstrap_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
        fixtures_url = "https://fantasy.premierleague.com/api/fixtures/"

        data = self._json_get(bootstrap_url)
        fixtures = self._json_get(fixtures_url)

        if data is None:
            self.logger.error("❌ Engin FPL bootstrap gögn fengust.")
            return {}

        dfs = {}
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        def dfize(obj, name):
            try:
                df = pd.json_normalize(obj)
                df['Last_Updated'] = timestamp
                dfs[name] = df
                self.logger.info(f"✅ FPL {name}: {len(df)} raðir")
            except Exception as e:
                self.logger.error(f"💥 Gat ekki umbreytt {name} í DataFrame: {e}")

        # Helstu listar í bootstrap-static
        for key in [
            'events',            # Umferðir (Gameweeks)
            'teams',             # FPL-lið (mapping við PL-lið)
            'elements',          # Leikmenn með FPL-eiginleikum
            'element_types',     # Stöðurnar (GK/DEF/MID/FWD)
            'phases',            # Fasalýsingar (t.d. pre-season/season)
            'game_settings'      # Stillingar (einn hlut, ekki listi)
        ]:
            val = data.get(key)
            if val is None:
                self.logger.warning(f"⚠️ '{key}' fannst ekki í FPL-gögnum.")
                continue
            if isinstance(val, list):
                dfize(val, f"FPL_{key.capitalize()}")
            else:
                # Eitt JSON-obj — setjum sem DataFrame með einni línu
                dfize([val], f"FPL_{key.capitalize()}")

        # Heildarfjöldi leikmanna í leiknum
        total_players = data.get('total_players')
        if total_players is not None:
            df_total = pd.DataFrame([{'total_players': total_players, 'Last_Updated': timestamp}])
            dfs['FPL_Total_Players'] = df_total
            self.logger.info("✅ FPL Total_Players bætt við")

        # Fixtures (allir leikir með FPL-ID, finished o.fl.)
        if fixtures is not None:
            try:
                df_fixt = pd.json_normalize(fixtures)
                df_fixt['Last_Updated'] = timestamp
                dfs['FPL_Fixtures_API'] = df_fixt
                self.logger.info(f"✅ FPL Fixtures: {len(df_fixt)} raðir")
            except Exception as e:
                self.logger.error(f"💥 Gat ekki umbreytt fixtures í DataFrame: {e}")

        return dfs

    # --------------------------- Sheets hjálparföll ------------------------- #
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
                # Breyttu netfangi hér ef þú vilt deila með öðrum
                sheet.share('your-email@example.com', perm_type='user', role='writer')

            try:
                worksheet = sheet.worksheet(worksheet_name)
                worksheet.clear()
            except gspread.WorksheetNotFound:
                # vel stórt default pláss
                worksheet = sheet.add_worksheet(title=worksheet_name, rows="5000", cols="200")

            if data is not None and not data.empty:
                # Tryggja að dálkheit séu strengir og unique
                cols = [str(c) for c in data.columns.tolist()]
                # Sumir JSON-reitir geta verið list/dict — varpa í streng fyrir Sheets
                df = data.copy()
                for c in df.columns:
                    df[c] = df[c].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)

                data_list = [cols] + df.values.tolist()
                cleaned_data = self.clean_data_for_sheets(data_list)
                try:
                    worksheet.update('A1', cleaned_data)
                    self.logger.info(f"✅ Uppfærði {worksheet_name} með {len(df)} röðum.")
                except Exception as e:
                    self.logger.error(f"💥 Villa við uppfærslu á worksheet.update fyrir {worksheet_name}: {e}")
            else:
                self.logger.warning(f"⚠️ Engin gögn til að uppfæra í {worksheet_name}.")
        except Exception as e:
            self.logger.error(f"💥 Villa við að nálgast eða búa til sheet/worksheet: {e}")

    # --------------------------- Keyrsluföll ------------------------------- #
    def full_update(self):
        self.logger.info("🚀 Byrja fulla uppfærslu...")
        if not self.test_google_connection():
            self.logger.error("❌ Engin virk Google tenging.")
            return

        sheet_name = "PL_Fantasy_Data"

        # FBref
        league = self.get_premier_league_table()
        players = self.get_player_stats()
        fixtures = self.get_fixtures_and_results()

        if league is not None:
            self.update_google_sheet(sheet_name, league, "League_Table")
        if players is not None:
            self.update_google_sheet(sheet_name, players, "Player_Stats")
        if fixtures is not None:
            self.update_google_sheet(sheet_name, fixtures, "Fixtures_Results")

        # FPL (nýtt!)
        fpl_dfs = self.get_fpl_data()
        for ws_name, df in fpl_dfs.items():
            self.update_google_sheet(sheet_name, df, ws_name)

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

# --------------------------- Einfaldur vefþjónn ---------------------------- #
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

# --------------------------- main ----------------------------------------- #
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
