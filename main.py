import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import schedule
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import logging
import os
import json
from io import StringIO

class PremierLeagueScraper:
    def __init__(self):
        """
        Frumstillir scraper-inn með öllum nauðsynlegum stillingum
        """
        self.base_url = "https://fbref.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.setup_logging()
        self.setup_google_sheets()
        
    def setup_logging(self):
        """Setur upp logging til að fylgjast með hvað er að gerast"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_google_sheets(self):
        """
        Setur upp tengingu við Google Sheets
        Notar environment variable fyrir credentials
        """
        try:
            self.logger.info("🔍 BYRJA Á GOOGLE SHEETS UPPSETNINGU...")
            
            # Reynir að fá credentials frá environment variable
            creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
            
            if creds_json:
                self.logger.info("✅ FANN GOOGLE_CREDENTIALS_JSON environment variable")
                
                # Býr til credentials frá JSON string
                creds_info = json.loads(creds_json)
                self.logger.info(f"📧 Service account email: {creds_info.get('client_email', 'EKKERT EMAIL')}")
                
                scope = [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"
                ]
                creds = Credentials.from_service_account_info(creds_info, scopes=scope)
                self.gc = gspread.authorize(creds)
                self.logger.info("✅ GOOGLE SHEETS TENGING TÓKST!")
                
                # Prófum tenginguna strax
                self.test_google_connection()
                
            else:
                self.logger.error("❌ ENGIN GOOGLE_CREDENTIALS_JSON environment variable!")
                # Fallback fyrir local testing
                if os.path.exists('credentials.json'):
                    self.logger.info("📁 Reyni local credentials.json skrá...")
                    scope = [
                        "https://spreadsheets.google.com/feeds",
                        "https://www.googleapis.com/auth/drive"
                    ]
                    creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
                    self.gc = gspread.authorize(creds)
                    self.logger.info("✅ GOOGLE SHEETS TENGING TÓKST (local)!")
                    self.test_google_connection()
                else:
                    self.logger.error("❌ ENGAR GOOGLE CREDENTIALS FUNDNAR!")
                    self.gc = None
                    
        except Exception as e:
            self.logger.error(f"💥 VILLA VIÐ GOOGLE SHEETS UPPSETNINGU: {e}")
            self.gc = None

    def test_google_connection(self):
        """Prófar Google tengingu með því að lista sheets"""
        if self.gc is None:
            self.logger.error("❌ ENGIN GOOGLE TENGING TIL AÐ PRÓFA!")
            return False
        
        try:
            self.logger.info("🧪 PRÓFA GOOGLE TENGINGU...")
            
            # Reynir bara að lista sheets - þarf ekki að búa til nýtt
            sheets = self.gc.list_spreadsheet_files()
            self.logger.info(f"✅ GOOGLE TENGING VIRKAR - FANN {len(sheets)} SHEETS")
            
            return True
            
        except Exception as e:
            self.logger.error(f"💥 VILLA VIÐ TEST: {e}")
            return False
            
    def get_premier_league_table(self):
        """Sækir Premier League töfluna"""
        try:
            self.logger.info("🏴󠁧󠁢󠁥󠁮󠁧󠁿 SÆKI PREMIER LEAGUE TÖFLU...")
            
            url = f"{self.base_url}/en/comps/9/Premier-League-Stats"
            response = requests.get(url, headers=self.headers)
            self.logger.info(f"📡 HTTP Status: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Finnur töfluna
            table = soup.find('table', {'class': 'stats_table'})
            
            if table:
                # Notar pandas til að lesa töfluna
                df = pd.read_html(StringIO(str(table)))[0]
                
                # Hreinsar töflu
                if len(df.columns) > 1:
                    # Fjarlægir multi-level headers ef til staðar
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(col).strip() for col in df.columns.values]
                    
                    df['Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    self.logger.info(f"✅ PL TAFLA: {len(df)} lið fundin")
                    return df
            else:
                self.logger.error("❌ FANN EKKI PL TÖFLU Á SÍÐUNNI")
                    
        except Exception as e:
            self.logger.error(f"💥 VILLA VIÐ PL TÖFLU: {e}")
            return None
            
    def get_player_stats(self):
        """
        Sækir leikmannastölfræði - mikilvægt fyrir fantasy og veðmál
        """
        try:
            self.logger.info("⚽ SÆKI LEIKMANNASTÖLFRÆÐI...")
            
            # Byrjum á grunnstölfræði
            url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
            response = requests.get(url, headers=self.headers)
            self.logger.info(f"📡 HTTP Status: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Finnur player stats töflu
            table = soup.find('table', {'id': 'stats_standard'})
            
            if table:
                df = pd.read_html(StringIO(str(table)))[0]
                
                # Hreinsar multi-level columns
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(col).strip() for col in df.columns.values]
                
                df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger.info(f"✅ LEIKMANNAGÖGN: {len(df)} leikmenn fundnir")
                return df
            else:
                self.logger.error("❌ FANN EKKI LEIKMANNASTÖLFRÆÐI")
                
        except Exception as e:
            self.logger.error(f"💥 VILLA VIÐ LEIKMANNAGÖGN: {e}")
            return None
            
    def get_fixtures_and_results(self):
        """
        Sækir leiki og niðurstöður - mikilvægt fyrir betting
        """
        try:
            self.logger.info("📅 SÆKI LEIKI OG NIÐURSTÖÐUR...")
            
            url = f"{self.base_url}/en/comps/9/schedule/Premier-League-Fixtures"
            response = requests.get(url, headers=self.headers)
            self.logger.info(f"📡 HTTP Status: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            table = soup.find('table', {'class': 'stats_table'})
            if table:
                df = pd.read_html(StringIO(str(table)))[0]
                df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger.info(f"✅ LEIKJAGÖGN: {len(df)} leikir fundnir")
                return df
            else:
                self.logger.error("❌ FANN EKKI LEIKJATÖFLU")
                
        except Exception as e:
            self.logger.error(f"💥 VILLA VIÐ LEIKJAGÖGN: {e}")
            return None
            
    def update_google_sheet(self, sheet_name, data, worksheet_name):
        """
        Uppfærir Google Sheet með nýjum gögnum
        """
        if self.gc is None:
            self.logger.error("❌ ENGIN GOOGLE SHEETS TENGING")
            return
            
        try:
            self.logger.info(f"📊 UPPFÆRI {worksheet_name} Í {sheet_name}...")
            
            # Opnar eða býr til sheet
            try:
                sheet = self.gc.open(sheet_name)
                self.logger.info(f"✅ OPNAÐI NÚVERANDI SHEET: {sheet.title}")
            except gspread.SpreadsheetNotFound:
                self.logger.info(f"🆕 BÝ TIL NÝTT SHEET: {sheet_name}")
                sheet = self.gc.create(sheet_name)
                # Deilir með þínum email
                sheet.share('arnortumio@gmail.com', perm_type='user', role='owner')
                self.logger.info("✅ SHEET DEILT MEÐ arnortumio@gmail.com")
                self.logger.info(f"🔗 SHEET URL: {sheet.url}")
            except Exception as create_error:
                self.logger.error(f"💥 VILLA VIÐ AÐ BÚA TIL SHEET: {create_error}")
                # Reynum að nota þitt persónulega Drive
                self.logger.info("🔄 REYNI AÐ NOTA EXISTING SHEET...")
                try:
                    # Býr til sheet með öðru nafni
                    import random
                    backup_name = f"PL_Data_{random.randint(1000,9999)}"
                    sheet = self.gc.create(backup_name)
                    sheet.share('arnortumio@gmail.com', perm_type='user', role='owner')
                    self.logger.info(f"✅ BACKUP SHEET BÚIÐ TIL: {sheet.url}")
                except Exception as e:
                    self.logger.error(f"💥 GETUR EKKI BÚIÐ TIL NEITT SHEET: {e}")
                    return
                
            # Opnar eða býr til worksheet
            try:
                worksheet = sheet.worksheet(worksheet_name)
                worksheet.clear()  # Hreinsar eldri gögn
                self.logger.info(f"♻️ HREINSAR NÚVERANDI {worksheet_name}")
            except:
                self.logger.info(f"🆕 BÝ TIL NÝJAN TAB: {worksheet_name}")
                worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="30")
                
            # Setur gögn inn
            if data is not None and not data.empty:
                # Breytir pandas DataFrame í lista af listum
                data_list = [data.columns.tolist()] + data.values.tolist()
                
                # Takmarkar við 1000 röðum til að vera viss um að falla ekki
                if len(data_list) > 1000:
                    data_list = data_list[:1000]
                    
                worksheet.update('A1', data_list)
                self.logger.info(f"✅ UPPFÆRÐI {worksheet_name} MEÐ {len(data)} RÖÐUM")
            else:
                self.logger.error(f"❌ ENGIN GÖGN TIL AÐ UPPFÆRA Í {worksheet_name}")
            
        except Exception as e:
            self.logger.error(f"💥 VILLA VIÐ UPPFÆRSLU Á {worksheet_name}: {e}")
            
    def full_update(self):
        """
        Keyrir alla scraping og uppfærir öll sheet
        """
        self.logger.info("🚀 BYRJA FULL UPDATE...")
        
        # Prófar Google tengingu fyrst
        if not self.test_google_connection():
            self.logger.error("❌ GOOGLE TENGING VIRKAR EKKI - HÆTTI")
            return
        
        # Sækir öll gögn
        self.logger.info("📊 SÆKI ÖLL GÖGN...")
        table_data = self.get_premier_league_table()
        player_data = self.get_player_stats()
        fixtures_data = self.get_fixtures_and_results()
        
        sheet_name = "PL_Fantasy_Data"
        
        # Uppfærir Premier League töflu
        if table_data is not None:
            self.update_google_sheet(sheet_name, table_data, "League_Table")
        else:
            self.logger.error("❌ ENGIN PL TAFLA TIL AÐ UPPFÆRA")
            
        # Uppfærir leikmannagögn
        if player_data is not None:
            self.update_google_sheet(sheet_name, player_data, "Player_Stats")
        else:
            self.logger.error("❌ ENGIN LEIKMANNAGÖGN TIL AÐ UPPFÆRA")
                
        # Uppfærir leiki og niðurstöður
        if fixtures_data is not None:
            self.update_google_sheet(sheet_name, fixtures_data, "Fixtures_Results")
        else:
            self.logger.error("❌ ENGIN LEIKJAGÖGN TIL AÐ UPPFÆRA")
            
        self.logger.info("🎉 FULL UPDATE LOKIÐ!")
        
    def run_once(self):
        """Keyrir eina uppfærslu - gott fyrir testing"""
        self.full_update()
        
    def start_scheduler(self):
        """
        Setur upp tímasetningar fyrir uppfærslur
        """
        # Uppfærir á hverjum 30 mín
        schedule.every(30).minutes.do(self.full_update)
        
        # Uppfærir einu sinni á dag
        schedule.every().day.at("08:00").do(self.full_update)
        
        self.logger.info("⏰ SCHEDULER SETTUR UPP!")
        
        # Keyrir fyrstu uppfærslu strax
        self.full_update()
        
        # Keyrir síðan í endalausa lykkju
        while True:
            schedule.run_pending()
            time.sleep(60)  # Athugar á mínútu fresti

def run_web_server():
    """Keyrir einfaldan web server fyrir Render"""
    from http.server import HTTPServer, SimpleHTTPRequestHandler
    import threading
    
    class Handler(SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>PL Scraper Status</title>
                <meta http-equiv="refresh" content="30">
            </head>
            <body>
                <h1>🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League Scraper</h1>
                <p><strong>Status:</strong> RUNNING! ✅</p>
                <p><strong>Check your Google Sheets for data:</strong></p>
                <ul>
                    <li>Sheet name: "PL_Fantasy_Data"</li>
                    <li>Tabs: League_Table, Player_Stats, Fixtures_Results</li>
                </ul>
                <p><em>This page refreshes every 30 seconds</em></p>
                <p><small>Deploy time: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</small></p>
            </body>
            </html>
            """
            
            self.wfile.write(html_content.encode('utf-8'))

    port = int(os.environ.get('PORT', 8000))
    server = HTTPServer(('0.0.0.0', port), Handler)
    
    def start_server():
        print(f"🌐 Web server running on port {port}")
        server.serve_forever()
    
    # Keyrir web server í background thread
    thread = threading.Thread(target=start_server)
    thread.daemon = True
    thread.start()

def main():
    """
    Aðalfall til að keyra scraper-inn
    """
    print("🚀 BYRJA PREMIER LEAGUE SCRAPER...")
    
    scraper = PremierLeagueScraper()
    
    # Athugar hvort þetta er production eða development
    if os.environ.get('RENDER') or os.environ.get('RAILWAY_ENVIRONMENT'):
        print("☁️ PRODUCTION MODE - KEYRI WEB SERVER OG SCHEDULER")
        # Production - keyrir web server og scheduler
        run_web_server()
        scraper.start_scheduler()
    else:
        # Development - keyrir bara einu sinni
        print("💻 DEVELOPMENT MODE - EINSKIPTIS UPPFÆRSLA")
        scraper.run_once()

if __name__ == "__main__":
    main()
