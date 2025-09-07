import requests
from bs4 import BeautifulSoup
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

class PremierLeagueScraper:
    def __init__(self):
        """
        Frumstillir scraper-inn með öllum nauðsynlegum stillingum
        """
        self.base_url = "https://fbref.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
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
            
            creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
            
            if creds_json:
                self.logger.info("✅ FANN GOOGLE_CREDENTIALS_JSON environment variable")
                
                creds_info = json.loads(creds_json)
                self.logger.info(f"📧 Service account email: {creds_info.get('client_email', 'EKKERT EMAIL')}")
                
                scope = [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"
                ]
                creds = Credentials.from_service_account_info(creds_info, scopes=scope)
                self.gc = gspread.authorize(creds)
                self.logger.info("✅ GOOGLE SHEETS TENGING TÓKST!")
                
                self.test_google_connection()
                
            else:
                self.logger.error("❌ ENGIN GOOGLE_CREDENTIALS_JSON environment variable!")
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
            
            sheets = self.gc.list_spreadsheet_files()
            self.logger.info(f"✅ GOOGLE TENGING VIRKAR - FANN {len(sheets)} SHEETS")
            
            return True
            
        except Exception as e:
            self.logger.error(f"💥 VILLA VIÐ TEST: {e}")
            return False
            
    def get_premier_league_table(self):
        """Sækir Premier League töfluna"""
        try:
            self.logger.info("🏴 SÆKI PREMIER LEAGUE TÖFLU...")
            
            url = f"{self.base_url}/en/comps/9/Premier-League-Stats"
            
            time.sleep(2)
            response = self.session.get(url, timeout=30)
            self.logger.info(f"📡 HTTP Status: {response.status_code}")
            
            if response.status_code != 200:
                self.logger.error(f"❌ BAD HTTP RESPONSE: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'class': 'stats_table'})
            
            if table:
                df = pd.read_html(StringIO(str(table)))[0]
                
                if len(df.columns) > 1:
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(col).strip() for col in df.columns.values]
                    
                    df['Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    self.logger.info(f"✅ PL tafla fundin: {len(df)} lið")
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
            
            url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
            response = self.session.get(url)
            self.logger.info(f"📡 HTTP Status: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'id': 'stats_standard'})
            
            if table:
                df = pd.read_html(StringIO(str(table)))[0]
                
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(col).strip() for col in df.columns.values]
                
                df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger.info(f"✅ Leikmenn fundnir: {len(df)}")
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
            response = self.session.get(url)
            self.logger.info(f"📡 HTTP Status: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'class': 'stats_table'})
            if table:
                df = pd.read_html(StringIO(str(table)))[0]
                df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger.info(f"✅ Leikir fundnir: {len(df)}")
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
            self.logger.info(f"📊 Uppfæri {worksheet_name} í {sheet_name}...")
            
            try:
                sheet = self.gc.open(sheet_name)
                self.logger.info(f"✅ Opnaði núverandi sheet: {sheet.title}")
            except gspread.SpreadsheetNotFound:
                self.logger.info(f"🆕 Bý til nýtt sheet: {sheet_name}")
                sheet = self.gc.create(sheet_name)
                sheet.share('arnortumio@gmail.com', perm_type='user', role='owner')
                self.logger.info("✅ Sheet deilt með arnortumio@gmail.com")
                self.logger.info(f"🔗 Sheet URL: {sheet.url}")
            except Exception as create_error:
                self.logger.error(f"💥 Villa við að búa til sheet: {create_error}")
                self.logger.info("🔄 Reyni að nota existing sheet...")
                try:
                    import random
                    backup_name = f"PL_Data_{random.randint(1000,9999)}"
                    sheet = self.gc.create(backup_name)
                    sheet.share('arnortumio@gmail.com', perm_type='user', role='owner')
                    self.logger.info(f"✅ Backup sheet búið til: {sheet.url}")
                except Exception as e:
                    self.logger.error(f"💥 Getur ekki búið til neitt sheet: {e}")
                    return
                
            try:
                worksheet = sheet.worksheet(worksheet_name)
                worksheet.clear()
                self.logger.info(f"♻️ Hreinsar núverandi {worksheet_name}")
            except gspread.WorksheetNotFound:
                self.logger.info(f"🆕 Bý til nýjan tab: {worksheet_name}")
                worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="30")
                
            if data is not None and not data.empty:
                # Lagað: Fjarlægja NaN til að forðast JSON villu í Google Sheets API
                data = data.fillna("")
                data_list = [data.columns.tolist()] + data.values.tolist()
                worksheet.update('A1', data_list)
                self.logger.info(f"✅ Uppfærði {worksheet_name} með {len(data)} röðum.")
            else:
                self.logger.warning(f"⚠️ Engin gögn til að uppfæra í {worksheet_name}.")
            
        except Exception as e:
            self.logger.error(f"💥 Villa við uppfærslu á {worksheet_name}: {e}")
            
    def full_update(self):
        """
        Keyrir alla scraping og uppfærir öll sheet
        """
        self.logger.info("🚀 Byrja fulla uppfærslu...")
        
        if not self.test_google_connection():
            self.logger.error("❌ Google tenging virkar ekki - hætti")
            return
        
        self.logger.info("📊 Sæki öll gögn...")
        table_data = self.get_premier_league_table()
        player_data = self.get_player_stats()
        fixtures_data = self.get_fixtures_and_results()
        
        sheet_name = "Premier_League_Data"
        
        self.update_google_sheet(sheet_name, table_data, "League_Table")
        self.update_google_sheet(sheet_name, player_data, "Player_Stats")
        self.update_google_sheet(sheet_name, fixtures_data, "Fixtures")
        
        self.logger.info("✅ Full uppfærsla loki")
        
    def start_scheduler(self):
        """
        Setur upp schedule til að keyra full_update tvisvar á dag
        """
        self.logger.info("⏰ Scheduler settur upp.")
        schedule.every().day.at("08:00").do(self.full_update)
        schedule.every().day.at("20:00").do(self.full_update)
        
        self.logger.info("🕒 Scheduler keyrir nú...")
        while True:
            schedule.run_pending()
            time.sleep(1)
            

if __name__ == "__main__":
    scraper = PremierLeagueScraper()
    scraper.full_update()
    scraper.start_scheduler()
