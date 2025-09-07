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
            # Reynir að fá credentials frá environment variable
            creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
            
            if creds_json:
                # Býr til credentials frá JSON string
                creds_info = json.loads(creds_json)
                scope = [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"
                ]
                creds = Credentials.from_service_account_info(creds_info, scopes=scope)
                self.gc = gspread.authorize(creds)
                self.logger.info("Google Sheets tenging tókst!")
            else:
                # Fallback fyrir local testing
                if os.path.exists('credentials.json'):
                    scope = [
                        "https://spreadsheets.google.com/feeds",
                        "https://www.googleapis.com/auth/drive"
                    ]
                    creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
                    self.gc = gspread.authorize(creds)
                    self.logger.info("Google Sheets tenging tókst (local)!")
                else:
                    self.logger.error("Engar Google credentials fundnar")
                    self.gc = None
                    
        except Exception as e:
            self.logger.error(f"Villa við tengingu við Google Sheets: {e}")
            self.gc = None
            
    def get_premier_league_table(self):
        """Sækir Premier League töfluna"""
        try:
            url = f"{self.base_url}/en/comps/9/Premier-League-Stats"
            response = requests.get(url, headers=self.headers)
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
                    self.logger.info(f"Sótti PL töflu með {len(df)} liðum")
                    return df
                    
        except Exception as e:
            self.logger.error(f"Villa við að sækja töflu: {e}")
            return None
            
    def get_player_stats(self):
        """
        Sækir leikmannastölfræði - mikilvægt fyrir fantasy og veðmál
        """
        try:
            # Byrjum á grunnstölfræði
            url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Finnur player stats töflu
            table = soup.find('table', {'id': 'stats_standard'})
            
            if table:
                df = pd.read_html(StringIO(str(table)))[0]
                
                # Hreinsar multi-level columns
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(col).strip() for col in df.columns.values]
                
                df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger.info(f"Sótti gögn um {len(df)} leikmenn")
                return df
                
        except Exception as e:
            self.logger.error(f"Villa við að sækja leikmannastölfræði: {e}")
            return None
            
    def get_fixtures_and_results(self):
        """
        Sækir leiki og niðurstöður - mikilvægt fyrir betting
        """
        try:
            url = f"{self.base_url}/en/comps/9/schedule/Premier-League-Fixtures"
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            table = soup.find('table', {'class': 'stats_table'})
            if table:
                df = pd.read_html(StringIO(str(table)))[0]
                df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger.info(f"Sótti {len(df)} leiki")
                return df
                
        except Exception as e:
            self.logger.error(f"Villa við að sækja leiki: {e}")
            return None
            
    def update_google_sheet(self, sheet_name, data, worksheet_name):
        """
        Uppfærir Google Sheet með nýjum gögnum
        """
        if self.gc is None:
            self.logger.error("Engin Google Sheets tenging")
            return
            
        try:
            # Opnar eða býr til sheet
            try:
                sheet = self.gc.open(sheet_name)
            except:
                sheet = self.gc.create(sheet_name)
                # Deilir með þínum email
                sheet.share('arnortumio@gmail.com', perm_type='user', role='owner')
                
            # Opnar eða býr til worksheet
            try:
                worksheet = sheet.worksheet(worksheet_name)
                worksheet.clear()  # Hreinsar eldri gögn
            except:
                worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="30")
                
            # Setur gögn inn
            if data is not None and not data.empty:
                # Breytir pandas DataFrame í lista af listum
                data_list = [data.columns.tolist()] + data.values.tolist()
                
                # Takmarkar við 1000 röðum til að vera viss um að falla ekki
                if len(data_list) > 1000:
                    data_list = data_list[:1000]
                    
                worksheet.update('A1', data_list)
                self.logger.info(f"Uppfærði {worksheet_name} með {len(data)} röðum")
            
        except Exception as e:
            self.logger.error(f"Villa við uppfærslu á Google Sheet: {e}")
            
    def full_update(self):
        """
        Keyrir alla scraping og uppfærir öll sheet
        """
        self.logger.info("Byrja full update...")
        
        # Sækir öll gögn
        table_data = self.get_premier_league_table()
        player_data = self.get_player_stats()
        fixtures_data = self.get_fixtures_and_results()
        
        sheet_name = "PL_Fantasy_Data"
        
        # Uppfærir Premier League töflu
        if table_data is not None:
            self.update_google_sheet(sheet_name, table_data, "League_Table")
            
        # Uppfærir leikmannagögn
        if player_data is not None:
            self.update_google_sheet(sheet_name, player_data, "Player_Stats")
                
        # Uppfærir leiki og niðurstöður
        if fixtures_data is not None:
            self.update_google_sheet(sheet_name, fixtures_data, "Fixtures_Results")
            
        self.logger.info("Full update lokið!")
        
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
        
        self.logger.info("Scheduler settur upp!")
        
        # Keyrir fyrstu uppfærslu strax
        self.full_update()
        
        # Keyrir síðan í endalausa lykkju
        while True:
            schedule.run_pending()
            time.sleep(60)  # Athugar á mínútu fresti

def main():
    """
    Aðalfall til að keyra scraper-inn
    """
    scraper = PremierLeagueScraper()
    
    # Athugar hvort þetta er production eða development
    if os.environ.get('RENDER') or os.environ.get('RAILWAY_ENVIRONMENT'):
        # Production - keyrir scheduler
        scraper.start_scheduler()
    else:
        # Development - keyrir bara einu sinni
        print("Keyrir í development mode - eintskiptis uppfærsla")
        scraper.run_once()

if __name__ == "__main__":
    main()