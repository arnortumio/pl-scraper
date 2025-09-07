import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import schedule
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import os
import json

class PremierLeagueScraper:
    def __init__(self):
        """
        Frumstillir scraper-inn með öllum nauðsynlegum stillingum
        """
        self.base_url = "https://fbref.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
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
        Þarf að hafa credentials.json skrá í sama möppunni
        """
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                'credentials.json', scope
            )
            self.gc = gspread.authorize(creds)
            self.logger.info("Google Sheets tenging tókst!")
        except Exception as e:
            self.logger.error(f"Villa við tengingu við Google Sheets: {e}")
            
    def get_premier_league_table(self):
        """Sækir Premier League töfluna"""
        try:
            url = f"{self.base_url}/en/comps/9/Premier-League-Stats"
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Finnur töfluna
            table = soup.find('table', {'id': 'results2024-202591_overall'})
            if not table:
                table = soup.find('table', class_='stats_table')
                
            if table:
                df = pd.read_html(str(table))[0]
                df['Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                return df
            
        except Exception as e:
            self.logger.error(f"Villa við að sækja töflu: {e}")
            return None
            
    def get_player_stats(self):
        """
        Sækir leikmannastölfræði - mikilvægt fyrir fantasy og veðmál
        """
        try:
            # Mismunandi tölfræðiflokkar
            stat_types = {
                'standard': 'standard',  # Grunnstölfræði
                'shooting': 'shooting',   # Skot og mörk
                'passing': 'passing',     # Sendingar
                'defense': 'defense',     # Vörn
                'possession': 'possession' # Boltaeign
            }
            
            all_player_data = []
            
            for stat_name, stat_url in stat_types.items():
                url = f"{self.base_url}/en/comps/9/{stat_url}/Premier-League-Stats"
                response = requests.get(url, headers=self.headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Finnur tölfræðitöfluna
                table = soup.find('table', {'id': f'stats_{stat_url}'})
                if table:
                    df = pd.read_html(str(table))[0]
                    df['stat_type'] = stat_name
                    df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    all_player_data.append(df)
                    
                time.sleep(2)  # Bíður svo við overload-um ekki síðuna
                
            return all_player_data
            
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
            
            table = soup.find('table', {'id': 'sched_2024-2025_9_1'})
            if table:
                df = pd.read_html(str(table))[0]
                df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                return df
                
        except Exception as e:
            self.logger.error(f"Villa við að sækja leiki: {e}")
            return None
            
    def get_injury_data(self):
        """
        Reynir að sækja meiðslaupplýsingar - mjög mikilvægt fyrir fantasy
        """
        try:
            # Þetta er flóknara því FBref hefur ekki alltaf injury data
            # En við getum skoðað hverjir eru ekki að spila
            url = f"{self.base_url}/en/comps/9/stats/Premier-League-Stats"
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Getum greint hverjir hafa ekki spilað nýlega
            # Þetta er basic útgáfa - betri injury data þarf aðrar síður
            
            return None  # Útfærum betur síðar
            
        except Exception as e:
            self.logger.error(f"Villa við meiðslagögn: {e}")
            return None
            
    def update_google_sheet(self, sheet_name, data, worksheet_name):
        """
        Uppfærir Google Sheet með nýjum gögnum
        """
        try:
            # Opnar eða býr til sheet
            try:
                sheet = self.gc.open(sheet_name)
            except:
                sheet = self.gc.create(sheet_name)
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
        if player_data:
            for i, df in enumerate(player_data):
                sheet_title = f"Player_Stats_{df['stat_type'].iloc[0] if 'stat_type' in df.columns else i}"
                self.update_google_sheet(sheet_name, df, sheet_title)
                
        # Uppfærir leiki og niðurstöður
        if fixtures_data is not None:
            self.update_google_sheet(sheet_name, fixtures_data, "Fixtures_Results")
            
        self.logger.info("Full update lokið!")
        
    def start_scheduler(self):
        """
        Setur upp tímasetningar fyrir uppfærslur
        """
        # Uppfærir á hverjum 30 mín á leikjadögum (laugardaga og sunnudaga)
        schedule.every(30).minutes.do(self.full_update).tag('game-day')
        
        # Uppfærir einu sinni á dag á virkum dögum
        schedule.every().day.at("08:00").do(self.full_update).tag('daily')
        
        # Uppfærir þrisvar sinnum á leikjadögum
        schedule.every().saturday.at("12:00").do(self.full_update)
        schedule.every().saturday.at("15:00").do(self.full_update)
        schedule.every().saturday.at("17:30").do(self.full_update)
        schedule.every().sunday.at("14:00").do(self.full_update)
        schedule.every().sunday.at("16:30").do(self.full_update)
        
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
    scraper.start_scheduler()

if __name__ == "__main__":
    main()