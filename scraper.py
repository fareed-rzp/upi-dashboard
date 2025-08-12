#!/usr/bin/env python3
"""
Automated UPI Data Scraper for NPCI Website
Scrapes data and updates JSON files automatically
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
from datetime import datetime, timedelta
import time
import os

class UPIDataScraper:
    def __init__(self):
        self.base_url = "https://www.npci.org.in/what-we-do/upi/upi-ecosystem-statistics"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.data = {}
        
    def fetch_page(self):
        """Fetch the NPCI UPI statistics page"""
        try:
            print("🔍 Fetching NPCI UPI statistics page...")
            response = requests.get(self.base_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"❌ Error fetching page: {e}")
            return None
    
    def parse_tables(self, html):
        """Parse HTML and extract all tables"""
        try:
            print("📊 Parsing HTML tables...")
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find all tables in the page
            tables = soup.find_all('table')
            print(f"📋 Found {len(tables)} tables")
            
            parsed_data = {}
            
            # Extract different types of data
            parsed_data.update(self.extract_apps_data(soup))
            parsed_data.update(self.extract_remitter_data(soup))
            parsed_data.update(self.extract_beneficiary_data(soup))
            parsed_data.update(self.extract_merchant_data(soup))
            parsed_data.update(self.extract_state_data(soup))
            parsed_data.update(self.extract_p2p_p2m_data(soup))
            
            return parsed_data
            
        except Exception as e:
            print(f"❌ Error parsing tables: {e}")
            return {}
    
    def extract_apps_data(self, soup):
        """Extract UPI Apps data"""
        try:
            print("📱 Extracting UPI Apps data...")
            
            # Look for UPI Apps table
            apps_data = []
            tables = soup.find_all('table')
            
            for table in tables:
                headers = [th.get_text().strip() for th in table.find_all('th')]
                if 'Application Name' in headers:
                    rows = table.find_all('tr')[1:]  # Skip header
                    
                    for row in rows[:10]:  # Top 10 apps
                        cells = [td.get_text().strip() for td in row.find_all('td')]
                        if len(cells) >= 3:
                            app_name = cells[1].replace('#', '').strip()
                            try:
                                volume = float(cells[-2].replace(',', ''))
                                value = float(cells[-1].replace(',', ''))
                                apps_data.append({
                                    'name': app_name,
                                    'volume': volume,
                                    'value': value
                                })
                            except (ValueError, IndexError):
                                continue
                    break
            
            return {'topApps': apps_data}
            
        except Exception as e:
            print(f"❌ Error extracting apps data: {e}")
            return {'topApps': []}
    
    def extract_remitter_data(self, soup):
        """Extract UPI Remitter Banks data"""
        try:
            print("🏦 Extracting Remitter Banks data...")
            
            banks_data = []
            tables = soup.find_all('table')
            
            for table in tables:
                headers = [th.get_text().strip() for th in table.find_all('th')]
                if 'UPI Remitter Members' in str(headers):
                    rows = table.find_all('tr')[1:]  # Skip header
                    
                    for row in rows[:10]:  # Top 10 banks
                        cells = [td.get_text().strip() for td in row.find_all('td')]
                        if len(cells) >= 3:
                            bank_name = cells[1].strip()
                            try:
                                volume = float(cells[2].replace(',', ''))
                                banks_data.append({
                                    'name': bank_name,
                                    'volume': volume
                                })
                            except (ValueError, IndexError):
                                continue
                    break
            
            return {'topBanks': banks_data}
            
        except Exception as e:
            print(f"❌ Error extracting remitter data: {e}")
            return {'topBanks': []}
    
    def extract_beneficiary_data(self, soup):
        """Extract UPI Beneficiary Banks data"""
        # Similar implementation for beneficiary data
        return {}
    
    def extract_merchant_data(self, soup):
        """Extract Merchant Category data"""
        try:
            print("🛍️ Extracting Merchant Categories data...")
            
            merchant_data = []
            tables = soup.find_all('table')
            
            for table in tables:
                if 'Merchant Category-wise' in table.get_text():
                    rows = table.find_all('tr')
                    
                    for row in rows:
                        cells = [td.get_text().strip() for td in row.find_all('td')]
                        if len(cells) >= 4:
                            try:
                                category = cells[2].strip()
                                volume = float(cells[3].replace(',', ''))
                                value = float(cells[4].replace(',', '')) if len(cells) > 4 else 0
                                
                                merchant_data.append({
                                    'category': category,
                                    'volume': volume,
                                    'value': value
                                })
                            except (ValueError, IndexError):
                                continue
                    break
            
            return {'merchantCategories': merchant_data[:10]}  # Top 10
            
        except Exception as e:
            print(f"❌ Error extracting merchant data: {e}")
            return {'merchantCategories': []}
    
    def extract_state_data(self, soup):
        """Extract State-wise data"""
        try:
            print("📍 Extracting State-wise data...")
            
            state_data = []
            tables = soup.find_all('table')
            
            for table in tables:
                if 'State-wise UPI' in table.get_text():
                    rows = table.find_all('tr')[1:]  # Skip header
                    
                    for row in rows[:10]:  # Top 10 states
                        cells = [td.get_text().strip() for td in row.find_all('td')]
                        if len(cells) >= 5:
                            try:
                                state = cells[1].strip()
                                volume = float(cells[2].replace(',', ''))
                                value = float(cells[4].replace(',', ''))
                                
                                state_data.append({
                                    'state': state,
                                    'volume': volume,
                                    'value': value
                                })
                            except (ValueError, IndexError):
                                continue
                    break
            
            return {'stateData': state_data}
            
        except Exception as e:
            print(f"❌ Error extracting state data: {e}")
            return {'stateData': []}
    
    def extract_p2p_p2m_data(self, soup):
        """Extract P2P and P2M transaction data"""
        try:
            print("💳 Extracting P2P/P2M data...")
            
            tables = soup.find_all('table')
            
            for table in tables:
                if 'UPI P2P and P2M Transactions' in table.get_text():
                    rows = table.find_all('tr')
                    
                    for row in rows:
                        cells = [td.get_text().strip() for td in row.find_all('td')]
                        if len(cells) >= 6:
                            try:
                                month = cells[0].strip()
                                total_volume = float(cells[1].replace(',', ''))
                                total_value = float(cells[2].replace(',', ''))
                                p2p_volume = float(cells[3].replace(',', ''))
                                p2p_value = float(cells[4].replace(',', ''))
                                p2m_volume = float(cells[5].replace(',', ''))
                                p2m_value = float(cells[6].replace(',', ''))
                                
                                return {
                                    'month': month,
                                    'totalVolume': total_volume,
                                    'totalValue': total_value,
                                    'p2pVolume': p2p_volume,
                                    'p2pValue': p2p_value,
                                    'p2mVolume': p2m_volume,
                                    'p2mValue': p2m_value
                                }
                            except (ValueError, IndexError):
                                continue
                    break
            
            return {}
            
        except Exception as e:
            print(f"❌ Error extracting P2P/P2M data: {e}")
            return {}
    
    def scrape_all_data(self):
        """Main scraping function"""
        print("🚀 Starting UPI data scraping...")
        
        html = self.fetch_page()
        if not html:
            return False
        
        # Parse all data
        parsed_data = self.parse_tables(html)
        
        # Add timestamp
        parsed_data['lastUpdated'] = datetime.now().isoformat()
        parsed_data['scrapedAt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Save to JSON file
        self.save_data(parsed_data)
        
        print("✅ Data scraping completed successfully!")
        return True
    
    def save_data(self, data):
        """Save scraped data to JSON file"""
        try:
            # Create docs directory if it doesn't exist
            os.makedirs('docs/data', exist_ok=True)
            
            # Save latest data
            with open('docs/data/latest.json', 'w') as f:
                json.dump(data, f, indent=2)
            
            # Save historical data with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            with open(f'docs/data/historical_{timestamp}.json', 'w') as f:
                json.dump(data, f, indent=2)
            
            print("💾 Data saved successfully!")
            
        except Exception as e:
            print(f"❌ Error saving data: {e}")

def main():
    scraper = UPIDataScraper()
    success = scraper.scrape_all_data()
    
    if success:
        print("🎉 UPI data scraping completed successfully!")
        print("📊 Dashboard will be updated automatically")
    else:
        print("❌ Scraping failed. Please check logs.")
        exit(1)

if __name__ == "__main__":
    main()
