#!/usr/bin/env python3
"""
Data processing script that updates the dashboard with scraped data
"""

import json
import os
from datetime import datetime
import glob

def process_and_update_dashboard():
    """Process scraped data and update dashboard files"""
    
    print("🔄 Processing scraped data...")
    
    try:
        # Load latest scraped data
        with open('docs/data/latest.json', 'r') as f:
            latest_data = json.load(f)
        
        # Load existing historical data
        historical_file = 'docs/js/historical_data.js'
        historical_data = load_historical_data(historical_file)
        
        # Update historical data with latest
        current_month = latest_data.get('month', datetime.now().strftime("%b-%y"))
        historical_data[current_month] = latest_data
        
        # Keep only last 24 months
        sorted_months = sorted(historical_data.keys(), 
                             key=lambda x: datetime.strptime(x, "%b-%y"), 
                             reverse=True)
        
        # Keep last 24 months
        recent_data = {month: historical_data[month] for month in sorted_months[:24]}
        
        # Save updated historical data
        save_historical_data(historical_file, recent_data)
        
        # Update dashboard HTML with latest stats
        update_dashboard_html(latest_data)
        
        print("✅ Dashboard data updated successfully!")
        print(f"📊 Total months in database: {len(recent_data)}")
        print(f"📅 Latest data: {current_month}")
        
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return False
    
    return True

def load_historical_data(file_path):
    """Load existing historical data"""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                # Extract JSON from JavaScript file
                start = content.find('{')
                end = content.rfind('}') + 1
                json_str = content[start:end]
                return json.loads(json_str)
        else:
            return {}
    except Exception as e:
        print(f"⚠️ Could not load historical data: {e}")
        return {}

def save_historical_data(file_path, data):
    """Save historical data as JavaScript file"""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        js_content = f"""// Auto-generated historical UPI data
// Last updated: {datetime.now().isoformat()}

const historicalUPIData = {json.dumps(data, indent=2)};

// Export for use in main application
if (typeof window !== 'undefined') {{
    window.historicalUPIData = historicalUPIData;
}}

if (typeof module !== 'undefined' && module.exports) {{
    module.exports = historicalUPIData;
}}"""
        
        with open(file_path, 'w') as f:
            f.write(js_content)
            
    except Exception as e:
        print(f"❌ Error saving historical data: {e}")

def update_dashboard_html(latest_data):
    """Update dashboard HTML with latest statistics"""
    try:
        html_file = 'docs/index.html'
        
        if os.path.exists(html_file):
            with open(html_file, 'r') as f:
                html_content = f.read()
            
            # Update meta tags with latest stats
            updated_html = html_content.replace(
                '<meta name="description" content="UPI Analytics Dashboard">',
                f'<meta name="description" content="UPI Analytics Dashboard - Latest: {latest_data.get("totalVolume", 0):.0f}M transactions">'
            )
            
            # Add last updated timestamp
            timestamp_comment = f'<!-- Last updated: {datetime.now().isoformat()} -->'
            updated_html = updated_html.replace('</head>', f'{timestamp_comment}\n</head>')
            
            with open(html_file, 'w') as f:
                f.write(updated_html)
                
    except Exception as e:
        print(f"⚠️ Could not update HTML: {e}")

def cleanup_old_files():
    """Clean up old historical files to save space"""
    try:
        # Keep only last 10 historical files
        historical_files = glob.glob('docs/data/historical_*.json')
        historical_files.sort(reverse=True)
        
        for old_file in historical_files[10:]:
            os.remove(old_file)
            print(f"🗑️ Removed old file: {old_file}")
            
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")

if __name__ == "__main__":
    success = process_and_update_dashboard()
    cleanup_old_files()
    
    if success:
        print("🎉 Data processing completed!")
    else:
        print("❌ Data processing failed!")
        exit(1)
