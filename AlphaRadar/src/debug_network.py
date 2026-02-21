import requests
import os
import sys
import urllib.request
import socket
import akshare as ak

TARGET_URL_HTTPS = "https://82.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1&po=1&np=1&fltt=2&invt=2&fid=f12&fs=m:0+t:6,m:0+t:80"
HOST = "82.push2.eastmoney.com"
BAIDU_URL = "https://www.baidu.com"

def test_connection(name, url, setup_env=None):
    print(f"\n--- Testing: {name} ---")
    print(f"Target: {url}")
    
    # Backup env
    old_env = os.environ.copy()
    
    # Apply setup
    if setup_env:
        setup_env()
        
    print(f"Env Proxies: HTTP='{os.environ.get('HTTP_PROXY')}' | NO='{os.environ.get('NO_PROXY')}'")
    
    try:
        # Timeout 5s
        resp = requests.get(url, timeout=5)
        print(f"Result: Status {resp.status_code}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Restore env
        os.environ.clear()
        os.environ.update(old_env)

def test_akshare_alternative():
    print("\n--- Testing AkShare Alternative (stock_info_a_code_name) ---")
    try:
        # This API might use a different endpoint
        df = ak.stock_info_a_code_name()
        print(f"Success! Retrieved {len(df)} records.")
        print(df.head())
    except Exception as e:
        print(f"AkShare Alternative Failed: {e}")

def setup_no_proxy_star():
    os.environ['HTTP_PROXY'] = ""
    os.environ['HTTPS_PROXY'] = ""
    os.environ['NO_PROXY'] = "*"
    os.environ['no_proxy'] = "*"

def main():
    print("=== Network Diagnostics v3 ===")
    
    # 0. DNS Check
    try:
        ip = socket.gethostbyname(HOST)
        print(f"DNS Resolution for {HOST}: {ip}")
        if ip.startswith("198.18"):
            print("WARNING: Detected Fake IP (Clash TUN). Direct connection (NO_PROXY) might require TUN handling.")
    except Exception as e:
        print(f"DNS Resolution Failed: {e}")

    # 1. Inspect System Proxies
    print(f"System Proxies: {urllib.request.getproxies()}")
    
    # 2. Test Baidu (Connectivity Check)
    test_connection("Baidu (NO_PROXY)", BAIDU_URL, setup_no_proxy_star)
    
    # 3. Test EastMoney HTTPS
    test_connection("EastMoney HTTPS (NO_PROXY)", TARGET_URL_HTTPS, setup_no_proxy_star)
    
    # 4. AkShare Alternative
    test_akshare_alternative()

if __name__ == "__main__":
    main()
