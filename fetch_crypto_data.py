import os    
import json    
import pandas as pd    
import logging    
import requests    
from datetime import datetime, timedelta, timezone    
from pybit.unified_trading import HTTP    
import time    
from typing import Dict, List, Tuple, Optional    
    
# Configure logging    
logging.basicConfig(    
    level=logging.INFO,    
    format='%(asctime)s - %(levelname)s - %(message)s',    
    handlers=[    
        logging.FileHandler('crypto_data_fetch.log'),    
        logging.StreamHandler()    
    ]    
)    
logger = logging.getLogger(__name__)    
    
class ProxiedHTTPManager:    
    """Custom HTTP manager that routes all requests through a proxy."""    
        
    def __init__(self, proxy_url: str):    
        self.proxy_url = proxy_url.rstrip('/')  # Remove trailing slash    
        self.session = requests.Session()    
            
    def make_request(self, method: str, url: str, params: dict = None, data: dict = None) -> dict:    
        """Make HTTP request through proxy with consistent URL construction."""    
        # Build the full URL with parameters for GET requests    
        if method == "GET" and params:    
            query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v is not None])    
            full_url = f"{url}?{query_string}" if query_string else url    
        else:    
            full_url = url    
                
        # Route through proxy - exactly like workflow test    
        proxied_url = f"{self.proxy_url}/{full_url}"    
            
        # Debug logging to verify proxy usage    
        logger.info(f"Making proxied request to: {proxied_url}")    
            
        try:    
            if method == "GET":    
                response = self.session.get(proxied_url, timeout=30)    
            else:    
                response = self.session.post(proxied_url, json=data, timeout=30)    
                    
            logger.info(f"Proxy response status: {response.status_code}")    
                
            if response.status_code == 200:    
                return response.json()    
            else:    
                logger.error(f"HTTP {response.status_code} error for proxied URL: {proxied_url}")    
                return {"retCode": response.status_code, "retMsg": f"HTTP {response.status_code} error"}    
        except Exception as e:    
            logger.error(f"Proxy request failed for {url}: {str(e)}")    
            return {"retCode": -1, "retMsg": str(e)}    
    
class CryptoDataFetcher:    
    """Fetches cryptocurrency candle data from Bybit API with full proxy support."""    
        
    def __init__(self, coin: str = "BTC", base_currency: str = "USDT", testnet: bool = False):    
        """Initialize the data fetcher with configurable coin parameters and proxy support."""    
        self.coin = coin.upper()    
        self.base_currency = base_currency.upper()    
        self.symbol = f"{self.coin}{self.base_currency}"    
        self.proxy_url = os.getenv('PROXY_URL')    
        self.testnet = testnet    
            
        if self.proxy_url:    
            logger.info(f"Using proxy for all API requests: {self.proxy_url}")    
            self.use_proxy = True    
            self.proxy_manager = ProxiedHTTPManager(self.proxy_url)    
        else:    
            logger.info("No proxy configured, using direct connection")    
            self.use_proxy = False    
            self.session = HTTP(testnet=testnet)    
            
        # Configure retry behavior    
        self.max_custom_retries = 3    
        self.custom_retry_delay = 5    
            
        self.intervals = {    
            '1': '1m', '3': '3m', '5': '5m', '15': '15m', '30': '30m',    
            '60': '1h', '120': '2h', '240': '4h', '360': '6h', '720': '12h'    
        }    
        self.categories = {    
            'spot': 'spot',    
            'perpetual': 'linear'    
        }    
            
    def get_previous_day_timestamps(self) -> Tuple[str, int, int]:    
        """Calculate timestamps for previous day data collection."""    
        now_utc = datetime.now(timezone.utc)    
        yesterday_utc = now_utc - timedelta(days=1)    
        date_str = yesterday_utc.strftime('%Y-%m-%d')    
            
        start_time = int(yesterday_utc.replace(    
            hour=0, minute=0, second=0, microsecond=0    
        ).timestamp() * 1000)    
        end_time = int(yesterday_utc.replace(    
            hour=23, minute=59, second=59, microsecond=999000    
        ).timestamp() * 1000)    
            
        return date_str, start_time, end_time    
        
    def get_api_base_url(self) -> str:    
        """Get the appropriate API base URL based on testnet setting."""    
        if self.testnet:    
            return "https://api-testnet.bybit.com"    
        else:    
            return "https://api.bybit.com"    
        
    def test_server_time(self) -> bool:    
        """Test server time endpoint through proxy - consistent with workflow test."""    
        base_url = self.get_api_base_url()    
        time_url = f"{base_url}/v5/market/time"    
            
        logger.info(f"Testing server time with proxy: {self.use_proxy}")    
        logger.info(f"Proxy URL: {self.proxy_url}")    
        logger.info(f"Target URL: {time_url}")    
            
        if self.use_proxy:    
            # Use proxy manager for consistency with all other requests    
            response = self.proxy_manager.make_request("GET", time_url)    
        else:    
            # Only use direct pybit connection if no proxy configured    
            response = self.session.get_server_time()    
                
        if response.get('retCode') == 0:    
            logger.info("✓ Server time test successful")    
            return True    
        else:    
            logger.error(f"✗ Server time test failed: {response.get('retMsg', 'Unknown error')}")    
            return False    
        
    def fetch_candle_data_via_proxy(self, category: str, symbol: str, interval: str,     
                                   start_time: int, end_time: int) -> Optional[Dict]:    
        """Fetch candle data via proxy using direct HTTP requests."""    
        base_url = self.get_api_base_url()    
        kline_url = f"{base_url}/v5/market/kline"    
            
        params = {    
            'category': category,    
            'symbol': symbol,    
            'interval': interval,    
            'start': start_time,    
            'end': end_time,    
            'limit': 1000    
        }    
            
        return self.proxy_manager.make_request("GET", kline_url, params=params)    
        
    def get_interval_minutes(self, interval: str) -> int:    
        """Convert interval string to minutes."""    
        interval_map = {    
            '1': 1, '3': 3, '5': 5, '15': 15, '30': 30,    
            '60': 60, '120': 120, '240': 240, '360': 360, '720': 720    
        }    
        return interval_map.get(interval, 1)    
        
    def _fetch_single_batch(self, category: str, interval: str, start_time: int, end_time: int) -> Optional[Dict]:    
        """Fetch a single batch of candle data with retry logic."""    
        for attempt in range(self.max_custom_retries):    
            try:    
                logger.info(f"Fetching {category} {interval} batch (attempt {attempt + 1})")    
                    
                if self.use_proxy:    
                    response = self.fetch_candle_data_via_proxy(    
                        category, self.symbol, interval, start_time, end_time    
                    )    
                else:    
                    response = self.session.get_kline(    
                        category=category,    
                        symbol=self.symbol,    
                        interval=interval,    
                        start=start_time,    
                        end=end_time,    
                        limit=1000    
                    )    
                    
                if response and response.get('retCode') == 0:    
                    return response    
                else:    
                    error_code = response.get('retCode', 'Unknown') if response else 'No response'    
                    error_msg = response.get('retMsg', 'Unknown error') if response else 'No response'    
                    logger.error(f"API error {error_code}: {error_msg}")    
                        
                    # Handle retryable errors    
                    if error_code in [10006, 10002, 30034, 30035, 130035, 130150]:    
                        if attempt < self.max_custom_retries - 1:    
                            time.sleep(self.custom_retry_delay)    
                            continue    
                    return None    
                        
            except Exception as e:    
                logger.error(f"Exception on attempt {attempt + 1}: {str(e)}")    
                if attempt < self.max_custom_retries - 1:    
                    time.sleep(self.custom_retry_delay)    
                    continue    
                return None    
            
        return None    
        
    def fetch_candle_data_with_retry(self, category: str, interval: str,    
                               start_time: int, end_time: int) -> Optional[Dict]:    
        """Fetch candle data with retry logic and pagination support."""    
        # Calculate if pagination is needed    
        interval_minutes = self.get_interval_minutes(interval)    
        total_minutes = (end_time - start_time) // (1000 * 60)  # Convert ms to minutes    
        candles_needed = total_minutes // interval_minutes    
            
        all_candles = []    
            
        if candles_needed <= 1000:    
            # Single request sufficient    
            response = self._fetch_single_batch(category, interval, start_time, end_time)    
            if response and response.get('retCode') == 0:    
                return response    
            return None    
        else:    
            # Multiple requests needed - implement pagination    
            current_start = start_time    
            batch_size_ms = 1000 * interval_minutes * 60 * 1000  # 1000 candles worth of milliseconds    
                
            while current_start < end_time:    
                current_end = min(current_start + batch_size_ms, end_time)    
                    
                response = self._fetch_single_batch(category, interval, current_start, current_end)    
                if response and response.get('retCode') == 0:    
                    batch_candles = response['result']['list']    
                    all_candles.extend(batch_candles)    
                    logger.info(f"Fetched {len(batch_candles)} candles for batch")    
                else:    
                    logger.error(f"Failed to fetch batch starting at {current_start}")    
                    return None    
                        
                current_start = current_end    
                time.sleep(0.2)  # Rate limiting between batches    
                
            # Return combined result in same format as single request    
            return {    
                'retCode': 0,    
                'retMsg': 'OK',    
                'result': {    
                    'list': all_candles    
                }    
            }    
        
    def save_data_efficiently(self, candles_data: List, date_str: str,     
                            category_name: str, folder_name: str, interval: str) -> bool:    
        """Save only the essential candle data with universal coin naming."""    
        try:    
            # Create directory structure    
            dir_path = f"{date_str}/{category_name}/{folder_name}"    
            os.makedirs(dir_path, exist_ok=True)    
                
            # Create meaningful filenames with coin symbol, interval, and date    
            json_filename = f"{self.symbol}_{interval}_{date_str}.json"    
            excel_filename = f"{self.symbol}_{interval}_{date_str}.xlsx"    
                
            json_path = f"{dir_path}/{json_filename}"    
            excel_path = f"{dir_path}/{excel_filename}"    
                
            if candles_data:    
                # Save only the candles data (not full API response) as JSON    
                with open(json_path, 'w') as f:    
                    json.dump(candles_data, f, indent=2)    
                    
                # Convert to DataFrame and save as Excel    
                df = pd.DataFrame(candles_data, columns=[    
                    'startTime', 'openPrice', 'highPrice', 'lowPrice',     
                    'closePrice', 'volume', 'turnover'    
                ])    
                    
                # Convert timestamps to readable format    
                df['startTime'] = pd.to_datetime(df['startTime'].astype(int), unit='ms')    
                df.to_excel(excel_path, index=False)    
                    
                logger.info(f"Saved {category_name}/{folder_name} data: "    
                          f"{len(candles_data)} candles for {self.symbol} to {dir_path}")    
                return True    
            else:    
                logger.warning(f"No candle data to save for {category_name}/{folder_name} {self.symbol}")    
                return False    
                    
        except Exception as e:    
            logger.error(f"Error saving {category_name}/{folder_name} data for {self.symbol}: {str(e)}")    
            return False    
        
    def fetch_all_data(self) -> bool:    
        """Main method to fetch all candle data for previous day."""    
        logger.info(f"Starting {self.symbol} candle data collection")    
            
        # Test connectivity first using the same proxy approach    
        if not self.test_server_time():    
            logger.error("Server connectivity test failed, aborting data collection")    
            return False    
            
        date_str, start_time, end_time = self.get_previous_day_timestamps()    
        logger.info(f"Fetching data for {date_str} (UTC)")    
        logger.info(f"Time range: {start_time} to {end_time}")    
            
        success_count = 0    
        total_requests = len(self.categories) * len(self.intervals)    
            
        for category_name, category_value in self.categories.items():    
            logger.info(f"Processing {category_name} category for {self.symbol}")    
                
            for interval, folder_name in self.intervals.items():    
                logger.info(f"  Fetching {folder_name} interval data")    
                    
                # Fetch data with enhanced retry logic - all through proxy    
                response = self.fetch_candle_data_with_retry(    
                    category_value, interval, start_time, end_time    
                )    
                    
                if response and response['retCode'] == 0:    
                    candles_data = response['result']['list']    
                        
                    # Save data efficiently with universal naming    
                    if self.save_data_efficiently(    
                        candles_data, date_str, category_name, folder_name, interval    
                    ):    
                        success_count += 1    
                    
                # Rate limiting - delay between requests    
                time.sleep(0.2)    
                
            # Additional delay between categories    
            time.sleep(0.5)    
            
        logger.info(f"Data collection completed for {self.symbol}: {success_count}/{total_requests} successful")    
        return success_count == total_requests    
    
def main():    
    """Main execution function with configurable coin support."""    
    # You can easily change the coin here    
    coin = os.getenv('COIN', 'BTC')  # Default to BTC, but can be overridden    
    base_currency = os.getenv('BASE_CURRENCY', 'USDT')    
        
    fetcher = CryptoDataFetcher(coin=coin, base_currency=base_currency, testnet=False)    
    success = fetcher.fetch_all_data()    
        
    if success:    
        logger.info(f"All {coin}{base_currency} data fetched successfully")    
        exit(0)    
    else:    
        logger.error(f"Some {coin}{base_currency} data fetching failed")    
        exit(1)    
    
if __name__ == "__main__":    
    main()
