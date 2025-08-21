import os  
import json  
import pandas as pd  
import logging  
import ccxt  
import time  
from datetime import datetime, timedelta, timezone  
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
  
class CryptoDataFetcher:  
    """Fetches cryptocurrency candle data using CCXT."""  
      
    def __init__(self, coin: str = "BTC", base_currency: str = "USDT", exchange_id: str = "coinbaseadvanced"):  
        """Initialize the data fetcher with CCXT exchange."""  
        self.coin = coin.upper()  
        self.base_currency = base_currency.upper()  
        self.symbol = f"{self.coin}/{self.base_currency}"  
          
        # Initialize CCXT exchange  
        exchange_class = getattr(ccxt, exchange_id)  
        self.exchange = exchange_class({  
            'sandbox': False,  
            'enableRateLimit': True,  
        })  
          
        # Load markets to get exchange info  
        self.exchange.load_markets()  
          
        # Use exchange's actual supported timeframes  
        self.timeframes = self.exchange.timeframes if self.exchange.timeframes else {  
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',   
            '1h': '1h', '6h': '6h', '1d': '1d'  
        }  
          
        # Categories for both spot and perpetual markets  
        self.categories = {  
            'spot': 'spot',  
            'perpetual': 'swap'  # CCXT uses 'swap' for perpetual contracts  
        }  
          
        # Validate symbol exists and create perpetual symbol  
        self.spot_symbol = self.symbol  
        self.perp_symbol = f"{self.coin}/{self.base_currency}:USDT"  # Standard perpetual format  
          
        if self.spot_symbol not in self.exchange.symbols:  
            # Try common symbol variations for spot  
            alt_symbols = [  
                f"{self.coin}-{self.base_currency}",  
                f"{self.coin}{self.base_currency}",  
                f"{self.coin}_{self.base_currency}"  
            ]  
              
            for alt_symbol in alt_symbols:  
                if alt_symbol in self.exchange.symbols:  
                    self.spot_symbol = alt_symbol  
                    break  
            else:  
                available_symbols = [s for s in self.exchange.symbols if self.coin in s]  
                logger.warning(f"Spot symbol {self.spot_symbol} not found. Available {self.coin} pairs: {available_symbols[:10]}")  
          
        # Check for perpetual symbol variations  
        if self.perp_symbol not in self.exchange.symbols:  
            perp_variations = [  
                f"{self.coin}/{self.base_currency}:USD",  
                f"{self.coin}-{self.base_currency}",  
                f"{self.coin}{self.base_currency}-PERP"  
            ]  
              
            for perp_var in perp_variations:  
                if perp_var in self.exchange.symbols:  
                    self.perp_symbol = perp_var  
                    break  
            else:  
                logger.warning(f"Perpetual symbol not found for {self.coin}")  
                self.perp_symbol = None  
          
        logger.info(f"Initialized {exchange_id} exchange")  
        logger.info(f"Spot symbol: {self.spot_symbol}")  
        logger.info(f"Perpetual symbol: {self.perp_symbol}")  
        logger.info(f"Supported timeframes: {list(self.timeframes.keys())}")  
          
        # Configure retry behavior  
        self.max_custom_retries = 3  
        self.custom_retry_delay = 5  
      
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
      
    def test_server_time(self) -> bool:  
        """Test exchange connectivity."""  
        try:  
            # Test with a simple market data fetch  
            if self.spot_symbol in self.exchange.symbols:  
                self.exchange.fetch_ticker(self.spot_symbol)  
            logger.info("✓ Exchange connectivity test successful")  
            return True  
        except Exception as e:  
            logger.error(f"✗ Exchange connectivity test failed: {str(e)}")  
            return False  
      
    def fetch_ohlcv_data(self, symbol: str, timeframe: str, start_time: int, end_time: int) -> Optional[List]:  
        """Fetch OHLCV data using CCXT."""  
        for attempt in range(self.max_custom_retries):  
            try:  
                logger.info(f"Fetching {symbol} {timeframe} data (attempt {attempt + 1})")  
                  
                # CCXT fetch_ohlcv method  
                ohlcv = self.exchange.fetch_ohlcv(  
                    symbol=symbol,  
                    timeframe=timeframe,  
                    since=start_time,  
                    limit=1000  
                )  
                  
                # Filter data within time range  
                filtered_data = [  
                    candle for candle in ohlcv   
                    if start_time <= candle[0] <= end_time  
                ]  
                  
                logger.info(f"Fetched {len(filtered_data)} candles for {symbol} {timeframe}")  
                return filtered_data  
                  
            except Exception as e:  
                logger.error(f"Exception on attempt {attempt + 1}: {str(e)}")  
                if attempt < self.max_custom_retries - 1:  
                    time.sleep(self.custom_retry_delay)  
                    continue  
                return None  
          
        return None  
      
    def save_data_efficiently(self, ohlcv_data: List, date_str: str, category_name: str, timeframe: str, symbol: str) -> bool:  
        """Save OHLCV data to JSON and Excel files."""  
        try:  
            # Create directory structure  
            dir_path = f"{date_str}/{category_name}/{timeframe}"  
            os.makedirs(dir_path, exist_ok=True)  
              
            # Create meaningful filenames  
            clean_symbol = symbol.replace('/', '').replace(':', '')  
            json_filename = f"{clean_symbol}_{timeframe}_{date_str}.json"  
            excel_filename = f"{clean_symbol}_{timeframe}_{date_str}.xlsx"  
              
            json_path = os.path.join(dir_path, json_filename)  
            excel_path = os.path.join(dir_path, excel_filename)  
              
            if ohlcv_data:  
                # Convert CCXT OHLCV format to your original format  
                candles_data = []  
                for candle in ohlcv_data:  
                    candles_data.append([  
                        candle[0],  # timestamp  
                        str(candle[1]),  # open  
                        str(candle[2]),  # high  
                        str(candle[3]),  # low  
                        str(candle[4]),  # close  
                        str(candle[5]),  # volume  
                        str(candle[1] * candle[5]) if candle[5] else "0"  # turnover  
                    ])  
                  
                # Save JSON  
                with open(json_path, 'w') as f:  
                    json.dump(candles_data, f, indent=2)  
                logger.info(f"Successfully wrote JSON to {json_path}")  
                  
                # Convert to DataFrame and save as Excel  
                df = pd.DataFrame(candles_data, columns=[  
                    'startTime', 'openPrice', 'highPrice', 'lowPrice',  
                    'closePrice', 'volume', 'turnover'  
                ])  
                  
                # Convert timestamps to readable format  
                df['startTime'] = pd.to_datetime(df['startTime'].astype(int), unit='ms')  
                df.to_excel(excel_path, index=False)  
                  
                logger.info(f"Saved {category_name}/{timeframe} data: {len(candles_data)} candles for {symbol}")  
                return True  
            else:  
                logger.warning(f"No data to save for {category_name}/{timeframe} {symbol}")  
                return False  
                  
        except Exception as e:  
            logger.error(f"Error saving {category_name}/{timeframe} data for {symbol}: {str(e)}")  
            return False  
      
    def fetch_all_data(self) -> bool:  
        """Main method to fetch all OHLCV data for previous day."""  
        logger.info(f"Starting data collection for {self.coin}")  
          
        # Test connectivity first  
        if not self.test_server_time():  
            logger.error("Exchange connectivity test failed, aborting data collection")  
            return False  
          
        date_str, start_time, end_time = self.get_previous_day_timestamps()  
        logger.info(f"Fetching data for {date_str} (UTC)")  
        logger.info(f"Time range: {start_time} to {end_time}")  
          
        success_count = 0  
        total_requests = 0  
          
        for category_name, category_type in self.categories.items():  
            logger.info(f"Processing {category_name} category")  
              
            # Select appropriate symbol based on category  
            if category_name == 'spot':  
                symbol = self.spot_symbol  
            elif category_name == 'perpetual':  
                symbol = self.perp_symbol  
                if not symbol:  
                    logger.warning(f"Skipping perpetual category - no symbol found")  
                    continue  
              
            for timeframe in self.timeframes.keys():  
                logger.info(f"  Fetching {timeframe} interval data for {symbol}")  
                total_requests += 1  
                  
                # Fetch OHLCV data  
                ohlcv_data = self.fetch_ohlcv_data(symbol, timeframe, start_time, end_time)  
                  
                if ohlcv_data:  
                    # Save data  
                    if self.save_data_efficiently(ohlcv_data, date_str, category_name, timeframe, symbol):  
                        success_count += 1  
                  
                # Rate limiting  
                time.sleep(0.2)  
              
            # Additional delay between categories  
            time.sleep(0.5)  
          
        logger.info(f"Data collection completed: {success_count}/{total_requests} successful")  
        return success_count == total_requests  
  
def main():  
    """Main execution function with configurable coin support."""  
    coin = os.getenv('COIN', 'BTC')  
    base_currency = os.getenv('BASE_CURRENCY', 'USDT')  
    exchange_id = os.getenv('EXCHANGE', 'coinbaseadvanced')  
      
    fetcher = CryptoDataFetcher(coin=coin, base_currency=base_currency, exchange_id=exchange_id)  
    success = fetcher.fetch_all_data()  
      
    if success:  
        logger.info(f"All {coin}{base_currency} data fetched successfully")  
        exit(0)  
    else:  
        logger.error(f"Some {coin}{base_currency} data fetching failed")  
        exit(1)  
  
if __name__ == "__main__":  
    main()
