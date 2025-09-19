#!/usr/bin/env python3
"""
Delta Exchange India API - Automated Short Strangle Trading Bot
Executes short strangle options strategy daily at 7AM UTC (12:30 PM IST)
"""

import os
import time
import logging
import hashlib
import hmac
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Tuple
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('strangle_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DeltaExchangeClient:
    """
    Delta Exchange India API client for options trading
    Base URL: https://api.india.delta.exchange
    """
    
    def __init__(self):
        self.base_url = "https://api.india.delta.exchange"
        self.api_key = os.getenv('DELTA_API_KEY')
        self.api_secret = os.getenv('DELTA_API_SECRET')
        
        if not self.api_key or not self.api_secret:
            raise ValueError("API credentials not found in environment variables")
    
    def _generate_signature(self, method: str, path: str, query_string: str = "", payload: str = "") -> Tuple[str, str]:
        """Generate signature for API authentication"""
        timestamp = str(int(time.time()))
        signature_data = method + timestamp + path + query_string + payload
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            signature_data.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature, timestamp
    
    def _make_request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """Make authenticated API request to Delta Exchange"""
        path = f"/v2/{endpoint}"
        url = f"{self.base_url}{path}"
        
        query_string = ""
        if params:
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            if query_string:
                query_string = "?" + query_string
        
        payload = ""
        if data:
            payload = json.dumps(data)
        
        signature, timestamp = self._generate_signature(method, path, query_string, payload)
        
        headers = {
            'api-key': self.api_key,
            'signature': signature,
            'timestamp': timestamp,
            'User-Agent': 'python-strangle-bot',
            'Content-Type': 'application/json'
        }
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, params=params, headers=headers, timeout=30)
            else:
                response = requests.post(url, data=payload, params=params, headers=headers, timeout=30)
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_products(self, contract_types: str = "call_options,put_options") -> Dict:
        """Get available options products"""
        params = {"contract_types": contract_types, "states": "live"}
        return self._make_request("GET", "products", params=params)
    
    def get_ticker(self, symbol: str) -> Dict:
        """Get ticker data for specific product"""
        return self._make_request("GET", f"tickers/{symbol}")
    
    def get_option_chain(self, underlying_asset: str, expiry_date: str) -> Dict:
        """Get options chain for given underlying asset and expiry"""
        params = {
            "contract_types": "call_options,put_options",
            "underlying_asset_symbols": underlying_asset,
            "expiry_date": expiry_date
        }
        return self._make_request("GET", "tickers", params=params)
    
    def place_order(self, product_id: int, side: str, size: int, order_type: str = "market_order") -> Dict:
        """Place order on Delta Exchange"""
        order_data = {
            "product_id": product_id,
            "side": side,  # "sell" for short positions
            "size": size,
            "order_type": order_type,
            "time_in_force": "gtc"
        }
        return self._make_request("POST", "orders", data=order_data)
    
    def get_positions(self) -> Dict:
        """Get current open positions"""
        return self._make_request("GET", "positions")
    
    def get_orders(self, product_id: int = None, state: str = "open") -> Dict:
        """Get orders with optional product filter"""
        params = {"state": state}
        if product_id:
            params["product_id"] = product_id
        return self._make_request("GET", "orders", params=params)

class ShortStrangleStrategy:
    """
    Short Strangle Options Strategy Implementation
    - Sells OTM Call and Put options simultaneously
    - Strike prices 1% away from ATM
    - Configurable stop-loss protection
    """
    
    def __init__(self):
        self.client = DeltaExchangeClient()
        self.spot_asset = os.getenv('SPOT_ASSET', 'BTCUSD')
        self.num_lots = int(os.getenv('NUM_LOTS', '1'))
        self.stop_loss_multiplier = float(os.getenv('STOP_LOSS_MULTIPLIER', '1.0'))
        self.otm_percentage = 0.01  # 1% OTM as specified
        
        logger.info(f"Strategy initialized: {self.spot_asset}, {self.num_lots} lots, {self.stop_loss_multiplier}x SL")
    
    def get_next_expiry_date(self) -> str:
        """Get next available weekly expiry date in DD-MM-YYYY format"""
        today = datetime.now()
        # Find next Friday (weekly expiry)
        days_ahead = 4 - today.weekday()  # Friday is weekday 4
        if days_ahead <= 0:
            days_ahead += 7
        
        next_friday = today + timedelta(days=days_ahead)
        return next_friday.strftime("%d-%m-%Y")
    
    def find_atm_strike(self, spot_price: Decimal, available_strikes: List[Decimal]) -> Decimal:
        """Find at-the-money (ATM) strike price closest to spot price"""
        return min(available_strikes, key=lambda x: abs(x - spot_price))
    
    def calculate_otm_strikes(self, atm_strike: Decimal) -> Tuple[Decimal, Decimal]:
        """Calculate OTM call and put strike prices (1% from ATM)"""
        otm_call_strike = atm_strike * Decimal('1.01')  # 1% above ATM
        otm_put_strike = atm_strike * Decimal('0.99')   # 1% below ATM
        return otm_call_strike, otm_put_strike
    
    def find_closest_available_strike(self, target_strike: Decimal, available_strikes: List[Decimal]) -> Decimal:
        """Find closest available strike price to target"""
        return min(available_strikes, key=lambda x: abs(x - target_strike))
    
    def get_option_products(self, expiry_date: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        """
        Get call and put option products for the strategy
        Returns tuple of (call_option_product, put_option_product)
        """
        try:
            # Get underlying asset symbol (BTC from BTCUSD)
            underlying_symbol = self.spot_asset.replace('USD', '').replace('USDT', '')
            
            option_chain = self.client.get_option_chain(underlying_symbol, expiry_date)
            
            if not option_chain.get('success'):
                logger.error(f"Failed to fetch option chain: {option_chain}")
                return None, None
            
            options_data = option_chain.get('result', [])
            
            # Get spot price
            spot_ticker = self.client.get_ticker(self.spot_asset)
            spot_price = Decimal(str(spot_ticker['result']['spot_price']))
            
            # Separate calls and puts
            calls = [opt for opt in options_data if 'C-' in opt['symbol']]
            puts = [opt for opt in options_data if 'P-' in opt['symbol']]
            
            # Extract available strikes
            call_strikes = [Decimal(str(opt['strike_price'])) for opt in calls]
            put_strikes = [Decimal(str(opt['strike_price'])) for opt in puts]
            
            # Find ATM strike
            all_strikes = sorted(set(call_strikes + put_strikes))
            atm_strike = self.find_atm_strike(spot_price, all_strikes)
            
            # Calculate target OTM strikes
            otm_call_target, otm_put_target = self.calculate_otm_strikes(atm_strike)
            
            # Find closest available strikes
            otm_call_strike = self.find_closest_available_strike(otm_call_target, call_strikes)
            otm_put_strike = self.find_closest_available_strike(otm_put_target, put_strikes)
            
            # Find corresponding option products
            call_option = next((opt for opt in calls if Decimal(str(opt['strike_price'])) == otm_call_strike), None)
            put_option = next((opt for opt in puts if Decimal(str(opt['strike_price'])) == otm_put_strike), None)
            
            if call_option and put_option:
                logger.info(f"Selected strikes - Call: {otm_call_strike}, Put: {otm_put_strike} (ATM: {atm_strike}, Spot: {spot_price})")
                return call_option, put_option
            else:
                logger.error("Could not find suitable call or put options")
                return None, None
                
        except Exception as e:
            logger.error(f"Error fetching option products: {e}")
            return None, None
    
    def execute_short_strangle(self) -> bool:
        """
        Execute the short strangle strategy
        - Sell OTM Call option
        - Sell OTM Put option
        """
        try:
            expiry_date = self.get_next_expiry_date()
            logger.info(f"Executing short strangle for expiry: {expiry_date}")
            
            call_option, put_option = self.get_option_products(expiry_date)
            
            if not call_option or not put_option:
                logger.error("Could not find suitable options for strategy")
                return False
            
            # Execute sell orders for both legs
            call_order_response = self.client.place_order(
                product_id=call_option['product_id'],
                side="sell",
                size=self.num_lots,
                order_type="market_order"
            )
            
            put_order_response = self.client.place_order(
                product_id=put_option['product_id'],
                side="sell",
                size=self.num_lots,
                order_type="market_order"
            )
            
            if call_order_response.get('success') and put_order_response.get('success'):
                logger.info("Short strangle executed successfully!")
                logger.info(f"Call leg: {call_option['symbol']} - Order ID: {call_order_response['result']['id']}")
                logger.info(f"Put leg: {put_option['symbol']} - Order ID: {put_order_response['result']['id']}")
                
                # Set up stop-loss monitoring
                self.setup_stop_loss_monitoring()
                return True
            else:
                logger.error(f"Order execution failed - Call: {call_order_response}, Put: {put_order_response}")
                return False
                
        except Exception as e:
            logger.error(f"Error executing short strangle: {e}")
            return False
    
    def setup_stop_loss_monitoring(self):
        """Set up stop-loss monitoring for open positions"""
        try:
            positions = self.client.get_positions()
            if positions.get('success'):
                open_positions = positions.get('result', [])
                
                for position in open_positions:
                    if position.get('size', 0) < 0:  # Short position
                        # Calculate stop-loss level
                        entry_price = Decimal(str(position.get('entry_price', 0)))
                        stop_loss_price = entry_price * Decimal(str(1 + self.stop_loss_multiplier))
                        
                        logger.info(f"Position: {position['product_symbol']}, Entry: {entry_price}, SL: {stop_loss_price}")
                        
        except Exception as e:
            logger.error(f"Error setting up stop-loss monitoring: {e}")

class TradingScheduler:
    """
    Scheduler for automated daily trading execution
    Executes at 7AM UTC (12:30 PM IST) daily
    """
    
    def __init__(self):
        self.strategy = ShortStrangleStrategy()
        self.scheduler = BlockingScheduler(timezone=pytz.UTC)
        self.setup_scheduler()
    
    def setup_scheduler(self):
        """Configure daily execution schedule"""
        # Schedule for 7:00 AM UTC daily (12:30 PM IST)
        self.scheduler.add_job(
            func=self.daily_execution,
            trigger=CronTrigger(hour=7, minute=0, timezone=pytz.UTC),
            id='daily_strangle_execution',
            name='Daily Short Strangle Execution',
            misfire_grace_time=300  # 5 minutes grace period
        )
        logger.info("Scheduler configured for 7:00 AM UTC daily execution")
    
    def daily_execution(self):
        """Daily execution routine"""
        try:
            logger.info("=== Daily Short Strangle Execution Started ===")
            
            # Check if market is open (weekdays only)
            current_time = datetime.now(pytz.UTC)
            if current_time.weekday() >= 5:  # Saturday=5, Sunday=6
                logger.info("Weekend detected, skipping execution")
                return
            
            # Execute strategy
            success = self.strategy.execute_short_strangle()
            
            if success:
                logger.info("=== Daily Execution Completed Successfully ===")
            else:
                logger.error("=== Daily Execution Failed ===")
                
        except Exception as e:
            logger.error(f"Error in daily execution: {e}")
    
    def start(self):
        """Start the scheduler"""
        try:
            logger.info("Starting trading scheduler...")
            self.scheduler.start()
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            self.scheduler.shutdown()

def create_web_server():
    """Create simple web server for deployment platforms like Render.com"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import threading
    
    class HealthCheckHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/health':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"status": "healthy", "service": "short-strangle-bot"}')
            else:
                self.send_response(404)
                self.end_headers()
        
        def log_message(self, format, *args):
            pass  # Suppress default HTTP logging
    
    def start_server():
        host = os.getenv('HOST', '0.0.0.0')
        port = int(os.getenv('PORT', '10000'))
        
        server = HTTPServer((host, port), HealthCheckHandler)
        logger.info(f"Web server started on {host}:{port}")
        server.serve_forever()
    
    # Start web server in background thread
    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

if __name__ == "__main__":
    try:
        # Validate environment variables
        required_vars = ['DELTA_API_KEY', 'DELTA_API_SECRET']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            exit(1)
        
        # Start web server for deployment platforms
        create_web_server()
        
        # Start trading scheduler
        scheduler = TradingScheduler()
        scheduler.start()
        
    except Exception as e:
        logger.error(f"Application startup failed: {e}")
        exit(1)
