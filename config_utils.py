import asyncio
import uuid
import logging
from datetime import datetime, timedelta, time, date
import warnings
from dotenv import load_dotenv
import os
import pytz
import json
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal, ROUND_HALF_UP, getcontext
from tinkoff.invest import (
    AsyncClient, OrderDirection, Quotation, OrderExecutionReportStatus,
    ExchangeOrderType, StopOrderExpirationType, PostOrderResponse, OrderType, StopOrderType,
    RequestError 
    )
import asyncio
import uuid
import logging
from datetime import datetime, timedelta, time
import warnings
from dotenv import load_dotenv
import os
import pytz
import json
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple

# Отключаем предупреждения от google.protobuf
warnings.filterwarnings("ignore", category=UserWarning, module='google.protobuf')

from tinkoff.invest import (
    AsyncClient, OrderDirection, Quotation, OrderExecutionReportStatus,
    ExchangeOrderType, StopOrderExpirationType, PostOrderResponse, OrderType, StopOrderType,
)
from tinkoff.invest.schemas import (
    OrderState, SubscribeTradesRequest, TradesStreamResponse, OrderTrades, OrderTrade,
    SubscribeLastPriceRequest, LastPriceInstrument,
    MarketDataRequest, SubscriptionAction, MarketDataResponse, SubscribeLastPriceResponse,
    SubscriptionStatus, StopOrder as TinkoffStopOrderSchema, # Переименовано для избежания конфликта
    StopOrderStatusOption,
    Ping
)
from tinkoff.invest._errors import RequestError
from decimal import Decimal, ROUND_HALF_UP, getcontext

getcontext().prec = 10
# Загрузка переменных окружения
load_dotenv()

# ====================== НАСТРОЙКИ ======================
TICKER = 'MTLR'
API_TOKEN = os.getenv("TINKOFF_API_TOKEN", "")
SELL_ACCOUNT_ID = os.getenv("TINKOFF_ACCOUNT_ID_SELL", "")
BUY_ACCOUNT_ID = os.getenv("TINKOFF_ACCOUNT_ID_BUY", "")
INVEST_GRPC_API = "invest-public-api.tinkoff.ru:443"
SELL_ACCOUNT_NAME_FOR_LOG = "SELL_ACCOUNT"
BUY_ACCOUNT_NAME_FOR_LOG = "BUY_ACCOUNT"
LOTS = 1
GRID_INTERVAL = Decimal('0.75')
PLACE_INITIAL_MARKET_ORDER = True
INTERVAL_CHECK = 10
CHECK_GRID_INTERVAL_SECONDS = 20
STATE_FILE_PATH = 'bot_state.json'
ORDER_LOG_FILE_PATH = 'order_log.json'
ORDER_STREAM_LOG_FILE_PATH = 'order_stream_log.json'
MAX_DAILY_LOSS_RUB_SELL = Decimal('500')
MAX_DAILY_LOSS_RUB_BUY = Decimal('500')
MAX_FREE_MARGIN_RUB = Decimal('500')
END_OF_SESSION_REDUCTION_TIME = time(23, 45, 0)
ACCOUNT_FOR_TRADING = 3
LOG_LEVEL_SETTING = "INFO"
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
TRADING_START_TIME = time(7, 0, 0)
TRADING_END_TIME = time(18, 45, 0)
EXT_TRADING_START_TIME = time(19, 0, 0)
EXT_TRADING_END_TIME = time(23, 50, 0)
ENABLE_TREND_FILTERING = True
TREND_CHECK_INTERVAL_SECONDS = 3600
TREND_THRESHOLD_PERCENT = Decimal('0.5')

# Настройки точности Decimal
getcontext().prec = 10

class CustomLogFormatter(logging.Formatter):
    _log_counter = 0
    def format(self, record):
        CustomLogFormatter._log_counter += 1
        record.log_sequence_number = CustomLogFormatter._log_counter
        return super().format(record)

class JsonStreamHandler(logging.Handler):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.logger = logging.getLogger("LevelGridBot")
    
    def emit(self, record):
        try:
            log_entry = self.format(record)
            with open(self.filename, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
                f.flush()
        except Exception as e:
            self.logger.error(f"Ошибка при записи в JSON лог {self.filename}: {e}", exc_info=True)

def setup_logging():
    logger = logging.getLogger("LevelGridBot")
    logger.setLevel(logging.DEBUG if LOG_LEVEL_SETTING == "DEBUG" else logging.INFO)
    handler = logging.StreamHandler()
    formatter = CustomLogFormatter("%(asctime)s - %(log_sequence_number)d: %(levelname)s - L%(lineno)d - %(message)s")
    handler.setFormatter(formatter)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    
    tinkoff_invest_logger = logging.getLogger("tinkoff.invest")
    tinkoff_invest_logger.setLevel(logging.WARNING)
    tinkoff_invest_logger.propagate = False
    
    return logger

def _quotation_to_decimal(quotation: 'Quotation') -> Decimal:
    if quotation is None:
        return Decimal('0')
    return Decimal(quotation.units) + Decimal(quotation.nano) / Decimal('1000000000')

def _decimal_to_quotation(value: Decimal) -> 'Quotation':
    if value is None:
        return Quotation(units=0, nano=0)
    is_negative = value < 0
    abs_value = abs(value)
    units = int(abs_value)
    nano = int((abs_value - Decimal(units)) * Decimal('1e9'))
    if is_negative:
        units = -units
        nano = -nano
    return Quotation(units=units, nano=nano)

def _quotation_to_str(q: 'Quotation') -> str:
    if q is None:
        return "0"
    return str(Decimal(q.units) + Decimal(q.nano) / Decimal('1e9'))

def _datetime_to_iso(dt: datetime) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=pytz.utc).isoformat()
    return dt.isoformat()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, time):  # Добавляем обработку времени
            return obj.isoformat()
        if isinstance(obj, datetime):  # Добавляем обработку даты/времени
            return obj.isoformat()
        if isinstance(obj, date):  # Добавляем обработку даты
            return obj.isoformat()
        return super().default(obj)