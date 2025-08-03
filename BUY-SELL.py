#Tbot_BUY-SELL_TAKE_PROFIT

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

# Загружаем переменные окружения из файла .env
load_dotenv()

# ====================== НАСТРОЙКИ ======================
TICKER = 'MTLR'
# =================================================================================
# Получаем значения из переменных окружения или устанавливаем пустые строки по умолчанию
API_TOKEN = os.getenv("TINKOFF_API_TOKEN", "")
# =================================================================================
SELL_ACCOUNT_ID = os.getenv("TINKOFF_ACCOUNT_ID_SELL", "")
BUY_ACCOUNT_ID = os.getenv("TINKOFF_ACCOUNT_ID_BUY", "")
# =================================================================================
INVEST_GRPC_API = "invest-public-api.tinkoff.ru:443"
# =================================================================================
SELL_ACCOUNT_NAME_FOR_LOG = "SELL_ACCOUNT"
BUY_ACCOUNT_NAME_FOR_LOG = "BUY_ACCOUNT"
# =================================================================================
LOTS = 1 # Количество лотов на каждый ордер (SH, SL, BL, BH)
# =================================================================================
GRID_INTERVAL = Decimal('0.25') # Интервал для размещения ордеров
# =================================================================================
PLACE_INITIAL_MARKET_ORDER = True # True для рыночного входа, False для лимитного
# =================================================================================
INTERVAL_CHECK = 10 # Интервал для периодической проверки позиции и логирования
# =================================================================================
CHECK_GRID_INTERVAL_SECONDS = 20 # Периодичность проверки и перестановки ордеров для входа, если они устарели
# =================================================================================
STATE_FILE_PATH = 'bot_state.json' # Путь к файлу для сохранения состояния бота (высокоуровневые данные)
ORDER_LOG_FILE_PATH = 'order_log.json' # Путь к файлу для сохранения детального журнала ордеров
ORDER_STREAM_LOG_FILE_PATH = 'order_stream_log.json' # Путь к файлу для логирования стрима ордеров
# =================================================================================
MAX_DAILY_LOSS_RUB_SELL = Decimal('500') # Максимальный дневной убыток для SELL счета
MAX_DAILY_LOSS_RUB_BUY = Decimal('500')  # Максимальный дневной убыток для BUY счета
# =================================================================================
MAX_FREE_MARGIN_RUB = Decimal('500') # Максимальная сумма используемой маржи, чтобы избежать комиссий
END_OF_SESSION_REDUCTION_TIME = time(23, 45, 0) # Время (по МСК) для проверки и сокращения позиции перед закрытием биржи
# =================================================================================
# DEVIATION_BUFFER_FACTOR = Decimal('0.50') # Фактор буфера отклонения для перестановки ордеров для входа -- УДАЛЕНО
# =================================================================================
ACCOUNT_FOR_TRADING = 3 # 1 (BUY), 2 (SELL), 3 (BUY-SELL)
# =================================================================================
LOG_LEVEL_SETTING = "INFO" # Can be "DEBUG" or "INFO"
# =================================================================================
# ====================== НАСТРОЙКИ ТОРГОВЫХ ЧАСОВ (для Московской биржи) ======================
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
TRADING_START_TIME = time(7, 0, 0) # Основная сессия Московской биржи начинается в 7:00 UTC+3
TRADING_END_TIME = time(18, 45, 0) # Основная сессия Московской биржи заканчивается в 18:45 UTC+3
EXT_TRADING_START_TIME = time(19, 0, 0) # Вечерняя сессия начинается в 19:00 UTC+3
EXT_TRADING_END_TIME = time(23, 50, 0) # Вечерняя сессия заканчивается в 23:50 UTC+3
# ===========================================================================================
# ====================== НАСТРОЙКИ ФИЛЬТРАЦИИ ПО ТРЕНДУ ======================
ENABLE_TREND_FILTERING = True # Включить/выключить фильтрацию по тренду
TREND_CHECK_INTERVAL_SECONDS =3600 # Интервал (в секундах) для пересчета тренда (например, 300с = 5 минут)
TREND_THRESHOLD_PERCENT = Decimal('0.5') # Процент изменения цены для определения тренда (0.05% = 0.0005)
# ===========================================================================================
# ====================== Пользовательский форматтер для нумерации логов ======================
class CustomLogFormatter(logging.Formatter):
    """
    Пользовательский форматтер для добавления порядкового номера к каждой записи лога.
    """
    _log_counter = 0 # Статический счетчик для всех записей лога
    def format(self, record):
        """
        Форматирует запись лога, добавляя порядковый номер.
        """
        CustomLogFormatter._log_counter += 1
        record.log_sequence_number = CustomLogFormatter._log_counter
        # Corrected: Pass 'record' to the superclass's format method
        return super().format(record)
# ====================== JSON логгер для стрима ордеров ======================
class JsonStreamHandler(logging.Handler):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.logger = logging.getLogger("LevelGridBot") # Используем основной логгер для ошибок
        self.logger.debug(f"Инициализация JsonStreamHandler для файла: {self.filename}")
    def emit(self, record):
        try:
            log_entry = self.format(record)
            with open(self.filename, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
                f.flush() # Принудительная запись на диск
            self.logger.debug(f"Запись в {self.filename} успешно выполнена.")
        except Exception as e:
            self.logger.error(f"Ошибка при записи в JSON лог {self.filename}: {e}", exc_info=True)
            self.handleError(record) # Вызываем стандартный обработчик ошибок
order_stream_json_logger = logging.getLogger('order_stream_json_logger')
order_stream_json_logger.setLevel(logging.INFO) # Можно установить на DEBUG для еще большей детализации
order_stream_json_logger.propagate = False # Важно: предотвращает дублирование логов в основном логгере
# Удаляем все существующие обработчики, чтобы избежать дублирования
if order_stream_json_logger.handlers:
    for handler in order_stream_json_logger.handlers:
        order_stream_json_logger.removeHandler(handler)
json_file_handler = JsonStreamHandler(ORDER_STREAM_LOG_FILE_PATH)
formatter = logging.Formatter('%(message)s') # Форматтер просто передает сообщение как есть
json_file_handler.setFormatter(formatter)
order_stream_json_logger.addHandler(json_file_handler)

# ====================== Вспомогательные функции для конвертации (из отладочного бота) ======================
def _quotation_to_decimal(quotation: Quotation) -> Decimal:
    """
    Конвертирует объект Quotation в Decimal.
    """
    if quotation is None:
        return Decimal('0')
    
    decimal_value = Decimal(quotation.units) + Decimal(quotation.nano) / Decimal('1000000000')
    return decimal_value.normalize()

def _decimal_to_quotation(value: Decimal) -> Quotation:
    """
    Конвертирует Decimal в объект Quotation.
    """
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

def _quotation_to_str(q: Quotation) -> str:
    """
    Helper to convert Quotation to string for logging.
    """
    if q is None:
        return "0"
    return str(Decimal(q.units) + Decimal(q.nano) / Decimal('1e9'))

def _datetime_to_iso(dt: datetime) -> Optional[str]:
    """
    Helper to convert datetime to ISO format string for logging.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=pytz.utc).isoformat() # Assume UTC if no timezone
    return dt.isoformat()

def process_incoming_order_stream_message(message):
    """
    Эта функция вызывается каждый раз, когда вы получаете новое сообщение
    из стрима ордеров (TradesStreamResponse).
    Корректно преобразует вложенные объекты в словари для логирования.
    (Обновлено из отладочного бота)
    """
    log_entry_data = None
    message_type = "UNKNOWN"
    
    # Process TradesStreamResponse
    if isinstance(message, TradesStreamResponse):
        if message.order_trades:
            order_trades_obj = message.order_trades
            trades_list = []
            for trade in order_trades_obj.trades:
                trades_list.append({
                    "date_time": _datetime_to_iso(trade.date_time),
                    "price": _quotation_to_str(trade.price),
                    "quantity": trade.quantity,
                    "trade_id": trade.trade_id
                })
            log_entry_data = {
                "order_id": order_trades_obj.order_id,
                "created_at": _datetime_to_iso(order_trades_obj.created_at),
                "direction": order_trades_obj.direction.name, # Convert enum to string
                "figi": order_trades_obj.figi,
                "trades": trades_list,
                "account_id": order_trades_obj.account_id,
                "instrument_uid": order_trades_obj.instrument_uid
            }
            message_type = "ORDER_TRADES"
        elif message.ping:
            ping_obj = message.ping
            log_entry_data = {
                "time": _datetime_to_iso(ping_obj.time),
                "stream_id": ping_obj.stream_id,
                "ping_request_time": _datetime_to_iso(ping_obj.ping_request_time)
            }
            message_type = "PING"
        elif hasattr(message, 'subscription') and message.subscription: # Use 'subscription' as per dir() output from diagnostic
            sub_obj = message.subscription
            log_entry_data = {
                "figi": sub_obj.figi,
                "subscription_action": sub_obj.subscription_action.name, # Convert enum to string
                "subscription_status": sub_obj.subscription_status.name, # Convert enum to string
                "instrument_uid": sub_obj.instrument_uid,
                "stream_id": sub_obj.stream_id,
                "account_id": sub_obj.account_id,
                "trades_subscriptions": [{
                    "figi": ts.figi,
                    "interval": ts.interval.name,
                    "subscription_status": ts.subscription_status.name
                } for ts in sub_obj.trades_subscriptions] if hasattr(sub_obj, 'trades_subscriptions') else []
            }
            message_type = "SUBSCRIPTION_STATUS"
        else:
            # Fallback for TradesStreamResponse with unexpected content
            log_entry = {
                "timestamp": datetime.now(pytz.timezone('Europe/Moscow')).isoformat(),
                "type": "UNKNOWN_TRADES_STREAM_RESPONSE",
                "data": str(message) # Log raw string if no known field is present
            }
            order_stream_json_logger.warning(log_entry)
            return

    # Process direct messages (less common for stream, but for robustness)
    elif isinstance(message, OrderTrades):
        trades_list = []
        for trade in message.trades:
            trades_list.append({
                "date_time": _datetime_to_iso(trade.date_time),
                "price": _quotation_to_str(trade.price),
                "quantity": trade.quantity,
                "trade_id": trade.trade_id
            })
        log_entry_data = {
            "order_id": message.order_id,
            "created_at": _datetime_to_iso(message.created_at),
            "direction": message.direction.name,
            "figi": message.figi,
            "trades": trades_list,
            "account_id": message.account_id,
            "instrument_uid": message.instrument_uid
        }
        message_type = "ORDER_TRADES_DIRECT"
    elif isinstance(message, Ping):
        log_entry_data = {
            "time": _datetime_to_iso(message.time),
            "stream_id": message.stream_id,
            "ping_request_time": _datetime_to_iso(message.ping_request_time)
        }
        message_type = "PING_DIRECT"
    elif isinstance(message, SubscriptionStatus):
        log_entry_data = {
            "figi": message.figi,
            "subscription_action": message.subscription_action.name,
            "subscription_status": message.subscription_status.name,
            "instrument_uid": message.instrument_uid,
            "stream_id": message.stream_id,
            "account_id": message.account_id
        }
        message_type = "SUBSCRIPTION_STATUS_DIRECT"
    else:
        # Log completely unknown types of messages
        log_entry = {
            "timestamp": datetime.now(pytz.timezone('Europe/Moscow')).isoformat(),
            "type": "UNKNOWN_MESSAGE_TYPE",
            "data": str(message)
        }
        order_stream_json_logger.warning(log_entry)
        return

    if log_entry_data is not None:
        log_entry = {
            "timestamp": datetime.now(pytz.timezone('Europe/Moscow')).isoformat(),
            "type": message_type,
            "data": log_entry_data
        }
        order_stream_json_logger.info(log_entry)

# ====================== OrderStateData (для журнала ордеров) ======================
@dataclass
class OrderStateData:
    order_id: str # This will be the Tinkoff order_id for limit/market, or stop_order_id for stop orders
    account_id: str
    figi: str
    direction: str # String representation of OrderDirection
    quantity_requested: int
    order_type: str # String representation of OrderType (LIMIT, MARKET) or ExchangeOrderType (for stop orders)
    stop_order_type: Optional[str] = None # String representation of StopOrderType
    price: Optional[str] = None # String Decimal for limit price
    stop_price: Optional[str] = None # String Decimal for stop price
    client_code: str = field(default_factory=lambda: f"TBot_GEN_{uuid.uuid4().hex[:8]}")
    status: str = "PENDING_CONFIRMATION" # Custom status: PENDING_CONFIRMATION, ACTIVE, PARTIALLY_FILLED, FILLED, CANCELLED, FAILED_TO_ACTIVATE, ERROR, ACTIVATED
    filled_quantity: int = 0
    fill_price: Optional[str] = None # String Decimal for fill price
    placed_at: str = field(default_factory=lambda: datetime.now(MOSCOW_TZ).isoformat())
    last_updated_at: str = field(default_factory=lambda: datetime.now(MOSCOW_TZ).isoformat())
    status_history: List[Dict[str, Any]] = field(default_factory=list)
    is_stop_order: bool = False # Helper flag for internal logic
    exchange_order_id: Optional[str] = None # New: ID of the actual exchange order created when a stop order activates
    original_stop_order_id: Optional[str] = None # New: For an exchange_order_id, this links back to the original stop_order_id
    def __post_init__(self):
        self._record_event(self.status, "BOT_PLACEMENT")
    def _record_event(self, status: str, source: str, **kwargs):
        event = {
            "timestamp": datetime.now(MOSCOW_TZ).isoformat(),
            "status": status,
            "source": source,
            **kwargs
        }
        self.status_history.append(event)
        self.status = status
        self.last_updated_at = datetime.now(MOSCOW_TZ).isoformat()
    def update_status(self, status: str, source: str, **kwargs):
        self._record_event(status, source, **kwargs)
    def to_dict(self):
        return {
            "order_id": self.order_id,
            "account_id": self.account_id,
            "figi": self.figi,
            "direction": self.direction,
            "quantity_requested": self.quantity_requested,
            "order_type": self.order_type,
            "stop_order_type": self.stop_order_type,
            "price": self.price,
            "stop_price": self.stop_price,
            "client_code": self.client_code,
            "status": self.status,
            "filled_quantity": self.filled_quantity,
            "fill_price": self.fill_price,
            "placed_at": self.placed_at,
            "last_updated_at": self.last_updated_at,
            "status_history": self.status_history,
            "is_stop_order": self.is_stop_order,
            "exchange_order_id": self.exchange_order_id, # New
            "original_stop_order_id": self.original_stop_order_id # New
        }
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        instance = cls(
            order_id=data['order_id'],
            account_id=data['account_id'],
            figi=data['figi'],
            direction=data['direction'],
            quantity_requested=data['quantity_requested'],
            order_type=data['order_type'],
            stop_order_type=data.get('stop_order_type'),
            price=data.get('price'),
            stop_price=data.get('stop_price'),
            client_code=data.get('client_code', f"TBot_GEN_{uuid.uuid4().hex[:8]}"),
            status=data['status'],
            filled_quantity=data.get('filled_quantity', 0),
            fill_price=data.get('fill_price'),
            placed_at=data['placed_at'],
            last_updated_at=data['last_updated_at'],
            status_history=data.get('status_history', []),
            is_stop_order=data.get('is_stop_order', False),
            exchange_order_id=data.get('exchange_order_id'), # New
            original_stop_order_id=data.get('original_stop_order_id') # New
        )
        return instance

# ====================== AccountStateData (для состояния каждого счета) ======================
@dataclass
class AccountStateData:
    account_id: str
    account_name_for_log: str
    is_sell_account: bool
    
    # Grid and strategy parameters (copied from AccountManager init)
    lots_in_one_lot: int
    grid_interval: Decimal
    max_daily_loss_rub: Decimal
    max_free_margin_rub: Decimal
    end_of_session_reduction_time: time
    check_grid_interval_seconds: int
    
    # State variables (copied from AccountManager)
    open_lots: List[Dict[str, Any]] = field(default_factory=list) # List of {'entry_price': Decimal, 'tp_order_id': str, 'quantity': int}
    active_entry_order_above: Tuple[Optional[str], Optional[bool]] = (None, None) # Stores (order_id: str | None, is_stop_order: bool | None)
    active_entry_order_below: Tuple[Optional[str], Optional[bool]] = (None, None) # Stores (order_id: str | None, is_stop_order: bool | None)
    initial_orders_placed: bool = False
    last_orders_placement_time: datetime = field(default_factory=lambda: datetime.min)
    orders_need_reposition: bool = False
    account_started: bool = False
    last_position_fill_price: Decimal = Decimal('0')
    daily_profit_loss: Decimal = Decimal('0')
    daily_loss_limit_hit: bool = False
    daily_pnl_reset_date: datetime.date = field(default_factory=lambda: datetime.min.date())
    margin_limit_hit: bool = False
    end_of_session_reduction_done: bool = False
    initial_entry_orders_placed_for_session: bool = False
    pending_initial_market_order_id: Optional[str] = None
    is_active_by_trend: bool = True # NEW: Флаг активности счета по тренду

    def to_dict(self):
        return {
            "account_id": self.account_id,
            "account_name_for_log": self.account_name_for_log,
            "is_sell_account": self.is_sell_account,
            "lots_in_one_lot": self.lots_in_one_lot,
            "grid_interval": str(self.grid_interval),
            "max_daily_loss_rub": str(self.max_daily_loss_rub),
            "max_free_margin_rub": str(self.max_free_margin_rub),
            "end_of_session_reduction_time": self.end_of_session_reduction_time.isoformat(),
            "check_grid_interval_seconds": self.check_grid_interval_seconds,
            "open_lots": [
                {"entry_price": str(lot['entry_price']), "tp_order_id": lot['tp_order_id'], "quantity": lot['quantity']}
                for lot in self.open_lots
            ],
            "active_entry_order_above": list(self.active_entry_order_above),
            "active_entry_order_below": list(self.active_entry_order_below),
            "initial_orders_placed": self.initial_orders_placed,
            "last_orders_placement_time": self.last_orders_placement_time.isoformat() if self.last_orders_placement_time != datetime.min else None,
            "orders_need_reposition": self.orders_need_reposition,
            "account_started": self.account_started,
            "last_position_fill_price": str(self.last_position_fill_price),
            "daily_profit_loss": str(self.daily_profit_loss),
            "daily_loss_limit_hit": self.daily_loss_limit_hit,
            "daily_pnl_reset_date": self.daily_pnl_reset_date.isoformat(),
            "margin_limit_hit": self.margin_limit_hit,
            "end_of_session_reduction_done": self.end_of_session_reduction_done,
            "initial_entry_orders_placed_for_session": self.initial_entry_orders_placed_for_session,
            "pending_initial_market_order_id": self.pending_initial_market_order_id,
            "is_active_by_trend": self.is_active_by_trend, # NEW
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        instance = cls(
            account_id=data['account_id'],
            account_name_for_log=data['account_name_for_log'],
            is_sell_account=data['is_sell_account'],
            lots_in_one_lot=data['lots_in_one_lot'],
            grid_interval=Decimal(data['grid_interval']),
            max_daily_loss_rub=Decimal(data['max_daily_loss_rub']),
            max_free_margin_rub=Decimal(data['max_free_margin_rub']),
            end_of_session_reduction_time=time.fromisoformat(data['end_of_session_reduction_time']),
            check_grid_interval_seconds=data['check_grid_interval_seconds'],
            open_lots=[
                {"entry_price": Decimal(lot['entry_price']), "tp_order_id": lot['tp_order_id'], "quantity": lot['quantity']}
                for lot in data.get("open_lots", [])
            ],
            active_entry_order_above=tuple(data.get("active_entry_order_above", (None, None))),
            active_entry_order_below=tuple(data.get("active_entry_order_below", (None, None))),
            initial_orders_placed=data.get("initial_orders_placed", False),
            last_orders_placement_time=datetime.fromisoformat(data["last_orders_placement_time"]) if data.get("last_orders_placement_time") else datetime.min,
            orders_need_reposition=data.get("orders_need_reposition", False),
            account_started=data.get("account_started", False),
            last_position_fill_price=Decimal(data.get("last_position_fill_price", '0')),
            daily_profit_loss=Decimal(data.get("daily_profit_loss", '0')),
            daily_loss_limit_hit=data.get("daily_loss_limit_hit", False),
            daily_pnl_reset_date=datetime.fromisoformat(data["daily_pnl_reset_date"]).date() if data.get("daily_pnl_reset_date") else datetime.min.date(),
            margin_limit_hit=data.get("margin_limit_hit", False),
            end_of_session_reduction_done=data.get("end_of_session_reduction_done", False),
            initial_entry_orders_placed_for_session=data.get("initial_entry_orders_placed_for_session", False),
            pending_initial_market_order_id=data.get("pending_initial_market_order_id"),
            is_active_by_trend=data.get("is_active_by_trend", True), # NEW
        )
        return instance

# ====================== OrderManager ======================
class OrderManager:
    SIMPLIFIED_ORDER_TAGS = {
        "SELL_ENTRY_UPPER_LIMIT": "на вход выше",
        "SELL_ENTRY_LOWER_STOP": "на вход ниже",
        "BUY_ENTRY_LOWER_LIMIT": "на вход ниже",
        "BUY_ENTRY_UPPER_STOP": "на вход выше",
        "TP_SELL_LOT": "TP",
        "TP_BUY_LOT": "TP",
        "INITIAL_MARKET_SELL": "на вход (рыночный)",
        "INITIAL_MARKET_BUY": "на вход (рыночный)",
        "SESSION_REDUCTION": "на закрытие (сокращение)",
    }
    def __init__(self, logger: logging.Logger, bot_client_code: str, figi: str, min_price_increment: Decimal, display_precision: int):
        self.logger = logger
        self.bot_client_code = bot_client_code
        self.figi = figi
        self.min_price_increment = min_price_increment
        self.display_precision = display_precision
        self.orders: Dict[str, OrderStateData] = {}  # Stores all managed orders by order_id (which can be stop_order_id or regular order_id)
        self.account_states: Dict[str, AccountStateData] = {} # Stores AccountStateData for each managed account
        self.order_log_file = ORDER_LOG_FILE_PATH
        self.state_file = STATE_FILE_PATH
        self.load_state() # Load both orders and account states
        self.verification_tasks = {} # To hold asyncio tasks for order verification
        self.orders_placement_lock = asyncio.Lock() # Lock for order placement to prevent race conditions
    
    def initialize_account_state(self, account_id: str, account_name_for_log: str, is_sell_account: bool,
                                 lots_in_one_lot: int, grid_interval: Decimal, max_daily_loss_rub: Decimal,
                                 max_free_margin_rub: Decimal, end_of_session_reduction_time: time,
                                 check_grid_interval_seconds: int):
        """Initializes or re-initializes account state data."""
        if account_id not in self.account_states:
            self.logger.debug(f"[OrderManager] Инициализация состояния для счета {account_name_for_log} ({account_id}).")
            self.account_states[account_id] = AccountStateData(
                account_id=account_id,
                account_name_for_log=account_name_for_log,
                is_sell_account=is_sell_account,
                lots_in_one_lot=lots_in_one_lot,
                grid_interval=grid_interval,
                max_daily_loss_rub=max_daily_loss_rub,
                max_free_margin_rub=max_free_margin_rub,
                end_of_session_reduction_time=end_of_session_reduction_time,
                check_grid_interval_seconds=check_grid_interval_seconds
            )
        else:
            self.logger.debug(f"[OrderManager] Состояние для счета {account_name_for_log} ({account_id}) уже существует. Обновляем параметры.")
            # Update parameters if they might have changed from settings
            account_state = self.account_states[account_id]
            account_state.lots_in_one_lot = lots_in_one_lot
            account_state.grid_interval = grid_interval
            account_state.max_daily_loss_rub = max_daily_loss_rub
            account_state.max_free_margin_rub = max_free_margin_rub
            account_state.end_of_session_reduction_time = end_of_session_reduction_time
            account_state.check_grid_interval_seconds = check_grid_interval_seconds

    async def _verify_order_active_task(self, client: AsyncClient, order_id: str, is_stop_order: bool, account_id: str, max_attempts=5, delay=0.5):
        """
        Task to verify if a newly placed order is active on the exchange.
        Updates the internal OrderStateData.
        For stop orders, also checks for exchange_order_id to mark as activated.
        """
        order_data = self.orders.get(order_id)
        if not order_data:
            self.logger.debug(f"[OrderManager] Верификация: Ордер {order_id} не найден во внутренней базе для верификации.")
            return
        for attempt in range(1, max_attempts + 1):
            try:
                found = None
                if is_stop_order:
                    # Check active stop orders first
                    active_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_ACTIVE)
                    found = next((so for so in active_stop_orders_response.stop_orders if so.stop_order_id == order_id and so.figi == self.figi), None)
                    
                    if found:
                        # If stop order is found active, update its status and store exchange_order_id if available
                        if found.exchange_order_id and not order_data.exchange_order_id:
                            order_data.exchange_order_id = found.exchange_order_id
                            order_data.update_status("ACTIVATED", "API_VERIFICATION_ACTIVATED", attempt=attempt, exchange_order_id=found.exchange_order_id)
                            self.logger.debug(f"[OrderManager] Верификация: Стоп-ордер {order_id} активирован. Биржевой ID: {found.exchange_order_id}.")
                            self.save_state()
                            return True # Stop verification for this order, it's activated
                        elif not order_data.exchange_order_id: # Still active but not yet activated
                             order_data.update_status("ACTIVE", "API_VERIFICATION", attempt=attempt)
                             self.logger.debug(f"[OrderManager] Верификация: Стоп-ордер {order_id} подтвержден как активный на бирже после {attempt} попыток.")
                             self.save_state()
                             return True # Stop verification for this order, it's active
                    
                    # Check executed stop orders
                    executed_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_EXECUTED)
                    found_executed = next((so for so in executed_stop_orders_response.stop_orders if so.stop_order_id == order_id and so.figi == self.figi), None)

                    if found_executed:
                        # If stop order is found executed, update its status and store exchange_order_id
                        if found_executed.exchange_order_id and not order_data.exchange_order_id:
                            order_data.exchange_order_id = found_executed.exchange_order_id
                            order_data.update_status("ACTIVATED", "API_VERIFICATION_EXECUTED_STOP", attempt=attempt, exchange_order_id=found_executed.exchange_order_id)
                            self.logger.debug(f"[OrderManager] Верификация: Стоп-ордер {order_id} исполнен. Биржевой ID: {found_executed.exchange_order_id}.")
                            self.save_state()
                            return True # Stop verification for this order
                        elif not order_data.exchange_order_id: # Executed but no exchange_order_id yet (unlikely but for robustness)
                            order_data.update_status("FILLED", "API_VERIFICATION_EXECUTED_STOP", attempt=attempt)
                            self.logger.debug(f"[OrderManager] Верификация: Стоп-ордер {order_id} исполнен (без exchange_order_id).")
                            self.save_state()
                            return True

                    # Check cancelled stop orders
                    canceled_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_CANCELED)
                    found_canceled = next((so for so in canceled_stop_orders_response.stop_orders if so.stop_order_id == order_id and so.figi == self.figi), None)
                    if found_canceled:
                        order_data.update_status("CANCELLED", "API_VERIFICATION_CANCELLED_STOP", attempt=attempt)
                        self.save_state()
                        return True # Stop verification for this order if cancelled

                else: # Regular order
                    active_orders_response = await client.orders.get_orders(account_id=account_id)
                    found = next((o for o in active_orders_response.orders if o.order_id == order_id and o.figi == self.figi), None)
                
                if found:
                    order_data.update_status("ACTIVE", "API_VERIFICATION", attempt=attempt)
                    self.logger.debug(f"[OrderManager] Верификация: Ордер {order_id} подтвержден как активный на бирже после {attempt} попыток.")
                    self.save_state()
                    return True
                else:
                    self.logger.debug(f"[OrderManager] Верификация: Ордер {order_id} не найден на бирже (попытка {attempt}/{max_attempts}).")
                    await asyncio.sleep(delay)
            except RequestError as e:
                self.logger.debug(f"[OrderManager] Верификация: Ошибка API при проверке ордера {order_id} (попытка {attempt}/{max_attempts}): {e}")
                await asyncio.sleep(delay)
            except Exception as e:
                self.logger.error(f"[OrderManager] Верификация: Непредвиденная ошибка при проверке ордера {order_id}: {e}", exc_info=True)
                order_data.update_status("ERROR", "VERIFICATION_FAILED_CRITICAL", error=str(e))
                self.save_state()
                return False # Critical error, stop trying
        
        order_data.update_status("FAILED_TO_ACTIVATE", "VERIFICATION_TIMEOUT")
        self.logger.debug(f"[OrderManager] Верификация: Ордер {order_id} не был подтвержден как активным на бирже после {max_attempts} попыток.")
        self.save_state()
        return False

    async def place_limit_order(self, client: AsyncClient, price: Decimal, quantity: int, direction: OrderDirection, order_tag: str, account_id: str) -> Optional[str]:
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно разместить ордер.")
            return None
        try:
            price_quantized = (price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
            if price_quantized <= 0:
                self.logger.debug(f"[OrderManager] Расчетная лимитная цена {price_quantized:.{self.display_precision}f} некорректна. Ордер не будет размещен.")
                return None
            
            order_uuid = str(uuid.uuid4())
            response = await client.orders.post_order(
                account_id=account_id,
                figi=self.figi,
                quantity=quantity,
                price=_decimal_to_quotation(price_quantized),
                direction=direction,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=order_uuid,
            )
            
            order_data = OrderStateData(
                order_id=response.order_id,
                account_id=account_id,
                figi=self.figi,
                direction=direction.name,
                quantity_requested=quantity,
                order_type=OrderType.ORDER_TYPE_LIMIT.name,
                price=str(price_quantized),
                client_code=self.bot_client_code,
                is_stop_order=False
            )
            self.orders[response.order_id] = order_data
            
            # Simplified log for INFO level
            if self.logger.isEnabledFor(logging.INFO):
                simplified_tag = self.SIMPLIFIED_ORDER_TAGS.get(order_tag, order_tag)
                self.logger.info(f"{account_state.account_name_for_log}-размещен ордер {simplified_tag}, цена {price_quantized:.{self.display_precision}f}, количество лотов {quantity}")
            
            self.save_state()
            # Start verification task
            self.verification_tasks[response.order_id] = asyncio.create_task(
                self._verify_order_active_task(client, response.order_id, False, account_id)
            )
            return response.order_id
        except Exception as e:
            self.logger.error(f"[OrderManager] Ошибка при размещении LIMIT ордера для {account_id}: {e}", exc_info=True)
            return None

    async def place_stop_order(self, client: AsyncClient, stop_price: Decimal, quantity: int, direction: OrderDirection, stop_order_type: StopOrderType, limit_price: Optional[Decimal], order_tag: str, account_id: str) -> Optional[str]:
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно разместить ордер.")
            return None
        try:
            stop_price_quantized = (stop_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
            if stop_price_quantized <= 0:
                self.logger.debug(f"[OrderManager] Расчетная стоп-цена {stop_price_quantized:.{self.display_precision}f} некорректна. Ордер не будет размещен.")
                return None
            
            price_for_stop_order_request = _decimal_to_quotation(limit_price or Decimal('0'))
            if stop_order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT and limit_price is None:
                self.logger.error("[OrderManager] Для STOP_LIMIT ордера необходимо указать limit_price.")
                return None
            order_uuid = str(uuid.uuid4())
            response = await client.stop_orders.post_stop_order(
                account_id=account_id,
                instrument_id=self.figi,
                quantity=quantity,
                stop_price=_decimal_to_quotation(stop_price_quantized),
                direction=direction, 
                expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
                stop_order_type=stop_order_type,
                exchange_order_type=ExchangeOrderType.EXCHANGE_ORDER_TYPE_LIMIT if stop_order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT else ExchangeOrderType.EXCHANGE_ORDER_TYPE_MARKET,
                price=price_for_stop_order_request,
                order_id=order_uuid,
            )
            
            order_data = OrderStateData(
                order_id=response.stop_order_id,
                account_id=account_id,
                figi=self.figi,
                direction=direction.name,
                quantity_requested=quantity,
                order_type=ExchangeOrderType.EXCHANGE_ORDER_TYPE_LIMIT.name if stop_order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT else ExchangeOrderType.EXCHANGE_ORDER_TYPE_MARKET.name,
                stop_order_type=stop_order_type.name,
                price=str(limit_price) if limit_price else None,
                stop_price=str(stop_price_quantized),
                client_code=self.bot_client_code,
                is_stop_order=True
            )
            self.orders[response.stop_order_id] = order_data
            
            # Simplified log for INFO level
            if self.logger.isEnabledFor(logging.INFO):
                simplified_tag = self.SIMPLIFIED_ORDER_TAGS.get(order_tag, order_tag)
                self.logger.info(f"{account_state.account_name_for_log}-размещен ордер {simplified_tag}, цена {stop_price_quantized:.{self.display_precision}f}, количество лотов {quantity}")
            
            self.save_state()
            # Start verification task
            self.verification_tasks[response.stop_order_id] = asyncio.create_task(
                self._verify_order_active_task(client, response.stop_order_id, True, account_id)
            )
            return response.stop_order_id
        except Exception as e:
            self.logger.error(f"[OrderManager] Ошибка при размещении STOP ордера для {account_id}: {e}", exc_info=True)
            return None

    async def place_market_order(self, client: AsyncClient, quantity: int, direction: OrderDirection, order_tag: str, account_id: str) -> Optional[str]:
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно разместить ордер.")
            return None
        try:
            order_uuid = str(uuid.uuid4())
            response = await client.orders.post_order(
                account_id=account_id,
                figi=self.figi,
                quantity=quantity,
                direction=direction,
                order_type=OrderType.ORDER_TYPE_MARKET,
                order_id=order_uuid,
            )
            
            order_data = OrderStateData(
                order_id=response.order_id,
                account_id=account_id,
                figi=self.figi,
                direction=direction.name,
                quantity_requested=quantity,
                order_type=OrderType.ORDER_TYPE_MARKET.name,
                client_code=self.bot_client_code,
                is_stop_order=False
            )
            self.orders[response.order_id] = order_data
            
            # Simplified log for INFO level
            if self.logger.isEnabledFor(logging.INFO):
                simplified_tag = self.SIMPLIFIED_ORDER_TAGS.get(order_tag, order_tag)
                self.logger.info(f"{account_state.account_name_for_log}-размещен ордер {simplified_tag}, количество лотов {quantity}")
            
            self.save_state()
            # Market orders are usually filled immediately, but still verify for consistency
            self.verification_tasks[response.order_id] = asyncio.create_task(
                self._verify_order_active_task(client, response.order_id, False, account_id)
            )
            return response.order_id
        except RequestError as e:
            error_code = str(e.code) if hasattr(e, 'code') else ''
            if error_code == '30079':
                self.logger.error(f"ОШИБКА: Инструмент {TICKER} недоступен для торговли. Не удалось разместить рыночный ордер для счета {account_id}. {e.message}")
            else:
                self.logger.error(f"[OrderManager] Ошибка при размещении MARKET ордера для {account_id}: {e}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(f"[OrderManager] Непредвиденная ошибка при размещении MARKET ордера для {account_id}: {e}", exc_info=True)
            return None

    async def cancel_order(self, client: AsyncClient, order_id: str, is_stop_order: bool, account_id: str) -> bool:
        order_data = self.orders.get(order_id)
        if not order_data:
            self.logger.debug(f"[OrderManager] Отмена: Ордер {order_id} не найден во внутренней базе. Возможно, уже отменен или исполнен.")
            return True # Assume it's already handled
        try:
            if is_stop_order:
                await client.stop_orders.cancel_stop_order(account_id=account_id, stop_order_id=order_id)
            else:
                await client.orders.cancel_order(account_id=account_id, order_id=order_id)
            
            order_data.update_status("CANCEL_REQUESTED", "BOT_CANCELLATION")
            self.logger.debug(f"[OrderManager] Отправлен запрос на отмену {'стоп-ордера' if is_stop_order else 'ордера'} {order_id} для счета {account_id}.")
            self.save_state()
            # Wait for confirmation of cancellation
            max_attempts = 5
            delay = 0.5
            for attempt in range(max_attempts):
                await asyncio.sleep(delay)
                found_on_exchange = None
                if is_stop_order:
                    # Check for CANCELLED or EXECUTED status directly
                    try:
                        # Get stop orders with specific status
                        cancelled_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_CANCELED)
                        executed_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_EXECUTED)
                        
                        if any(so.stop_order_id == order_id for so in cancelled_stop_orders_response.stop_orders):
                            order_data.update_status("CANCELLED", "API_CONFIRMED_CANCELLATION")
                            self.logger.debug(f"[Стоп-ордер] {order_id} для счета {account_id} успешно отменен (подтверждено API).")
                            self.save_state()
                            return True
                        elif any(so.stop_order_id == order_id for so in executed_stop_orders_response.stop_orders):
                            self.logger.debug(f"[Стоп-ордер] {order_id} для счета {account_id} был исполнен, а не отменен.")
                            order_data.update_status("FILLED", "API_CONFIRMED_FILLED") # Mark as filled if it was executed
                            self.save_state()
                            return True
                        else:
                            # Still check active list if not found in cancelled/executed
                            active_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_ACTIVE)
                            found_on_exchange = next((so for so in active_stop_orders_response.stop_orders if so.stop_order_id == order_id and so.figi == self.figi), None)
                    except Exception as e:
                        self.logger.warning(f"[OrderManager] Отмена: Ошибка при проверке статуса стоп-ордера {order_id} через API: {e}")
                        # Fallback to just checking active list if status query fails
                        active_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_ACTIVE)
                        found_on_exchange = next((so for so in active_stop_orders_response.stop_orders if so.stop_order_id == order_id and so.figi == self.figi), None)
                else: # Regular order (not a stop order)
                    active_orders_response = await client.orders.get_orders(account_id=account_id)
                    found_on_exchange = next((o for o in active_orders_response.orders if o.order_id == order_id and o.figi == self.figi), None)
                
                if not found_on_exchange: # If not found in active list, assume cancelled
                    order_data.update_status("CANCELLED", "API_CONFIRMED_CANCELLATION")
                    self.logger.debug(f"[{'Стоп-ордер' if is_stop_order else 'Ордер'}] {order_id} для счета {account_id} успешно отменен.")
                    self.save_state()
                    return True
            
            order_data.update_status("CANCELLATION_FAILED", "API_TIMEOUT")
            self.logger.debug(f"[OrderManager] Не удалось подтвердить отмену {'стоп-ордера' if is_stop_order else 'ордера'} {order_id} для счета {account_id} после {max_attempts} попыток. Возможно, он все еще активен.")
            self.save_state()
            return False
        except RequestError as e:
            error_code = str(e.code) if hasattr(e, 'code') else ''
            error_message = str(e.message) if hasattr(e, 'message') else str(e)
            
            # Улучшенное логирование для "безобидных" ошибок отмены
            if "partially or fully executed" in error_message or \
               "30059" in error_code or "70001" in error_code or \
               "50006" in error_code or "Stop-order not found" in error_message:
                order_data.update_status("ALREADY_INACTIVE", "API_ERROR_MESSAGE")
                self.logger.debug(f"[{'Стоп-ордер' if is_stop_order else 'Ордер'}] {order_id} для {account_id} уже неактивен (исполнен/не найден/внутренняя ошибка).")
                self.save_state()
                return True
            else:
                order_data.update_status("ERROR", "API_ERROR", error=str(e))
                self.logger.error(f"[OrderManager] Ошибка API при отмене {'стоп-ордера' if is_stop_order else 'ордера'} {order_id} для {account_id}: {e}", exc_info=True)
                self.save_state()
                return False
        except Exception as e:
            order_data.update_status("ERROR", "UNEXPECTED_ERROR", error=str(e))
            self.logger.debug(f"[OrderManager] Непредвиденная ошибка при отмене {'стоп-ордера' if is_stop_order else 'ордера'} {order_id} для {account_id}: {e}", exc_info=True)
            self.save_state()
            return False

    async def _place_individual_take_profit_order(self, client, account_id: str, lot_entry_price: Decimal, quantity: int):
        """
        Размещает индивидуальный тейк-профит ордер для одного лота.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно разместить TP ордер.")
            return None

        direction_to_close = OrderDirection.ORDER_DIRECTION_BUY if account_state.is_sell_account else OrderDirection.ORDER_DIRECTION_SELL
        
        # Для шорта (счет SELL): TP ниже цены входа
        # Для лонга (счет BUY): TP выше цены входа
        tp_price = lot_entry_price - account_state.grid_interval if account_state.is_sell_account else lot_entry_price + account_state.grid_interval
        
        # Квантуем цену TP
        tp_price_quantized = (tp_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
        
        if tp_price_quantized <= 0:
            self.logger.warning(f"Расчетная TP цена {tp_price_quantized:.{self.display_precision}f} некорректна для лота с ценой {lot_entry_price:.{self.display_precision}f}. TP ордер не будет размещен.")
            return None
        order_tag = "TP_SELL_LOT" if account_state.is_sell_account else "TP_BUY_LOT"
        
        try:
            response_id = await self.place_stop_order(
                client,
                account_id=account_id,
                stop_price=tp_price_quantized,
                quantity=quantity,
                direction=direction_to_close,
                stop_order_type=StopOrderType.STOP_ORDER_TYPE_TAKE_PROFIT,
                limit_price=None, # TP обычно рыночный ордер при срабатывании стопа
                order_tag=order_tag
            )
            return response_id
        except Exception as e:
            self.logger.error(f"Ошибка при размещении индивидуального TP ордера для {account_state.account_name_for_log}: {e}")
            return None

    async def _cancel_individual_take_profit_order(self, client, account_id: str, tp_order_id: str):
        """
        Отменяет индивидуальный тейк-профит ордер.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно отменить TP ордер.")
            return

        if not tp_order_id:
            return
        try:
            success = await self.cancel_order(
                client,
                order_id=tp_order_id,
                is_stop_order=True, # TP is a stop order
                account_id=account_id
            )
            if success:
                self.logger.info(f"Для {account_state.account_name_for_log}: Успешно отменен TP ордер {tp_order_id}.")
                # После успешной отмены, обновить внутреннее состояние лота
                for lot in account_state.open_lots:
                    if lot.get('tp_order_id') == tp_order_id:
                        lot['tp_order_id'] = None # Сбрасываем ID TP ордера
                        break # Нашли и обновили, выходим
            else:
                # Это сообщение теперь будет выводиться только если ордер НЕ перешел в конечное состояние
                self.logger.warning(f"Для {account_state.account_name_for_log}: Не удалось подтвердить отмену TP ордера {tp_order_id}.")
        except Exception as e:
            self.logger.warning(f"Для {account_state.account_name_for_log}: Непредвиденная ошибка при отмене TP ордера {tp_order_id}: {e}")

    async def _place_directional_entry_orders(self, client, account_id: str, base_price: Decimal):
        """
        Размещает или переразмещает лимитные и стоп-ордера для входа в позицию
        или добавления к ней, исходя из базовой цены.
        Отменяет старые соответствующие ордера перед размещением новых.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно разместить ордера для входа.")
            return

        # NEW: Проверка активности счета по тренду
        if ENABLE_TREND_FILTERING and not account_state.is_active_by_trend:
            self.logger.debug(f"[{account_state.account_name_for_log}] Счет неактивен по тренду. Пропускаем размещение ордеров для входа.")
            return

        async with self.orders_placement_lock:
            # Проверяем, не достигнут ли лимит маржи
            if account_state.margin_limit_hit:
                self.logger.warning(f"[{account_state.account_name_for_log}] Лимит использования маржи (потенциальное обеспечение) достигнут ({account_state.max_free_margin_rub:.{self.display_precision}f} RUB). Новые ордера для входа не размещаются.")
                return
            self.logger.info(f"Размещение/обновление ордеров для входа для {account_state.account_name_for_log}. Базовая цена: {base_price:.{self.display_precision}f}")
            interval = account_state.grid_interval
            
            # --- Шаг 1: Отмена старых ордеров для входа ---
            old_upper_entry_id, old_upper_is_stop = account_state.active_entry_order_above
            old_lower_entry_id, old_lower_is_stop = account_state.active_entry_order_below
            
            orders_to_cancel_tasks = []
            if old_upper_entry_id:
                order_status = self.get_order_status(old_upper_entry_id)
                if order_status not in ["FILLED", "CANCELLED", "FAILED_TO_ACTIVATE", "ERROR", "ALREADY_INACTIVE", "ACTIVATED"]:
                    orders_to_cancel_tasks.append(self.cancel_order(client, old_upper_entry_id, old_upper_is_stop, account_id))
                else:
                    self.logger.debug(f"[{account_state.account_name_for_log}] Отмена пропущена для верхнего ордера {old_upper_entry_id}, т.к. статус уже {order_status}.")
            if old_lower_entry_id:
                order_status = self.get_order_status(old_lower_entry_id)
                if order_status not in ["FILLED", "CANCELLED", "FAILED_TO_ACTIVATE", "ERROR", "ALREADY_INACTIVE", "ACTIVATED"]:
                    orders_to_cancel_tasks.append(self.cancel_order(client, old_lower_entry_id, old_lower_is_stop, account_id))
                else:
                    self.logger.debug(f"[{account_state.account_name_for_log}] Отмена пропущена для нижнего ордера {old_lower_entry_id}, т.к. статус уже {order_status}.")
            
            if orders_to_cancel_tasks:
                self.logger.info(f"Для {account_state.account_name_for_log}: Отмена старых ордеров для входа (верхний: {old_upper_entry_id}, нижний: {old_lower_entry_id}) перед размещением новых.")
                # Выполняем отмену параллельно
                cancellation_results = await asyncio.gather(*orders_to_cancel_tasks, return_exceptions=True)
                for res in cancellation_results:
                    if isinstance(res, Exception):
                        self.logger.error(f"Ошибка при параллельной отмене ордера: {res}")
                
                # После попытки отмены, очищаем внутренние ID
                account_state.active_entry_order_above = (None, None)
                account_state.active_entry_order_below = (None, None)
            else:
                self.logger.debug(f"Для {account_state.account_name_for_log}: Старые ордера для входа не найдены для отмены.")

            # --- Шаг 2: Размещение новых лимитных и стоп-ордеров для входа ---
            placement_tasks = []
            
            # Переменные для хранения будущих ID ордеров
            upper_order_future = None
            lower_order_future = None

            if account_state.is_sell_account: # SELL Account (Shorting)
                upper_price = base_price + interval
                upper_price_quantized = (upper_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
                if upper_price_quantized > 0:
                    upper_order_future = self.place_limit_order(
                        client, upper_price_quantized, LOTS, OrderDirection.ORDER_DIRECTION_SELL, 
                        order_tag="SELL_ENTRY_UPPER_LIMIT", account_id=account_id
                    )
                    placement_tasks.append(upper_order_future)
                else:
                    self.logger.debug(f"[OrderManager] Расчетная верхняя лимитная цена {upper_price_quantized:.{self.display_precision}f} некорректна. Верхний ордер не будет размещен.")

                lower_price = base_price - interval
                lower_price_quantized = (lower_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
                if lower_price_quantized > 0:
                    lower_order_future = self.place_stop_order(
                        client,
                        account_id=account_id,
                        stop_price=lower_price_quantized,
                        quantity=LOTS,
                        direction=OrderDirection.ORDER_DIRECTION_SELL,
                        stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
                        limit_price=None,
                        order_tag="SELL_ENTRY_LOWER_STOP"
                    )
                    placement_tasks.append(lower_order_future)
                else:
                    self.logger.debug(f"[OrderManager] Расчетная нижняя стоп-цена {lower_price_quantized:.{self.display_precision}f} некорректна. Нижний ордер не будет размещен.")

            else: # BUY Account (Longing)
                lower_price = base_price - interval
                lower_price_quantized = (lower_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
                if lower_price_quantized > 0:
                    lower_order_future = self.place_limit_order(
                        client, lower_price_quantized, LOTS, OrderDirection.ORDER_DIRECTION_BUY, 
                        order_tag="BUY_ENTRY_LOWER_LIMIT", account_id=account_id
                    )
                    placement_tasks.append(lower_order_future)
                else:
                    self.logger.debug(f"[OrderManager] Расчетная нижняя лимитная цена {lower_price_quantized:.{self.display_precision}f} некорректна. Нижний ордер не будет размещен.")

                upper_price = base_price + interval
                upper_price_quantized = (upper_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
                if upper_price_quantized > 0:
                    upper_order_future = self.place_stop_order(
                        client,
                        account_id=account_id,
                        stop_price=upper_price_quantized,
                        quantity=LOTS,
                        direction=OrderDirection.ORDER_DIRECTION_BUY,
                        stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
                        limit_price=None,
                        order_tag="BUY_ENTRY_UPPER_STOP"
                    )
                    placement_tasks.append(upper_order_future)
                else:
                    self.logger.debug(f"[OrderManager] Расчетная верхняя стоп-цена {upper_price_quantized:.{self.display_precision}f} некорректна. Верхний ордер не будет размещен.")
            
            # Выполняем размещение ордеров параллельно
            if placement_tasks:
                # asyncio.gather сохраняет порядок результатов, соответствующий порядку задач
                placement_results = await asyncio.gather(*placement_tasks, return_exceptions=True)
                
                # Обновляем active_entry_order_above/below на основе результатов
                # Предполагается, что порядок задач в placement_tasks соответствует порядку upper/lower_order_future
                if account_state.is_sell_account:
                    upper_response_id = placement_results[0] if len(placement_results) > 0 and not isinstance(placement_results[0], Exception) else None
                    lower_response_id = placement_results[1] if len(placement_results) > 1 and not isinstance(placement_results[1], Exception) else None
                else: # BUY Account
                    lower_response_id = placement_results[0] if len(placement_results) > 0 and not isinstance(placement_results[0], Exception) else None
                    upper_response_id = placement_results[1] if len(placement_results) > 1 and not isinstance(placement_results[1], Exception) else None

                if account_state.is_sell_account:
                    if upper_response_id:
                        account_state.active_entry_order_above = (upper_response_id, False)
                    else:
                        self.logger.error(f"Не удалось разместить верхний SELL LIMIT ордер для входа на {upper_price_quantized:.{self.display_precision}f} для {account_state.account_name_for_log}")
                    if lower_response_id:
                        account_state.active_entry_order_below = (lower_response_id, True)
                    else:
                        self.logger.error(f"Не удалось разместить нижний SELL STOP ордер для входа на {lower_price_quantized:.{self.display_precision}f} для {account_state.account_name_for_log}")
                else: # BUY Account
                    if lower_response_id:
                        account_state.active_entry_order_below = (lower_response_id, False)
                    else:
                        self.logger.error(f"Не удалось разместить нижний BUY LIMIT ордер для входа на {lower_price_quantized:.{self.display_precision}f} для {account_state.account_name_for_log}")
                    if upper_response_id:
                        account_state.active_entry_order_above = (upper_response_id, True)
                    else:
                        self.logger.error(f"Не удалось разместить верхний BUY STOP ордер для входа на {upper_price_quantized:.{self.display_precision}f} для {account_state.account_name_for_log}")
            else:
                self.logger.info(f"Для {account_state.account_name_for_log}: Нет ордеров для размещения.")
            self.logger.info(f"Размещение/обновление ордеров для входа для {account_state.account_name_for_log} завершено.")
            account_state.last_orders_placement_time = datetime.now()
            account_state.orders_need_reposition = False # Сбрасываем флаг после успешного размещения
            account_state.initial_entry_orders_placed_for_session = True # НОВОЕ: Отмечаем, что начальные ордера для сессии размещены
            self.save_state() # Сохраняем состояние после размещения ордеров

    async def _identify_order_type_and_tag(self, client: AsyncClient, account_id: str, incoming_order_id: str, order_direction: OrderDirection) -> tuple[str, bool, bool, Optional[OrderStateData]]:
        """
        Идентифицирует тип исполненного ордера (открывающий позицию, TP) и его тег.
        Возвращает (order_tag, is_opening_order_fill, is_tp_order_fill, identified_order_data).
        identified_order_data will be the OrderStateData object for the original order (stop or regular).
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно идентифицировать ордер.")
            return "UNKNOWN", False, False, None

        order_tag = "UNKNOWN"
        is_opening_order_fill = False
        is_tp_order_fill = False
        
        identified_order_data = None

        # 1. Try to find by direct order_id (original stop_order_id or regular order_id)
        identified_order_data = self.orders.get(incoming_order_id)

        # 2. If not found by direct ID, try to find by exchange_order_id for any known stop order
        if not identified_order_data:
            for oid, od in self.orders.items():
                if od.is_stop_order and od.exchange_order_id == incoming_order_id:
                    identified_order_data = od
                    break
        
        # 3. Fallback: If still not identified, try to fetch from API and link it to a pending stop order
        if not identified_order_data:
            self.logger.debug(f"[{account_state.account_name_for_log}] Идентификация: Ордер {incoming_order_id} не найден внутренне. Попытка получить данные через API для связывания со стоп-ордером.")
            try:
                api_found_stop_order = None

                # First, check active stop orders
                active_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_ACTIVE)
                api_found_stop_order = next((so for so in active_stop_orders_response.stop_orders if so.figi == self.figi and (so.exchange_order_id == incoming_order_id or so.stop_order_id == incoming_order_id)), None)

                if not api_found_stop_order:
                    # If not found in active, check executed stop orders
                    executed_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_EXECUTED)
                    api_found_stop_order = next((so for so in executed_stop_orders_response.stop_orders if so.figi == self.figi and (so.exchange_order_id == incoming_order_id or so.stop_order_id == incoming_order_id)), None)

                if api_found_stop_order:
                    # Found a stop order on the API (active or executed) that matches the incoming_order_id.
                    # Now, try to find our internal OrderStateData that corresponds to this API stop order.
                    for oid, od in self.orders.items():
                        if od.is_stop_order and od.account_id == account_id and od.figi == self.figi:
                            # Match by original stop_order_id
                            if od.order_id == api_found_stop_order.stop_order_id:
                                identified_order_data = od
                                # Crucial: Update our internal OrderStateData with the exchange_order_id if it's missing
                                if api_found_stop_order.exchange_order_id and not identified_order_data.exchange_order_id:
                                    identified_order_data.exchange_order_id = api_found_stop_order.exchange_order_id
                                    # Update status based on API status
                                    if api_found_stop_order.status == StopOrderStatusOption.STOP_ORDER_STATUS_EXECUTED:
                                        identified_order_data.update_status("FILLED", "API_SYNC_EXECUTED_STOP_STREAM_MISS")
                                        self.logger.info(f"[{account_state.account_name_for_log}] Идентификация: Стоп-ордер {identified_order_data.order_id} (API Sync) исполнен. Биржевой ID: {api_found_stop_order.exchange_order_id}.")
                                    else: # Must be active
                                        identified_order_data.update_status("ACTIVATED", "API_SYNC_ACTIVATED_STREAM_MISS")
                                        self.logger.info(f"[{account_state.account_name_for_log}] Идентификация: Стоп-ордер {identified_order_data.order_id} (API Sync) активирован. Биржевой ID: {api_found_stop_order.exchange_order_id}.")
                                    self.save_state()
                                break
                    
                    if not identified_order_data:
                        self.logger.warning(f"[{account_state.account_name_for_log}] Идентификация: Стоп-ордер {incoming_order_id} найден на бирже, но не удалось связать с внутренним ордером бота. Возможно, ордер был размещен извне или внутреннее состояние повреждено.")
                        return order_tag, is_opening_order_fill, is_tp_order_fill, None

                else:
                    self.logger.debug(f"[{account_state.account_name_for_log}] Идентификация: Ордер {incoming_order_id} не найден как стоп-ордер (активный или исполненный) на бирже.")

            except RequestError as e:
                self.logger.warning(f"[{account_state.account_name_for_log}] Идентификация: Ошибка API при поиске стоп-ордеров для {incoming_order_id}: {e}")
            except Exception as e:
                self.logger.error(f"[{account_state.account_name_for_log}] Идентификация: Непредвиденная ошибка при поиске стоп-ордера {incoming_order_id}: {e}", exc_info=True)

        # If after all attempts, it's still not identified
        if not identified_order_data:
            self.logger.warning(f"Не удалось найти данные ордера {incoming_order_id} (или его оригинальный стоп-ордер) в OrderManager. Невозможно определить тип.")
            return order_tag, is_opening_order_fill, is_tp_order_fill, None

        # Now use identified_order_data for all checks
        
        # 1. Проверяем, является ли исполненный ордер "открывающим" ордером
        if account_state.is_sell_account:
            if order_direction == OrderDirection.ORDER_DIRECTION_SELL:
                if identified_order_data.order_type == OrderType.ORDER_TYPE_LIMIT.name:
                    order_tag = "SELL_ENTRY_UPPER_LIMIT"
                    is_opening_order_fill = True
                elif identified_order_data.stop_order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LOSS.name:
                    order_tag = "SELL_ENTRY_LOWER_STOP"
                    is_opening_order_fill = True
                elif identified_order_data.order_type == OrderType.ORDER_TYPE_MARKET.name:
                    order_tag = "INITIAL_MARKET_SELL"
                    is_opening_order_fill = True
        else: # BUY account
            if order_direction == OrderDirection.ORDER_DIRECTION_BUY:
                if identified_order_data.order_type == OrderType.ORDER_TYPE_LIMIT.name:
                    order_tag = "BUY_ENTRY_LOWER_LIMIT"
                    is_opening_order_fill = True
                elif identified_order_data.stop_order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LOSS.name:
                    order_tag = "BUY_ENTRY_UPPER_STOP"
                    is_opening_order_fill = True
                elif identified_order_data.order_type == OrderType.ORDER_TYPE_MARKET.name:
                    order_tag = "INITIAL_MARKET_BUY"
                    is_opening_order_fill = True
        
        # 2. Проверяем, является ли исполненный ордер Тейк-Profiтом (TP)
        if identified_order_data.is_stop_order and identified_order_data.stop_order_type == StopOrderType.STOP_ORDER_TYPE_TAKE_PROFIT.name:
            if (account_state.is_sell_account and order_direction == OrderDirection.ORDER_DIRECTION_BUY) or \
               (not account_state.is_sell_account and order_direction == OrderDirection.ORDER_DIRECTION_SELL):
                is_linked_to_lot = any(lot.get('tp_order_id') == identified_order_data.order_id for lot in account_state.open_lots)
                if is_linked_to_lot:
                    order_tag = "TP_SELL_LOT" if account_state.is_sell_account else "TP_BUY_LOT"
                    is_tp_order_fill = True
                    is_opening_order_fill = False # TP не является открывающим ордером
                else:
                    self.logger.warning(f"Исполненный TP ордер {identified_order_data.order_id} не связан с активным лотом. Возможно, лот уже закрыт или это старый ордер.")
        
        self.logger.debug(f"[{account_state.account_name_for_log}] Идентификация ордера {incoming_order_id} (mapped to {identified_order_data.order_id if identified_order_data else 'None'}): tag={order_tag}, is_opening={is_opening_order_fill}, is_tp={is_tp_order_fill}")
        return order_tag, is_opening_order_fill, is_tp_order_fill, identified_order_data

    async def _process_opening_order_fill(self, client, account_id: str, order_trades_data: OrderTrades, filled_trade_item: OrderTrade, effective_order_id_for_processing: str, order_tag: str):
        """
        Обрабатывает исполнение ордера, открывающего позицию или добавляющего к ней.
        Обновляет last_position_fill_price на цену последнего исполнения.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно обработать исполнение открывающего ордера.")
            return

        filled_price_decimal = _quotation_to_decimal(filled_trade_item.price)
        filled_lots = filled_trade_item.quantity
        new_lot = {'entry_price': filled_price_decimal, 'tp_order_id': None, 'quantity': filled_lots}
        account_state.open_lots.append(new_lot)
        
        tp_order_id = await self._place_individual_take_profit_order(client, account_id, filled_price_decimal, filled_lots)
        if tp_order_id:
            new_lot['tp_order_id'] = tp_order_id
        
        account_state.account_started = True
        account_state.initial_orders_placed = True
        account_state.orders_need_reposition = True # Флаг для перестановки ордеров входа
        account_state.last_position_fill_price = filled_price_decimal # Обновляем базовую цену на цену последнего исполнения
        self.logger.debug(f"[{account_state.account_name_for_log}] _process_opening_order_fill: orders_need_reposition установлен в True, last_position_fill_price = {account_state.last_position_fill_price:.{self.display_precision}f}")
        
        # --- Логика очистки ID исполненного ордера ---
        # Теперь полагаемся на OrderManager для отслеживания статуса
        # Если ордер был исполнен, OrderManager уже обновит его статус.
        # Здесь мы просто очищаем наши внутренние ссылки, чтобы не пытаться переразместить.
        upper_id_tracked, upper_is_stop_tracked = account_state.active_entry_order_above
        lower_id_tracked, lower_is_stop_tracked = account_state.active_entry_order_below
        if effective_order_id_for_processing == account_state.pending_initial_market_order_id:
            account_state.pending_initial_market_order_id = None
            self.logger.debug(f"[{account_state.account_name_for_log}] Очищен pending_initial_market_order_id: {effective_order_id_for_processing}")
        
        if effective_order_id_for_processing == upper_id_tracked:
            account_state.active_entry_order_above = (None, None)
            self.logger.debug(f"[{account_state.account_name_for_log}] Очищен active_entry_order_above после исполнения: {effective_order_id_for_processing}")
        
        if effective_order_id_for_processing == lower_id_tracked:
            account_state.active_entry_order_below = (None, None)
            self.logger.debug(f"[{account_state.account_name_for_log}] Очищен active_entry_order_below после исполнения: {effective_order_id_for_processing}")
        # Simplified log for INFO level
        if self.logger.isEnabledFor(logging.INFO):
            simplified_tag = self.SIMPLIFIED_ORDER_TAGS.get(order_tag, order_tag)
            self.logger.info(f"{account_state.account_name_for_log}-исполнен ордер {simplified_tag}, цена {filled_price_decimal:.{self.display_precision}f}, количество лотов {filled_lots}")
        self.save_state()

    async def _process_tp_order_fill(self, client, account_id: str, effective_order_id_for_processing: str, filled_price_decimal: Decimal, filled_lots: int):
        """
        Обрабатывает исполнение тейк-профит ордера.
        Теперь использует original_stop_order_id для точной удаления лота.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно обработать исполнение TP ордера.")
            return

        lots_to_keep = []
        lots_closed_count = 0
        found_lot_to_remove = False
        
        # Получаем данные исполненного ордера из OrderManager
        # effective_order_id_for_processing уже является ID оригинального стоп-ордера
        executed_order_data = self.get_order_data(effective_order_id_for_processing)
        if not executed_order_data:
            self.logger.warning(f"Для {account_state.account_name_for_log}: Исполненный TP ордер {effective_order_id_for_processing} не найден в OrderManager. Невозможно точно обновить позицию.")
            # В этом случае мы не можем точно удалить лот, поэтому просто возвращаемся.
            # Полная синхронизация позже должна будет исправить расхождение.
            return
        
        # Обновление дневной прибыли/убытка
        closed_lot_entry_price = None
        for lot_data in account_state.open_lots:
            # Сравниваем tp_order_id лота с effective_order_id_for_processing
            if not found_lot_to_remove and lot_data.get('tp_order_id') == effective_order_id_for_processing:
                closed_lot_entry_price = lot_data['entry_price']
                lots_closed_count += lot_data['quantity']
                self.logger.debug(f"Для {account_state.account_name_for_log}: TP ордер {effective_order_id_for_processing} сработал, закрыт лот с ценой входа {lot_data['entry_price']:.{self.display_precision}f}.")
                found_lot_to_remove = True
            else:
                lots_to_keep.append(lot_data)
        
        account_state.open_lots = lots_to_keep
        if closed_lot_entry_price:
            pnl = (closed_lot_entry_price - filled_price_decimal) * filled_lots * account_state.lots_in_one_lot if account_state.is_sell_account else \
                  (filled_price_decimal - closed_lot_entry_price) * filled_lots * account_state.lots_in_one_lot
            account_state.daily_profit_loss += pnl
            self.logger.info(f"[{account_state.account_name_for_log}] Дневная прибыль/убыток обновлен: {account_state.daily_profit_loss:.{self.display_precision}f}")
            if account_state.daily_profit_loss < -account_state.max_daily_loss_rub:
                account_state.daily_loss_limit_hit = True
                self.logger.critical(f"[{account_state.account_name_for_log}] Дневной лимит убытка ({account_state.max_daily_loss_rub:.{self.display_precision}f} RUB) ДОСТИГНУТ! Торговля для этого счета будет остановлена.")
                
                # Отменяем все ордера через OrderManager
                await self.cancel_all_orders_for_account(
                    client, account_id, account_state.account_name_for_log, cancel_entry_orders_only=False
                )
        self.logger.info(f"Для {account_state.account_name_for_log}: После срабатывания TP закрыто {lots_closed_count} лот(ов). Текущая позиция: {sum(l['quantity'] for l in account_state.open_lots)} лот(ов).")
        
        if not account_state.open_lots:
            self.logger.info(f"Позиция {account_state.account_name_for_log} полностью закрыта через TP.")
            account_state.account_started = False
            account_state.initial_orders_placed = False
            account_state.orders_need_reposition = False
            account_state.last_position_fill_price = Decimal('0')
            # Отменяем все ордера через OrderManager
            await self.cancel_all_orders_for_account(
                client, account_id, account_state.account_name_for_log, cancel_entry_orders_only=False
            )
        else:
            account_state.orders_need_reposition = True # Переставляем ордера входа после закрытия TP
            self.logger.debug(f"[{account_state.account_name_for_log}] _process_tp_order_fill: orders_need_reposition установлен в True после TP.")
        
        # Simplified log for INFO level
        if self.logger.isEnabledFor(logging.INFO):
            simplified_tag = self.SIMPLIFIED_ORDER_TAGS.get("TP_SELL_LOT" if account_state.is_sell_account else "TP_BUY_LOT", "TP")
            self.logger.info(f"{account_state.account_name_for_log}-исполнен ордер {simplified_tag}, цена {filled_price_decimal:.{self.display_precision}f}, количество лотов {filled_lots}")
        self.save_state()

    async def handle_order_fill(self, client, order_trades_data: OrderTrades, filled_trade_item: OrderTrade):
        incoming_order_id = order_trades_data.order_id # This is the ID from the stream
        filled_price_decimal = _quotation_to_decimal(filled_trade_item.price)
        filled_lots = filled_trade_item.quantity
        order_direction = order_trades_data.direction
        account_id = order_trades_data.account_id

        order_tag, is_opening_order_fill, is_tp_order_fill, identified_order_data = await self._identify_order_type_and_tag(client, account_id, incoming_order_id, order_direction)
        
        if not identified_order_data:
            self.logger.warning(f"Не удалось идентифицировать ордер {incoming_order_id}. Пропускаем обработку исполнения.")
            return

        # Use the ID of the identified_order_data for further processing
        effective_order_id_for_processing = identified_order_data.order_id

        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно обработать исполнение ордера.")
            return

        # Update the OrderStateData for the identified order
        identified_order_data.filled_quantity += filled_lots
        identified_order_data.fill_price = str(filled_price_decimal) # Store the last fill price
        identified_order_data.update_status("FILLED" if identified_order_data.filled_quantity == identified_order_data.quantity_requested else "PARTIALLY_FILLED", "STREAM_FILL", trade_id=filled_trade_item.trade_id)

        if is_opening_order_fill:
            await self._process_opening_order_fill(client, account_id, order_trades_data, filled_trade_item, effective_order_id_for_processing, order_tag)
        elif is_tp_order_fill:
            await self._process_tp_order_fill(client, account_id, effective_order_id_for_processing, filled_price_decimal, filled_lots)
        else:
            self.logger.warning(f"Для {account_state.account_name_for_log}: Исполнен ордер {incoming_order_id} (внутренний ID: {effective_order_id_for_processing}, тег: {order_tag}), но он не является ни открывающим, ни TP. Будет выполнена пересинхронизация по расписанию.")
        
        total_quantity = sum(lot['quantity'] for lot in account_state.open_lots)
        total_cost = sum(lot['entry_price'] * lot['quantity'] for lot in account_state.open_lots)
        current_position_lots = total_quantity
        average_position_price = total_cost / total_quantity if total_quantity > 0 else Decimal('0')
        self.logger.info(f"Позиция {account_state.account_name_for_log} обновлена: {current_position_lots} лот(ов) по средней цене {average_position_price:.{self.display_precision}f}")
        
        self.save_state()

    async def _reset_daily_flags_if_new_day(self, client, account_id: str, last_known_market_price: Decimal):
        """Сбрасывает дневные флаги и PnL, если наступил новый день."""
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно сбросить дневные флаги.")
            return

        now_date = datetime.now(MOSCOW_TZ).date()
        if now_date > account_state.daily_pnl_reset_date:
            account_state.daily_profit_loss = Decimal('0')
            account_state.daily_loss_limit_hit = False
            account_state.daily_pnl_reset_date = now_date
            account_state.end_of_session_reduction_done = False
            account_state.initial_entry_orders_placed_for_session = False
            self.logger.info(f"[{account_state.account_name_for_log}] Дневной PnL и флаги сброшены на {now_date}.")
            # On new day, also cancel all existing orders to ensure a clean start
            self.logger.info(f"[{account_state.account_name_for_log}] Новый день. Отменяем все активные ордера для чистого старта.")
            await self.cancel_all_orders_for_account(
                client, account_id, account_state.account_name_for_log, cancel_entry_orders_only=False
            )
            # After cancelling all orders, force a re-sync to ensure internal state matches
            portfolio = await client.operations.get_portfolio(account_id=account_id)
            current_figi_position = next((pos for pos in portfolio.positions if pos.figi == self.figi), None)
            current_lots_held_from_api = abs(_quotation_to_decimal(current_figi_position.quantity_lots)).normalize() if current_figi_position else Decimal('0')
            current_avg_price_from_api = _quotation_to_decimal(current_figi_position.average_position_price) if current_figi_position and current_figi_position.average_position_price else Decimal('0')
            await self._sync_position_with_api(client, account_id, current_lots_held_from_api, current_avg_price_from_api, last_known_market_price) # Pass last_known_market_price
        self.save_state()

    async def _check_margin_and_manage_orders(self, client, account_id: str):
        """
        Проверяет текущее использование маржи (теперь minimal_margin) и устанавливает флаг margin_limit_hit.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно проверить маржу.")
            return

        try:
            margin_attributes = await client.users.get_margin_attributes(account_id=account_id)
            # ИЗМЕНЕНО: Используем minimal_margin для контроля лимита
            # Если minimal_margin равен None, используем 0 для безопасного сравнения
            current_margin_requirement = _quotation_to_decimal(margin_attributes.minimal_margin) if margin_attributes.minimal_margin else Decimal('0')
            self.logger.debug(f"[{account_state.account_name_for_log}] Текущее минимальное маржинальное требование (minimal_margin): {current_margin_requirement:.{self.display_precision}f} RUB. Лимит: {account_state.max_free_margin_rub:.{self.display_precision}f} RUB.")
            if current_margin_requirement >= account_state.max_free_margin_rub:
                if not account_state.margin_limit_hit:
                    self.logger.warning(f"[{account_state.account_name_for_log}] Лимит использования маржи (потенциальное обеспечение) ({account_state.max_free_margin_rub:.{self.display_precision}f} RUB) ДОСТИГНУТ! Размещение новых ордеров для входа не размещается.")
                account_state.margin_limit_hit = True
            else:
                if account_state.margin_limit_hit:
                    self.logger.info(f"[{account_state.account_name_for_log}] Лимит использования маржи (потенциальное обеспечение) ({account_state.max_free_margin_rub:.{self.display_precision}f} RUB) СНЯТ. Размещение новых ордеров для входа разрешено.")
                account_state.margin_limit_hit = False
        except RequestError as e:
            self.logger.warning(f"Ошибка получения атрибутов маржи для {account_state.account_name_for_log}: {e}. Невозможно проверить лимит маржи.")
        except Exception as e:
            self.logger.error(f"Непредвиденная ошибка при проверке маржи для {account_state.account_name_for_log}: {e}", exc_info=True)
        self.save_state()

    async def _perform_end_of_session_margin_reduction(self, client, account_id: str, last_known_market_price: Decimal):
        """
        Выполняет сокращение позиции в конце торговой сессии, если маржа превышает лимит.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно выполнить сокращение позиции.")
            return

        now = datetime.now(MOSCOW_TZ)
        if now.time() >= account_state.end_of_session_reduction_time and now.time() <= EXT_TRADING_END_TIME:
            if account_state.end_of_session_reduction_done:
                self.logger.debug(f"[{account_state.account_name_for_log}] Сокращение позиции на конец сессии уже выполнено сегодня.")
                return
            self.logger.info(f"[{account_state.account_name_for_log}] Запуск проверки маржи для сокращения позиции перед закрытием сессии.")
            try:
                margin_attributes = await client.users.get_margin_attributes(account_id=account_id)
                # ИЗМЕНЕНО: Используем minimal_margin для проверки сокращения
                current_margin_requirement = _quotation_to_decimal(margin_attributes.minimal_margin) if margin_attributes.minimal_margin else Decimal('0')
                
                self.logger.info(f"[{account_state.account_name_for_log}] Текущее минимальное маржинальное требование (minimal_margin): {current_margin_requirement:.{self.display_precision}f} RUB. Лимит: {account_state.max_free_margin_rub:.{self.display_precision}f} RUB.")
                if current_margin_requirement > account_state.max_free_margin_rub: # Проверка на превышение лимита
                    self.logger.warning(f"[{account_state.account_name_for_log}] Минимальное маржинальное требование ({current_margin_requirement:.{self.display_precision}f} RUB) превышает лимит ({account_state.max_free_margin_rub:.{self.display_precision}f} RUB). Начинаем сокращение позиции.")
                    
                    # Отменяем только активные ордера для входа через OrderManager
                    await self.cancel_all_orders_for_account(
                        client, account_id, account_state.account_name_for_log, cancel_entry_orders_only=True
                    )
                    portfolio = await client.operations.get_portfolio(account_id=account_id)
                    current_figi_position = next((pos for pos in portfolio.positions if pos.figi == self.figi), None)
                    current_lots_held_from_api = abs(_quotation_to_decimal(current_figi_position.quantity_lots)).normalize() if current_figi_position else Decimal('0')
                    lots_to_close_total = 0
                    # Loop while margin requirement exceeds limit AND there are still lots to close
                    while current_margin_requirement > account_state.max_free_margin_rub and current_lots_held_from_api > 0:
                        lots_to_close_this_step = min(LOTS, int(current_lots_held_from_api))
                        if lots_to_close_this_step == 0:
                            break
                        
                        # Ensure we don't try to close more than available
                        if lots_to_close_this_step > current_lots_held_from_api:
                            lots_to_close_this_step = int(current_lots_held_from_api)
                            if lots_to_close_this_step == 0:
                                break
                        self.logger.info(f"[{account_state.account_name_for_log}] Сокращаем позицию на {lots_to_close_this_step} лот(ов) для снижения маржинального требования.")
                        
                        direction_to_close = OrderDirection.ORDER_DIRECTION_BUY if account_state.is_sell_account else OrderDirection.ORDER_DIRECTION_SELL
                        market_order_id = await self.place_market_order(
                            client, lots_to_close_this_step, direction_to_close, "SESSION_REDUCTION", 
                            account_id
                        )
                        if market_order_id:
                            # Wait for market order to fill
                            order_data = self.get_order_data(market_order_id)
                            timeout = 10 # seconds
                            start_time = datetime.now()
                            while order_data and order_data.status not in ["FILLED", "PARTIALLY_FILLED", "CANCELLED", "ERROR"] and (datetime.now() - start_time).total_seconds() < timeout:
                                await asyncio.sleep(0.5)
                                order_data = self.get_order_data(market_order_id)
                            
                            if order_data and order_data.status in ["FILLED", "PARTIALLY_FILLED"]:
                                lots_to_close_total += order_data.filled_quantity
                                self.logger.info(f"[{account_state.account_name_for_log}] Рыночный ордер на закрытие {order_data.filled_quantity} лот(ов) исполнен.")
                                await asyncio.sleep(1) # Give some time for API to update
                                margin_attributes = await client.users.get_margin_attributes(account_id=account_id)
                                current_margin_requirement = _quotation_to_decimal(margin_attributes.minimal_margin) if margin_attributes.minimal_margin else Decimal('0')
                                
                                portfolio = await client.operations.get_portfolio(account_id=account_id)
                                current_figi_position = next((pos for pos in portfolio.positions if pos.figi == self.figi), None)
                                current_lots_held_from_api = abs(_quotation_to_decimal(current_figi_position.quantity_lots)).normalize() if current_figi_position else Decimal('0')
                                
                                self.logger.info(f"[{account_state.account_name_for_log}] Маржинальное требование после сокращения: {current_margin_requirement:.{self.display_precision}f} RUB. Оставшаяся позиция: {current_lots_held_from_api} лот(ов).")
                                
                                pass # Let the main loop's _sync_position_with_api handle the reconciliation
                            else:
                                self.logger.error(f"[{account_state.account_name_for_log}] Рыночный ордер {market_order_id} не был исполнен или завершился ошибкой/отменой. Статус: {order_data.status if order_data else 'None'}. Прерываем сокращение.")
                                break
                        else:
                            self.logger.error(f"[{account_state.account_name_for_log}] Не удалось разместить рыночный ордер на сокращение позиции. Возможно, инструмент недоступен или нет ликвидности.")
                            break
                    if current_margin_requirement <= account_state.max_free_margin_rub:
                        self.logger.info(f"[{account_state.account_name_for_log}] Маржинальное требование успешно снижено до {current_margin_requirement:.{self.display_precision}f} RUB (ниже лимита {account_state.max_free_margin_rub:.{self.display_precision}f} RUB). Закрыто всего {lots_to_close_total} лот(ов).")
                    else:
                        self.logger.warning(f"[{account_state.account_name_for_log}] Не удалось снизить маржинальное требование ниже лимита. Текущее требование: {current_margin_requirement:.{self.display_precision}f} RUB. Закрыто всего {lots_to_close_total} лот(ов). Требуется ручное вмешательство.")
                
                account_state.end_of_session_reduction_done = True
                self.save_state()
            
            except RequestError as e:
                self.logger.warning(f"Ошибка API при сокращении позиции для {account_state.account_name_for_log}: {e}. Повторная попытка в следующем цикле.")
            except Exception as e:
                self.logger.error(f"Непредвиденная ошибка при сокращении позиции для {account_state.account_name_for_log}: {e}", exc_info=True)

    async def _sync_position_with_api(self, client, account_id: str, current_lots_held_from_api: Decimal, current_avg_price_from_api: Decimal, last_known_market_price: Decimal):
        """
        Синхронизирует внутреннее состояние позиции бота с фактической позицией на бирже.
        Этот метод теперь сохраняет активные TP ордера и размещает новые для непокрытых лотов.
        Использует информацию об исполненных/отмененных стоп-ордерах.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно синхронизировать позицию.")
            return

        self.logger.warning(f"[{account_state.account_name_for_log}] Синхронизация: внутренняя позиция ({sum(l['quantity'] for l in account_state.open_lots)}), API ({int(current_lots_held_from_api)}).")
        self.logger.debug(f"[{account_state.account_name_for_log}] SYNC: Начальное open_lots: {account_state.open_lots}")

        # --- Шаг 1: Получаем все активные, исполненные и отмененные ордера с биржи ---
        active_exchange_orders = []
        active_exchange_stop_orders = []
        executed_stop_orders = []
        canceled_stop_orders = []
        try:
            active_exchange_orders_response = await client.orders.get_orders(account_id=account_id)
            active_exchange_orders = [o for o in active_exchange_orders_response.orders if o.figi == self.figi]
        except RequestError as e:
            self.logger.warning(f"[{account_state.account_name_for_log}] SYNC: Ошибка при получении активных лимитных ордеров: {e}")
        except Exception as e:
            self.logger.error(f"[{account_state.account_name_for_log}] SYNC: Непредвиденная ошибка при получении активных лимитных ордеров: {e}", exc_info=True)
        try:
            active_exchange_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_ACTIVE)
            active_exchange_stop_orders = [so for so in active_exchange_stop_orders_response.stop_orders if so.figi == self.figi]
            
            executed_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_EXECUTED)
            executed_stop_orders = [so for so in executed_stop_orders_response.stop_orders if so.figi == self.figi]
            canceled_stop_orders_response = await client.stop_orders.get_stop_orders(account_id=account_id, status=StopOrderStatusOption.STOP_ORDER_STATUS_CANCELED)
            canceled_stop_orders = [so for so in canceled_stop_orders_response.stop_orders if so.figi == self.figi]
        except RequestError as e:
            self.logger.warning(f"[{account_state.account_name_for_log}] SYNC: Ошибка при получении стоп-ордеров (активных/исполненных/отмененных): {e}")
        except Exception as e:
            self.logger.error(f"[{account_state.account_name_for_log}] SYNC: Непредвиденная ошибка при получении стоп-ордеров: {e}", exc_info=True)

        # --- Шаг 2: Отмена ордеров для входа (если они еще активны) ---
        entry_orders_to_cancel_ids = []
        upper_id, upper_is_stop = account_state.active_entry_order_above
        lower_id, lower_is_stop = account_state.active_entry_order_below
        if upper_id:
            order_status = self.get_order_status(upper_id)
            if order_status not in ["FILLED", "CANCELLED", "FAILED_TO_ACTIVATE", "ERROR", "ALREADY_INACTIVE", "ACTIVATED"]:
                if (upper_is_stop and any(so.stop_order_id == upper_id for so in active_exchange_stop_orders)) or \
                   (not upper_is_stop and any(o.order_id == upper_id for o in active_exchange_orders)):
                    entry_orders_to_cancel_ids.append((upper_id, upper_is_stop))
            else:
                self.logger.debug(f"[{account_state.account_name_for_log}] SYNC: Отмена пропущена для верхнего ордера {upper_id}, т.к. статус уже {order_status}.")
        if lower_id:
            order_status = self.get_order_status(lower_id)
            if order_status not in ["FILLED", "CANCELLED", "FAILED_TO_ACTIVATE", "ERROR", "ALREADY_INACTIVE", "ACTIVATED"]:
                if (lower_is_stop and any(so.stop_order_id == lower_id for so in active_exchange_stop_orders)) or \
                   (not lower_is_stop and any(o.order_id == lower_id for o in active_exchange_orders)):
                    entry_orders_to_cancel_ids.append((lower_id, lower_is_stop))
            else:
                self.logger.debug(f"[{account_state.account_name_for_log}] SYNC: Отмена пропущена для нижнего ордера {lower_id}, т.к. статус уже {order_status}.")

        if entry_orders_to_cancel_ids:
            self.logger.info(f"[{account_state.account_name_for_log}] SYNC: Отменяем активные ордера для входа: {entry_orders_to_cancel_ids}")
            cancellation_tasks = []
            for order_id, is_stop in entry_orders_to_cancel_ids:
                cancellation_tasks.append(self.cancel_order(client, order_id, is_stop, account_id))
            await asyncio.gather(*cancellation_tasks, return_exceptions=True)
        
        account_state.active_entry_order_above = (None, None)
        account_state.active_entry_order_below = (None, None)

        # --- Шаг 3: Реконсиляция внутреннего списка open_lots с API позицией и активными TP ---
        reconciled_open_lots = []
        
        # Фаза 1: Валидация существующих внутренних лотов и их TP.
        for lot in account_state.open_lots:
            tp_id = lot.get('tp_order_id')
            if tp_id:
                # Проверяем, активен ли TP на бирже
                if any(so.stop_order_id == tp_id for so in active_exchange_stop_orders):
                    reconciled_open_lots.append(lot)
                # Проверяем, был ли TP исполнен или отменен
                elif any(so.stop_order_id == tp_id for so in executed_stop_orders) or \
                     any(so.stop_order_id == tp_id for so in canceled_stop_orders):
                    self.logger.debug(f"[{account_state.account_name_for_log}] SYNC: TP ордер {tp_id} для лота {lot['entry_price']:.{self.display_precision}f} неактивен на бирже (исполнен/отменен). Сбрасываем ID TP.")
                    new_lot = lot.copy()
                    new_lot['tp_order_id'] = None # Сбрасываем ID TP ордера
                    reconciled_open_lots.append(new_lot)
                    # Обновим статус в OrderManager
                    order_data_in_manager = self.get_order_data(tp_id)
                    if order_data_in_manager and order_data_in_manager.status == "ACTIVE":
                        if any(so.stop_order_id == tp_id for so in executed_stop_orders):
                            order_data_in_manager.update_status("FILLED", "SYNC_EXECUTED_TP")
                        elif any(so.stop_order_id == tp_id for so in canceled_stop_orders):
                            order_data_in_manager.update_status("CANCELLED", "SYNC_CANCELLED_TP")
                else:
                    # TP не найден ни в активных, ни в исполненных, ни в отмененных. Считаем, что его нет.
                    self.logger.info(f"[{account_state.account_name_for_log}] SYNC: TP ордер {tp_id} для лота {lot['entry_price']:.{self.display_precision}f} не найден на бирже ни в каком статусе. Сбрасываем ID TP.")
                    new_lot = lot.copy()
                    new_lot['tp_order_id'] = None
                    reconciled_open_lots.append(new_lot)
                    order_data_in_manager = self.get_order_data(tp_id)
                    if order_data_in_manager and order_data_in_manager.status == "ACTIVE":
                        order_data_in_manager.update_status("ALREADY_INACTIVE", "SYNC_MISSING_TP")
            else:
                reconciled_open_lots.append(lot) # Лот без TP, сохраняем

        internal_total_quantity = sum(l['quantity'] for l in reconciled_open_lots)
        
        # Обработка расхождений в количестве лотов
        if int(current_lots_held_from_api) > internal_total_quantity:
            # Обнаружены новые лоты (сработал ордер на вход, пока бот был неактивен)
            new_lots_to_add_count = int(current_lots_held_from_api) - internal_total_quantity
            self.logger.info(f"[{account_state.account_name_for_log}] SYNC: Обнаружено {new_lots_to_add_count} новых лот(ов) по API. Добавляем во внутреннее состояние.")
            for _ in range(new_lots_to_add_count):
                # Используем текущую рыночную цену для новых лотов, если стрим события не дошли
                reconciled_open_lots.append({'entry_price': last_known_market_price, 'tp_order_id': None, 'quantity': LOTS})
            
            # CRITICAL: If new lots were added via sync, force last_position_fill_price to current market price
            account_state.last_position_fill_price = last_known_market_price
            self.logger.debug(f"[{account_state.account_name_for_log}] SYNC: last_position_fill_price обновлена до рыночной цены ({account_state.last_position_fill_price:.{self.display_precision}f}) из-за новых лотов, обнаруженных синхронизацией.")
        elif int(current_lots_held_from_api) < internal_total_quantity:
            # Лоты были закрыты извне (например, вручную)
            lots_to_remove_count = internal_total_quantity - int(current_lots_held_from_api)
            self.logger.warning(f"[{account_state.account_name_for_log}] SYNC: Обнаружено {lots_to_remove_count} лот(ов), закрытых извне. Удаляем из внутреннего состояния.")
            # Удаляем лоты из конца списка (простейший способ)
            reconciled_open_lots = reconciled_open_lots[:-lots_to_remove_count]
            
            # CRITICAL: If lots were removed, the base price for repositioning should be the current market price,
            # as the old average value might be incorrect or the position significantly changed.
            account_state.last_position_fill_price = last_known_market_price
            account_state.orders_need_reposition = True # Force reposition after external closure
            self.logger.debug(f"[{account_state.account_name_for_log}] SYNC: last_position_fill_price обновлена до рыночной цены ({account_state.last_position_fill_price:.{self.display_precision}f}) из-за лотов, закрытых извне.")
        
        account_state.open_lots = reconciled_open_lots # Обновляем основной список
        self.logger.debug(f"[{account_state.account_name_for_log}] SYNC: Обновленный open_lots: {account_state.open_lots}")

        # --- Шаг 4: Размещение недостающих TP ордеров ---
        self.logger.info(f"[{account_state.account_name_for_log}] SYNC: Размещаем недостающие TP ордера для {len([l for l in account_state.open_lots if l['tp_order_id'] is None])} лотов.")
        tp_placement_tasks = []
        lots_awaiting_tp_id = [] # Для сопоставления результатов с конкретными лотами

        for lot in account_state.open_lots:
            if lot['tp_order_id'] is None:
                tp_placement_tasks.append(
                    self._place_individual_take_profit_order(client, account_id, lot['entry_price'], lot['quantity'])
                )
                lots_awaiting_tp_id.append(lot) # Сохраняем ссылку на лот
        
        if tp_placement_tasks:
            tp_results = await asyncio.gather(*tp_placement_tasks, return_exceptions=True)
            for i, result in enumerate(tp_results):
                lot = lots_awaiting_tp_id[i]
                if not isinstance(result, Exception) and result:
                    lot['tp_order_id'] = result
                else:
                    self.logger.error(f"[{account_state.account_name_for_log}] SYNC: НЕ УДАЛОСЬ РАЗМЕСТИТЬ TP для лота с ценой входа {lot['entry_price']:.{self.display_precision}f}. Ошибка: {result}. Лот останется без TP!")

        # --- Шаг 5: Обновление флагов состояния бота на основе синхронизированной позиции ---
        # Recalculate internal_total_quantity after potential additions/removals
        internal_total_quantity = sum(l['quantity'] for l in account_state.open_lots)
        if internal_total_quantity > 0:
            account_state.account_started = True
            account_state.initial_orders_placed = True # If there's a position, we've "started" and "placed initial orders"
            # If new lots were *not* added via sync, and lots were *not* removed via sync,
            # then last_position_fill_price should be the current_avg_price_from_api.
            # Otherwise, it's already set to market price above.
            if not (int(current_lots_held_from_api) != internal_total_quantity): # If no discrepancy in lots, use API avg price
                account_state.last_position_fill_price = current_avg_price_from_api
            
            account_state.orders_need_reposition = True # Always re-evaluate entry orders after sync with a position
            account_state.initial_entry_orders_placed_for_session = True # If we have a position, we consider initial entry for session done
        else: # internal_total_quantity is 0
            account_state.account_started = False
            account_state.initial_orders_placed = False # If no position, we haven't "started" or "placed initial orders" for this session
            account_state.last_position_fill_price = Decimal('0')
            account_state.orders_need_reposition = False # No position, no need to reposition entry orders yet.
            account_state.initial_entry_orders_placed_for_session = False # Reset this too if position is zero after sync
        self.logger.info(f"[{account_state.account_name_for_log}] Синхронизация завершена. Текущая внутренняя позиция: {internal_total_quantity} лот(ов). account_started: {account_state.account_started}, initial_orders_placed: {account_state.initial_orders_placed}")
        self.save_state()

    async def manage_account_orders(self, client, account_id: str, current_market_price: Decimal):
        """
        Проверяет текущую позицию и управляет ордерами для конкретного счета.
        """
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.error(f"[OrderManager] Не найдено состояние для счета {account_id}. Невозможно управлять ордерами.")
            return

        await self._reset_daily_flags_if_new_day(client, account_id, current_market_price)
        if account_state.daily_loss_limit_hit:
            self.logger.warning(f"[{account_state.account_name_for_log}] Дневной лимит убытка достигнут. Торговля для этого счета приостановлена.")
            return
        await self._check_margin_and_manage_orders(client, account_id)

        # Always fetch portfolio first to get the freshest API state
        portfolio = None
        try:
            portfolio = await client.operations.get_portfolio(account_id=account_id)
        except RequestError as e:
            self.logger.warning(f"Ошибка получения портфолио для {account_state.account_name_for_log}: {e}. Пропускаем текущую проверку.")
            return
        except Exception as e:
            self.logger.error(f"Непредвиденная ошибка при получении портфолио для {account_state.account_name_for_log}: {e}", exc_info=True)
            return

        current_figi_position = next((pos for pos in portfolio.positions if pos.figi == self.figi), None)
        current_lots_held_from_api = Decimal('0')
        current_avg_price_from_api = Decimal('0')
        if current_figi_position:
            current_lots_held_from_api = abs(_quotation_to_decimal(current_figi_position.quantity_lots)).normalize()
            if current_figi_position.average_position_price:
                current_avg_price_from_api = _quotation_to_decimal(current_figi_position.average_position_price)
        
        internal_lots_count = sum(lot['quantity'] for lot in account_state.open_lots)
        self.logger.debug(f"[{account_state.account_name_for_log}] (Перед синхронизацией) Позиция по портфолио (API): {int(current_lots_held_from_api)} лот(ов) по средней цене {current_avg_price_from_api:.{self.display_precision}f}.")
        self.logger.debug(f"[{account_state.account_name_for_log}] (Перед синхронизацией) Текущая рыночная цена (Бот): {current_market_price:.{self.display_precision}f}.")
        self.logger.debug(f"[{account_state.account_name_for_log}] (Перед синхронизацией) Позиция внутренняя (Бот): {internal_lots_count} лот(ов) по средней цене {sum(l['entry_price'] * l['quantity'] for l in account_state.open_lots) / internal_lots_count if internal_lots_count > 0 else Decimal('0'):.{self.display_precision}f}.")
        self.logger.debug(f"[{account_state.account_name_for_log}] (Перед синхронизацией) Account started: {account_state.account_started}, Initial orders placed: {account_state.initial_orders_placed}, Orders need reposition: {account_state.orders_need_reposition}")
        self.logger.debug(f"[{account_state.account_name_for_log}] (Перед синхронизацией) Internal open_lots_list: {account_state.open_lots}")

        needs_sync = False
        if int(current_lots_held_from_api) != internal_lots_count:
            self.logger.info(f"[{account_state.account_name_for_log}] Расхождение в количестве лотов (API: {int(current_lots_held_from_api)}, Внутренний: {internal_lots_count}). Требуется синхронизация.")
            needs_sync = True
        elif not account_state.account_started and int(current_lots_held_from_api) > 0:
            self.logger.info(f"[{account_state.account_name_for_log}] Бот не запущен, но есть позиция по API. Требуется синхронизация.")
            needs_sync = True
        elif internal_lots_count == 0 and int(current_lots_held_from_api) > 0:
            self.logger.info(f"[{account_state.account_name_for_log}] Внутренняя позиция 0, но есть позиция по API. Требуется синхронизация.")
            needs_sync = True
        elif account_state.account_started and internal_lots_count == 0 and int(current_lots_held_from_api) == 0 and account_state.initial_orders_placed:
            self.logger.info(f"[{account_state.account_name_for_log}] Бот считает, что начал работу, но позиция 0. Сбрасываем initial_orders_placed. Требуется синхронизация.")
            needs_sync = True
            
        if needs_sync:
            await self._sync_position_with_api(client, account_id, current_lots_held_from_api, current_avg_price_from_api, current_market_price)
            
            # Recalculate internal_lots_count and current_lots_held_from_api after sync, as they might have changed
            portfolio = await client.operations.get_portfolio(account_id=account_id)
            current_figi_position = next((pos for pos in portfolio.positions if pos.figi == self.figi), None)
            current_lots_held_from_api = abs(_quotation_to_decimal(current_figi_position.quantity_lots)).normalize() if current_figi_position else Decimal('0')
            current_avg_price_from_api = _quotation_to_decimal(current_figi_position.average_position_price) if current_figi_position and current_figi_position.average_position_price else Decimal('0')
            internal_lots_count = sum(l['quantity'] for l in account_state.open_lots)
            self.logger.debug(f"[{account_state.account_name_for_log}] (После синхронизации) Внутренняя позиция: {internal_lots_count}, API: {int(current_lots_held_from_api)}. account_started: {account_state.account_started}, initial_orders_placed: {account_state.initial_orders_placed}")

        # Initial market order placement logic - now after all reconciliation
        self.logger.debug(f"[{account_state.account_name_for_log}] Проверка начального рыночного ордера: PLACE_INITIAL_MARKET_ORDER={PLACE_INITIAL_MARKET_ORDER}, internal_lots_count={internal_lots_count}, current_lots_held_from_api={int(current_lots_held_from_api)}, initial_orders_placed={account_state.initial_orders_placed}")
        if PLACE_INITIAL_MARKET_ORDER and internal_lots_count == 0 and int(current_lots_held_from_api) == 0 and not account_state.initial_orders_placed:
            if not account_state.is_sell_account:
                self.logger.info(f"Для {account_state.account_name_for_log} аккаунта размещён начальный рыночный ордер BUY (позиция 0) при старте/после сброса.")
                market_order_id = await self.place_market_order(
                    client, LOTS, OrderDirection.ORDER_DIRECTION_BUY, "INITIAL_MARKET_BUY", 
                    account_id
                )
                if not market_order_id:
                    self.logger.error(f"Не удалось разместить начальный рыночный ордер BUY для {account_state.account_name_for_log}. Бот не начнет работу.")
                else:
                    account_state.pending_initial_market_order_id = market_order_id
                self.save_state()
            elif account_state.is_sell_account:
                self.logger.info(f"Для {account_state.account_name_for_log} аккаунта размещён начальный рыночный ордер SELL (позиция 0) при старте/после сброса.")
                market_order_id = await self.place_market_order(
                    client, LOTS, OrderDirection.ORDER_DIRECTION_SELL, "INITIAL_MARKET_SELL", 
                    account_id
                )
                if not market_order_id:
                    self.logger.error(f"Не удалось разместить начальный рыночный ордер SELL для {account_state.account_name_for_log}. Бот не начнет работу.")
                else:
                    account_state.pending_initial_market_order_id = market_order_id
                self.save_state()
        
        # This check should be based on the actual state after potential initial order placement or sync
        if not account_state.account_started: # This flag is set by _sync_position_with_api or _process_opening_order_fill
            self.logger.debug(f"[{account_state.account_name_for_log}] Аккаунт еще не начал торговлю (нет позиции). Пропускаем управление ордерами для входа.")
            return

        # --- Логика определения необходимости перестановки ордеров для входа ---
        should_reposition_orders = False
        effective_base_price_for_placement = current_market_price # Use the single market price from TradingBot

        # Проверяем активность ордеров на бирже через OrderManager
        upper_id, upper_is_stop = account_state.active_entry_order_above
        lower_id, lower_is_stop = account_state.active_entry_order_below
        
        has_upper_entry_order_active = False
        if upper_id:
            status = self.get_order_status(upper_id)
            self.logger.debug(f"[{account_state.account_name_for_log}] Проверка активности верхнего ордера {upper_id}: статус {status}")
            if status in ["ACTIVE", "PARTIALLY_FILLED", "PENDING_CONFIRMATION", "ACTIVATED"]:
                has_upper_entry_order_active = True
            else:
                self.logger.info(f"Для {account_state.account_name_for_log}: Верхний ордер для входа (ID: {upper_id}) неактивен (статус: {status}).")
                account_state.active_entry_order_above = (None, None) # Очищаем внутренний ID, если неактивен
        
        has_lower_entry_order_active = False
        if lower_id:
            status = self.get_order_status(lower_id)
            self.logger.debug(f"[{account_state.account_name_for_log}] Проверка активности нижнего ордера {lower_id}: статус {status}")
            if status in ["ACTIVE", "PARTIALLY_FILLED", "PENDING_CONFIRMATION", "ACTIVATED"]:
                has_lower_entry_order_active = True
            else:
                self.logger.info(f"Для {account_state.account_name_for_log}: Нижний ордер для входа (ID: {lower_id}) неактивен (статус: {status}).")
                account_state.active_entry_order_below = (None, None) # Очищаем внутренний ID, если неактивен

        # Определяем, нужна ли перестановка ордеров
        if account_state.orders_need_reposition:
            self.logger.info(f"[{account_state.account_name_for_log}] Флаг 'orders_need_reposition' установлен. Инициируем перестановку ордеров.")
            should_reposition_orders = True
        elif (datetime.now() - account_state.last_orders_placement_time).total_seconds() >= account_state.check_grid_interval_seconds:
            if not has_upper_entry_order_active or not has_lower_entry_order_active:
                self.logger.info(f"[{account_state.account_name_for_log}] Прошло достаточно времени ({int((datetime.now() - account_state.last_orders_placement_time).total_seconds())}s) и один из ордеров сетки неактивен. Инициируем перестановку.")
                should_reposition_orders = True
            else:
                self.logger.debug(f"Для {account_state.account_name_for_log}: Периодическая проверка ордеров для входа пропущена (все ордера активны).")
        else:
            self.logger.debug(f"Для {account_state.account_name_for_log}: Периодическая проверка ордеров для входа пропущена (недостаточно времени прошло).")

        # Если нужно переставить ордера, размещаем новые
        if should_reposition_orders:
            if effective_base_price_for_placement != Decimal('0'):
                await self._place_directional_entry_orders(client, account_id, effective_base_price_for_placement)
                account_state.orders_need_reposition = False # Сбрасываем флаг после успешного размещения
            else:
                self.logger.warning(f"[{account_state.account_name_for_log}] Откладываем перестановку ордеров для входа, так как не удалось определить подходящую базовую цену (рыночная цена = 0).")
        else:
            self.logger.debug(f"Для {account_state.account_name_for_log}: Периодическая проверка ордеров для входа пропущена (нет причин для перестановки).")
        self.save_state()

    def get_order_status(self, order_id: str) -> Optional[str]:
        """Returns the current internal status of a tracked order."""
        order_data = self.orders.get(order_id)
        return order_data.status if order_data else None

    def get_order_data(self, order_id: str) -> Optional[OrderStateData]:
        """Returns the full OrderStateData object for a tracked order."""
        return self.orders.get(order_id)

    def save_state(self):
        """Saves the current state of all managed orders and account states to JSON files."""
        try:
            # Save orders
            with open(self.order_log_file, 'w', encoding='utf-8') as f:
                json.dump([order.to_dict() for order in self.orders.values()], f, indent=4)
            self.logger.debug(f"[OrderManager] Журнал ордеров успешно сохранен в {self.order_log_file}")

            # Save account states
            account_states_data = {
                acc_id: acc_state.to_dict()
                for acc_id, acc_state in self.account_states.items()
            }
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(account_states_data, f, indent=4)
            self.logger.debug(f"[OrderManager] Состояние аккаунтов успешно сохранено в {self.state_file}")

        except Exception as e:
            self.logger.error(f"[OrderManager] Ошибка при сохранении состояния: {e}")

    def load_state(self):
        """Loads the state of managed orders and account states from JSON files."""
        try:
            # Load orders
            if os.path.exists(self.order_log_file):
                with open(self.order_log_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.orders = {item['order_id']: OrderStateData.from_dict(item) for item in data}
                self.logger.debug(f"[OrderManager] Журнал ордеров успешно загружен из {self.order_log_file}. Загружено {len(self.orders)} ордеров.")
            else:
                self.logger.debug(f"[OrderManager] Файл журнала ордеров {self.order_log_file} не найден. Начинаем с пустого журнала.")

            # Load account states
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict): # Add this check
                    self.account_states = {
                        acc_id: AccountStateData.from_dict(acc_data)
                        for acc_id, acc_data in data.items()
                    }
                    self.logger.info(f"[OrderManager] Состояние аккаунтов успешно загружено из {self.state_file}. Загружено {len(self.account_states)} аккаунтов.")
                else:
                    self.logger.error(f"[OrderManager] Ошибка: Файл состояния аккаунтов {self.state_file} содержит данные в неожиданном формате (ожидался словарь, получен {type(data)}). Начинаем с чистого состояния.")
                    self.account_states = {} # Reset to empty
            else:
                self.logger.info(f"[OrderManager] Файл состояния аккаунтов {self.state_file} не найден. Начинаем с чистого состояния.")

        except json.JSONDecodeError as e:
            self.logger.error(f"[OrderManager] Ошибка декодирования JSON из файла состояния: {e}. Начинаем с чистого состояния.")
            self.orders = {}
            self.account_states = {}
        except Exception as e:
            self.logger.error(f"[OrderManager] Непредвиденная ошибка при загрузке состояния: {e}. Начинаем с чистого состояния.")
            self.orders = {}
            self.account_states = {}

    def cleanup_orders(self):
        """
        Удаляет исполненные, отмененные или ошибочные ордера из внутреннего состояния.
        """
        orders_to_keep = {}
        cleaned_up_count = 0
        final_statuses = {"FILLED", "CANCELLED", "FAILED_TO_ACTIVATE", "ERROR", "ALREADY_INACTIVE", "ACTIVATED"}
        
        for order_id, order_data in self.orders.items():
            is_active_entry_order = False
            is_active_tp_order_in_lot_list = False

            account_state = self.account_states.get(order_data.account_id)
            if account_state:
                # Check if it's an active entry order
                if order_id == account_state.active_entry_order_above[0] or \
                   order_id == account_state.active_entry_order_below[0] or \
                   order_id == account_state.pending_initial_market_order_id:
                    is_active_entry_order = True
                
                # Also check if it's a TP order still linked to an open lot
                for lot in account_state.open_lots:
                    if lot.get('tp_order_id') == order_id:
                        is_active_tp_order_in_lot_list = True
                        break

            # Keep activated stop orders if their exchange order is still active/pending
            # Or if the exchange order itself is still active/pending
            if order_data.status == "ACTIVATED" and order_data.exchange_order_id:
                exchange_order_status = self.get_order_status(order_data.exchange_order_id)
                if exchange_order_status in ["ACTIVE", "PARTIALLY_FILLED", "PENDING_CONFIRMATION"]:
                    orders_to_keep[order_id] = order_data
                    continue # Don't clean up yet if the activated order is still active
            
            # If it's an exchange order that originated from a stop order, check its status
            # Only keep if its original stop order is still active/activated or if the exchange order itself is not in a final state
            if order_data.original_stop_order_id:
                original_stop_order_status = self.get_order_status(order_data.original_stop_order_id)
                if original_stop_order_status in ["ACTIVE", "ACTIVATED"] or \
                   order_data.status not in final_statuses:
                    orders_to_keep[order_id] = order_data
                    continue
            
            # Only clean up if it's in a final status AND not an active entry/TP order
            if order_data.status in final_statuses and not is_active_entry_order and not is_active_tp_order_in_lot_list:
                cleaned_up_count += 1
            else:
                orders_to_keep[order_id] = order_data
        
        if cleaned_up_count > 0:
            self.logger.debug(f"[OrderManager] Очищено {cleaned_up_count} исполненных/неактуальных ордеров из внутреннего состояния.")
            self.orders = orders_to_keep
            self.save_state() # Сохраняем журнал после очистки
        else:
            self.logger.debug("[OrderManager] Нет ордеров для очистки.")

    async def cancel_all_orders_for_account(self, client: AsyncClient, account_id: str, account_name_for_log: str, 
                                            cancel_entry_orders_only: bool = False) -> tuple[set[str], set[str]]:
        """
        Отменяет активные заявки для указанного счета через OrderManager.
        Если cancel_entry_orders_only=True, отменяет только ордера для входа,
        иначе отменяет все обычные и стоп-ордера (включая TP).
        Возвращает два множества: (отмененные_лимитные_ID, отмененные_стоп_ID).
        """
        self.logger.info(f"Отмена активных заявок для счета {account_name_for_log} ({account_id})...")
            
        cancelled_limit_ids = set()
        cancelled_stop_ids = set()
        orders_to_process = []
        
        account_state = self.account_states.get(account_id)
        if not account_state:
            self.logger.warning(f"Неизвестный account_id {account_id} или менеджер неактивен для отмены entry orders. Пропускаем.")
            return cancelled_limit_ids, cancelled_stop_ids

        # Collect entry orders to cancel
        upper_id, upper_is_stop = account_state.active_entry_order_above
        lower_id, lower_is_stop = account_state.active_entry_order_below
        if upper_id:
            order_status = self.get_order_status(upper_id)
            if order_status not in ["FILLED", "CANCELLED", "FAILED_TO_ACTIVATE", "ERROR", "ALREADY_INACTIVE", "ACTIVATED"]:
                orders_to_process.append((upper_id, upper_is_stop))
            else:
                self.logger.debug(f"[{account_name_for_log}] Отмена пропущена для верхнего ордера {upper_id}, т.к. статус уже {order_status}.")
        if lower_id:
            order_status = self.get_order_status(lower_id)
            if order_status not in ["FILLED", "CANCELLED", "FAILED_TO_ACTIVATE", "ERROR", "ALREADY_INACTIVE", "ACTIVATED"]:
                orders_to_process.append((lower_id, lower_is_stop))
            else:
                self.logger.debug(f"[{account_name_for_log}] Отмена пропущена для нижнего ордера {lower_id}, т.к. статус уже {order_status}.")
        
        # If not cancelling entry orders only, add all other active orders from OrderManager
        if not cancel_entry_orders_only:
            for order_data in self.orders.values():
                # Check if it's for the current account, figi, and is active/pending
                if order_data.account_id == account_id and order_data.figi == self.figi and \
                   order_data.status in ["ACTIVE", "PARTIALLY_FILLED", "PENDING_CONFIRMATION", "ACTIVATED"]:
                    # Add all orders, including TP, if not just cancelling entry orders
                    # Ensure we don't duplicate entry orders already added
                    if (order_data.order_id, order_data.is_stop_order) not in orders_to_process:
                        orders_to_process.append((order_data.order_id, order_data.is_stop_order))
        
        # Execute cancellations
        cancellation_tasks = []
        for order_id, is_stop in orders_to_process:
            cancellation_tasks.append(self.cancel_order(client, order_id, is_stop, account_id))
        
        results = await asyncio.gather(*cancellation_tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            order_id, is_stop = orders_to_process[i]
            if result is True: # Successfully cancelled or already inactive
                if is_stop:
                    cancelled_stop_ids.add(order_id)
                else:
                    cancelled_limit_ids.add(order_id)
            elif isinstance(result, Exception):
                self.logger.error(f"Ошибка при отмене ордера {order_id}: {result}")
            else:
                self.logger.debug(f"Не удалось отменить ордер {order_id}.")

        self.logger.info(f"Для счета {account_name_for_log}: Завершена отмена ордеров. Отменено лимитных: {len(cancelled_limit_ids)}, стоп-ордеров: {len(cancelled_stop_ids)}.")
        self.save_state()
        return cancelled_limit_ids, cancelled_stop_ids

# ====================== МЕНЕДЖЕР АККАУНТА (теперь тонкий слой) ======================
class AccountManager:
    """
    Управляет высокоуровневой логикой для отдельного торгового счета (BUY или SELL),
    делегируя все операции с ордерами OrderManager.
    """
    def __init__(self, account_id: str, account_name_for_log: str, is_sell_account: bool,
                 lots_in_one_lot: int, grid_interval: Decimal, max_daily_loss_rub: Decimal,
                 max_free_margin_rub: Decimal, end_of_session_reduction_time: time,
                 check_grid_interval_seconds: int, logger: logging.Logger, order_manager: OrderManager):
        
        self.account_id = account_id
        self.account_name_for_log = account_name_for_log
        self.is_sell_account = is_sell_account
        self.logger = logger
        self.order_manager = order_manager # Ссылка на OrderManager
        
        # Инициализируем или обновляем состояние этого аккаунта в OrderManager
        self.order_manager.initialize_account_state(
            account_id=self.account_id,
            account_name_for_log=self.account_name_for_log,
            is_sell_account=self.is_sell_account,
            lots_in_one_lot=lots_in_one_lot,
            grid_interval=grid_interval,
            max_daily_loss_rub=max_daily_loss_rub,
            max_free_margin_rub=max_free_margin_rub,
            end_of_session_reduction_time=end_of_session_reduction_time,
            check_grid_interval_seconds=check_grid_interval_seconds
        )
        self.account_state = self.order_manager.account_states[self.account_id] # Прямая ссылка на состояние в OrderManager
        
    async def manage_position(self, client: AsyncClient, current_market_price: Decimal):
        """
        Координирует управление позицией для своего счета, используя OrderManager.
        """
        await self.order_manager.manage_account_orders(client, self.account_id, current_market_price)

    async def perform_end_of_session_margin_reduction(self, client: AsyncClient, last_known_market_price: Decimal):
        """
        Делегирует выполнение сокращения позиции OrderManager.
        """
        await self.order_manager._perform_end_of_session_margin_reduction(client, self.account_id, last_known_market_price)

    async def sync_position_with_api(self, client: AsyncClient, current_lots_held_from_api: Decimal, current_avg_price_from_api: Decimal, last_known_market_price: Decimal):
        """
        Делегирует синхронизацию позиции OrderManager.
        """
        await self.order_manager._sync_position_with_api(client, self.account_id, current_lots_held_from_api, current_avg_price_from_api, last_known_market_price)

# ====================== ТОРГОВЫЙ БОТ ======================
class TradingBotV10_4_0:
    def __init__(self):
        self.running = True
        self.figi = None
        self.lots_in_one_lot = 1
        self.min_price_increment = Decimal('0.01')
        self.display_precision = 2
        
        self.BOT_CLIENT_CODE = f"TBot_{TICKER}_{uuid.uuid4().hex[:8]}"
        self.setup_logging() # Вызов setup_logging перемещен сюда
        self.warned_stop_order_ids_missing_type = set()
        # Инициализация атрибутов для управления переподключением стримов
        self.orders_reconnect_delay = 1
        self.orders_max_reconnect_delay = 60
        self.market_data_reconnect_delay = 1
        self.market_data_max_reconnect_delay = 60
        
        # Флаги для отслеживания состояния связи
        self.orders_stream_connected = False
        self.market_data_stream_connected = False

        # Инициализация OrderManager
        self.order_manager = OrderManager(self.logger, self.BOT_CLIENT_CODE, self.figi, self.min_price_increment, self.display_precision)
        
        # Инициализация менеджеров аккаунтов
        self.sell_account_manager = None
        self.buy_account_manager = None
        
        if ACCOUNT_FOR_TRADING == 2 or ACCOUNT_FOR_TRADING == 3:
            self.sell_account_manager = AccountManager(
                account_id=SELL_ACCOUNT_ID,
                account_name_for_log=SELL_ACCOUNT_NAME_FOR_LOG,
                is_sell_account=True,
                lots_in_one_lot=self.lots_in_one_lot, # Будет обновлен
                grid_interval=GRID_INTERVAL,
                max_daily_loss_rub=MAX_DAILY_LOSS_RUB_SELL,
                max_free_margin_rub=MAX_FREE_MARGIN_RUB,
                end_of_session_reduction_time=END_OF_SESSION_REDUCTION_TIME,
                check_grid_interval_seconds=CHECK_GRID_INTERVAL_SECONDS,
                logger=self.logger,
                order_manager=self.order_manager # Передача ссылки на OrderManager
            )
        if ACCOUNT_FOR_TRADING == 1 or ACCOUNT_FOR_TRADING == 3:
            self.buy_account_manager = AccountManager(
                account_id=BUY_ACCOUNT_ID,
                account_name_for_log=BUY_ACCOUNT_NAME_FOR_LOG,
                is_sell_account=False,
                lots_in_one_lot=self.lots_in_one_lot, # Будет обновлен
                grid_interval=GRID_INTERVAL,
                max_daily_loss_rub=MAX_DAILY_LOSS_RUB_BUY,
                max_free_margin_rub=MAX_FREE_MARGIN_RUB,
                end_of_session_reduction_time=END_OF_SESSION_REDUCTION_TIME,
                check_grid_interval_seconds=CHECK_GRID_INTERVAL_SECONDS,
                logger=self.logger,
                order_manager=self.order_manager # Передача ссылки на OrderManager
            )
        
        self.interval_check = INTERVAL_CHECK
        
        self.last_known_market_price = Decimal('0')
        self.market_data_stream_ready = asyncio.Event()
        self.orders_stream_ready = asyncio.Event() # New event for orders stream
        self.was_closed = False

        # NEW: Атрибуты для фильтрации по тренду
        self.last_trend_check_price: Decimal = Decimal('0')
        # Initialize last_trend_check_time as timezone-aware
        self.last_trend_check_time: datetime = datetime.min.replace(tzinfo=MOSCOW_TZ)

    def setup_logging(self):
        self.logger = logging.getLogger("LevelGridBot")
        
        # Set level based on global setting
        if LOG_LEVEL_SETTING == "DEBUG":
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO) # Default to INFO
        handler = logging.StreamHandler()
        formatter = CustomLogFormatter("%(asctime)s - %(log_sequence_number)d: %(levelname)s - L%(lineno)d - %(message)s")
        handler.setFormatter(formatter)
        
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.addHandler(handler)
        self.logger.propagate = False
        tinkoff_invest_logger = logging.getLogger("tinkoff.invest")
        tinkoff_invest_logger.setLevel(logging.WARNING)
        tinkoff_invest_logger.propagate = False

    async def get_instrument_data(self, client):
        try:
            instruments = await client.instruments.shares()
            for instrument in instruments.instruments:
                if instrument.ticker == TICKER:
                    self.figi = instrument.figi
                    self.lots_in_one_lot = instrument.lot
                    if instrument.min_price_increment:
                        self.min_price_increment = _quotation_to_decimal(instrument.min_price_increment)
                        self.display_precision = abs(self.min_price_increment.as_tuple().exponent)
                        if self.display_precision < 2:
                            self.display_precision = 2
                        getcontext().prec = max(getcontext().prec, self.display_precision + 2)
                    self.logger.info(f"Для {TICKER}: 1 лот = {self.lots_in_one_lot} акция(ий), шаг цены = {self.min_price_increment:.{self.display_precision}f}.")
                    
                    # Обновляем данные инструмента в OrderManager
                    self.order_manager.figi = self.figi
                    self.order_manager.min_price_increment = self.min_price_increment
                    self.order_manager.display_precision = self.display_precision
                    
                    # Обновляем lots_in_one_lot в AccountStateData через OrderManager
                    for account_state in self.order_manager.account_states.values():
                        account_state.lots_in_one_lot = self.lots_in_one_lot
                    return True
            self.logger.error(f"Инструмент с тикером {TICKER} не найден.")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка получения данных инструмента: {str(e)}")
            return False

    async def _orders_stream_listener(self, client: AsyncClient, accounts_to_subscribe: List[str]):
        self.logger.info("Запуск слушателя стрима ордеров...")
        if not accounts_to_subscribe:
            self.logger.warning("Нет активных счетов для подписки на стрим ордеров. Слушатель не будет запущен.")
            self.orders_stream_ready.set() # Set event to unblock main loop
            return
        while self.running: 
            try:
                if not self.orders_stream_connected:
                    self.logger.debug(f"Подключение к стриму ордеров для счетов {accounts_to_subscribe}. Текущая задержка переподключения: {self.orders_reconnect_delay}с")
                
                async for trade_event_response in client.orders_stream.trades_stream(accounts=accounts_to_subscribe):
                    # Логирование всех входящих сообщений стрима ордеров в JSON файл
                    process_incoming_order_stream_message(trade_event_response)
                    
                    if not self.orders_stream_connected:
                        # ИЗМЕНЕНО: Правильный доступ к status объекта subscription
                        if trade_event_response.subscription and \
                           trade_event_response.subscription.status == SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS:
                            self.logger.info(f"!!!!!!!!!!!Связь восстановлена-продолжаем работу!!!!!!!!!!!")
                            self.orders_reconnect_delay = 1
                            self.orders_stream_connected = True
                            self.orders_stream_ready.set() # Set the event when stream is ready
                        elif trade_event_response.order_trades or trade_event_response.ping:
                            if self.orders_reconnect_delay > 1:
                                self.logger.info(f"!!!!!!!!!!!Связь восстановлена-продолжаем работу!!!!!!!!!!!")
                            self.orders_reconnect_delay = 1
                            self.orders_stream_connected = True
                            self.orders_stream_ready.set() # Set the event when stream is ready
                    
                    if trade_event_response.order_trades:
                        order_trades_data = trade_event_response.order_trades
                        
                        if order_trades_data.figi != self.figi:
                            self.logger.debug(f"Стрим ордеров: Получено событие для другого FIGI ({order_trades_data.figi}), ожидается {self.figi}. Пропускаем.")
                            continue
                        
                        # Iterate through individual trades within the OrderTrades event
                        for trade_item in order_trades_data.trades:
                            # Pass the client along
                            await self.order_manager.handle_order_fill(client, order_trades_data, trade_item)
                    elif trade_event_response.ping:
                        self.logger.debug("Пинг от стрима сделок.")
                    elif trade_event_response.subscription_status:
                        self.logger.debug(f"Стрим ордеров: Получен статус подписки: {trade_event_response.subscription_status.status.name}")
                    else:
                        self.logger.warning(f"Неизвестное событие в trades_stream: {trade_event_response}")
            except Exception as e:
                if not self.orders_stream_connected: # If already disconnected, log at DEBUG
                    self.logger.debug(f"Ошибка в стриме ордеров: {e}. Попытка переподключения через {self.orders_reconnect_delay} секунд.")
                else: # First time disconnection, log at WARNING
                    self.logger.warning(f"!!!!!!Связь потеряна!!!!!!!!!!! Ошибка в стриме ордеров: {e}. Попытка переподключения через {self.orders_reconnect_delay} секунд.")
                self.orders_stream_connected = False # Mark as disconnected
                await asyncio.sleep(self.orders_reconnect_delay)
                self.orders_reconnect_delay = min(self.orders_reconnect_delay * 2, self.orders_max_reconnect_delay)
                self.orders_stream_ready.clear() # Clear the event on error
        self.logger.info("Слушатель стрима ордеров завершает работу.") 

    async def _process_market_data_response(self, response):
        if not isinstance(response, MarketDataResponse):
            self.logger.error(f"Получен неожиданный тип объекта в потоке рыночных данных: {type(response)}. Ожидался MarketDataResponse. Объект: {response!r}")
            return
        try:
            if response.subscribe_last_price_response:
                self.logger.debug(f"Стрим рыночных данных: Получен ответ на подписку на последнюю цену: {response.subscribe_last_price_response.last_price_subscriptions}")
            
            elif response.last_price:
                if response.last_price.figi == self.figi:
                    current_stream_price = _quotation_to_decimal(response.last_price.price)
                    self.last_known_market_price = current_stream_price
                    # Log at INFO level for visibility
                    self.logger.debug(f"Стрим рыночных данных: Обновлена последняя известная рыночная цена: {self.last_known_market_price:.{self.display_precision}f}")
                return
            
            elif response.ping:
                self.logger.debug("Пинг от стрима рыночных данных.")
                return
            
            elif response.subscription_status:
                self.logger.debug(f"Стрим рыночных данных: Получен статус подписки: {response.subscription_status.status.name}")
            
            else:
                    self.logger.warning(f"Получен неизвестный тип MarketDataResponse (неизвестное поле): {response}")
        except Exception as e:
            self.logger.error(f"Ошибка при обработке MarketDataResponse: {e}. Объект ответа: {response!r}. Тип: {type(response)}. Атрибуты: {dir(response)}")

    async def _listen_market_data(self, client: AsyncClient):
        self.logger.info("Запуск слушателя стрима рыночных данных...")
        self.client_instance = client
        while self.running: 
            try:
                if not self.market_data_stream_connected:
                    self.logger.debug(f"Подключение к стриму рыночных данных. Текущая задержка переподключения: {self.market_data_reconnect_delay}с")
                market_data_manager = client.create_market_data_stream()
                market_data_manager.last_price.subscribe(
                    instruments=[
                        LastPriceInstrument(figi=self.figi),
                    ]
                )
                self.logger.info(f"Запрос на подписку на последние цены для FIGI {self.figi} отправлен.")
                successfully_connected_this_attempt = False
                async for response in market_data_manager:
                    if not self.market_data_stream_connected:
                        if response.subscribe_last_price_response and response.subscribe_last_price_response.last_price_subscriptions:
                            for sub in response.subscribe_last_price_response.last_price_subscriptions:
                                if sub.figi == self.figi and sub.subscription_status == SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS:
                                    self.logger.info(f"!!!!!!!!!!!Связь восстановлена-продолжаем работу!!!!!!!!!!!")
                                    self.market_data_reconnect_delay = 1
                                    self.market_data_stream_connected = True
                                    self.market_data_stream_ready.set()
                                    break
                        elif response.last_price or response.ping:
                            if self.market_data_reconnect_delay > 1:
                                self.logger.info(f"!!!!!!!!!!!Связь восстановлена-продолжаем работу!!!!!!!!!!!")
                            self.market_data_reconnect_delay = 1
                            self.market_data_stream_connected = True
                            self.market_data_stream_ready.set()
                    await self._process_market_data_response(response)
            except Exception as e:
                if not self.market_data_stream_connected: # If already disconnected, log at DEBUG
                    self.logger.debug(f"Ошибка в потоке рыночных данных: {e}. Попытка переподключения через {self.market_data_reconnect_delay} секунд.")
                else: # First time disconnection, log at WARNING
                    self.logger.warning(f"!!!!!!Связь потеряна!!!!!!!!!!! Ошибка в потоке рыночных данных: {e}. Попытка переподключения через {self.market_data_reconnect_delay} секунд.")
                self.market_data_stream_connected = False # Mark as disconnected
                await asyncio.sleep(self.market_data_reconnect_delay)
                self.market_data_reconnect_delay = min(self.market_data_reconnect_delay * 2, self.market_data_max_reconnect_delay)
                self.market_data_stream_ready.clear()
        self.logger.info("Слушатель стрима рыночных данных завершает работу.") 

    async def _save_state(self):
        """Сохраняет текущее состояние бота в JSON-файл через OrderManager."""
        self.order_manager.save_state()

    # Removed _load_state method from TradingBotV10_4_0 as OrderManager handles it in its __init__
    # This prevents double loading and potential state clearing.

    async def _update_trend_based_account_activity(self, client: AsyncClient):
        """
        Обновляет активность счетов на основе текущего тренда.
        Деактивирует счета, торгующие против тренда, отменяя их ордера на вход.
        Активирует счета, торгующие по тренду, устанавливая orders_need_reposition.
        """
        if not ENABLE_TREND_FILTERING or self.last_known_market_price == Decimal('0'):
            return

        now = datetime.now(MOSCOW_TZ)
        if (now - self.last_trend_check_time).total_seconds() < TREND_CHECK_INTERVAL_SECONDS:
            return

        if self.last_trend_check_price == Decimal('0'):
            self.last_trend_check_price = self.last_known_market_price
            self.last_trend_check_time = now
            self.logger.info(f"Инициализация цены для определения тренда: {self.last_trend_check_price:.{self.display_precision}f}")
            return

        price_change = self.last_known_market_price - self.last_trend_check_price
        percentage_change = (price_change / self.last_trend_check_price) * 100 if self.last_trend_check_price != Decimal('0') else Decimal('0')

        self.logger.debug(f"Проверка тренда: Изменение цены за {TREND_CHECK_INTERVAL_SECONDS}с: {percentage_change:.2f}% (от {self.last_trend_check_price:.{self.display_precision}f} до {self.last_known_market_price:.{self.display_precision}f})")

        # Определяем тренд
        is_uptrend = percentage_change >= TREND_THRESHOLD_PERCENT
        is_downtrend = percentage_change <= -TREND_THRESHOLD_PERCENT
        is_sideways = not is_uptrend and not is_downtrend

        # Обновляем last_trend_check_price и last_trend_check_time для следующей итерации
        self.last_trend_check_price = self.last_known_market_price
        self.last_trend_check_time = now

        if self.buy_account_manager:
            buy_state = self.buy_account_manager.account_state
            if is_uptrend or is_sideways:
                if not buy_state.is_active_by_trend:
                    self.logger.info(f"[{buy_state.account_name_for_log}] Активирован по тренду (Восходящий/Боковой).")
                    buy_state.is_active_by_trend = True
                    buy_state.orders_need_reposition = True # Заставляем переставить ордера
            else: # is_downtrend
                if buy_state.is_active_by_trend:
                    self.logger.info(f"[{buy_state.account_name_for_log}] Деактивирован по тренду (Нисходящий). Отменяем ордера для входа.")
                    buy_state.is_active_by_trend = False
                    await self.order_manager.cancel_all_orders_for_account(client, buy_state.account_id, buy_state.account_name_for_log, cancel_entry_orders_only=True)
                    buy_state.orders_need_reposition = False # Не нужно переставлять, если неактивен
        
        if self.sell_account_manager:
            sell_state = self.sell_account_manager.account_state
            if is_downtrend or is_sideways:
                if not sell_state.is_active_by_trend:
                    self.logger.info(f"[{sell_state.account_name_for_log}] Активирован по тренду (Нисходящий/Боковой).")
                    sell_state.is_active_by_trend = True
                    sell_state.orders_need_reposition = True # Заставляем переставить ордера
            else: # is_uptrend
                if sell_state.is_active_by_trend:
                    self.logger.info(f"[{sell_state.account_name_for_log}] Деактивирован по тренду (Восходящий). Отменяем ордера для входа.")
                    sell_state.is_active_by_trend = False
                    await self.order_manager.cancel_all_orders_for_account(client, sell_state.account_id, sell_state.account_name_for_log, cancel_entry_orders_only=True)
                    sell_state.orders_need_reposition = False # Не нужно переставлять, если неактивен
        
        self.order_manager.save_state() # Сохраняем обновленные статусы активности по тренду

    async def run(self):
        async with AsyncClient(API_TOKEN, target=INVEST_GRPC_API) as client:
            try:
                if not await self.get_instrument_data(client):
                    raise ValueError(f"Не удалось найти данные инструмента для тикера {TICKER}")
                
                accounts_to_subscribe_to_orders_stream = []
                if self.sell_account_manager:
                    accounts_to_subscribe_to_orders_stream.append(SELL_ACCOUNT_ID)
                if self.buy_account_manager:
                    accounts_to_subscribe_to_orders_stream.append(BUY_ACCOUNT_ID)
                
                # Start both market data and orders listeners concurrently
                market_data_listener_task = asyncio.create_task(self._listen_market_data(client))
                orders_listener_task = asyncio.create_task(self._orders_stream_listener(client, accounts_to_subscribe_to_orders_stream))
                
                # Wait for both streams to be ready and market data to have a price
                self.logger.info("Ожидание готовности всех стримов и первой цены перед синхронизацией...")
                await asyncio.gather(self.market_data_stream_ready.wait(), self.orders_stream_ready.wait())
                while self.last_known_market_price == Decimal('0'):
                    await asyncio.sleep(0.5)
                self.logger.info(f"Все стримы готовы и получена первая цена: {self.last_known_market_price:.{self.display_precision}f}. Продолжаем синхронизацию.")
                
                # Initial sync after all streams are ready and initial price is known
                self.logger.info("Выполняем начальную синхронизацию позиций и TP ордеров.")
                
                if self.sell_account_manager:
                    sell_portfolio = await client.operations.get_portfolio(account_id=SELL_ACCOUNT_ID)
                    sell_pos = next((pos for pos in sell_portfolio.positions if pos.figi == self.figi), None)
                    sell_lots = abs(_quotation_to_decimal(sell_pos.quantity_lots)).normalize() if sell_pos else Decimal('0')
                    sell_avg_price = _quotation_to_decimal(sell_pos.average_position_price) if sell_pos and sell_pos.average_position_price else Decimal('0')
                    await self.sell_account_manager.sync_position_with_api(client, sell_lots, sell_avg_price, self.last_known_market_price)
                if self.buy_account_manager:
                    buy_portfolio = await client.operations.get_portfolio(account_id=BUY_ACCOUNT_ID)
                    buy_pos = next((pos for pos in buy_portfolio.positions if pos.figi == self.figi), None)
                    buy_lots = abs(_quotation_to_decimal(buy_pos.quantity_lots)).normalize() if buy_pos else Decimal('0')
                    buy_avg_price = _quotation_to_decimal(buy_pos.average_position_price) if buy_pos and buy_pos.average_position_price else Decimal('0')
                    await self.buy_account_manager.sync_position_with_api(client, buy_lots, buy_avg_price, self.last_known_market_price)
                
                self.logger.info("Начальная синхронизация позиций и TP ордеров завершена.")
                
                while self.running:
                    now = datetime.now(MOSCOW_TZ)
                    is_trading_time = (TRADING_START_TIME <= now.time() <= TRADING_END_TIME) or \
                                      (EXT_TRADING_START_TIME <= now.time() <= EXT_TRADING_END_TIME)
                    
                    if not is_trading_time:
                        if not self.was_closed:
                            self.logger.info("Биржа закрыта = режим паузы. Ожидание открытия. Ордера не отменяются.")
                            self.was_closed = True
                            await self._save_state() # Сохраняем состояние, чтобы не потерять активные ордера
                        await asyncio.sleep(60)
                        continue
                    else:
                        if self.was_closed:
                            self.logger.info("Биржа открыта = продолжаем работу. Инициируем проверку ордеров для входа и синхронизацию позиций.")
                            self.was_closed = False
                            # Force reposition and manage position on market open
                            if self.sell_account_manager:
                                self.sell_account_manager.account_state.orders_need_reposition = True
                                self.sell_account_manager.account_state.initial_entry_orders_placed_for_session = False
                                # NEW: При открытии рынка, если фильтр тренда активен, пересчитываем тренд сразу
                                if ENABLE_TREND_FILTERING:
                                    self.last_trend_check_price = Decimal('0') # Сброс для немедленной переоценки тренда
                                    # Ensure last_trend_check_time is timezone-aware before calling _update_trend_based_account_activity
                                    self.last_trend_check_time = datetime.now(MOSCOW_TZ)
                                    await self._update_trend_based_account_activity(client)
                                await self.sell_account_manager.manage_position(client, self.last_known_market_price)
                            if self.buy_account_manager:
                                self.buy_account_manager.account_state.orders_need_reposition = True
                                self.buy_account_manager.account_state.initial_entry_orders_placed_for_session = False
                                # NEW: При открытии рынка, если фильтр тренда активен, пересчитываем тренд сразу
                                if ENABLE_TREND_FILTERING:
                                    self.last_trend_check_price = Decimal('0') # Сброс для немедленной переоценки тренда
                                    # Ensure last_trend_check_time is timezone-aware before calling _update_trend_based_account_activity
                                    self.last_trend_check_time = datetime.now(MOSCOW_TZ)
                                    await self._update_trend_based_account_activity(client)
                                await self.buy_account_manager.manage_position(client, self.last_known_market_price)

                    # NEW: Вызов функции обновления активности по тренду
                    await self._update_trend_based_account_activity(client)
                    
                    # Эти вызовы теперь будут происходить регулярно, независимо от состояния стримов.
                    # Внутренние методы manage_position и OrderManager обработают возможные ошибки API.
                    if self.sell_account_manager:
                        await self.sell_account_manager.manage_position(client, self.last_known_market_price)
                        await self.sell_account_manager.perform_end_of_session_margin_reduction(client, self.last_known_market_price)
                    
                    if self.buy_account_manager:
                        await self.buy_account_manager.manage_position(client, self.last_known_market_price)
                        await self.buy_account_manager.perform_end_of_session_margin_reduction(client, self.last_known_market_price) # Раскомментируйте, если нужно для BUY счета
                    
                    # Вызов очистки ордеров
                    self.order_manager.cleanup_orders()
                    
                    if orders_listener_task.done():
                        try:
                            await orders_listener_task
                        except asyncio.CancelledError:
                            self.logger.info("Слушатель стрима ордеров был отменен.")
                        except Exception as e:
                            self.logger.error(f"Слушатель стрима ордеров завершился с ошибкой: {e}. Перезапуск...")
                        orders_listener_task = asyncio.create_task(self._orders_stream_listener(client, accounts_to_subscribe_to_orders_stream))
                    
                    if market_data_listener_task.done():
                        try:
                            await market_data_listener_task 
                        except asyncio.CancelledError:
                            self.logger.info("Слушатель стрима рыночных данных был отменен.")
                        except Exception as e:
                            self.logger.error(f"Слушатель стрима рыночных данных завершился с ошибкой: {e}. Перезапуск...")
                        market_data_listener_task = asyncio.create_task(self._listen_market_data(client))
                    
                    await asyncio.sleep(self.interval_check)

            except KeyboardInterrupt:
                self.logger.info("Бот остановлен пользователем (KeyboardInterrupt).")
            except Exception as e:
                self.logger.error(f"Глобальная ошибка в боте: {str(e)}", exc_info=True)
            finally:
                self.running = False
                if 'orders_listener_task' in locals() and not orders_listener_task.done():
                    orders_listener_task.cancel()
                # Corrected: Removed redundant 'in locals()'
                if 'market_data_listener_task' in locals() and not market_data_listener_task.done():
                    market_data_listener_task.cancel()
                
                try:
                    await asyncio.gather(orders_listener_task, market_data_listener_task, return_exceptions=True)
                except Exception as e:
                    self.logger.warning(f"Ошибка при ожидании завершения задач стримов: {e}")
                await asyncio.sleep(0.5)
                self.logger.info("Отмена всех ордеров перед выходом.")
                # Отменяем все ордера через OrderManager
                if self.sell_account_manager:
                    await self.order_manager.cancel_all_orders_for_account(client, SELL_ACCOUNT_ID, SELL_ACCOUNT_NAME_FOR_LOG, cancel_entry_orders_only=False)
                if self.buy_account_manager:
                    await self.order_manager.cancel_all_orders_for_account(client, BUY_ACCOUNT_ID, BUY_ACCOUNT_NAME_FOR_LOG, cancel_entry_orders_only=False)
                
                await self._save_state()
                self.logger.info("Бот завершил работу.")

    def stop(self):
        self.running = False

# ====================== ЗАПУСК ======================
async def run_bot():
    print("=== Tbot_BUY-SELL_TAKE_PROFIT - Бот для сеточной стратегии (BUY и SELL на разных счетах) ===")
    print("========================================================================")
    print(f"Тикер: {TICKER} | Лотов на ордер: 1")
    bot = TradingBotV10_4_0()
    print(f"Интервал для размещения ордеров (I): {GRID_INTERVAL:.{bot.display_precision}f}")
   
    if ACCOUNT_FOR_TRADING == 1:
        print(f"Торговля настроена только для BUY счета.")
       
    elif ACCOUNT_FOR_TRADING == 2:
        print(f"Торговля настроена только для SELL счета.")
        
    elif ACCOUNT_FOR_TRADING == 3:
        print(f"Торговля настроена для обоих счетов (BUY и SELL).")
        
    else:
        print(f"ВНИМАНИЕ: Неизвестное значение ACCOUNT_FOR_TRADING: {ACCOUNT_FOR_TRADING}. Бот не будет торговать ни на одном счете.")
    print(f"Размещение начального рыночного ордера: {'Да' if PLACE_INITIAL_MARKET_ORDER else 'Нет'}")
    print(f"Периодичность проверки ордеров для входа: {CHECK_GRID_INTERVAL_SECONDS} сек.")
    print(f"Максимальный дневной убыток для SELL счета: 500.00 RUB")
    print(f"Максимальный дневной убыток для BUY счета: 500.00 RUB")
    print(f"Максимальные заемные средства для предотвращения комиссий: 500.00 RUB")
    print(f"Время сокращения позиции в конце сессии: 23:45:00")
    
    if ENABLE_TREND_FILTERING:
        print(f"Фильтрация по тренду: Включена")
        print(f"  - Интервал проверки тренда: {TREND_CHECK_INTERVAL_SECONDS} сек.")
        print(f"  - Порог изменения цены для тренда: {TREND_THRESHOLD_PERCENT:.2f}%")
    else:
        print(f"Фильтрация по тренду: Отключена")

    print(f"==========================================================================")
    await bot.run()
if __name__ == "__main__":
     asyncio.run(run_bot())
