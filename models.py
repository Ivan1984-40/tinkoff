from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal
from datetime import datetime, time, date
from config_utils import MOSCOW_TZ

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
@dataclass
class OrderStateData:
    order_id: str
    account_id: str
    figi: str
    direction: str
    quantity_requested: int
    order_type: str
    stop_order_type: Optional[str] = None
    price: Optional[str] = None
    stop_price: Optional[str] = None
    client_code: str = field(default_factory=lambda: f"TBot_GEN_{uuid.uuid4().hex[:8]}")
    status: str = "PENDING_CONFIRMATION"
    filled_quantity: int = 0
    fill_price: Optional[str] = None
    placed_at: str = field(default_factory=lambda: datetime.now(MOSCOW_TZ).isoformat())
    last_updated_at: str = field(default_factory=lambda: datetime.now(MOSCOW_TZ).isoformat())
    status_history: List[Dict[str, Any]] = field(default_factory=list)
    is_stop_order: bool = False
    exchange_order_id: Optional[str] = None
    original_stop_order_id: Optional[str] = None
    
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
        data = {field.name: getattr(self, field.name) 
               for field in self.__dataclass_fields__.values()}
        # Преобразуем специальные типы
        if 'placed_at' in data and isinstance(data['placed_at'], str):
            data['placed_at'] = datetime.fromisoformat(data['placed_at'])
        if 'last_updated_at' in data and isinstance(data['last_updated_at'], str):
            data['last_updated_at'] = datetime.fromisoformat(data['last_updated_at'])
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        return cls(**data)

@dataclass
class AccountStateData:
    account_id: str
    account_name_for_log: str
    is_sell_account: bool
    lots_in_one_lot: int
    grid_interval: Decimal
    max_daily_loss_rub: Decimal
    max_free_margin_rub: Decimal
    end_of_session_reduction_time: time
    check_grid_interval_seconds: int
    open_lots: List[Dict[str, Any]] = field(default_factory=list)
    active_entry_order_above: Tuple[Optional[str], Optional[bool]] = (None, None)
    active_entry_order_below: Tuple[Optional[str], Optional[bool]] = (None, None)
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
    is_active_by_trend: bool = True
    
    def to_dict(self):
        data = {field.name: getattr(self, field.name) 
               for field in self.__dataclass_fields__.values()}
        # Преобразуем специальные типы
        if 'last_orders_placement_time' in data:
            if isinstance(data['last_orders_placement_time'], datetime):
                data['last_orders_placement_time'] = data['last_orders_placement_time'].isoformat()
        if 'daily_pnl_reset_date' in data:
            if isinstance(data['daily_pnl_reset_date'], date):
                data['daily_pnl_reset_date'] = data['daily_pnl_reset_date'].isoformat()
        return data
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        return cls(**data)