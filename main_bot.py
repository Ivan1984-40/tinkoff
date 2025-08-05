from tinkoff.invest import AsyncClient
from config_utils import *
from models import *
from order_manager import OrderManager
import asyncio
from config_utils import _quotation_to_decimal, _decimal_to_quotation

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
