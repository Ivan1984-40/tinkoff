import asyncio
import uuid
import logging
from datetime import datetime, timedelta
import warnings

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
    SubscriptionStatus
)
from tinkoff.invest._errors import RequestError # Импортируем RequestError для более точной обработки исключений

from decimal import Decimal, ROUND_HALF_UP, getcontext
getcontext().prec = 10
# ====================== НАСТРОЙКИ ======================
TICKER = 'MTLR'
# =================================================================================
TOKEN = "===" # Ваш токен. ОБЯЗАТЕЛЬНО ЗАМЕНИТЕ ЭТОТ ПЛЕЙСХОЛДЕР НА ВАШ РЕАЛЬНЫЙ ТОКЕН!
# =================================================================================
SELL_ACCOUNT_ID = "==="
BUY_ACCOUNT_ID = "===" # <--- Вставьте сюда ID вашего счета для покупок!
# =================================================================================
INVEST_GRPC_API = "invest-public-api.tinkoff.ru:443"
# =================================================================================
SELL_ACCOUNT_NAME_FOR_LOG = "SELL_ACCOUNT"
BUY_ACCOUNT_NAME_FOR_LOG = "BUY_ACCOUNT"
# =================================================================================
LOTS = 1 # Количество лотов на каждый ордер (SH, SL, BL, BH)
# =================================================================================
GRID_INTERVAL = Decimal('0.15') # Интервал сетки (I) - Изменено на 0.15 согласно логу
# =================================================================================
SETKA_THRESHOLD = 3 # Порог для фиксации прибыли (Take Profit) в лотах (для каждой позиции)
# =================================================================================
PLACE_INITIAL_MARKET_ORDER = True # True для рыночного входа, False для лимитного
# =================================================================================
INTERVAL_CHECK = 5 # Интервал для периодической проверки позиции и логирования
# =================================================================================
CHECK_GRID_INTERVAL_SECONDS = 20 # Периодичность проверки сетки и ее перестановки, если она устарела
GRID_REPOSITION_TOLERANCE_FACTOR = Decimal('2') # Во сколько раз от GRID_INTERVAL цена должна отклониться, чтобы триггернуть перестановку сетки
# =================================================================================

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
        # Добавляем новый атрибут к записи лога, чтобы его можно было использовать в строке формата
        record.log_sequence_number = CustomLogFormatter._log_counter
        # Вызываем родительский метод format, который использует обновленный record
        return super().format(record)

# ====================== ТОРГОВЫЙ БОТ ======================
class TradingBotV10_4_0: # Изменили имя класса на TradingBotV10_4_0
    def __init__(self):
        self.running = True
        self.figi = None
        self.lots_in_one_lot = 1
        self.min_price_increment = Decimal('0.01') # Changed to 0.01 based on log for MTLR
        self.display_precision = 2
        self.setup_logging()
        
        # --- АТРИБУТЫ СОСТОЯНИЯ ДЛЯ СЧЕТА ПРОДАЖ (ШОРТ) ---
        self.sell_last_filled_trade_price = Decimal('0') # LFT (Last Filled Trade Price) - цена последней ПРОДАЖИ (открытия шорта)
        self.sell_current_position_lots = 0 # Абсолютное количество лотов в короткой позиции
        self.sell_average_position_price = Decimal('0') # AP (Average Position Price) - средняя цена открытия шорта
        self.sell_active_sell_higher_limit_order_id = None # SH (Sell Higher) - лимитный ордер на продажу (открытие шорта)
        self.sell_active_sell_lower_stop_limit_order_id = None # SL (Sell Lower) - стоп-лимитный ордер на продажу (открытия шорта)
        self.sell_active_sl_order_ids = {} # Словарь {sl_order_id: (lots_covered, entry_price_for_sl)} - теперь для стоп-лоссов для шортов (ордера на ПОКУПКУ для покрытия)
        self.sell_initial_orders_placed = False
        self.sell_in_setka_tp_process = False
        self.sell_tp_recovery_mode = False
        self.sell_last_grid_reposition_time = datetime.min # Время последней перестановки сетки
        self.sell_tp_retry_delay_until = datetime.min # Время до которого откладываем повторную попытку TP

        # --- АТРИБУТЫ СОСТОЯНИЯ ДЛЯ СЧЕТА ПОКУПОК (ЛОНГ) ---
        self.buy_last_filled_trade_price = Decimal('0') # LFT - цена последней ПОКУПКИ (открытия лонга)
        self.buy_current_position_lots = 0 # Абсолютное количество лотов в длинной позиции
        self.buy_average_position_price = Decimal('0') # AP - средняя цена открытия лонга
        self.buy_active_buy_lower_limit_order_id = None # BL (Buy Lower) - лимитный ордер на покупку (открытия лонга)
        self.buy_active_buy_higher_stop_limit_order_id = None # BH (Buy Higher) - стоп-лимитный ордер на покупку (открытия лонга)
        self.buy_active_bl_order_ids = {} # Словарь {bl_order_id: (lots_covered, entry_price_for_bl)} - для стоп-лоссов для лонгов (ордера на ПРОДАЖУ для покрытия)
        self.buy_initial_orders_placed = False
        self.buy_in_setka_tp_process = False
        self.buy_tp_recovery_mode = False
        self.buy_last_grid_reposition_time = datetime.min # Время последней перестановки сетки
        self.buy_tp_retry_delay_until = datetime.min # Время до которого откладываем повторную попытку TP

        self.interval_check = INTERVAL_CHECK
        self.SETKA_THRESHOLD = SETKA_THRESHOLD
        self.CHECK_GRID_INTERVAL_SECONDS = CHECK_GRID_INTERVAL_SECONDS
        self.GRID_REPOSITION_TOLERANCE_FACTOR = GRID_REPOSITION_TOLERANCE_FACTOR
        self.MAX_LOTS_FOR_SH_BL_PLACEMENT = LOTS * 2 # Максимальное количество лотов, при котором SH/BL ордера еще размещаются

        # === АТРИБУТЫ ДЛЯ УПРАВЛЕНИЯ ЗАДЕРЖКОЙ ПЕРЕПОДКЛЮЧЕНИЯ ===
        self.orders_reconnect_delay = 1 # Начальная задержка для стрима ордеров (сек)
        self.orders_max_reconnect_delay = 60 # Максимальная задержка для стрима ордеров (сек)
        self.market_data_reconnect_delay = 1 # Начальная задержка для стрима рыночных данных (сек)
        self.market_data_max_reconnect_delay = 60 # Максимальная задержка для стрима рыночных данных (сек)

        # === АТРИБУТЫ ДЛЯ ОТСЛЕЖИВАНИЯ РЫНОЧНОЙ ЦЕНЫ ===
        self.last_known_market_price = Decimal('0') # Последняя известная рыночная цена из стрима
        self.market_data_stream_ready = asyncio.Event() # Событие для сигнализации готовности стрима

    def setup_logging(self):
        # Настройка логгера для бота
        self.logger = logging.getLogger("LevelGridBot")
        self.logger.setLevel(logging.INFO)

        # Создаем обработчик и устанавливаем наш пользовательский форматтер
        handler = logging.StreamHandler()
        # Формат: порядковый номер лога: Уровень - LНомерСтрокиКода - Сообщение
        formatter = CustomLogFormatter("%(log_sequence_number)d: %(levelname)s - L%(lineno)d - %(message)s")
        handler.setFormatter(formatter)
        
        # Очищаем существующие обработчики, чтобы избежать дублирования
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.addHandler(handler)
        self.logger.propagate = False # Предотвращаем дублирование сообщений в корневом логгера

        # Настройка логгера tinkoff.invest для подавления информационных сообщений
        tinkoff_invest_logger = logging.getLogger("tinkoff.invest")
        tinkoff_invest_logger.setLevel(logging.WARNING) # Устанавливаем уровень WARNING, чтобы скрыть INFO/DEBUG
        tinkoff_invest_logger.propagate = False # Предотвращаем дублирование сообщений в корневом логгера

    def _quotation_to_decimal(self, quotation: Quotation) -> Decimal:
        if quotation is None:
            return Decimal('0')
        
        if quotation.units < 0 and quotation.nano > 0:
            return Decimal(f"{quotation.units}.{quotation.nano:09d}")
        elif quotation.units == 0 and quotation.nano < 0:
            return Decimal(f"-0.{abs(quotation.nano):09d}")
        else:
            return Decimal(f"{quotation.units}.{quotation.nano:09d}")

    def _decimal_to_quotation(self, value: Decimal) -> Quotation:
        if value is None:
            return Quotation(units=0, nano=0)

        is_negative = value < 0
        abs_value = abs(value)

        units = int(abs_value)
        nano = int((abs_value - Decimal(units)) * Decimal('1e9'))

        if is_negative:
            units = -units
            if units == 0 and value < 0:
                nano = -nano
            elif units < 0 and nano < 0:
                nano = abs(nano)

        return Quotation(units=units, nano=nano)

    async def get_instrument_data(self, client):
        try:
            instruments = await client.instruments.shares()
            for instrument in instruments.instruments:
                if instrument.ticker == TICKER:
                    self.figi = instrument.figi
                    self.lots_in_one_lot = instrument.lot
                    if instrument.min_price_increment:
                        self.min_price_increment = self._quotation_to_decimal(instrument.min_price_increment)
                        self.display_precision = abs(self.min_price_increment.as_tuple().exponent)
                        if self.display_precision < 2:
                            self.display_precision = 2
                        getcontext().prec = max(getcontext().prec, self.display_precision + 2)
                    self.logger.info(f"Для {TICKER}: 1 лот = {self.lots_in_one_lot} акция(ий), шаг цены = {self.min_price_increment:.{self.display_precision}f}.")
                    return True
            self.logger.error(f"Инструмент с тикером {TICKER} не найден.")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка получения данных инструмента: {str(e)}")
            return False

    async def cancel_all_orders_for_account(self, client, account_id, account_name_for_log): # Добавили account_name_for_log для более понятного лога
        self.logger.info(f"Отмена всех активных заявок для счета {account_name_for_log} ({account_id})...")
        try:
            orders = await client.orders.get_orders(account_id=account_id)
            for order in orders.orders:
                if order.figi == self.figi: # Отменяем только ордера по нашему инструменту
                    try:
                        await client.orders.cancel_order(
                            account_id=account_id,
                            order_id=order.order_id
                        )
                        self.logger.info(f"Отменена обычная заявка {order.order_id} для счета {account_name_for_log}")
                    except RequestError as e: # Ловим специфическое исключение RequestError
                        error_code = str(e.code) if hasattr(e, 'code') else ''
                        error_message = str(e.message) if hasattr(e, 'message') else str(e)
                        # Игнорируем ошибки отмены, если ордер уже исполнен, не найден или внутренняя ошибка биржи
                        if "partially or fully executed" in error_message or "30059" in error_code or "70001" in error_code:
                            # Изменено на INFO, так как это ожидаемое поведение
                            self.logger.info(f"Ордер {order.order_id} для {account_name_for_log} уже неактивен (исполнен/не найден/внутренняя ошибка). Игнорируем ошибку отмены.")
                        else:
                            self.logger.warning(f"Не удалось отменить обычную заявку {order.order_id} для {account_name_for_log}: {e}")
                    except Exception as e:
                        self.logger.warning(f"Не удалось отменить обычную заявку {order.order_id} для {account_name_for_log}: {e}")

            stop_orders = await client.stop_orders.get_stop_orders(account_id=account_id)
            for stop_order in stop_orders.stop_orders:
                if stop_order.figi == self.figi: # Отменяем только стоп-ордера по нашему инструменту
                    try:
                        await client.stop_orders.cancel_stop_order(
                            account_id=account_id,
                            stop_order_id=stop_order.stop_order_id
                        )
                        self.logger.info(f"Отменена стоп-заявка {stop_order.stop_order_id} для счета {account_name_for_log}")
                    except RequestError as e: # Ловим специфическое исключение RequestError
                        error_code = str(e.code) if hasattr(e, 'code') else ''
                        error_message = str(e.message) if hasattr(e, 'message') else str(e)
                        # Игнорируем ошибки отмены, если ордер уже исполнен, не найден или внутренняя ошибка биржи
                        if "partially or fully executed" in error_message or "30059" in error_code or "70001" in error_code or "50006" in error_code:
                            # Изменено на INFO, так как это ожидаемое поведение
                            self.logger.info(f"Стоп-ордер {stop_order.stop_order_id} для {account_name_for_log} уже неактивен (исполнен/не найден/внутренняя ошибка). Игнорируем ошибку отмены.")
                        else:
                            self.logger.warning(f"Не удалось отменить стоп-заявку {stop_order.stop_order_id} для {account_name_for_log}: {e}")
                    except Exception as e:
                        self.logger.warning(f"Не удалось отменить стоп-заявку {stop_order.stop_order_id} для {account_name_for_log}: {e}")
            
            # Обновление ID активных ордеров после отмены, специфичное для каждого счета
            if account_id == SELL_ACCOUNT_ID:
                self.sell_active_sell_higher_limit_order_id = None
                self.sell_active_sell_lower_stop_limit_order_id = None
                self.sell_active_sl_order_ids = {}
            elif account_id == BUY_ACCOUNT_ID:
                self.buy_active_buy_lower_limit_order_id = None
                self.buy_active_buy_higher_stop_limit_order_id = None
                self.buy_active_bl_order_ids = {}

            self.logger.info(f"Все заявки по {TICKER} отменены для счета {account_name_for_log}")
        except Exception as e:
            self.logger.error(f"Ошибка отмены заявок для счета {account_name_for_log}: {str(e)}")

    async def place_stop_order(self, client, account_id, stop_price, quantity, direction: OrderDirection, stop_order_type: StopOrderType, limit_price: Decimal | None = None, order_tag: str = "", account_name_for_log: str = ""):
        try:
            stop_price_quantized = (stop_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
            if stop_price_quantized <= 0:
                self.logger.warning(f"Расчетная стоп-цена {stop_price_quantized:.{self.display_precision}f} некорректна. Ордер не будет размещен.")
                return None

            price_for_stop_order_request = self._decimal_to_quotation(limit_price or Decimal('0'))
            
            if stop_order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT:
                if limit_price is None:
                    self.logger.error("Для STOP_LIMIT ордера необходимо указать limit_price.")
                    return None
                price_for_stop_order_request = self._decimal_to_quotation(limit_price)
            elif stop_order_type in [StopOrderType.STOP_ORDER_TYPE_STOP_LOSS, StopOrderType.STOP_ORDER_TYPE_TAKE_PROFIT]:
                price_for_stop_order_request = self._decimal_to_quotation(Decimal('0'))

            if self.last_known_market_price != Decimal('0'):
                self.logger.info(f"Текущая цена: {self.last_known_market_price:.{self.display_precision}f}")

            response = await client.stop_orders.post_stop_order(
                account_id=account_id,
                instrument_id=self.figi,
                quantity=quantity,
                stop_price=self._decimal_to_quotation(stop_price_quantized),
                direction=direction, 
                expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
                stop_order_type=stop_order_type,
                exchange_order_type=ExchangeOrderType.EXCHANGE_ORDER_TYPE_LIMIT if stop_order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT else ExchangeOrderType.EXCHANGE_ORDER_TYPE_MARKET,
                price=price_for_stop_order_request,
                order_id=str(uuid.uuid4())
            )
            
            self.logger.info(f"{account_name_for_log}-установлен{order_tag}, {stop_price_quantized:.{self.display_precision}f},")
            return response.stop_order_id
        except Exception as e:
            self.logger.error(f"Ошибка стоп-заявки для счета {account_name_for_log}: {str(e)}")
        return None

    async def place_limit_order(self, client, account_id, price, quantity, direction: OrderDirection, order_tag: str = "", account_name_for_log: str = "") -> PostOrderResponse | None:
        try:
            price_quantized = (price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
            if price_quantized <= 0:
                self.logger.warning(f"Расчетная лимитная цена {price_quantized:.{self.display_precision}f} некорректна. Ордер не будет размещен.")
                return None
            
            if self.last_known_market_price != Decimal('0'):
                self.logger.info(f"Текущая цена: {self.last_known_market_price:.{self.display_precision}f}")

            response = await client.orders.post_order(
                account_id=account_id,
                figi=self.figi,
                quantity=quantity,
                price=self._decimal_to_quotation(price_quantized),
                direction=direction,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=str(uuid.uuid4()),
            )
            self.logger.info(f"{account_name_for_log}-установлен{order_tag}, {price_quantized:.{self.display_precision}f},")
            return response
        except Exception as e:
            self.logger.error(f"Ошибка лимитного ордера для счета {account_name_for_log}: {str(e)}")
        return None

    async def place_market_order(self, client, account_id, quantity, direction: OrderDirection, account_name_for_log: str = "") -> PostOrderResponse | None:
        try:
            response = await client.orders.post_order(
                account_id=account_id,
                figi=self.figi,
                quantity=quantity,
                direction=direction,
                order_type=OrderType.ORDER_TYPE_MARKET,
                order_id=str(uuid.uuid4())
            )
            self.logger.info(f"Для {account_name_for_log} аккаунта размещён рыночный ордер, {quantity} лотов, {direction.name}.")
            return response
        except RequestError as e:
            error_code = str(e.code) if hasattr(e, 'code') else ''
            if error_code == '30079': # Instrument is not available for trading
                self.logger.error(f"ОШИБКА: Инструмент {TICKER} недоступен для торговли. Не удалось разместить рыночный ордер для счета {account_name_for_log}. {e.message}")
                return None # Возвращаем None, чтобы сигнализировать о конкретной ошибке
            else:
                self.logger.error(f"Ошибка рыночного ордера для счета {account_name_for_log}: {str(e)}")
                return None
        except Exception as e:
            self.logger.error(f"Ошибка рыночного ордера для счета {account_name_for_log}: {str(e)}")
        return None

    async def _place_sell_grid_orders(self, client, current_market_price_decimal):
        """
        Размещает или переразмещает ордера сетки для короткой позиции (SELL ACCOUNT).
        """
        self.logger.info(f"Размещение/обновление ордеров сетки для SELL_ACCOUNT. Текущая цена: {current_market_price_decimal:.{self.display_precision}f}")
        interval = GRID_INTERVAL

        # --- ОТМЕНА СТАРЫХ SH И SL ОРДЕРОВ ПЕРЕД РАЗМЕЩЕНИЕМ НОВЫХ ---
        # Отменяем SL (Sell Lower)
        if self.sell_active_sell_lower_stop_limit_order_id:
            try:
                await client.stop_orders.cancel_stop_order(
                    account_id=SELL_ACCOUNT_ID,
                    stop_order_id=self.sell_active_sell_lower_stop_limit_order_id
                )
                self.logger.info(f"Успешно отменен старый SELL LOWER стоп-ордер для {SELL_ACCOUNT_NAME_FOR_LOG}.")
                self.sell_active_sell_lower_stop_limit_order_id = None
            except RequestError as e:
                error_code = str(e.code) if hasattr(e, 'code') else ''
                error_message = str(e.message) if hasattr(e, 'message') else str(e)
                if "partially or fully executed" in error_message or "30059" in error_code or "70001" in error_code or "50006" in error_code:
                    # Изменено на INFO, так как это ожидаемое поведение
                    self.logger.info(f"Ордер SELL LOWER {self.sell_active_sell_lower_stop_limit_order_id} для {SELL_ACCOUNT_NAME_FOR_LOG} уже неактивен. Игнорируем ошибку отмены.")
                    self.sell_active_sell_lower_stop_limit_order_id = None
                else:
                    self.logger.warning(f"Не удалось отменить SELL LOWER стоп-ордер {self.sell_active_sell_lower_stop_limit_order_id} для {SELL_ACCOUNT_NAME_FOR_LOG}: {e}")
            except Exception as e:
                self.logger.warning(f"Не удалось отменить SELL LOWER стоп-ордер {self.sell_active_sell_lower_stop_limit_order_id} для {SELL_ACCOUNT_NAME_FOR_LOG}: {e}")

        # Отменяем SH (Sell Higher)
        if self.sell_active_sell_higher_limit_order_id:
            try:
                await client.orders.cancel_order(
                    account_id=SELL_ACCOUNT_ID,
                    order_id=self.sell_active_sell_higher_limit_order_id
                )
                self.logger.info(f"Успешно отменен старый SELL HIGHER лимитный ордер для {SELL_ACCOUNT_NAME_FOR_LOG}.")
                self.sell_active_sell_higher_limit_order_id = None
            except RequestError as e:
                error_code = str(e.code) if hasattr(e, 'code') else ''
                error_message = str(e.message) if hasattr(e, 'message') else str(e)
                if "partially or fully executed" in error_message or "30059" in error_code or "70001" in error_code:
                    # Изменено на INFO, так как это ожидаемое поведение
                    self.logger.info(f"Ордер SELL HIGHER {self.sell_active_sell_higher_limit_order_id} для {SELL_ACCOUNT_NAME_FOR_LOG} уже неактивен. Игнорируем ошибку отмены.")
                    self.sell_active_sell_higher_limit_order_id = None
                else:
                    self.logger.warning(f"Не удалось отменить SELL HIGHER лимитный ордер {self.sell_active_sell_higher_limit_order_id} для {SELL_ACCOUNT_NAME_FOR_LOG}: {e}")
            except Exception as e:
                self.logger.warning(f"Не удалось отменить SELL HIGHER лимитный ордер {self.sell_active_sell_higher_limit_order_id} для {SELL_ACCOUNT_NAME_FOR_LOG}: {e}")

        # --- РАЗМЕЩЕНИЕ ОРДЕРА "ПРОДАЖА ВЫШЕ" (SELL HIGHER - SH) - Лимитный ---
        # Размещаем SH только если позиция меньше MAX_LOTS_FOR_SH_BL_PLACEMENT
        if self.sell_current_position_lots < self.MAX_LOTS_FOR_SH_BL_PLACEMENT:
            sell_higher_price = current_market_price_decimal + interval
            sell_higher_final_price = (sell_higher_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
            
            sh_response = await self.place_limit_order(client, SELL_ACCOUNT_ID, sell_higher_final_price, LOTS, OrderDirection.ORDER_DIRECTION_SELL, order_tag="SH", account_name_for_log=SELL_ACCOUNT_NAME_FOR_LOG)
            if sh_response:
                self.sell_active_sell_higher_limit_order_id = sh_response.order_id
            else:
                self.logger.error(f"Не удалось разместить лимитный ордер SELL HIGHER (SH) на {sell_higher_final_price:.{self.display_precision}f} для {SELL_ACCOUNT_NAME_FOR_LOG}")
        else:
            self.logger.info(f"Не размещаем SELL HIGHER (SH) ордер для {SELL_ACCOUNT_NAME_FOR_LOG}, т.к. позиция ({self.sell_current_position_lots} лотов) достигла или превысила {self.MAX_LOTS_FOR_SH_BL_PLACEMENT} лотов (лимит для SH).")
        
        # --- РАЗМЕЩЕНИЕ ОРДЕРА "ПРОДАЖА НИЖЕ" (SELL LOWER - SL) - Стоп-Рыночный ---
        # SL размещается независимо от количества лотов, но только если позиция не достигла порога TP
        if self.sell_current_position_lots < self.SETKA_THRESHOLD:
            sell_lower_price = current_market_price_decimal - interval
            sell_lower_final_price = (sell_lower_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
            
            sl_response_id = await self.place_stop_order(
                client,
                account_id=SELL_ACCOUNT_ID,
                stop_price=sell_lower_final_price,
                quantity=LOTS,
                direction=OrderDirection.ORDER_DIRECTION_SELL, # Открытие шорта
                stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
                limit_price=None,
                order_tag="SL",
                account_name_for_log=SELL_ACCOUNT_NAME_FOR_LOG
            )
            if sl_response_id:
                self.sell_active_sell_lower_stop_limit_order_id = sl_response_id
            else:
                self.logger.error(f"Не удалось разместить стоп-рыночный ордер SELL LOWER (SL) на стоп-цене {sell_lower_final_price:.{self.display_precision}f} для {SELL_ACCOUNT_NAME_FOR_LOG}")
        else:
            self.logger.info(f"Не размещаем SELL LOWER (SL) ордер для {SELL_ACCOUNT_NAME_FOR_LOG}, т.к. позиция ({self.sell_current_position_lots} лотов) достигла или превысила SETKA_THRESHOLD ({self.SETKA_THRESHOLD}).")

        self.logger.info(f"Размещение/обновление сетки для {SELL_ACCOUNT_NAME_FOR_LOG} завершено.")
        # Обновляем время последней перестановки сетки
        self.sell_last_grid_reposition_time = datetime.now()


    async def _place_buy_grid_orders(self, client, current_market_price_decimal):
        """
        Размещает или переразмещает ордера сетки для длинной позиции (BUY ACCOUNT).
        """
        self.logger.info(f"Размещение/обновление ордеров сетки для BUY_ACCOUNT. Текущая цена: {current_market_price_decimal:.{self.display_precision}f}")
        interval = GRID_INTERVAL

        # --- ОТМЕНА СТАРЫХ BL И BH ОРДЕРОВ ПЕРЕД РАЗМЕЩЕНИЕМ НОВЫХ ---
        # Отменяем BH (Buy Higher)
        if self.buy_active_buy_higher_stop_limit_order_id:
            try:
                await client.stop_orders.cancel_stop_order(
                    account_id=BUY_ACCOUNT_ID,
                    stop_order_id=self.buy_active_buy_higher_stop_limit_order_id
                )
                self.logger.info(f"Успешно отменен старый BUY HIGHER стоп-ордер для {BUY_ACCOUNT_NAME_FOR_LOG}.")
                self.buy_active_buy_higher_stop_limit_order_id = None
            except RequestError as e:
                error_code = str(e.code) if hasattr(e, 'code') else ''
                error_message = str(e.message) if hasattr(e, 'message') else str(e)
                if "partially or fully executed" in error_message or "30059" in error_code or "70001" in error_code or "50006" in error_code:
                    # Изменено на INFO, так как это ожидаемое поведение
                    self.logger.info(f"Ордер BUY HIGHER {self.buy_active_buy_higher_stop_limit_order_id} для {BUY_ACCOUNT_NAME_FOR_LOG} уже неактивен. Игнорируем ошибку отмены.")
                    self.buy_active_buy_higher_stop_limit_order_id = None
                else:
                    self.logger.warning(f"Не удалось отменить BUY HIGHER стоп-ордер {self.buy_active_buy_higher_stop_limit_order_id} для {BUY_ACCOUNT_NAME_FOR_LOG}: {e}")
            except Exception as e:
                self.logger.warning(f"Не удалось отменить BUY HIGHER стоп-ордер {self.buy_active_buy_higher_stop_limit_order_id} для {BUY_ACCOUNT_NAME_FOR_LOG}: {e}")

        # Отменяем BL (Buy Lower)
        if self.buy_active_buy_lower_limit_order_id:
            try:
                await client.orders.cancel_order(
                    account_id=BUY_ACCOUNT_ID,
                    order_id=self.buy_active_buy_lower_limit_order_id
                )
                self.logger.info(f"Успешно отменен старый BUY LOWER лимитный ордер для {BUY_ACCOUNT_NAME_FOR_LOG}.")
                self.buy_active_buy_lower_limit_order_id = None
            except RequestError as e:
                error_code = str(e.code) if hasattr(e, 'code') else ''
                error_message = str(e.message) if hasattr(e, 'message') else str(e)
                if "partially or fully executed" in error_message or "30059" in error_code or "70001" in error_code:
                    # Изменено на INFO, так как это ожидаемое поведение
                    self.logger.info(f"Ордер BUY LOWER {self.buy_active_buy_lower_limit_order_id} для {BUY_ACCOUNT_NAME_FOR_LOG} уже неактивен. Игнорируем ошибку отмены.")
                    self.buy_active_buy_lower_limit_order_id = None
                else:
                    self.logger.warning(f"Не удалось отменить BUY LOWER лимитный ордер {self.buy_active_buy_lower_limit_order_id} для {BUY_ACCOUNT_NAME_FOR_LOG}: {e}")
            except Exception as e:
                self.logger.warning(f"Не удалось отменить BUY LOWER лимитный ордер {self.buy_active_buy_lower_limit_order_id} для {BUY_ACCOUNT_NAME_FOR_LOG}: {e}")

        # --- РАЗМЕЩЕНИЕ ОРДЕРА "ПОКУПКА НИЖЕ" (BUY LOWER - BL) - Лимитный ---
        # Размещаем BL только если позиция меньше MAX_LOTS_FOR_SH_BL_PLACEMENT
        if self.buy_current_position_lots < self.MAX_LOTS_FOR_SH_BL_PLACEMENT:
            buy_lower_price = current_market_price_decimal - interval
            buy_lower_final_price = (buy_lower_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
            
            bl_response = await self.place_limit_order(client, BUY_ACCOUNT_ID, buy_lower_final_price, LOTS, OrderDirection.ORDER_DIRECTION_BUY, order_tag="BL", account_name_for_log=BUY_ACCOUNT_NAME_FOR_LOG)
            if bl_response:
                self.buy_active_buy_lower_limit_order_id = bl_response.order_id
            else:
                self.logger.error(f"Не удалось разместить лимитный ордер BUY LOWER (BL) на {buy_lower_final_price:.{self.display_precision}f} для {BUY_ACCOUNT_NAME_FOR_LOG}")
        else:
            self.logger.info(f"Не размещаем BUY LOWER (BL) ордер для {BUY_ACCOUNT_NAME_FOR_LOG}, т.к. позиция ({self.buy_current_position_lots} лотов) достигла или превысила {self.MAX_LOTS_FOR_SH_BL_PLACEMENT} лотов (лимит для BL).")

        # --- РАЗМЕЩЕНИЕ ОРДЕРА "ПОКУПКА ВЫШЕ" (BUY HIGHER - BH) - Стоп-Рыночный ---
        # BH размещается независимо от количества лотов, но только если позиция не достигла порога TP
        if self.buy_current_position_lots < self.SETKA_THRESHOLD:
            buy_higher_price = current_market_price_decimal + interval
            buy_higher_final_price = (buy_higher_price / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment
            
            bh_response_id = await self.place_stop_order(
                client,
                account_id=BUY_ACCOUNT_ID,
                stop_price=buy_higher_final_price,
                quantity=LOTS,
                direction=OrderDirection.ORDER_DIRECTION_BUY, # Открытие лонга
                stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
                limit_price=None,
                order_tag="BH",
                account_name_for_log=BUY_ACCOUNT_NAME_FOR_LOG
            )
            if bh_response_id:
                self.buy_active_buy_higher_stop_limit_order_id = bh_response_id
            else:
                self.logger.error(f"Не удалось разместить стоп-рыночный ордер BUY HIGHER (BH) на стоп-цене {buy_higher_final_price:.{self.display_precision}f} для {BUY_ACCOUNT_NAME_FOR_LOG}")
        else:
            self.logger.info(f"Не размещаем BUY HIGHER (BH) ордер для {BUY_ACCOUNT_NAME_FOR_LOG}, т.к. позиция ({self.buy_current_position_lots} лотов) достигла или превысила SETKA_THRESHOLD ({self.SETKA_THRESHOLD}).")

        self.logger.info(f"Размещение/обновление сетки для {BUY_ACCOUNT_NAME_FOR_LOG} завершено.")
        # Обновляем время последней перестановки сетки
        self.buy_last_grid_reposition_time = datetime.now()


    async def _place_stop_for_filled_lots(self, client, filled_price: Decimal, filled_lots: int, account_id: str, account_name_for_log: str, order_direction: OrderDirection):
        """
        Размещает Stop-Loss/Take-Profit для только что открытых позиций.
        Для BUY позиции (лонг), SL будет SELL ордером на (filled_price - GRID_INTERVAL).
        Для SELL позиции (шорт), SL будет BUY ордером на (filled_price + GRID_INTERVAL).
        """
        if order_direction == OrderDirection.ORDER_DIRECTION_BUY: # Открытие лонга
            stop_price_level = filled_price - GRID_INTERVAL
            sl_direction = OrderDirection.ORDER_DIRECTION_SELL # Продажа для закрытия лонга
            order_tag_prefix = "BL_COVER"
        elif order_direction == OrderDirection.ORDER_DIRECTION_SELL: # Открытие шорта
            stop_price_level = filled_price + GRID_INTERVAL
            sl_direction = OrderDirection.ORDER_DIRECTION_BUY # Покупка для закрытия шорта
            order_tag_prefix = "SL_COVER"
        else:
            self.logger.error(f"Неизвестное направление ордера для установки стоп-лосса: {order_direction.name}")
            return

        stop_final_price = (stop_price_level / self.min_price_increment).quantize(Decimal('1.'), rounding=ROUND_HALF_UP) * self.min_price_increment

        stop_id = await self.place_stop_order(
            client, 
            account_id=account_id, 
            stop_price=stop_final_price, 
            quantity=filled_lots, 
            direction=sl_direction,
            stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
            order_tag=order_tag_prefix,
            account_name_for_log=account_name_for_log
        )
        if stop_id:
            if account_id == BUY_ACCOUNT_ID:
                self.buy_active_bl_order_ids[stop_id] = (filled_lots, filled_price)
            elif account_id == SELL_ACCOUNT_ID:
                self.sell_active_sl_order_ids[stop_id] = (filled_lots, filled_price)
        else:
            self.logger.error(f"Не удалось разместить {order_tag_prefix} для {filled_lots} лот(ов) на {stop_final_price:.{self.display_precision}f} для {account_name_for_log}.")

    async def _update_state_after_fill(self, client, order_trades_data: OrderTrades, filled_trade_item: OrderTrade):
        filled_order_id = order_trades_data.order_id
        filled_price_decimal = self._quotation_to_decimal(filled_trade_item.price)
        filled_lots = filled_trade_item.quantity
        order_direction = order_trades_data.direction
        account_id = order_trades_data.account_id

        # Определяем, к какому счету относится сделка и получаем соответствующие атрибуты
        if account_id == SELL_ACCOUNT_ID:
            account_name_for_log = SELL_ACCOUNT_NAME_FOR_LOG
            current_position_lots = self.sell_current_position_lots
            average_position_price = self.sell_average_position_price
            last_filled_trade_price = self.sell_last_filled_trade_price
            initial_orders_placed = self.sell_initial_orders_placed
            in_setka_tp_process = self.sell_in_setka_tp_process
            tp_recovery_mode = self.sell_tp_recovery_mode
            active_limit_order_id = self.sell_active_sell_higher_limit_order_id
            active_stop_order_id = self.sell_active_sell_lower_stop_limit_order_id
            active_stop_cover_ids = self.sell_active_sl_order_ids
        elif account_id == BUY_ACCOUNT_ID:
            account_name_for_log = BUY_ACCOUNT_NAME_FOR_LOG
            current_position_lots = self.buy_current_position_lots
            average_position_price = self.buy_average_position_price
            last_filled_trade_price = self.buy_last_filled_trade_price
            initial_orders_placed = self.buy_initial_orders_placed
            in_setka_tp_process = self.buy_in_setka_tp_process
            tp_recovery_mode = self.buy_tp_recovery_mode
            active_limit_order_id = self.buy_active_buy_lower_limit_order_id
            active_stop_order_id = self.buy_active_buy_higher_stop_limit_order_id
            active_stop_cover_ids = self.buy_active_bl_order_ids
        else:
            self.logger.warning(f"Исполнен ордер для неизвестного аккаунта: {account_id}. Пропускаем.")
            return

        order_tag = "UNKNOWN"
        is_initial_market_trade = False

        if order_direction == OrderDirection.ORDER_DIRECTION_SELL: # Открытие или добавление к короткой позиции
            if filled_order_id == active_limit_order_id:
                order_tag = "SH"
            elif filled_order_id == active_stop_order_id:
                order_tag = "SL"
            elif not initial_orders_placed and PLACE_INITIAL_MARKET_ORDER and account_id == SELL_ACCOUNT_ID:
                order_tag = "INITIAL_MARKET_SELL"
                is_initial_market_trade = True
            else:
                order_tag = "SELL_EXTERNAL"
        elif order_direction == OrderDirection.ORDER_DIRECTION_BUY: # Открытие или добавление к длинной позиции ИЛИ закрытие короткой
            if account_id == BUY_ACCOUNT_ID: # Это для BUY ACCOUNT
                if filled_order_id == active_limit_order_id:
                    order_tag = "BL"
                elif filled_order_id == active_stop_order_id:
                    order_tag = "BH"
                elif not initial_orders_placed and PLACE_INITIAL_MARKET_ORDER and account_id == BUY_ACCOUNT_ID:
                    order_tag = "INITIAL_MARKET_BUY"
                    is_initial_market_trade = True
                elif filled_order_id in active_stop_cover_ids: # Это стоп-лосс для лонга (продажа)
                    order_tag = "BL_COVER"
                else:
                    order_tag = "BUY_EXTERNAL"
            elif account_id == SELL_ACCOUNT_ID: # Это для SELL ACCOUNT (покупка для покрытия шорта)
                if filled_order_id in active_stop_cover_ids:
                    order_tag = "SL_COVER"
                elif in_setka_tp_process:
                    order_tag = "TP"
                else:
                    order_tag = "BUY_EXTERNAL"

        if not is_initial_market_trade:
            self.logger.info(f"{account_name_for_log}-исполнен{order_tag}, {filled_price_decimal:.{self.display_precision}f},")
        else:
            self.logger.info(f"Для {account_name_for_log} аккаунта исполнен начальный ордер-{filled_price_decimal:.{self.display_precision}f},")

        current_market_price = self.last_known_market_price
        if current_market_price == Decimal('0'):
            try:
                orderbook = await client.market_data.get_order_book(figi=self.figi, depth=1)
                if orderbook.last_price:
                    current_market_price = self._quotation_to_decimal(orderbook.last_price)
                    self.logger.debug(f"Получена текущая цена из стакана для обновления сетки: {current_market_price:.{self.display_precision}f}")
            except Exception as e:
                self.logger.warning(f"Не удалось получить текущую цену из стакана для обновления сетки: {e}. Будем использовать 0, что может привести к ошибкам.")

        # --- ОБНОВЛЕНИЕ СОСТОЯНИЯ И ПЕРЕРАЗМЕЩЕНИЕ ОРДЕРОВ ДЛЯ SELL_ACCOUNT ---
        if account_id == SELL_ACCOUNT_ID:
            if order_direction == OrderDirection.ORDER_DIRECTION_SELL: # Открытие/добавление к короткой позиции
                old_total_cost = self.sell_average_position_price * self.sell_current_position_lots
                new_total_cost = old_total_cost + (filled_price_decimal * filled_lots)
                self.sell_current_position_lots += filled_lots
                self.sell_average_position_price = new_total_cost / self.sell_current_position_lots if self.sell_current_position_lots > 0 else Decimal('0')
                self.sell_last_filled_trade_price = filled_price_decimal
                self.logger.info(f"Позиция {SELL_ACCOUNT_NAME_FOR_LOG} обновлена: {self.sell_current_position_lots} лот(ов) по средней цене {self.sell_average_position_price:.{self.display_precision}f}")

                if self.sell_tp_recovery_mode:
                    self.logger.info(f"{SELL_ACCOUNT_NAME_FOR_LOG} в режиме восстановления TP. Новые ордера на ПРОДАЖУ и SL_COVER не размещаются.")
                    return

                await self._place_stop_for_filled_lots(client, filled_price_decimal, filled_lots, SELL_ACCOUNT_ID, SELL_ACCOUNT_NAME_FOR_LOG, OrderDirection.ORDER_DIRECTION_SELL)
                await self._place_sell_grid_orders(client, current_market_price)
                self.sell_initial_orders_placed = True

            elif order_direction == OrderDirection.ORDER_DIRECTION_BUY: # Закрытие короткой позиции (покупка для покрытия)
                self.sell_current_position_lots -= filled_lots
                
                removed_sl_id = None
                if filled_order_id in self.sell_active_sl_order_ids:
                    removed_sl_id = filled_order_id
                else:
                    if not self.sell_in_setka_tp_process:
                        for sl_id_key, (lots_covered, entry_price_for_sl) in list(self.sell_active_sl_order_ids.items()):
                            if lots_covered == filled_lots:
                                removed_sl_id = sl_id_key
                                self.logger.info(f"Исполнен ордер на покупку {filled_order_id} для {SELL_ACCOUNT_NAME_FOR_LOG}, не найден по ID, но совпал по лотам ({filled_lots}). Вероятно, это SL_COVER. Удаляем {sl_id_key}.")
                                break
                        if removed_sl_id is None:
                            self.logger.warning(f"Исполнен ордер на покупку {filled_order_id} для {SELL_ACCOUNT_NAME_FOR_LOG}, но он не найден среди активных SL_COVER. Возможно, это внешний ордер.")

                if removed_sl_id:
                    del self.sell_active_sl_order_ids[removed_sl_id]
                    self.logger.info(f"Сработавший SL_COVER {removed_sl_id} для {SELL_ACCOUNT_NAME_FOR_LOG} удален из списка активных. Осталось активных SL_COVER: {len(self.sell_active_sl_order_ids)}")
                
                if self.sell_current_position_lots <= 0:
                    if self.sell_in_setka_tp_process:
                        self.logger.info(f"Сработал TP для {SELL_ACCOUNT_NAME_FOR_LOG}-перезапускаем бота")
                    else:
                        self.logger.info(f"Позиция {SELL_ACCOUNT_NAME_FOR_LOG} полностью закрыта по стоп-лоссу или тейк-профиту.")
                    self.sell_current_position_lots = 0
                    self.sell_average_position_price = Decimal('0')
                    self.sell_last_filled_trade_price = Decimal('0')
                    
                    await self.cancel_all_orders_for_account(client, SELL_ACCOUNT_ID, SELL_ACCOUNT_NAME_FOR_LOG)
                    
                    self.sell_initial_orders_placed = False
                    self.sell_in_setka_tp_process = False
                    self.sell_tp_recovery_mode = False
                else:
                    self.logger.info(f"Частичное исполнение ордера на покупку для SELL_ACCOUNT. Текущая позиция: {self.sell_current_position_lots} лотов.")
                    self.logger.info(f"Переставляем SH и SL ордера для SELL_ACCOUNT после частичного срабатывания SL_COVER.")
                    await self._place_sell_grid_orders(client, current_market_price)

        # --- ОБНОВЛЕНИЕ СОСТОЯНИЯ И ПЕРЕРАЗМЕЩЕНИЕ ОРДЕРОВ ДЛЯ BUY_ACCOUNT ---
        elif account_id == BUY_ACCOUNT_ID:
            if order_direction == OrderDirection.ORDER_DIRECTION_BUY: # Открытие/добавление к длинной позиции
                old_total_cost = self.buy_average_position_price * self.buy_current_position_lots
                new_total_cost = old_total_cost + (filled_price_decimal * filled_lots)
                self.buy_current_position_lots += filled_lots
                self.buy_average_position_price = new_total_cost / self.buy_current_position_lots if self.buy_current_position_lots > 0 else Decimal('0')
                self.buy_last_filled_trade_price = filled_price_decimal
                self.logger.info(f"Позиция {BUY_ACCOUNT_NAME_FOR_LOG} обновлена: {self.buy_current_position_lots} лот(ов) по средней цене {self.buy_average_position_price:.{self.display_precision}f}")

                if self.buy_tp_recovery_mode:
                    self.logger.info(f"{BUY_ACCOUNT_NAME_FOR_LOG} в режиме восстановления TP. Новые ордера на ПОКУПКУ и BL_COVER не размещаются.")
                    return

                await self._place_stop_for_filled_lots(client, filled_price_decimal, filled_lots, BUY_ACCOUNT_ID, BUY_ACCOUNT_NAME_FOR_LOG, OrderDirection.ORDER_DIRECTION_BUY)
                await self._place_buy_grid_orders(client, current_market_price)
                self.buy_initial_orders_placed = True

            elif order_direction == OrderDirection.ORDER_DIRECTION_SELL: # Закрытие длинной позиции (продажа для покрытия)
                self.buy_current_position_lots -= filled_lots
                
                removed_bl_id = None
                if filled_order_id in self.buy_active_bl_order_ids:
                    removed_bl_id = filled_order_id
                else:
                    if not self.buy_in_setka_tp_process:
                        for bl_id_key, (lots_covered, entry_price_for_bl) in list(self.buy_active_bl_order_ids.items()):
                            if lots_covered == filled_lots:
                                removed_bl_id = bl_id_key
                                self.logger.info(f"Исполнен ордер на продажу {filled_order_id} для {BUY_ACCOUNT_NAME_FOR_LOG}, не найден по ID, но совпал по лотам ({filled_lots}). Вероятно, это BL_COVER. Удаляем {bl_id_key}.")
                                break
                        if removed_bl_id is None:
                            self.logger.warning(f"Исполнен ордер на продажу {filled_order_id} для {BUY_ACCOUNT_NAME_FOR_LOG}, но он не найден среди активных BL_COVER. Возможно, это внешний ордер.")

                if removed_bl_id:
                    del self.buy_active_bl_order_ids[removed_bl_id]
                    self.logger.info(f"Сработавший BL_COVER {removed_bl_id} для {BUY_ACCOUNT_NAME_FOR_LOG} удален из списка активных. Осталось активных BL_COVER: {len(self.buy_active_bl_order_ids)}")
                
                if self.buy_current_position_lots <= 0:
                    if self.buy_in_setka_tp_process:
                        self.logger.info(f"Сработал TP для {BUY_ACCOUNT_NAME_FOR_LOG}-перезапускаем бота")
                    else:
                        self.logger.info(f"Позиция {BUY_ACCOUNT_NAME_FOR_LOG} полностью закрыта по стоп-лоссу или тейк-профиту.")
                    self.buy_current_position_lots = 0
                    self.buy_average_position_price = Decimal('0')
                    self.buy_last_filled_trade_price = Decimal('0')
                    
                    await self.cancel_all_orders_for_account(client, BUY_ACCOUNT_ID, BUY_ACCOUNT_NAME_FOR_LOG)
                    
                    self.buy_initial_orders_placed = False
                    self.buy_in_setka_tp_process = False
                    self.buy_tp_recovery_mode = False
                else:
                    self.logger.info(f"Частичное исполнение ордера на продажу для BUY_ACCOUNT. Текущая позиция: {self.buy_current_position_lots} лотов.")
                    self.logger.info(f"Переставляем BL и BH ордера для BUY_ACCOUNT после частичного срабатывания BL_COVER.")
                    await self._place_buy_grid_orders(client, current_market_price)

    async def _orders_stream_listener(self, client: AsyncClient):
        self.logger.info("Запуск слушателя стрима ордеров...")
        # Теперь подписываемся на оба аккаунта
        accounts_to_subscribe = [SELL_ACCOUNT_ID, BUY_ACCOUNT_ID]
        while self.running: 
            try:
                self.logger.info(f"Подключение к стриму ордеров для счетов {accounts_to_subscribe}. Текущая задержка переподключения: 1с") # Reset delay to 1s on reconnect attempt
                successfully_connected_this_attempt = False

                async for trade_event_response in client.orders_stream.trades_stream(accounts=accounts_to_subscribe):
                    if not successfully_connected_this_attempt:
                        if hasattr(trade_event_response, 'subscription_status') and trade_event_response.subscription_status and \
                           trade_event_response.subscription_status.status == SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS:
                            self.logger.info(f"Стрим ордеров успешно установлен. Задержка переподключения сброшена до 1с.")
                            self.orders_reconnect_delay = 1
                            successfully_connected_this_attempt = True
                        elif trade_event_response.order_trades or trade_event_response.ping:
                            if self.orders_reconnect_delay > 1:
                                self.logger.info("Данные получены из стрима ордеров. Задержка переподключения сброшена до 1с.")
                            self.orders_reconnect_delay = 1
                            successfully_connected_this_attempt = True

                    if trade_event_response.order_trades:
                        order_trades_data = trade_event_response.order_trades
                        
                        if order_trades_data.figi != self.figi:
                            continue

                        for trade_data_item in order_trades_data.trades:
                            await self._update_state_after_fill(client, order_trades_data, trade_data_item)
                    elif trade_event_response.ping:
                        self.logger.debug("Пинг от стрима сделок.")
                    elif hasattr(trade_event_response, 'subscription_status') and trade_event_response.subscription_status:
                        pass
                    else:
                        self.logger.warning(f"Неизвестное событие в trades_stream: {trade_event_response}")
            except Exception as e:
                self.logger.error(f"Ошибка в стриме ордеров: {e}. Попытка переподключения через {self.orders_reconnect_delay} секунд.")
                await asyncio.sleep(self.orders_reconnect_delay)
                self.orders_reconnect_delay = min(self.orders_reconnect_delay * 2, self.orders_max_reconnect_delay)
        self.logger.info("Слушатель стрима ордеров завершает работу.") 

    async def _process_market_data_response(self, response):
        if not isinstance(response, MarketDataResponse):
            self.logger.error(f"Получен неожиданный тип объекта в потоке рыночных данных: {type(response)}. Ожидался MarketDataResponse. Объект: {response!r}")
            return

        try:
            if response.subscribe_last_price_response:
                pass
            
            elif response.last_price:
                if response.last_price.figi == self.figi:
                    current_stream_price = self._quotation_to_decimal(response.last_price.price)
                    self.last_known_market_price = current_stream_price
                return
            
            elif response.ping:
                self.logger.debug("Пинг от стрима рыночных данных.")
                return
            
            elif hasattr(response, 'subscription_status') and response.subscription_status:
                pass
            
            else:
                    self.logger.warning(f"Получен неизвестный тип MarketDataResponse (неизвестное поле): {response}")
        except Exception as e:
            self.logger.error(f"Ошибка при обработке MarketDataResponse: {e}. Объект ответа: {response!r}. Тип: {type(response)}. Атрибуты: {dir(response)}")


    async def _listen_market_data(self, client: AsyncClient):
        self.logger.info("Запуск слушателя стрима рыночных данных...")
        self.client_instance = client
        while self.running: 
            try:
                self.logger.info(f"Подключение к стриму рыночных данных. Текущая задержка переподключения: 1с") # Reset delay to 1s on reconnect attempt
                market_data_manager = client.create_market_data_stream()

                market_data_manager.last_price.subscribe(
                    instruments=[
                        LastPriceInstrument(figi=self.figi),
                    ]
                )
                self.logger.info(f"Запрос на подписку на последние цены для FIGI {self.figi} отправлен.")

                successfully_connected_this_attempt = False

                async for response in market_data_manager:
                    if not successfully_connected_this_attempt:
                        if hasattr(response, 'subscribe_last_price_response') and response.subscribe_last_price_response and response.subscribe_last_price_response.last_price_subscriptions:
                            for sub in response.subscribe_last_price_response.last_price_subscriptions:
                                if sub.figi == self.figi and sub.subscription_status == SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS:
                                    self.logger.info(f"Стрим рыночных данных успешно установлен для FIGI {self.figi}. Задержка переподключения сброшена до 1с.")
                                    self.market_data_reconnect_delay = 1
                                    successfully_connected_this_attempt = True
                                    self.market_data_stream_ready.set()
                                    break
                        elif response.last_price or response.ping:
                            if self.market_data_reconnect_delay > 1:
                                self.logger.info("Данные получены из стрима рыночных данных. Задержка переподключения сброшена до 1с.")
                            self.market_data_reconnect_delay = 1
                            successfully_connected_this_attempt = True
                            self.market_data_stream_ready.set()

                    await self._process_market_data_response(response)

            except Exception as e:
                self.logger.error(f"Ошибка в потоке рыночных данных: {e}. Попытка переподключения через {self.market_data_reconnect_delay} секунд.")
                await asyncio.sleep(self.market_data_reconnect_delay)
                self.market_data_reconnect_delay = min(self.market_data_reconnect_delay * 2, self.market_data_max_reconnect_delay)
                self.market_data_stream_ready.clear()
        self.logger.info("Слушатель стрима рыночных данных завершает работу.") 

    async def _check_and_manage_position_for_account(self, client, account_id: str, account_name_for_log: str, is_sell_account: bool):
        """
        Проверяет текущую позицию и управляет ордерами для конкретного счета (BUY или SELL).
        """
        portfolio = None
        try:
            portfolio = await client.operations.get_portfolio(account_id=account_id)
        except RequestError as e:
            self.logger.warning(f"Ошибка получения портфолио для {account_name_for_log}: {e}. Пропускаем текущую проверку.")
            return
        except Exception as e:
            self.logger.error(f"Непредвиденная ошибка при получении портфолио для {account_name_for_log}: {e}", exc_info=True)
            return

        current_figi_position = next((pos for pos in portfolio.positions if pos.figi == self.figi), None)
        current_lots_held = Decimal('0')

        if is_sell_account:
            current_position_lots = self.sell_current_position_lots
            average_position_price = self.sell_average_position_price
            last_filled_trade_price = self.sell_last_filled_trade_price
            initial_orders_placed = self.sell_initial_orders_placed
            in_setka_tp_process = self.sell_in_setka_tp_process
            tp_recovery_mode = self.sell_tp_recovery_mode
            last_grid_reposition_time = self.sell_last_grid_reposition_time
            active_sell_higher_limit_order_id = self.sell_active_sell_higher_limit_order_id
            active_sell_lower_stop_limit_order_id = self.sell_active_sell_lower_stop_limit_order_id
            tp_retry_delay_until = self.sell_tp_retry_delay_until # Добавлено
        else: # BUY account
            current_position_lots = self.buy_current_position_lots
            average_position_price = self.buy_average_position_price
            last_filled_trade_price = self.buy_last_filled_trade_price
            initial_orders_placed = self.buy_initial_orders_placed
            in_setka_tp_process = self.buy_in_setka_tp_process
            tp_recovery_mode = self.buy_tp_recovery_mode
            last_grid_reposition_time = self.buy_last_grid_reposition_time
            active_buy_lower_limit_order_id = self.buy_active_buy_lower_limit_order_id
            active_buy_higher_stop_limit_order_id = self.buy_active_buy_higher_stop_limit_order_id
            tp_retry_delay_until = self.buy_tp_retry_delay_until # Добавлено


        if current_figi_position:
            # Для шорт-позиции quantity_lots будет отрицательным, для лонг - положительным.
            # Нас интересует абсолютное значение для current_lots_held
            current_lots_held = abs(self._quotation_to_decimal(current_figi_position.quantity_lots)).normalize()
            
            if current_figi_position.average_position_price:
                # average_position_price для шорта будет средняя цена продажи, для лонга - средняя цена покупки
                current_avg_price_from_api = self._quotation_to_decimal(current_figi_position.average_position_price)
                if is_sell_account:
                    self.sell_average_position_price = current_avg_price_from_api
                    if self.sell_last_filled_trade_price == Decimal('0'): # Инициализируем LFT, если еще не было сделок в этой сессии
                        self.sell_last_filled_trade_price = self.sell_average_position_price
                else: # BUY account
                    self.buy_average_position_price = current_avg_price_from_api
                    if self.buy_last_filled_trade_price == Decimal('0'):
                        self.buy_last_filled_trade_price = self.buy_average_position_price

        # Обновляем количество лотов из портфолио
        if is_sell_account:
            self.sell_current_position_lots = int(current_lots_held)
            current_position_lots = self.sell_current_position_lots # Обновляем локальную переменную
        else:
            self.buy_current_position_lots = int(current_lots_held)
            current_position_lots = self.buy_current_position_lots # Обновляем локальную переменную


        # --- Логика Take Profit (по SETKA_THRESHOLD) ---
        if in_setka_tp_process:
            if current_position_lots <= 0:
                self.logger.info(f"Процесс Take Profit для {account_name_for_log} завершен: позиция полностью закрыта.")
                if is_sell_account:
                    self.sell_current_position_lots = 0
                    self.sell_average_position_price = Decimal('0')
                    self.sell_last_filled_trade_price = Decimal('0')
                    self.sell_initial_orders_placed = False
                    self.sell_in_setka_tp_process = False
                    self.sell_tp_recovery_mode = False
                    self.sell_tp_retry_delay_until = datetime.min # Сброс
                else:
                    self.buy_current_position_lots = 0
                    self.buy_average_position_price = Decimal('0')
                    self.buy_last_filled_trade_price = Decimal('0')
                    self.buy_initial_orders_placed = False
                    self.buy_in_setka_tp_process = False
                    self.buy_tp_recovery_mode = False
                    self.buy_tp_retry_delay_until = datetime.min # Сброс
                self.logger.info(f"Бот готов к новому циклу для {account_name_for_log} после Take Profit.")
            else:
                # Если TP в процессе, но позиция еще открыта, и не пришло время повторной попытки
                if datetime.now() < tp_retry_delay_until:
                    self.logger.warning(f"Процесс Take Profit для {account_name_for_log} продолжается. Позиция все еще открыта: {current_position_lots} лотов. Ожидание до {tp_retry_delay_until.strftime('%H:%M:%S')} перед повторной попыткой.")
                    return # Пропускаем попытку, если время ожидания не истекло

                self.logger.warning(f"Процесс Take Profit для {account_name_for_log} продолжается. Позиция все еще открыта: {current_position_lots} лотов. Повторная попытка {'купить для покрытия' if is_sell_account else 'продать для покрытия'}.")
                if is_sell_account:
                    self.sell_tp_recovery_mode = True
                else:
                    self.buy_tp_recovery_mode = True
                await self.cancel_all_orders_for_account(client, account_id, account_name_for_log)
                
                tp_direction = OrderDirection.ORDER_DIRECTION_BUY if is_sell_account else OrderDirection.ORDER_DIRECTION_SELL
                tp_response = await self.place_market_order(client, account_id, current_position_lots, tp_direction, account_name_for_log)
                if tp_response:
                    self.logger.info(f"Рыночный ордер на {'покупку' if is_sell_account else 'продажу'} всей позиции ({current_position_lots} лотов) размещен для {account_name_for_log} в режиме восстановления TP. ID: {tp_response.order_id}")
                    # Если ордер успешно размещен, сбрасываем задержку
                    if is_sell_account:
                        self.sell_tp_retry_delay_until = datetime.min
                    else:
                        self.buy_tp_retry_delay_until = datetime.min
                else:
                    # Если place_market_order вернул None (из-за ошибки 30079), устанавливаем задержку
                    self.logger.error(f"Не удалось разместить повторный рыночный ордер для Take Profit в режиме восстановления для {account_name_for_log}. Устанавливаем задержку перед следующей попыткой.")
                    if is_sell_account:
                        self.sell_tp_retry_delay_until = datetime.now() + timedelta(seconds=60) # Задержка 60 секунд
                    else:
                        self.buy_tp_retry_delay_until = datetime.now() + timedelta(seconds=60) # Задержка 60 секунд
            return

        if current_position_lots >= self.SETKA_THRESHOLD and not in_setka_tp_process:
            self.logger.info(f"Достигнут SETKA_THRESHOLD ({self.SETKA_THRESHOLD} лотов) для {account_name_for_log}. Запускаем Take Profit: {'покупаем для покрытия' if is_sell_account else 'продаем для покрытия'} всю позицию.")
            if is_sell_account:
                self.sell_in_setka_tp_process = True
                self.sell_tp_recovery_mode = True
            else:
                self.buy_in_setka_tp_process = True
                self.buy_tp_recovery_mode = True
            await self.cancel_all_orders_for_account(client, account_id, account_name_for_log)
            
            tp_direction = OrderDirection.ORDER_DIRECTION_BUY if is_sell_account else OrderDirection.ORDER_DIRECTION_SELL
            tp_response = await self.place_market_order(client, account_id, current_position_lots, tp_direction, account_name_for_log)
            if tp_response:
                self.logger.info(f"Рыночный ордер на {'покупку' if is_sell_account else 'продажу'} всей позиции ({current_position_lots} лотов) размещен для {account_name_for_log} для Take Profit. ID: {tp_response.order_id}")
                # Если ордер успешно размещен, сбрасываем задержку
                if is_sell_account:
                    self.sell_tp_retry_delay_until = datetime.min
                else:
                    self.buy_tp_retry_delay_until = datetime.min
            else:
                # Если place_market_order вернул None (из-за ошибки 30079), устанавливаем задержку
                    self.logger.error(f"Не удалось разместить рыночный ордер для Take Profit для {account_name_for_log}. Устанавливаем задержку перед следующей попыткой.")
                    if is_sell_account:
                        self.sell_tp_retry_delay_until = datetime.now() + timedelta(seconds=60) # Задержка 60 секунд
                    else:
                        self.buy_tp_retry_delay_until = datetime.now() + timedelta(seconds=60) # Задержка 60 секунд
            return

        # --- НОВАЯ ЛОГИКА: Периодическая проверка и переразмещение сетки ---
        current_time = datetime.now()
        if (current_time - last_grid_reposition_time).total_seconds() >= self.CHECK_GRID_INTERVAL_SECONDS:
            # Переставляем сетку только если есть открытая позиция ИЛИ если initial_orders_placed
            # (чтобы бот выставил первую сетку, даже если рыночный ордер не сработал сразу)
            # и при этом не находимся в процессе TP.
            if (current_position_lots > 0 or (initial_orders_placed and not self.PLACE_INITIAL_MARKET_ORDER)) and not in_setka_tp_process:
                self.logger.info(f"Периодическая проверка сетки для {account_name_for_log}. Текущая цена: {self.last_known_market_price:.{self.display_precision}f}")
                should_reposition_grid = False
                
                # Получаем текущие активные ордера (лимитные)
                active_orders_api = await client.orders.get_orders(account_id=account_id)
                active_grid_limit_orders = [o for o in active_orders_api.orders if o.figi == self.figi and o.order_type == OrderType.ORDER_TYPE_LIMIT]

                # Получаем текущие активные стоп-ордера (стоп-лимитные/стоп-рыночные)
                active_stop_orders_api = await client.stop_orders.get_stop_orders(account_id=account_id)
                active_grid_stop_orders = [so for so in active_stop_orders_api.stop_orders if so.figi == self.figi]

                # Проверка для SELL ACCOUNT (SH/SL)
                if is_sell_account:
                    has_sh = False
                    sh_order = None
                    if active_sell_higher_limit_order_id:
                        sh_order = next((o for o in active_grid_limit_orders if o.order_id == active_sell_higher_limit_order_id), None)
                        if sh_order:
                            has_sh = True
                        else: # Если ID существовал, но ордер не найден в активном списке, очищаем его внутренне
                            self.logger.info(f"Для {account_name_for_log}: Лимитный ордер SH (ID: {active_sell_higher_limit_order_id}) не найден в активных. Очищаем внутренний ID.")
                            self.sell_active_sell_higher_limit_order_id = None # Очищаем внутренний ID
                            should_reposition_grid = True # Все равно запускаем переразмещение, так как это изменение состояния

                    has_sl = False
                    sl_order = None
                    if active_sell_lower_stop_limit_order_id:
                        sl_order = next((so for so in active_grid_stop_orders if so.stop_order_id == active_sell_lower_stop_limit_order_id), None)
                        if sl_order:
                            has_sl = True
                        else: # Если ID существовал, но ордер не найден в активном списке, очищаем его внутренне
                            self.logger.info(f"Для {account_name_for_log}: Стоп-ордер SL (ID: {active_sell_lower_stop_limit_order_id}) не найден в активных. Очищаем внутренний ID.")
                            self.sell_active_sell_lower_stop_limit_order_id = None # Очищаем внутренний ID
                            should_reposition_grid = True # Все равно запускаем переразмещение, так как это изменение состояния


                    # Определяем, должны ли SH ордера быть выставлены в принципе при текущей позиции
                    can_place_sh = current_position_lots < self.MAX_LOTS_FOR_SH_BL_PLACEMENT
                    
                    # Условие 1: Отсутствие ордеров
                    # Переразмещаем, если SH отсутствует И при этом он ДОЛЖЕН был быть выставлен (can_place_sh == True)
                    # ИЛИ если SL отсутствует (SL ставится всегда, независимо от объема)
                    if (not has_sh and can_place_sh) or not has_sl:
                        self.logger.info(f"Для {account_name_for_log}: Отсутствует один или оба ключевых ордера сетки (SH: {has_sh}, SL: {has_sl}). Переразмещаем сетку.")
                        should_reposition_grid = True
                    # Условие 2: Ордера найдены, но их цены далеки
                    elif has_sh and has_sl: # Если оба ордера есть, проверяем их цены
                        sh_price = self._quotation_to_decimal(sh_order.initial_security_price) # Для лимитных ордеров цена в initial_security_price
                        sl_stop_price = self._quotation_to_decimal(sl_order.stop_price)
                        
                        if abs(sh_price - self.last_known_market_price) > (GRID_INTERVAL * self.GRID_REPOSITION_TOLERANCE_FACTOR) or \
                           abs(sl_stop_price - self.last_known_market_price) > (GRID_INTERVAL * self.GRID_REPOSITION_TOLERANCE_FACTOR):
                            self.logger.info(f"Для {account_name_for_log}: Ордера сетки слишком далеки от текущей цены. Переразмещаем.")
                            should_reposition_grid = True
                    # Условие 3: ID есть, но ордера не найдены в активных (этот блок теперь менее вероятен, т.к. ID очищаются выше)
                    else: 
                        # Этот else блок будет срабатывать, если sh_order или sl_order стали None
                        # после того, как были найдены в get_orders/get_stop_orders, но до того, как
                        # их проверили на has_sh/has_sl. Или если ID был None, но can_place_sh/has_sl
                        # не сработали. В целом, эта ветка теперь должна быть реже.
                        self.logger.warning(f"Для {account_name_for_log}: Не удалось найти детали активных ордеров SH/SL, хотя ID присутствуют (вторичная проверка). Переразмещаем сетку на всякий случай.")
                        should_reposition_grid = True

                    if should_reposition_grid:
                        await self._place_sell_grid_orders(client, self.last_known_market_price)
                        self.sell_last_grid_reposition_time = current_time # Обновляем время 
                
                # Проверка для BUY ACCOUNT (BL/BH)
                else: # is_buy_account
                    has_bl = False
                    bl_order = None
                    if active_buy_lower_limit_order_id:
                        bl_order = next((o for o in active_grid_limit_orders if o.order_id == active_buy_lower_limit_order_id), None)
                        if bl_order:
                            has_bl = True
                        else: # Если ID существовал, но ордер не найден в активном списке, очищаем его внутренне
                            self.logger.info(f"Для {account_name_for_log}: Лимитный ордер BL (ID: {active_buy_lower_limit_order_id}) не найден в активных. Очищаем внутренний ID.")
                            self.buy_active_buy_lower_limit_order_id = None # Очищаем внутренний ID
                            should_reposition_grid = True # Все равно запускаем переразмещение, так как это изменение состояния

                    has_bh = False
                    bh_order = None
                    if active_buy_higher_stop_limit_order_id:
                        bh_order = next((so for so in active_grid_stop_orders if so.stop_order_id == active_buy_higher_stop_limit_order_id), None)
                        if bh_order:
                            has_bh = True
                        else: # Если ID существовал, но ордер не найден в активном списке, очищаем его внутренне
                            self.logger.info(f"Для {account_name_for_log}: Стоп-ордер BH (ID: {active_buy_higher_stop_limit_order_id}) не найден в активных. Очищаем внутренний ID.")
                            self.buy_active_buy_higher_stop_limit_order_id = None # Очищаем внутренний ID
                            should_reposition_grid = True # Все равно запускаем переразмещение, так как это изменение состояния


                    # Определяем, должны ли BL ордера быть выставлены в принципе при текущей позиции
                    can_place_bl = current_position_lots < self.MAX_LOTS_FOR_SH_BL_PLACEMENT
                    
                    # Условие 1: Отсутствие ордеров
                    # Переразмещаем, если BL отсутствует И при этом он ДОЛЖЕН был быть выставлен (can_place_bl == True)
                    # ИЛИ если BH отсутствует (BH ставится всегда, независимо от объема)
                    if (not has_bl and can_place_bl) or not has_bh:
                        self.logger.info(f"Для {account_name_for_log}: Отсутствует один или оба ключевых ордера сетки (BL: {has_bl}, BH: {has_bh}). Переразмещаем сетку.")
                        should_reposition_grid = True
                    # Условие 2: Ордера найдены, но их цены далеки
                    elif has_bl and has_bh: # Если оба ордера есть, проверяем их цены
                        bl_price = self._quotation_to_decimal(bl_order.initial_security_price)
                        bh_stop_price = self._quotation_to_decimal(bh_order.stop_price)
                        
                        if abs(bl_price - self.last_known_market_price) > (GRID_INTERVAL * self.GRID_REPOSITION_TOLERANCE_FACTOR) or \
                           abs(bh_stop_price - self.last_known_market_price) > (GRID_INTERVAL * self.GRID_REPOSITION_TOLERANCE_FACTOR):
                            self.logger.info(f"Для {account_name_for_log}: Ордера сетки слишком далеки от текущей цены. Переразмещаем.")
                            should_reposition_grid = True
                    # Условие 3: ID есть, но ордера не найдены в активных (этот блок теперь менее вероятен, т.к. ID очищаются выше)
                    else: 
                        self.logger.warning(f"Для {account_name_for_log}: Не удалось найти детали активных ордеров BL/BH, хотя ID присутствуют (вторичная проверка). Переразмещаем сетку на всякий случай.")
                        should_reposition_grid = True

                    if should_reposition_grid:
                        await self._place_buy_grid_orders(client, self.last_known_market_price)
                        self.buy_last_grid_reposition_time = current_time # Обновляем время 
            elif current_position_lots == 0 and not PLACE_INITIAL_MARKET_ORDER and not initial_orders_placed and not in_setka_tp_process:
                # Если позиция 0, не используем рыночный вход, и сетка еще не была размещена (первый запуск)
                # Выставляем первый лимитный ордер, чтобы инициировать торговлю
                self.logger.info(f"Для {account_name_for_log}: Позиция 0, начальный рыночный ордер не используется. Размещаем первый лимитный ордер для инициации сетки.")
                if is_sell_account:
                    initial_sh_price = self.last_known_market_price + GRID_INTERVAL
                    sh_response = await self.place_limit_order(client, SELL_ACCOUNT_ID, initial_sh_price, LOTS, OrderDirection.ORDER_DIRECTION_SELL, order_tag="SH", account_name_for_log=SELL_ACCOUNT_NAME_FOR_LOG)
                    if sh_response:
                        self.sell_active_sell_higher_limit_order_id = sh_response.order_id
                    self.sell_initial_orders_placed = True
                else:
                    initial_bl_price = self.last_known_market_price - GRID_INTERVAL
                    bl_response = await self.place_limit_order(client, BUY_ACCOUNT_ID, initial_bl_price, LOTS, OrderDirection.ORDER_DIRECTION_BUY, order_tag="BL", account_name_for_log=BUY_ACCOUNT_NAME_FOR_LOG)
                    if bl_response:
                        self.buy_active_buy_lower_limit_order_id = bl_response.order_id
                    self.buy_initial_orders_placed = True
                
                # Обновляем время последней перестановки, чтобы не выставлять сразу снова
                if is_sell_account:
                    self.sell_last_grid_reposition_time = current_time
                else:
                    self.buy_last_grid_reposition_time = current_time

            # Если позиция 0 и PLACE_INITIAL_MARKET_ORDER, то сетка не нужна до исполнения начального ордера
            # Если в режиме TP, то сетка тоже не нужна.
            else:
                self.logger.debug(f"Для {account_name_for_log}: Периодическая проверка сетки пропущена (нет позиции/идет TP).") # Для дебага

        else:
            self.logger.debug(f"Для {account_name_for_log}: Периодическая проверка сетки пропущена (интервал еще не истек).") # Для дебага


        # --- КОНЕЦ НОВОЙ ЛОГИКИ ---


        # --- БЛОК: РАЗМЕЩЕНИЕ ПЕРВОЙ СЕТКИ ИЛИ ИНИЦИАЛИЗАЦИЯ ПОЗИЦИИ (КОГДА БОТ ТОЛЬКО СТАРТУЕТ) ---
        # Эта часть остаётся, чтобы обеспечить правильный старт
        # Однако, она будет срабатывать только один раз при первом запуске,
        # так как initial_orders_placed будет быстро установлен в True
        if is_sell_account:
            if not self.sell_initial_orders_placed:
                if current_position_lots == 0:
                    if PLACE_INITIAL_MARKET_ORDER:
                        self.logger.info(f"Для {SELL_ACCOUNT_NAME_FOR_LOG} аккаунта размещён начальный рыночный ордер SELL.")
                        market_order_response = await self.place_market_order(client, SELL_ACCOUNT_ID, LOTS, OrderDirection.ORDER_DIRECTION_SELL, SELL_ACCOUNT_NAME_FOR_LOG)
                        if not market_order_response:
                            self.logger.error(f"Не удалось разместить начальный рыночный ордер SELL для {SELL_ACCOUNT_NAME_FOR_LOG}. Бот не начнет работу.")
                    # else: (логика для первого лимитного ордера теперь обрабатывается в новой периодической проверке)
                else:
                    self.logger.info(f"Обнаружена существующая короткая позиция ({current_position_lots} лот(ов)) на {SELL_ACCOUNT_NAME_FOR_LOG} при запуске. Устанавливаем LFT как AP и размещаем сетку.")
                    self.sell_last_filled_trade_price = self.sell_average_position_price
                    await self._place_sell_grid_orders(client, self.last_known_market_price)
                    if self.sell_current_position_lots > 0 and not self.sell_active_sl_order_ids:
                        await self._place_stop_for_filled_lots(client, self.sell_average_position_price, self.sell_current_position_lots, SELL_ACCOUNT_ID, SELL_ACCOUNT_NAME_FOR_LOG, OrderDirection.ORDER_DIRECTION_SELL)
                self.sell_initial_orders_placed = True
        else: # BUY account
            if not self.buy_initial_orders_placed:
                if current_position_lots == 0:
                    if PLACE_INITIAL_MARKET_ORDER:
                        self.logger.info(f"Для {BUY_ACCOUNT_NAME_FOR_LOG} аккаунта размещён начальный рыночный ордер BUY.")
                        market_order_response = await self.place_market_order(client, BUY_ACCOUNT_ID, LOTS, OrderDirection.ORDER_DIRECTION_BUY, BUY_ACCOUNT_NAME_FOR_LOG)
                        if not market_order_response:
                            self.logger.error(f"Не удалось разместить начальный рыночный ордер BUY для {BUY_ACCOUNT_NAME_FOR_LOG}. Бот не начнет работу.")
                    # else: (логика для первого лимитного ордера теперь обрабатывается в новой периодической проверке)
                else:
                    self.logger.info(f"Обнаружена существующая длинная позиция ({current_lots_held} лот(ов)) на {BUY_ACCOUNT_NAME_FOR_LOG} при запуске. Устанавливаем LFT как AP и размещаем сетку.")
                    self.buy_last_filled_trade_price = self.buy_average_position_price
                    await self._place_buy_grid_orders(client, self.last_known_market_price)
                    if self.buy_current_position_lots > 0 and not self.buy_active_bl_order_ids:
                        await self._place_stop_for_filled_lots(client, self.buy_average_position_price, self.buy_current_position_lots, BUY_ACCOUNT_ID, BUY_ACCOUNT_NAME_FOR_LOG, OrderDirection.ORDER_DIRECTION_BUY)
                self.buy_initial_orders_placed = True

    async def run(self):
        async with AsyncClient(TOKEN, target=INVEST_GRPC_API) as client:
            try:
                if not await self.get_instrument_data(client):
                    raise ValueError(f"Не удалось найти данные инструмента для тикера {TICKER}")

                # Create tasks for listeners. They will manage their own loops and reconnects.
                orders_listener_task = asyncio.create_task(self._orders_stream_listener(client))
                market_data_listener_task = asyncio.create_task(self._listen_market_data(client))

                while self.running:
                    # Run the main logic periodically
                    if not self.market_data_stream_ready.is_set() or self.last_known_market_price == Decimal('0'):
                        self.logger.info("Ожидание готовности стрима рыночных данных и первой цены перед началом работы с ордерами...")
                        await self.market_data_stream_ready.wait()
                        while self.last_known_market_price == Decimal('0'):
                            await asyncio.sleep(1)
                        self.logger.info("Стрим рыночных данных готов и получена первая цена. Продолжаем работу с ордерами.")

                    # Вызываем проверку для каждого счета
                    await self._check_and_manage_position_for_account(client, SELL_ACCOUNT_ID, SELL_ACCOUNT_NAME_FOR_LOG, True)
                    await self._check_and_manage_position_for_account(client, BUY_ACCOUNT_ID, BUY_ACCOUNT_NAME_FOR_LOG, False)
                    
                    # Check if listener tasks are still running. If not, restart them.
                    if orders_listener_task.done():
                        try:
                            await orders_listener_task
                        except asyncio.CancelledError:
                            self.logger.info("Слушатель стрима ордеров был отменен.")
                        except Exception as e:
                            self.logger.error(f"Слушатель стрима ордеров завершился с ошибкой: {e}. Перезапуск...")
                        orders_listener_task = asyncio.create_task(self._orders_stream_listener(client))
                    
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
                if 'market_data_listener_task' in locals() and not market_data_listener_task.done():
                    market_data_listener_task.cancel()
                
                await asyncio.gather(orders_listener_task, market_data_listener_task, return_exceptions=True)

                await asyncio.sleep(0.5) 
                
                self.logger.info("Отмена всех ордеров перед выходом.")
                if SELL_ACCOUNT_ID:
                    await self.cancel_all_orders_for_account(client, SELL_ACCOUNT_ID, SELL_ACCOUNT_NAME_FOR_LOG)
                if BUY_ACCOUNT_ID:
                    await self.cancel_all_orders_for_account(client, BUY_ACCOUNT_ID, BUY_ACCOUNT_NAME_FOR_LOG)
                self.logger.info("Бот завершил работу.")

    def stop(self):
        self.running = False

# ====================== ЗАПУСК ======================
async def run_bot():
    print("=== TradingBot 10.4.0 - Бот для сеточной стратегии (BUY и SELL на разных счетах) ===")
    print("Более надежная обработка стримов: безопасный доступ к атрибутам TradesStreamResponse.")
    print(f"Тикер: {TICKER} | Лотов на ордер: {LOTS}")
    bot = TradingBotV10_4_0()

    print(f"Интервал сетки (I): {GRID_INTERVAL:.{bot.display_precision}f}")
    print(f"Take Profit по объему (SETKA_THRESHOLD): {SETKA_THRESHOLD} лотов")
    print(f"Ограничение SELL HIGHER (SH) и BUY LOWER (BL): до {bot.MAX_LOTS_FOR_SH_BL_PLACEMENT} лотов включительно") # Обновлено
    print(f"*** ВАЖНО: SL (Sell Lower) и BH (Buy Higher) теперь стоп-рыночные ордера. ***")
    print(f"ID счета ПРОДАЖ: {SELL_ACCOUNT_ID} ({SELL_ACCOUNT_NAME_FOR_LOG})")
    print(f"ID счета ПОКУПОК: {BUY_ACCOUNT_ID} ({BUY_ACCOUNT_NAME_FOR_LOG})")
    print(f"Размещение начального рыночного ордера: {'Да' if PLACE_INITIAL_MARKET_ORDER else 'Нет'}")
    print(f"Периодичность проверки сетки: {CHECK_GRID_INTERVAL_SECONDS} сек.")
    print(f"Фактор отклонения для перестановки сетки: {GRID_REPOSITION_TOLERANCE_FACTOR} (от {GRID_INTERVAL}x{GRID_REPOSITION_TOLERANCE_FACTOR} = {GRID_INTERVAL * GRID_REPOSITION_TOLERANCE_FACTOR:.{bot.display_precision}f})")

    await bot.run()

if __name__ == "__main__":
    asyncio.run(run_bot())
