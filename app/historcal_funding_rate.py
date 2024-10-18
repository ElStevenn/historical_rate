from datetime import datetime, time, timedelta
import aiohttp
import asyncio
import uuid
import pytz
import numpy as np
import logging
import time as lowtime

from app.redis_layer import RedisService
from app.bitget_layer import BitgetService
from app.chart_analysis import FundingRateChart
from typing import Tuple, Literal
from config import COINMARKETCAP_APIKEY


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MainServiceLayer:
    def __init__(self) -> None:
        self.redis_service = RedisService()
        self.bitget_service = BitgetService()
        self.timezone = "Europe/Amsterdam"

    # FUNCTION EVERY DAY
    async def crypto_rebase(self):
        """Everyday Update the list of cryptos as well as gets its logos"""
        list_current_cryptos = self.redis_service.get_list_cryptos(); print("current cryptos redis -> ", list_current_cryptos)
        logger.info(f"Current cryptos -> {list_current_cryptos}")
        bitget_cryptos = await self.bitget_service.get_all_cryptos()

        # Find cryptos that don't match
        if list_current_cryptos.any():
            cryptos_to_delete = list_current_cryptos[~np.isin(list_current_cryptos, bitget_cryptos)]
            cryptos_to_add = bitget_cryptos[~np.isin(bitget_cryptos, list_current_cryptos)]
        else:
            cryptos_to_delete = np.array([])
            cryptos_to_add = bitget_cryptos

        # Remove outdated cryptos
        if cryptos_to_delete.any():
            for crypto_to_remove in cryptos_to_delete:
                self.redis_service.delete_crypto(crypto_to_remove)
                logger.info(f"Removed crypto: {crypto_to_remove}")

        # Add new cryptos
        if cryptos_to_add.any():
            for crypto in cryptos_to_add:
                if str(crypto).lower().startswith('1000'):
                    crypto = crypto[4:]

                logger.info(f"Adding crypto: {crypto}")
                try:
                    crypto_logo, name, description = await self.get_crypto_logo(crypto)
                except Exception as e:
                    logger.error(f"Failed to get logo for {crypto}: {e}")
                    continue

                # Modify logo and set it to 128 pixels
                crypto_logo = crypto_logo.replace("64x64", "128x128")
                
                try:
                    funding_rate = await self.bitget_service.get_funding_rate_period(symbol=crypto)
                except Exception as e:
                    logger.error(f"Failed to get funding rate for {crypto}: {e}")
                    funding_rate = "N/A" 

                # Add the new crypto to Redis
                self.redis_service.add_crypto_metadata(crypto, name, crypto_logo, description, funding_rate)
                logger.info(f"Added crypto to Redis: {crypto}")

    # FUNCTION EVERY 8 or 4 HOURS - DEPENDING | every XX and 1 minute!
    async def schedule_set_analysis(self, period: Literal['4h', '8h']):
        """
        Save new funding rate in order to build the chart:
        - Set an analysis if funding rate was less than -0.5, otherwise it'll save the new funding rate.
        - Function executed every 8 or 4 hours, depending on the period.
        """
        period_value = int(period[:-1])
        exec_time = int(self.get_last_period_funding_rate(period_value).timestamp() * 1000)

        # Fetch cryptos based on the period
        cryptos = (self.redis_service.get_cryptos_by_fr_expiration_optimized('4h') 
                if period == '4h' 
                else self.redis_service.get_list_cryptos())

        logger.info(f"Cryptos to analyze for period {period}: {cryptos}")

        if cryptos.size == 0:
            logger.warning("No cryptos available for analysis.")
            return

        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(5)

        # Process cryptos in batches
        for i in range(0, len(cryptos), 40):
            batch = cryptos[i:i+40]
            logger.info(f"Processing batch {i // 40 + 1} with {len(batch)} cryptos.")

        for crypto in batch:
            await self.decide_analysis_crypto(crypto, exec_time, semaphore)
            await asyncio.sleep(0.3)  # Delay of 0.3 seconds between each request



            # Wait 1 minute before processing the next batch
            await asyncio.sleep(60)

        logger.info("Finished analyzing all cryptos.")


    async def decide_analysis_crypto(self, crypto: str, exec_time, semaphore):
        """
            The result of this analysis includes 2 things:
                 - Get current Funding Rate, current price index and current datetime
                 - Get the last FR and analysis what happened over the 8 hours, and save it
        """
        try:
            # Retrieve the last funding rate analysis
            fund_rate_ans, fund_period_ans, _ = self.redis_service.get_last_funding_rate(symbol=crypto)

        except Exception as e:
            logger.error(f"Failed to get last funding rate for {crypto}: {e}")
            return None

        async with semaphore:
            try:
                # Get the latest funding rate from Bitget
                fund_rate, fund_period = await self.bitget_service.get_last_contract_funding_rate(crypto, False)
            except Exception as e:
                logger.error(f"Failed to get current funding rate for {crypto}: {e}")
                return None

        async with semaphore:
            try:
                # Get Current price
                index_period_price = await self.bitget_service.get_price_of_period(symbol=crypto, period=exec_time)
            except Exception as e:
                logger.error(f"Failed to get price for {crypto}: {e}")
                return None

        # Create a new funding rate analysis entry
        current_analysis = {
            "period": fund_period,
            "funding_rate_value": float(fund_rate),
            "index_period_price": index_period_price,
            "analysis": {}
        }
        logger.info(f"Current analysis for {crypto}: {current_analysis}")

        # Add the new funding rate entry to Redis
        if fund_rate > -0.5:
            current_analysis["key_moment"] = False
            self.redis_service.add_funding_rate_analysis(symbol=crypto, funding_rate_analysis=current_analysis)
            logger.info(f"Added new funding rate entry for {crypto}: {current_analysis}")
            return None 
        else:
            current_analysis["key_moment"] = True

        # Conditional Analysis: If previous funding rate <= -0.5, add analysis to pre-last entry
        if fund_rate_ans is not None and float(fund_rate_ans) <= -0.5 and fund_period_ans:
            logger.info(f"Funding rate <= -0.5 for {crypto}. Adding analysis to previous entry.")
            try:
                analysis_chart = FundingRateChart(symbol=crypto)
                last_analysis = analysis_chart.set_analysis(period=int(fund_period_ans))
            except Exception as e:
                logger.error(f"Failed to generate analysis for {crypto}: {e}")
                return None

            # Prepare the updated analysis data
            analysis_data = last_analysis

            # Use the correct method
            try:
                self.redis_service.set_last_analysis(symbol=crypto, analysis_data=analysis_data)
                logger.info(f"Added analysis to pre-last funding rate for {crypto}")
            except Exception as e:
                logger.error(f"Failed to add analysis to pre-last funding rate for {crypto}: {e}")
                return None

                

    async def get_crypto_logo(self, symbol: str) -> Tuple[str, str, str]:
        """Fetches the crypto logo, name, and description from CoinMarketCap API."""
        if symbol.lower().endswith('usdt'):
            symbol = symbol[:-4]

        api_url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/info?symbol={symbol}"
        headers = {
            "X-CMC_PRO_API_KEY": COINMARKETCAP_APIKEY,
            "Accept": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url=api_url, headers=headers) as response:
                if response.status == 200:
                    api_response = await response.json()

                    try:
                        data = api_response['data'][symbol]
                        logo_url = data['logo']
                        name = data['name']
                        description = data['description']
                        return logo_url, name, description
                    except KeyError:
                        error_msg = f"Missing expected data for symbol: {symbol} in API response."
                        logger.error(error_msg)
                        raise KeyError(error_msg)
                else:
                    text_response = await response.text()
                    error_msg = f"API request failed for symbol: {symbol}. Status: {response.status}, Response: {text_response}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

    def get_next_funding_rate(self, delay: Literal[8,4], ans = False):
        # Define next execution times
        execution_times = [time(hour, 0) for hour in range(2, 23, delay)]

        timezone = pytz.timezone(self.timezone)
        now_datetime = datetime.now(timezone)
        now_time = now_datetime.time()

        sorted_times = sorted(t for t in execution_times if t > now_time)
        if sorted_times:
            next_time_of_day = sorted_times[1] if ans and len(sorted_times) > 1 else sorted_times[0]
        else:
            next_time_of_day = execution_times[0]

        # Combine current date with next_time_of_day
        naive_datetime = datetime.combine(now_datetime.date(), next_time_of_day)

        # Localize the naive datetime to the specified timezone
        next_execution_datetime = timezone.localize(naive_datetime)

        # If the scheduled time is in the past, schedule for the next day
        if next_execution_datetime <= now_datetime:
            next_execution_datetime += timedelta(days=1)

        return next_execution_datetime

    def get_last_period_funding_rate(self, delay: Literal[8, 4], ans=False):
        # Define previous execution times
        execution_times = [time(hour, 0) for hour in range(2, 23, delay)]

        timezone = pytz.timezone(self.timezone)
        now_datetime = datetime.now(timezone)
        now_time = now_datetime.time()

        sorted_times = sorted(t for t in execution_times if t < now_time)
        if sorted_times:
            last_time_of_day = sorted_times[-2] if ans and len(sorted_times) > 1 else sorted_times[-1]
        else:
            last_time_of_day = execution_times[-1]

        # Combine current date with last_time_of_day
        naive_datetime = datetime.combine(now_datetime.date(), last_time_of_day)

        # Localize the naive datetime to the specified timezone
        last_execution_datetime = timezone.localize(naive_datetime)

        # If the scheduled time is in the future, schedule for the previous day
        if last_execution_datetime >= now_datetime:
            last_execution_datetime -= timedelta(days=1)

        return last_execution_datetime


async def main_testing():
    myown_service = MainServiceLayer()

    # Uncomment the following line to run crypto_rebase
    await myown_service.crypto_rebase()
    # print(myown_service.get_next_funding_rate(4, True))


    # await myown_service.schedule_set_analysis('8h')
    # await myown_service.decide_analysis_crypto('BIGTIMEUSDT')
    # await myown_service.get_crypto_logo("BTCUSDT")
    # print(myown_service.get_last_period_funding_rate(8))
    logger.info("***** Analysis Completed *****")


if __name__ == "__main__":
    asyncio.run(main_testing())