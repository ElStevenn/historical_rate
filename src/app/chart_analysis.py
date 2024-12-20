from datetime import datetime, timezone
from typing import Literal
import pandas as pd
import numpy as np
import asyncio

from src.app.crypto_data_service import CryptoDataService, Granularity

class FundingRateChart:
    
    def __init__(self, symbol):
        self.symbol = symbol
        self.bitget_service = CryptoDataService()
        self.df8h = None
        self.df10m = None
        self.dfdialy = None
        self.dfweekly = None

    async def set_analysis(self, period: int) -> dict:
        """
            period: when funding rate was over
            Call this function if funding rate was higer than 0.5
            Response:
            {
                "description": List[],  
                "period": str,
                "8h_variation": float,
                "10m_variation": float,
                "dialy_trend": Literal[ "bullish", "bearish", "neutral", "highly bullish", "highly bearish", "volatile", "sideways", "corrective", "strongly bullish", "strongly bearish" ],
                "weekly_trend": Literl[ "bullish", "bearish", "neutral", "highly bullish", "highly bearish", "volatile", "sideways", "corrective", "strongly bullish", "strongly bearish" ],
                "volatility_index": float
                "average_trading_volume": int
                "market_sentiment": Literal[ "positive", "negative", "neutral", "highly positive", "highly negative", "mixed", "uncertain", "fearful", "optimistic", "pessimistic", "bullish", "bearish" ]
            }
        """
        # Get all tasks as faster as possible
        tasks = [
            self.get_8h_variation(period),
            self.get_10m_variation(period),
            self.get_daily_trend(period),
            self.get_weekly_trends(period),
            self.get_average_trading_volume(period)
        ]

        h8_variation, h10m_variation, daily_trend, weekly_trend, average_trading_volume = await asyncio.gather(*tasks)

        # Since get_volatility_index depends on df10m, call it after get_10m_variation
        volatility_index = await self.get_volatility_index()

        # Assign results to instance variables for reuse
        self.h8_variation = h8_variation
        self.h10m_variation = h10m_variation
        self.daily_trend = daily_trend
        self.weekly_trend = weekly_trend
        self.volatility_index = volatility_index
        self.average_trading_volume = average_trading_volume

        # Now, call market_sentiment without redundant data fetching
        market_sentiment = await self.market_sentiment(period)

        result = {
            "description": [],
            "period": datetime.fromtimestamp(period / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
            "8h_variation": h8_variation,
            "10m_variation": h10m_variation,
            "daily_trend": daily_trend,
            "weekly_trend": weekly_trend,
            "volatility_index": volatility_index,
            "average_trading_volume": average_trading_volume,
            "market_sentiment": market_sentiment
        }

        # Check for NaN or Inf values in result
        for key, value in result.items():
            if isinstance(value, float) and (pd.isna(value) or np.isinf(value)):
                print(f"Warning: {key} has invalid value ({value}). Replacing with None.")
                result[key] = None

        return result

    async def get_8h_variation(self, period: int):
        """Get variation since funding rate was up until 8 hours later"""
        # Get Candlestick data
        granularity = '1H'
        end_time = period + 8 * 60 * 60 * 1000
        candle_stick_data = await self.bitget_service.get_candlestick_chart(self.symbol, granularity, start_time=period, end_time=end_time)

        if not candle_stick_data.any():
            candle_stick_data = await self.bitget_service.get_candlestick_chart(self.symbol, '4H', start_time=period, end_time=end_time)
            if not candle_stick_data.any():
                raise Exception("The chart is not avariable, so i think i shouldn't be possible to access")
        
        self.df8h = pd.DataFrame(candle_stick_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'notional'])

        # Calculate variation
        start_price = self.df8h['open'].iloc[0]
        regression = self.df8h['low'].min()
        end_price = self.df8h['close'].iloc[-1]
        
        volatility = ((start_price - end_price) / end_price) * 100    
        regg = ((start_price - regression) / regression) * 100

        return volatility

    async def get_10m_variation(self, period: int):
        granularity = '1m'
        end_time = period + 10 * 60 * 1000
        candle_stick_data = await self.bitget_service.get_candlestick_chart(
            self.symbol, granularity, start_time=period, end_time=end_time
        )

        if not candle_stick_data.any():
            raise Exception("The chart is not available.")

        self.df10m = pd.DataFrame(candle_stick_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'notional'
        ])

        # Ensure numeric data types
        self.df10m['close'] = pd.to_numeric(self.df10m['close'], errors='coerce')

        # Drop rows with NaN values
        self.df10m.dropna(subset=['close'], inplace=True)

        # Check if there are enough data points
        if len(self.df10m) < 2:
            print("Not enough data points in df10m.")
            return 0.0
            
        # Calculate variation
        start_price = self.df10m['open'].iloc[0]
        # end_price = df['close'].iloc[-1]
        lowest_price = self.df10m['low'].min()

        volatility = ((start_price - lowest_price) / lowest_price) 
        return volatility


    async def get_daily_trend(self, period: int) -> Literal[
        "bullish", "bearish", "neutral", "highly bullish", "highly bearish",
        "volatile", "sideways", "corrective", "strongly bullish", "strongly bearish"
    ]:
        """
        Analyze the daily trend based on candlestick data and return a trend descriptor.
        """
        # Correctly define the time range for one day
        end_time = period
        start_time = period - 24 * 60 * 60 * 1000 

        # Fetch candlestick data for the day at 15-minute intervals
        granularity = '15m'
        candle_stick_data = await self.bitget_service.get_candlestick_chart(
            self.symbol, granularity, start_time=start_time, end_time=end_time
        )

        if not candle_stick_data.any():
            print("No data fetched. Please check the time range and data availability.")
            return "neutral"

        # Create DataFrame
        self.dfdaily = pd.DataFrame(candle_stick_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'notional'
        ])

        # Ensure numeric data types
        for col in ['open', 'high', 'low', 'close', 'volume', 'notional']:
            self.dfdaily[col] = pd.to_numeric(self.dfdaily[col], errors='coerce')

        # Drop rows with NaN values
        self.dfdaily.dropna(subset=['close'], inplace=True)

        # Convert timestamp to datetime
        self.dfdaily['datetime'] = pd.to_datetime(self.dfdaily['timestamp'], unit='ms')

        # Sort the DataFrame by datetime in ascending order
        self.dfdaily.sort_values('datetime', inplace=True)
        self.dfdaily.reset_index(drop=True, inplace=True)

        # Verify data integrity
        if len(self.dfdaily) == 0:
            print("No data available after cleaning. Cannot proceed with analysis.")
            return "neutral"

        # Calculate price changes
        self.dfdaily['price_change'] = self.dfdaily['close'].diff()
        self.dfdaily['price_change_pct'] = self.dfdaily['close'].pct_change() * 100

        # Calculate moving averages with shorter periods
        self.dfdaily['ma5'] = self.dfdaily['close'].rolling(window=5).mean()
        self.dfdaily['ma15'] = self.dfdaily['close'].rolling(window=15).mean()

        # Calculate volatility (standard deviation of price changes)
        volatility = self.dfdaily['price_change_pct'].std()

        # Determine trend based on moving averages
        latest_close = self.dfdaily['close'].iloc[-1]
        latest_ma5 = self.dfdaily['ma5'].iloc[-1]
        latest_ma15 = self.dfdaily['ma15'].iloc[-1]

        # Ensure moving averages are valid
        if pd.isna(latest_ma5) or pd.isna(latest_ma15):
            print("Moving averages are NaN. Not enough data points.")
            return "neutral"

        # Initialize trend
        trend = "neutral"

        # Define thresholds
        volatility_threshold = 2.0  # Adjusted volatility threshold

        # Analyze trend
        if latest_close < latest_ma5 and latest_ma5 < latest_ma15:
            trend = "strongly bearish"
        elif latest_close > latest_ma5 and latest_ma5 > latest_ma15:
            trend = "strongly bullish"
        elif latest_close < latest_ma5:
            trend = "bearish"
        elif latest_close > latest_ma5:
            trend = "bullish"
        else:
            trend = "neutral"

        # Adjust sideways movement detection
        if abs(latest_ma5 - latest_ma15) / latest_ma15 < 0.003:
            trend = "sideways"

        # Adjust for volatility without overriding the trend completely
        if volatility > volatility_threshold:
            if "bearish" in trend:
                trend = "volatile bearish"
            elif "bullish" in trend:
                trend = "volatile bullish"
            else:
                trend = "volatile"

        # Return the determined trend
        return trend




    async def get_weekly_trends(self, period: int) -> Literal[
            "bullish", "bearish", "neutral", "highly bullish", "highly bearish",
            "volatile", "sideways", "corrective", "strongly bullish", "strongly bearish"
        ]:
            """
            Analyze the weekly trend based on candlestick data and return a trend descriptor.
            """
            # Define the time range for one week (7 days)
            start_time = period
            end_time = period + 7 * 24 * 60 * 60 * 1000  # Add 7 days in milliseconds

            # Fetch candlestick data for the week at hourly intervals
            granularity = '1H'  # 1-hour intervals
            candle_stick_data = await self.bitget_service.get_candlestick_chart(
                self.symbol, granularity, start_time=start_time, end_time=end_time
            )

            if not candle_stick_data.any():
                raise Exception("The chart data is not available for the specified period.")

            # Create DataFrame
            self.dfweekly = pd.DataFrame(candle_stick_data, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume', 'notional'
            ])

            # Ensure numeric data types
            for col in ['open', 'high', 'low', 'close', 'volume', 'notional']:
                self.dfweekly[col] = pd.to_numeric(self.dfweekly[col])

            # Convert timestamp to datetime
            self.dfweekly['datetime'] = pd.to_datetime(self.dfweekly['timestamp'], unit='ms')

            # Calculate price changes
            self.dfweekly['price_change'] = self.dfweekly['close'].diff()
            self.dfweekly['price_change_pct'] = self.dfweekly['close'].pct_change() * 100

            # Calculate moving averages
            self.dfweekly['ma20'] = self.dfweekly['close'].rolling(window=20).mean()
            self.dfweekly['ma50'] = self.dfweekly['close'].rolling(window=50).mean()

            # Calculate volatility (standard deviation of price changes)
            volatility = self.dfweekly['price_change_pct'].std()

            # Determine trend based on moving averages
            latest_close = self.dfweekly['close'].iloc[-1]
            latest_ma20 = self.dfweekly['ma20'].iloc[-1]
            latest_ma50 = self.dfweekly['ma50'].iloc[-1]

            # Initialize trend
            trend = "neutral"

            # Define thresholds
            volatility_threshold = 2.0  # Adjust based on asset volatility
            price_change_threshold = 5.0  # Percentage change threshold for strong trends

            # Calculate total percentage change over the week
            weekly_change_pct = ((latest_close - self.dfweekly['open'].iloc[0]) / self.dfweekly['open'].iloc[0]) * 100

            # Analyze trend based on moving averages and price changes
            if latest_close > latest_ma20 > latest_ma50:
                if weekly_change_pct > price_change_threshold:
                    trend = "strongly bullish"
                else:
                    trend = "bullish"
            elif latest_close < latest_ma20 < latest_ma50:
                if weekly_change_pct < -price_change_threshold:
                    trend = "strongly bearish"
                else:
                    trend = "bearish"
            elif abs(weekly_change_pct) < 1.0:
                trend = "neutral"
            else:
                # Check for corrective or sideways movement
                if abs(latest_ma20 - latest_ma50) / latest_ma50 < 0.01:
                    trend = "sideways"
                elif weekly_change_pct > 0:
                    trend = "corrective"
                else:
                    trend = "volatile"

            # Adjust for high volatility
            if volatility > volatility_threshold:
                trend = "volatile"

            # Return the determined trend
            return trend

    async def get_volatility_index(self) -> float:
        """
        Calculate the volatility index based on the closing prices in the DataFrame.
        Returns:
            volatility_index (float): The volatility index as a percentage.
        """
        # Ensure that the DataFrame is available
        if self.df10m is None:
            raise ValueError("DataFrame is empty. Please fetch data before calculating volatility.")

        # Ensure numeric data types
        self.df10m['close'] = pd.to_numeric(self.df10m['close'], errors='coerce')

        # Drop any rows with NaN values in 'close'
        self.df10m = self.df10m.dropna(subset=['close'])

        # Remove non-positive prices
        self.df10m = self.df10m[self.df10m['close'] > 0]

        # Calculate log returns
        self.df10m['log_return'] = np.log(self.df10m['close'] / self.df10m['close'].shift(1))

        # Drop the first row which will have NaN log_return
        self.df10m = self.df10m.dropna(subset=['log_return'])

        # Ensure there are enough data points
        if len(self.df10m['log_return']) < 2:
            print("Not enough data points to calculate volatility.")
            return None  # or return 0.0

        # Calculate the standard deviation of log returns
        volatility = self.df10m['log_return'].std()

        # Annualize the volatility
        # For 1-minute data, adjust the periods per year accordingly
        periods_per_year = 252 * (1440)  # 1,440 minutes per day

        annualized_volatility = volatility * np.sqrt(periods_per_year)

        # Convert volatility to percentage
        volatility_index = annualized_volatility * 100

        # Handle NaN or infinite values
        if pd.isna(volatility_index) or np.isinf(volatility_index):
            print("Calculated volatility index is invalid.")
            return None  # or set to 0.0

        return volatility_index

    async def get_average_trading_volume(self, period: int) -> float:
        """
        Calculate the average trading volume over the specified period.

        Args:
            period (int): The starting timestamp in milliseconds.

        Returns:
            float: The average trading volume.
        """
        # Define the time range for one day (24 hours)
        start_time = period
        end_time = period + 24 * 60 * 60 * 1000  # Add 24 hours in milliseconds

        # Fetch candlestick data for the day at 1-hour intervals
        granularity = '1H'
        candle_stick_data = await self.bitget_service.get_candlestick_chart(
            self.symbol, granularity, start_time=start_time, end_time=end_time
        )

        if not candle_stick_data.any():
            raise Exception("The chart data is not available for the specified period.")

        # Create DataFrame
        df_volume = pd.DataFrame(candle_stick_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'notional'
        ])

        # Ensure numeric data types
        df_volume['volume'] = pd.to_numeric(df_volume['volume'], errors='coerce')

        # Drop any rows with NaN values in 'volume'
        df_volume = df_volume.dropna(subset=['volume'])

        # Calculate average trading volume
        average_volume = df_volume['volume'].mean()

        # Store the DataFrame for potential future use
        self.dfdaily = df_volume

        return average_volume

    async def market_sentiment(self, period: int) -> Literal[
        "positive", "negative", "neutral", "highly positive", "highly negative",
        "mixed", "uncertain", "fearful", "optimistic", "pessimistic", "bullish", "bearish"
    ]:
        """
        Determine the market sentiment based on various market indicators.

        Args:
            period (int): The starting timestamp in milliseconds.

        Returns:
            str: The market sentiment descriptor.
        """
        # Fetch necessary data if not already fetched
        if self.df8h is None or self.df10m is None or self.dfdaily is None or self.dfweekly is None:
            raise ValueError("Data not available. Please run the necessary methods before calling market_sentiment().")

 

        # Get trends and volatility
        daily_trend = self.daily_trend 
        weekly_trend = self.weekly_trend
        volatility_index = self.volatility_index  
        average_volume = self.average_trading_volume 

        # Ensure volatility is valid
        if volatility_index is None:
            volatility_index = 0.0

        # 3. Volatility Analysis
        if volatility_index > 50:
            volatility_sentiment = "uncertain"
        elif volatility_index < 10:
            volatility_sentiment = "stable"
        else:
            volatility_sentiment = "moderate"

        # Recent price changes
        h8_variation = await self.get_8h_variation(period)
        h10m_variation = await self.get_10m_variation(period)

        # Analyze funding rates (assuming you have a method to get the funding rate)
        # For this example, let's assume a placeholder value
        funding_rate = await self.get_funding_rate(period)  # You need to implement this method

        # Initialize sentiment
        sentiment = "neutral"

        # Analyze factors

        # 1. Funding Rate Analysis
        if funding_rate > 0.1:
            funding_sentiment = "bearish"
        elif funding_rate < -0.1:
            funding_sentiment = "bullish"
        else:
            funding_sentiment = "neutral"

        # 2. Price Trend Analysis
        if daily_trend in ["strongly bullish", "highly bullish", "bullish"]:
            price_sentiment = "bullish"
        elif daily_trend in ["strongly bearish", "highly bearish", "bearish"]:
            price_sentiment = "bearish"
        else:
            price_sentiment = "neutral"

        # 3. Volatility Analysis
        if volatility_index > 50:
            volatility_sentiment = "uncertain"
        elif volatility_index < 10:
            volatility_sentiment = "stable"
        else:
            volatility_sentiment = "moderate"

        # 4. Volume Analysis
        # Let's compare the current volume to the average volume over the last week
        weekly_average_volume = await self.get_average_trading_volume_weekly(period)
        volume_change = ((average_volume - weekly_average_volume) / weekly_average_volume) * 100

        if volume_change > 20:
            volume_sentiment = "optimistic"
        elif volume_change < -20:
            volume_sentiment = "pessimistic"
        else:
            volume_sentiment = "neutral"

        # Aggregate sentiments
        sentiments = [funding_sentiment, price_sentiment, volatility_sentiment, volume_sentiment]

        # Determine final sentiment
        bullish_count = sentiments.count("bullish") + sentiments.count("optimistic") + sentiments.count("positive")
        bearish_count = sentiments.count("bearish") + sentiments.count("pessimistic") + sentiments.count("negative")

        if bullish_count > bearish_count:
            if bullish_count >= 3:
                sentiment = "highly positive"
            else:
                sentiment = "positive"
        elif bearish_count > bullish_count:
            if bearish_count >= 3:
                sentiment = "highly negative"
            else:
                sentiment = "negative"
        else:
            sentiment = "mixed"

        # Adjust for volatility
        if volatility_sentiment == "uncertain":
            sentiment = "uncertain"

        # Handle specific cases
        if sentiment == "positive" and price_sentiment == "bullish":
            sentiment = "bullish"
        elif sentiment == "negative" and price_sentiment == "bearish":
            sentiment = "bearish"

        # Return the determined sentiment
        return sentiment

    async def get_funding_rate(self, period: int) -> float:
        """
        Placeholder method to get the funding rate at a given period.
        You need to implement this method based on your data source.
        """
        # For the purpose of this example, let's return a dummy value
        return 0.01  # Positive funding rate indicates bullish sentiment

    async def get_average_trading_volume_weekly(self, period: int) -> float:
        """
        Calculate the average trading volume over the last week.

        Args:
            period (int): The starting timestamp in milliseconds.

        Returns:
            float: The average trading volume over the week.
        """
        # Define the time range for one week (7 days)
        start_time = period - 7 * 24 * 60 * 60 * 1000 
        end_time = period

        # Fetch candlestick data for the week at 4-hour intervals
        granularity = '4H'
        candle_stick_data = await self.bitget_service.get_candlestick_chart(
            self.symbol, granularity, start_time=start_time, end_time=end_time
        )

        if not candle_stick_data.any():
            raise Exception("The chart data is not available for the specified period.")

        # Create DataFrame
        df_volume_weekly = pd.DataFrame(candle_stick_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'notional'
        ])

        # Ensure numeric data types
        df_volume_weekly['volume'] = pd.to_numeric(df_volume_weekly['volume'], errors='coerce')

        # Drop any rows with NaN values in 'volume'
        df_volume_weekly = df_volume_weekly.dropna(subset=['volume'])

        # Calculate average trading volume
        average_volume_weekly = df_volume_weekly['volume'].mean()

        # Store the DataFrame for potential future use
        self.dfweekly = df_volume_weekly

        return average_volume_weekly

    async def set_description(self, regression_8h, volatility_10m, dialy_trend, weekly_tend):
        pass

    def calculate_rsi(self, prices, period=14):
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        average_gain = gain.rolling(window=period).mean()
        average_loss = loss.rolling(window=period).mean()
        rs = average_gain / average_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    

async def main_testing():
    chart_analysis = FundingRateChart("DOGUSDT")
    period = int(datetime(2024, 9, 29).timestamp() * 1000)
    
    # Fetch data required for df10m
    analysis = await chart_analysis.set_analysis(period)

    print(analysis)

if __name__ == "__main__":
    asyncio.run(main_testing())


if __name__ == "__main__":
    asyncio.run(main_testing())