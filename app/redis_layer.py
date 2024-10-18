import socket
import redis
import uuid
import json
import time
import re
import numpy as np
from typing import List, Literal, Dict, TypedDict, Optional, Tuple
from fastapi import HTTPException
from datetime import datetime, timezone
from pprint import pprint

class FundingRateAnalysis(TypedDict, total=False):
    period: datetime
    funding_rate_value: float
    analysis: Optional[Dict]

class Analysis(TypedDict, total=False):
    description: List[str]
    eight_hour_variation: float
    ten_minute_variation: float
    one_minute_variation: float

class CryptoMetadata(TypedDict):
    id: int
    symbol: str
    name: str
    picture_url: str
    funding_rate_del: str
    description: str

class RedisService:
    def __init__(self) -> None:
        hostname = socket.gethostname()
        print("HOSTNAME! -> ", hostname)
        if hostname == 'mamadocomputer':
            # Local machine
            redis_host = 'localhost'
            port = 6378 
        elif hostname == 'scw-hungry-leavitt': 
            redis_host = 'localhost'
            port = 6379    
        else: 
            # Server deployment
            redis_host = 'redis_tasks'  
            port = 6379

        self._r = redis.Redis(host=redis_host, port=port, decode_responses=True)

    # ------------------- LIST_CRYPTO FUNCTIONS -------------------

    def get_list_cryptos(self) -> np.ndarray:
        """
        Retrieves the list of cryptocurrency symbols as a NumPy array.

        Returns:
            np.ndarray: Array of cryptocurrency symbols.
        """
        crypto_list = self._r.get("list_crypto")
        if crypto_list:
            try:
                crypto_list = json.loads(crypto_list)
                if isinstance(crypto_list, list):
                    return np.array(crypto_list)
            except json.JSONDecodeError:
                print("Error decoding 'list_crypto' from Redis.")
                return np.array([])
        return np.array([])

    def add_to_list_crypto(self, symbol: str):
        crypto_array = self.get_list_cryptos()
        crypto_list = crypto_array.tolist()  
        if symbol not in crypto_list:
            crypto_list.append(symbol)
            self._r.set("list_crypto", json.dumps(crypto_list))
        else:
            print(f"Symbol '{symbol}' already exists in 'list_crypto'.")


    def remove_from_list_crypto(self, symbol: str) -> None:
        crypto_array = self.get_list_cryptos()
        crypto_list = crypto_array.tolist() 
        if symbol in crypto_list:
            crypto_list.remove(symbol)
            self._r.set("list_crypto", json.dumps(crypto_list))
            print(f"Removed symbol '{symbol}' from 'list_crypto'.")
        else:
            print(f"Symbol '{symbol}' not found in 'list_crypto'.")

    # ------------------- CRYPTO_METADATA FUNCTIONS -------------------

    def add_crypto_metadata(self, symbol: str, name: str, picture_url: str, description: str, funding_rate_del: str) -> None:
        """
        Adds metadata for a new cryptocurrency. Also updates the list_crypto.
        """
        # Check if metadata already exists
        existing_metadata = self._r.hget("crypto_metadata", symbol)
        if existing_metadata:
            print(f"Metadata for symbol {symbol} already exists.")
            return

        # Generate a unique ID for the cryptocurrency
        crypto_id = self.add_crypto_offset()

        # Create metadata entry
        metadata_entry: CryptoMetadata = {
            "id": crypto_id,
            "symbol": symbol,
            "name": name,
            "picture_url": picture_url,
            "funding_rate_del": funding_rate_del,
            "description": description
        }

        # Save metadata to Redis
        self._r.hset("crypto_metadata", symbol, json.dumps(metadata_entry))

        # Add symbol to list_crypto
        self.add_to_list_crypto(symbol)

    def get_crypto_metadata(self, symbol: str) -> Optional[CryptoMetadata]:
        """
        Retrieves metadata for a given cryptocurrency symbol.
        """
        metadata_json = self._r.hget("crypto_metadata", symbol)
        if metadata_json:
            try:
                metadata = json.loads(metadata_json)
                return metadata
            except json.JSONDecodeError:
                print(f"Malformed metadata JSON for symbol: {symbol}")
        return None

    def update_crypto_metadata(self, symbol: str, updates: Dict) -> bool:
        """
        Updates metadata fields for a given cryptocurrency symbol.
        """
        metadata = self.get_crypto_metadata(symbol)
        if not metadata:
            print(f"No metadata found for symbol: {symbol}")
            return False

        metadata.update(updates)
        try:
            self._r.hset("crypto_metadata", symbol, json.dumps(metadata))
            print(f"Successfully updated metadata for symbol: {symbol}")
            return True
        except redis.RedisError as e:
            print(f"Redis error while updating metadata for symbol {symbol}: {e}")
            return False

    def delete_crypto_metadata(self, symbol: str) -> bool:
        """
        Deletes metadata for a given cryptocurrency symbol.
        """
        try:
            result = self._r.hdel("crypto_metadata", symbol)
            if result:
                print(f"Successfully deleted metadata for symbol: {symbol}")
                self.remove_from_list_crypto(symbol)
                self.delete_all_analysis_for_symbol(symbol)
                return True
            else:
                print(f"No metadata found for symbol: {symbol}")
                return False
        except redis.RedisError as e:
            print(f"Redis error while deleting metadata for symbol {symbol}: {e}")
            return False

    # ------------------- ALL_CRYPTO_ANALYSIS FUNCTIONS -------------------

    def add_funding_rate_analysis(self, symbol: str, funding_rate_analysis: FundingRateAnalysis) -> None:
        """
        Adds a new funding rate analysis entry for a given cryptocurrency symbol.
        """
        existing_data = self._r.hget("all_crypto_analysis", symbol)

        if not existing_data:
            new_data = {
                "symbol": symbol,
                "data": [funding_rate_analysis]
            }
        else:
            try:
                existing_data = json.loads(existing_data)
                if "data" in existing_data and isinstance(existing_data["data"], list):
                    if len(existing_data["data"]) >= 500:
                        existing_data["data"].pop(0)  # Maintain a maximum of 500 entries
                    existing_data["data"].append(funding_rate_analysis)
                else:
                    existing_data["data"] = [funding_rate_analysis]
                new_data = existing_data
            except json.JSONDecodeError:
                # If existing data is malformed, reset it
                new_data = {
                    "symbol": symbol,
                    "data": [funding_rate_analysis]
                }

        self._r.hset("all_crypto_analysis", symbol, json.dumps(new_data))

    def set_last_analysis(self, symbol: str, analysis_data: Dict) -> bool:
        """
        Adds analysis data to the pre-last funding rate entry for the given symbol.
        """
        crypto_data = self._r.hget("all_crypto_analysis", symbol)

        if not crypto_data:
            print(f"No existing analysis data found for symbol: {symbol}")
            return False

        try:
            crypto_data = json.loads(crypto_data)
        except json.JSONDecodeError:
            print(f"Malformed analysis JSON data for symbol: {symbol}")
            return False

        data_list = crypto_data.get("data")

        if not isinstance(data_list, list):
            print(f"'data' field is missing or not a list for symbol: {symbol}")
            return False

        if len(data_list) < 2:
            print(f"Not enough funding rate entries to add analysis for symbol: {symbol}")
            return False

        pre_last_entry = data_list[-2]

        if 'analysis' not in pre_last_entry or not isinstance(pre_last_entry['analysis'], dict):
            pre_last_entry['analysis'] = {}

        pre_last_entry['analysis'].update(analysis_data)

        try:
            self._r.hset("all_crypto_analysis", symbol, json.dumps(crypto_data))
            print(f"Successfully added analysis to pre-last funding rate for symbol: {symbol}")
            return True
        except redis.RedisError as e:
            print(f"Redis error while updating analysis for symbol {symbol}: {e}")
            return False

    def get_funding_rate_history(self, symbol: str, limit: Optional[int] = None) -> Optional[Dict]:
        """
        Retrieves the funding rate history for a given cryptocurrency symbol.
        """
        crypto_data = self._r.hget("all_crypto_analysis", symbol)
        if crypto_data:
            try:
                json_data = json.loads(crypto_data)
                if limit:
                    json_data["data"] = json_data["data"][-limit:]
                return json_data
            except json.JSONDecodeError:
                print(f"Malformed analysis JSON data for symbol: {symbol}")
                return {}
        return {}

    def get_last_funding_rate(self, symbol: str) -> Tuple[Optional[float], Optional[int], Optional[str]]:
        """
        Retrieves the last registered funding rate for the given symbol.
        """
        crypto_data = self._r.hget("all_crypto_analysis", symbol)
        
        if not crypto_data:
            return None, None, None
        
        try:
            crypto_data = json.loads(crypto_data)
        except json.JSONDecodeError:
            return None, None, None
        
        data_list = crypto_data.get("data")
        
        if not isinstance(data_list, list) or not data_list:
            return None, None, None
        
        last_entry = data_list[-1]
        
        funding_rate = last_entry.get("funding_rate_value")
        period_str = last_entry.get("period")
        
        funding_rate_value = float(funding_rate) if isinstance(funding_rate, (float, int)) else None
        period_timestamp = None
            
        if isinstance(period_str, str):
            try:
                period_dt = datetime.fromisoformat(period_str)
                period_timestamp = int(period_dt.replace(tzinfo=timezone.utc).timestamp())
            except ValueError:
                period_timestamp = None
        
        return funding_rate_value, period_timestamp

    def read_crypto_analysis(self, symbol: str, limit: int = 20) -> Tuple[List[Dict], Optional[str]]:
        """
        Retrieves the latest 'limit' number of analysis entries for a given symbol.
        """
        crypto_data = self._r.hget("all_crypto_analysis", symbol)
        
        if not crypto_data:
            return [], None
        
        try:
            crypto_data = json.loads(crypto_data)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in Redis for the specified symbol.")
        
        if "data" not in crypto_data or not isinstance(crypto_data["data"], list):
            raise ValueError("Invalid data structure in Redis for the specified symbol.")
        
        analysis_list = crypto_data["data"][-limit:]
        fr_expiration = crypto_data.get("fr_expiration")
        
        return analysis_list, fr_expiration

    def delete_all_analysis_for_symbol(self, symbol: str) -> None:
        """
        Deletes all analysis-related data for a specific cryptocurrency symbol.
        """
        self._r.hdel("all_crypto_analysis", symbol)

    def delete_all_analysis(self) -> str:
        """
        Deletes all analysis-related data from all cryptocurrency entries in the 'all_crypto_analysis' hash.
        """
        try:
            self._r.delete("all_crypto_analysis")
            return "All analysis-related data has been successfully deleted from 'all_crypto_analysis'."
        except redis.RedisError as e:
            raise HTTPException(status_code=400, detail=f"An error occurred while deleting analysis data: {e}")

    # ------------------- UTILITY FUNCTIONS -------------------

    def add_crypto_offset(self) -> int:
        """
        Increments and returns the crypto count.
        """
        try:
            count = self._r.incr("crypto_count")
            return count
        except redis.RedisError as e:
            print(f"Redis error while incrementing crypto_count: {e}")
            return 0

    def delete_everything(self) -> None:
        """
        Flushes all data from Redis. Use with caution.
        """
        self._r.flushall()

    # ------------------- QUERY FUNCTIONS -------------------

    def get_all_cryptos(self) -> List[Dict]:
        """
            Get all cryptos incuding all its metadata
        """
        all_symbols = self.get_list_cryptos()
        result = []
        for symbol in all_symbols:
            metadata = self.get_crypto_metadata(symbol)
            if metadata:
                result.append(metadata)
        return result

    def get_list_query(self, query: Optional[str] = None, limit: Optional[int] = None, offset: Optional[int] = 0) -> List[Dict]:
        """
        Retrieves a list of cryptocurrencies based on the provided query with pagination.
        """
        all_cryptos = self.get_all_cryptos()
        
        if not query:
            sorted_all = sorted(
                all_cryptos, 
                key=lambda x: x.get('id', float('inf')) if isinstance(x.get('id'), int) else float('inf')
            )
        else:
            query_lower = query.lower()
            starts_with = []
            contains = []
            
            for crypto in all_cryptos:
                symbol = crypto.get('symbol', '').lower()
                name = crypto.get('name', '').lower()
                
                if symbol.startswith(query_lower) or name.startswith(query_lower):
                    starts_with.append(crypto)
                elif query_lower in symbol or query_lower in name:
                    contains.append(crypto)
            
            starts_with_sorted = sorted(
                starts_with, 
                key=lambda x: x.get('id', float('inf')) if isinstance(x.get('id'), int) else float('inf')
            )
            contains_sorted = sorted(
                contains, 
                key=lambda x: x.get('id', float('inf')) if isinstance(x.get('id'), int) else float('inf')
            )
            
            sorted_all = starts_with_sorted + contains_sorted
        
        if offset:
            sorted_all = sorted_all[offset:]
        if limit:
            sorted_all = sorted_all[:limit]
        
        return sorted_all

    def get_cryptos_by_fr_expiration_optimized(self, expirations: List[str] = ["4h", "8h"]) -> List[Dict]:
        """
        Retrieves cryptocurrencies filtered by funding rate expiration times.
        """
        matching_cryptos = []
        symbols = set()

        for exp in expirations:
            symbols.update(self._r.smembers(f"fr_expiration:{exp}"))

        if symbols:
            pipeline = self._r.pipeline()
            for symbol in symbols:
                pipeline.hget("all_crypto_analysis", symbol)
            crypto_data_json_list = pipeline.execute()

            for crypto_data_json in crypto_data_json_list:
                if crypto_data_json:
                    try:
                        crypto_data = json.loads(crypto_data_json)
                        matching_cryptos.append(crypto_data)
                    except json.JSONDecodeError:
                        continue

        return matching_cryptos

    # ------------------- DELETION FUNCTIONS -------------------

    def delete_crypto(self, symbol: str) -> bool:
        """
        Deletes all data related to a specific cryptocurrency symbol.
        """
        try:
            self._r.hdel("crypto_metadata", symbol)
            self.delete_all_analysis_for_symbol(symbol)
            self.remove_from_list_crypto(symbol)
            print(f"Successfully deleted all data for symbol: {symbol}")
            return True
        except redis.RedisError as e:
            print(f"Redis error while deleting symbol {symbol}: {e}")
            return False


        

## TESTING & EXAMPLE OF REDIS ## 
if __name__ == "__main__":
    redis_service = RedisService()

    # Test data
    symbol = "DOGUSDT"
    whole_analysis_until_now = [
        {
            "id": str(uuid.uuid4()),
            "period": datetime.now(timezone.utc).isoformat()
            ,
            "funding_rate_value": 0.55,  # Funding rate > 0.5, includes analysis
            "analysis": {
                "description": [
                    "crypto went up by 5% in the next 8 hours",
                    "crypto dropped by 2% in the first 10 minutes",
                    "steady increase of 0.1% every minute after initial drop"
                ],
                "8h_variation": 5.0,
                "10m_variation": -2.0,
                "1m_variation": 0.1,
                "daily_trend": "bullish",
                "weekly_trend": "slightly bullish",
                "volatility_index": 1.5,
                "average_trading_volume": 1000000,
                "market_sentiment": "positive"
            }
        },
        {
            "id": str(uuid.uuid4()),
            "period": datetime.now(timezone.utc).isoformat(),
            "funding_rate_value": 0.45,
            "analysis": {}
        },
        {
            "id": str(uuid.uuid4()),
            "period": datetime.now(timezone.utc).isoformat(),
            "funding_rate_value": 0.6,  
            "analysis": {
                "description": [
                    "massive spike of 10% in 30 minutes due to external market factors",
                    "sharp correction of 3% within the next 4 hours",
                    "volatile movements due to speculation"
                ],
                "8h_variation": 7.0,
                "10m_variation": 2.5,
                "1m_variation": 1.0,
                "daily_trend": "highly volatile",
                "weekly_trend": "bullish",
                "volatility_index": 3.0,
                "average_trading_volume": 5000000,
                "market_sentiment": "highly speculative"
            }
        },
        {
            "id": str(uuid.uuid4()),
            "period": datetime.now(timezone.utc).isoformat(),
            "funding_rate_value": 0.3,  
            "analysis": {}
        },
        {
            "id": str(uuid.uuid4()),
            "period": datetime.now(timezone.utc).isoformat(),
            "funding_rate_value": 0.75,  
            "analysis": {
                "description": [
                    "steady increase of 0.5% per hour over 8 hours",
                    "positive sentiment due to upcoming major news announcement"
                ],
                "8h_variation": 4.0,
                "10m_variation": 0.5,
                "1m_variation": 0.05,
                "daily_trend": "bullish",
                "weekly_trend": "slightly bullish",
                "volatility_index": 1.0,
                "average_trading_volume": 2000000,
                "market_sentiment": "anticipatory positive"
            }
        }
    ]


    # Call the add_new_crypto method
    # print(redis_service.get_all_cryptos())
    # print(redis_service.get_crypto_metadata('BTCUSDT'))
    # redis_service.delete_everything()
    print(redis_service.get_funding_rate_history('BTCUSDT'))
    # print(redis_service.get_list_query("bit"))
    # print(redis_service.get_crypto_logo("BTCUSDT"))

    """
    new_period =     {
            "id": str(uuid.uuid4()),
            "period": datetime.now(timezone.utc).isoformat(),
            "funding_rate_value": 0.22,   
            "analysis": {}
        }
    
    redis_service.set_analysis_last_funding(symbol, new_period)
    """
    # crypto = redis_service.read_crypto_analysis(symbol)

    # print(redis_service.get_4h_cryptos())
    


    