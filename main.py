# main.py
# Author: Pau Mateu
# Developer email: paumat17@gmail.com

from fastapi import FastAPI, HTTPException, Request, WebSocket, Query, WebSocketDisconnect, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from contextlib import asynccontextmanager
from typing import Optional, List
from datetime import datetime, timezone, timedelta
import asyncio, logging, pytz

from app.bitget_layer import BitgetService
from app.redis_layer import RedisService
from app.schedule_layer import ScheduleLayer
from app.chart_analysis import FundingRateChart
from app.historcal_funding_rate import MainServiceLayer
from app.schemas import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize services
async_scheduler = ScheduleLayer("Europe/Amsterdam")
redis_memory = RedisService()
main_services = MainServiceLayer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the scheduler
    async_scheduler.scheduler.start()
    logger.info("Scheduler started.")

    # Schedule the daily crypto_rebase job at 9:00 AM Spanish time
    async_scheduler.schedule_daily_job(9, 0, main_services.crypto_rebase)

    # Calculate the next 2 AM in Europe/Amsterdam timezone and set the params (exec_time)
    next_run_time = main_services.get_next_funding_rate(8)
    logger.info(f"Next run time for schedule_set_analysis: {next_run_time}")
    params1 = {"period": '8h'} 


    # Schedule the set_analysis job every 8 hours starting at next_run_time
    async_scheduler.schedule_interval_job(
        hours=8, 
        function_to_call=main_services.schedule_set_analysis, 
        start_date=next_run_time,
        **params1
    )
 
    try:
        yield
    finally:
        # Shutdown the scheduler
        async_scheduler.scheduler.shutdown()
        logger.info("Scheduler shut down.")

app = FastAPI(
    title="Historical Funding Rate API",
    summary="Cruccial piece of Pau's Funding rate sercice, this API manages ",
    lifespan=lifespan
)

origins = [
    "http://0.0.0.0:80",
    "http://localhost:8080",
    "http://3.143.209.3/",
    
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/get_historical_funding_rate/{symbol}", 
    description="### Get Historical Funding Rates\n\n From a given symbol returns a list with the historical funding rate and if there was a controversial period, provides an analysis about what happened with the price", 
    tags=["Base Funding Rate"])
async def get_historical_funding_rate(
    symbol: str = Path(..., description="Symbol to be searched"),  
    limit: Optional[int] = Query(50, ge=10, lt=500, description="Number of given data") 
):
    # Get Historical Funding Rate Analysis
    historical_fundin_rate_analysis = redis_memory.get_funding_rate_history(symbol, limit=limit)
    final_data = historical_fundin_rate_analysis.get('data', None)

    if final_data:
        # Parse timestamp to readable value
        final_result = [
            {
                "funding_rate_value": data["funding_rate_value"],
                "period": datetime.fromtimestamp(int(data["period"]) / 1000, pytz.timezone(async_scheduler.timezone)),
                "period_index_price": data["index_period_price"],
                "analysis": data["analysis"]
            }
            for data in final_data
        ]

        return final_result
    else:
        return []

@app.get("/get_today_analysis/{symbol}", description="### Get today analysis from a given crypto\n\n", tags=["Crypto Analysis"])
async def get_today_analysis(symbol: str):
    chart_analysis = FundingRateChart(symbol)
    period = int(datetime.now(timezone.utc).timestamp() * 1000)

    # Get Analysis
    analysis = await chart_analysis.set_analysis(period)

    return analysis


@app.get("/get_detail_event/{symbol}", description="Get name and logo of the crypto", tags=["Crypto Search"],  response_model=Crypto)
async def get_detail_event(symbol: str):
    try:
        crypto_metadata = redis_memory.get_crypto_metadata(symbol)

        if symbol.lower().endswith('usdt'):
            symbol = symbol[:-4]

        next_execution_time = main_services.get_next_funding_rate(crypto_metadata["funding_rate_del"])

        return {
            "symbol": symbol, 
            "name": crypto_metadata["name"], 
            "image": crypto_metadata["picture_url"], 
            "description": crypto_metadata["description"], 
            "funding_rate_delay": '4h' if crypto_metadata["funding_rate_del"] == 4 else '8h', 
            "next_execution_time": next_execution_time.isoformat()
            }
    
    except TypeError:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")



@app.get(
    "/search",
    description= """
                ### Search for cryptocurrencies based on a query. If no query is provided, returns all cryptos sorted by 'id'.
                
                - **query**: The search string to filter cryptos by symbol or name.
                - **limit**: The maximum number of results to return (default: 50, max: 100).
                - **offset**: The number of results to skip for pagination (default: 0).
                """,
    tags=["Crypto Search"],
    response_model=List[CryptoSearch]
)
async def search_crypto(
    query: Optional[str] = Query(None, description="Search query for symbol or name"),
    limit: Optional[int] = Query(50, ge=1, le=100, description="Number of results to return"),
    offset: Optional[int] = Query(0, ge=0, description="Number of results to skip")
):
    
    # Fetch the queried data from Redis and set response
    queried_data = redis_memory.get_list_query(query=query, limit=limit, offset=offset)
    response = [{"id": cp['id'], "symbol": cp['symbol'], "name": cp['name'], "image": cp['picture_url']} for cp in queried_data]

    return response
 



@app.websocket("/search-crypto-ws")
async def websocket_search_crypto(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Receive query and limit from the client
            data = await websocket.receive_json()  
            query = data.get('query')
            offset = data.get('offset')
            limit = data.get('limit')

            # Get data from Redis (implement limit if needed)
            queried_data = redis_memory.get_list_query(query=query, limit=limit, offset=offset)
            response = [{"id": cp['id'], "symbol": cp['symbol'], "name": cp['name'], "image": cp['picture_url']} for cp in queried_data]

            # Send the queried data back to the WebSocket client
            await websocket.send_json(response)

    except WebSocketDisconnect:
        print("Client disconnected")

    except Exception as e:
        await websocket.close()
        print(f"Error: {e}")


@app.delete("/delete_all_analysis", description="### Administrative function\n\n - This function is used to clear all the **current analysis**\n\n - Doesn't include the crytpos", tags=["Administrative"])
async def delete_all_cryptos_analysis():
    response = redis_memory.delete_all_analysis()
        
    return response


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8080)