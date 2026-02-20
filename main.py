import time
from fastapi import FastAPI, HTTPException
import uvicorn
import akshare as ak
import pandas as pd

app = FastAPI(title="Stock Data Fetcher API", description="纯净股票数据搬运工")

def fetch_data_with_retry(fetch_func, *args, **kwargs):
    """
    通用容错处理：遇到接口报错或超时，自动执行 3 次重试，每次间隔 10 秒。
    防范服务器休眠或网络波动导致的采集失败。
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return fetch_func(*args, **kwargs)
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"抓取异常，10秒后进行第 {attempt + 2} 次重试 (错误信息: {e})")
                time.sleep(10)
            else:
                raise Exception(f"连续 {max_retries} 次拉取失败，请检查网络或目标接口。详细错误: {e}")

@app.get("/api/stock/{stock_code}")
def get_stock_data(stock_code: str):
    """
    API 接口：传入股票代码（如 600000 或 000001），返回结构化 JSON
    """
    try:
        # ---------------------------------------------------------
        # Step 1 & 3: 定位与捕获 (静态基础信息 + 当日实时盘口)
        # ---------------------------------------------------------
        def get_realtime_and_info():
            # 获取 A 股实时行情数据（包含基础信息）
            df = ak.stock_zh_a_spot_em() 
            # 过滤出目标股票
            stock_row = df[df['代码'] == stock_code]
            if stock_row.empty:
                return None
            return stock_row.iloc[0].to_dict()
            
        realtime_data = fetch_data_with_retry(get_realtime_and_info)
        
        if not realtime_data:
            raise HTTPException(status_code=404, detail=f"未找到代码为 {stock_code} 的股票数据")

        # 组装 info 静态字段 (市值转换为亿元)
        info_dict = {
            "code": stock_code,
            "name": realtime_data.get("名称", "未知"),
            "industry": realtime_data.get("板块名称", "暂无"), # 视接口具体返回字段而定
            "total_mv": round(realtime_data.get("总市值", 0) / 100000000, 2), 
            "pe_ttm": realtime_data.get("市盈率-动态", 0.0),
            "roe": realtime_data.get("净资产收益率", 0.0) 
        }
        
        # 组装 realtime 实时盘口与资金快照
        realtime_dict = {
            "current_price": realtime_data.get("最新价", 0.0),
            "volume_ratio": realtime_data.get("量比", 0.0),
            "turnover_rate": realtime_data.get("换手率", 0.0),
            "pct_change": realtime_data.get("涨跌幅", 0.0)
        }

        # ---------------------------------------------------------
        # Step 2: 拉取 (获取历史序列，前复权，最近 250 天)
        # ---------------------------------------------------------
        def get_history_k_data():
            # 强制 adjust="qfq" 获取前复权数据
            hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq")
            # 完整抓取最近 250 个交易日，如果上市不足 250 天则按实际最大天数输出
            return hist_df.tail(250)
            
        history_df = fetch_data_with_retry(get_history_k_data)
        
        history_list = []
        for _, row in history_df.iterrows():
            history_list.append({
                "date": str(row["日期"]),
                "open": round(float(row["开盘"]), 3),
                "high": round(float(row["最高"]), 3),
                "low": round(float(row["最低"]), 3),
                "close": round(float(row["收盘"]), 3),
                "volume": int(row["成交量"]), 
                "turnover": round(float(row["换手率"]), 4) # 筹码分布的关键线索
            })

        # ---------------------------------------------------------
        # Step 4: 组装输出
        # ---------------------------------------------------------
        result = {
            "info": info_dict,
            "history": history_list,
            "realtime": realtime_dict
        }

        # Step 5: FastAPI 会自动将 result 字典序列化为标准的 JSON 格式输出
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # 运行服务器，默认绑定 8000 端口
    uvicorn.run(app, host="0.0.0.0", port=8000)
