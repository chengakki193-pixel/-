from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import akshare as ak
import pandas as pd
import time
import requests

app = FastAPI(title="股市分析助手 V4 - 数据引擎 API")

# 配置 CORS，允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= 全局缓存区 (保护 Render 内存与接口限流) =================
# 缓存全市场数据，每 10 分钟只允许请求一次
CACHE = {
    "market_data": None,
    "last_update": 0,
    "cache_duration": 600  # 10 分钟缓存
}

def get_cached_market_data():
    """获取全市场快照数据（带缓存机制）"""
    current_time = time.time()
    if CACHE["market_data"] is None or (current_time - CACHE["last_update"] > CACHE["cache_duration"]):
        try:
            # 获取东财全市场 A 股实时快照
            df = ak.stock_zh_a_spot_em()
            CACHE["market_data"] = df
            CACHE["last_update"] = current_time
        except Exception as e:
            if CACHE["market_data"] is None:
                raise HTTPException(status_code=500, detail="数据源获取失败")
    return CACHE["market_data"]

# ================= 核心接口 1：个股深度行情 (包含 MACD 与 资金流) =================

@app.get("/api/stock/price")
def get_stock_price(code: str, detail: bool = False):
    """
    获取个股深度行情。
    当 detail=true 时，返回 MACD (DIF, DEA, Hist) 和 大单资金流向。
    """
    result = {"code": code}
    
    if detail:
        try:
            # 1. 获取最近 100 天数据用于精准计算最新的 MACD 
            # (Pandas 计算 EMA 需要前置数据平滑)
            hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
            if not hist_df.empty:
                close_prices = hist_df['收盘']
                # 使用纯 Pandas 计算 MACD，抛弃臃肿的 TA-Lib
                ema12 = close_prices.ewm(span=12, adjust=False).mean()
                ema26 = close_prices.ewm(span=26, adjust=False).mean()
                dif = ema12 - ema26
                dea = dif.ewm(span=9, adjust=False).mean()
                macd_hist = (dif - dea) * 2
                
                # 提取最后一个交易日的数值
                result["macd_dif"] = round(float(dif.iloc[-1]), 3)
                result["macd_dea"] = round(float(dea.iloc[-1]), 3)
                result["macd_hist"] = round(float(macd_hist.iloc[-1]), 3)
            else:
                result["macd_dif"] = result["macd_dea"] = result["macd_hist"] = 0.0

            # 2. 获取资金流向 (超大单/大单)
            # 调用同花顺或东财的资金流接口（此处做容错处理，防止接口变动）
            try:
                fund_df = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith("6") else "sz")
                if not fund_df.empty:
                    # 假设返回字段包含超大单和大单净流入
                    result["super_in"] = float(fund_df.iloc[-1].get("超大单净流入-净额", 0.0))
                    result["large_in"] = float(fund_df.iloc[-1].get("大单净流入-净额", 0.0))
                else:
                    result["super_in"] = result["large_in"] = 0.0
            except:
                result["super_in"] = result["large_in"] = 0.0
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"深度指标计算失败: {str(e)}")
            
    return result

# ================= 核心接口 2：RPS 强势股榜单 (使用缓存防止超时) =================

@app.get("/api/rps/top/{period}")
def get_rps_top(period: int, limit: int = 100):
    """
    获取 RPS 涨幅榜。基于 Render 性能限制，我们利用全市场快照的 60日/今年涨幅 来模拟 50/120/250 RPS。
    """
    df = get_cached_market_data()
    
    # 根据周期选择对应的涨跌幅列进行排序
    if period <= 60:
        sort_col = "60日涨跌幅"
    else:
        sort_col = "年初至今涨跌幅" # 模拟 120/250 日
        
    if sort_col not in df.columns:
        return {"error": f"数据源缺失 {sort_col} 字段"}

    # 过滤掉 NaN 数据并排序
    df_sorted = df.dropna(subset=[sort_col]).sort_values(by=sort_col, ascending=False)
    
    # 截取前 limit 名
    top_stocks = df_sorted.head(limit)
    
    results = []
    for _, row in top_stocks.iterrows():
        results.append({
            "code": str(row["代码"]),
            "name": str(row["名称"]),
            "increase_rate": float(row[sort_col])
        })
        
    return {"period": period, "count": len(results), "top_stocks": results}

# ================= 健康检查接口 (用于 Render 唤醒) =================
@app.get("/api/market/stats")
def market_stats():
    """用于测试 API 是否存活"""
    return {"status": "ok", "message": "股市分析助手 V4 数据引擎已上线"}
