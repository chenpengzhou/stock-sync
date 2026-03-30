#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare财务数据同步脚本 v1.1
- 同步内容：财务指标、利润表、资产负债表、现金流量表
- 分批并行：获取所有股票列表后分批并行请求
- 增量同步：按end_date去重
使用方法:
    python sync_financial.py [token]
    或设置环境变量: TUSHARE_TOKEN
"""
import os
import sys
import time
import sqlite3
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
LOG_DIR = os.path.expanduser("~/.openclaw/logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/financial_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库路径
DB_PATH = os.path.expanduser("~/.openclaw/data/stock.db")

# 并行线程数（避免触发限速）
MAX_WORKERS = 8
API_INTERVAL = 0.1  # 每次API调用间隔


def get_tushare(token: str):
    """获取Tushare连接"""
    try:
        import tushare as ts
        pro = ts.pro_api(token)
        logger.info("Tushare连接成功")
        return pro
    except Exception as e:
        logger.error(f"Tushare连接失败: {e}")
        return None


def ensure_tables(conn):
    """确保财务数据表存在"""
    cur = conn.cursor()
    
    # 财务指标表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS financial_indicators (
            ts_code TEXT,
            ann_date TEXT,
            end_date TEXT,
            basic_eps REAL,
            diluted_eps REAL,
            total_revenue REAL,
            revenue REAL,
            net_profit REAL,
            total_assets REAL,
            total_liabilities REAL,
            equity_parent_tot REAL,
            roe REAL,
            roa REAL,
            gross_profit_rate REAL,
            net_profit_rate REAL,
            PRIMARY KEY (ts_code, end_date)
        )
    """)
    
    # 利润表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS income_statement (
            ts_code TEXT,
            ann_date TEXT,
            end_date TEXT,
            total_revenue REAL,
            revenue REAL,
            oper_cost REAL,
            total_profit REAL,
            net_profit REAL,
            net_profit_attr_e REAL,
            basic_eps REAL,
            PRIMARY KEY (ts_code, end_date)
        )
    """)
    
    # 资产负债表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS balance_sheet (
            ts_code TEXT,
            ann_date TEXT,
            end_date TEXT,
            total_assets REAL,
            total_liabilities REAL,
            total_equity REAL,
            equity_parent REAL,
            curr_assets REAL,
            PRIMARY KEY (ts_code, end_date)
        )
    """)
    
    # 现金流量表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cashflow (
            ts_code TEXT,
            ann_date TEXT,
            end_date TEXT,
            net_cash_flow REAL,
            net_cash_flow_2 REAL,
            net_cash_flow_3 REAL,
            PRIMARY KEY (ts_code, end_date)
        )
    """)
    
    conn.commit()


def get_all_stock_codes(pro) -> list:
    """获取所有股票代码"""
    try:
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
        return df['ts_code'].tolist() if df is not None else []
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return []


def sync_single_stock_financial(pro, ts_code: str, fields: dict) -> int:
    """同步单支股票的财务数据（所有类型）"""
    synced = 0
    try:
        # 财务指标（最新一期）
        try:
            df = pro.fina_indicator(ts_code=ts_code)
            time.sleep(API_INTERVAL)
            if df is not None and not df.empty:
                conn = sqlite3.connect(DB_PATH)
                for _, row in df.head(1).iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO financial_indicators
                        (ts_code, ann_date, end_date, basic_eps, diluted_eps, total_revenue, revenue,
                         net_profit, total_assets, total_liabilities, equity_parent_tot, roe, roa,
                         gross_profit_rate, net_profit_rate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                        row.get('eps'), row.get('dt_eps'), row.get('total_revenue'),
                        row.get('revenue'), row.get('net_profit'), row.get('total_assets'),
                        row.get('total_liabilities'), row.get('equity_parent_tot'),
                        row.get('roe'), row.get('roa'),
                        row.get('gross_profit_rate'), row.get('net_profit_rate')
                    ))
                conn.commit()
                conn.close()
                synced += 1
        except Exception as e:
            pass
        
        # 利润表（最新一期）
        try:
            df = pro.income(ts_code=ts_code)
            time.sleep(API_INTERVAL)
            if df is not None and not df.empty:
                conn = sqlite3.connect(DB_PATH)
                for _, row in df.head(1).iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO income_statement
                        (ts_code, ann_date, end_date, total_revenue, revenue, oper_cost,
                         total_profit, net_profit, net_profit_attr_e, basic_eps)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                        row.get('total_revenue'), row.get('revenue'), row.get('oper_cost'),
                        row.get('total_profit'), row.get('net_profit'), row.get('net_profit_attr_e'),
                        row.get('eps')
                    ))
                conn.commit()
                conn.close()
                synced += 1
        except Exception as e:
            pass
        
        # 资产负债表（最新一期）
        try:
            df = pro.balancesheet(ts_code=ts_code)
            time.sleep(API_INTERVAL)
            if df is not None and not df.empty:
                conn = sqlite3.connect(DB_PATH)
                for _, row in df.head(1).iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO balance_sheet
                        (ts_code, ann_date, end_date, total_assets, total_liabilities,
                         total_equity, equity_parent, curr_assets)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                        row.get('total_assets'), row.get('total_liabilities'),
                        row.get('total_equity'), row.get('equity_parent'),
                        row.get('curr_assets')
                    ))
                conn.commit()
                conn.close()
                synced += 1
        except Exception as e:
            pass
        
        # 现金流量表（最新一期）
        try:
            df = pro.cashflow(ts_code=ts_code)
            time.sleep(API_INTERVAL)
            if df is not None and not df.empty:
                conn = sqlite3.connect(DB_PATH)
                for _, row in df.head(1).iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO cashflow
                        (ts_code, ann_date, end_date, net_cash_flow, net_cash_flow_2, net_cash_flow_3)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                        row.get('net_cash_flow'), row.get('net_cash_flow_2'), row.get('net_cash_flow_3')
                    ))
                conn.commit()
                conn.close()
                synced += 1
        except Exception as e:
            pass
        
    except Exception as e:
        pass
    
    return synced


def sync_all_stocks_financial(pro, stock_codes: list) -> int:
    """并行同步所有股票的财务数据"""
    total_synced = 0
    total = len(stock_codes)
    
    logger.info(f"开始并行同步 {total} 支股票的财务数据...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(sync_single_stock_financial, pro, code, {}): code 
                   for code in stock_codes}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            total_synced += result
            if completed % 500 == 0:
                logger.info(f"进度: {completed}/{total}")
    
    return total_synced


def main():
    """主函数"""
    token = os.environ.get('TUSHARE_TOKEN') or (sys.argv[1] if len(sys.argv) > 1 else None)
    
    if not token:
        logger.error("请设置TUSHARE_TOKEN环境变量或提供Token参数")
        print("使用方法:")
        print("  python sync_financial.py <your_tushare_token>")
        print("  或设置环境变量: export TUSHARE_TOKEN=your_token")
        sys.exit(1)
    
    logger.info(f"========== Tushare 财务数据同步 v1.1 ==========")
    
    pro = get_tushare(token)
    if not pro:
        sys.exit(1)
    
    # 获取所有股票列表
    stock_codes = get_all_stock_codes(pro)
    logger.info(f"获取到 {len(stock_codes)} 支股票")
    
    if not stock_codes:
        logger.error("无法获取股票列表")
        sys.exit(1)
    
    # 确保表存在
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    conn.close()
    
    # 同步财务数据
    total = sync_all_stocks_financial(pro, stock_codes)
    
    logger.info(f"========== 财务数据同步完成，覆盖股票数: {total} ==========")


if __name__ == '__main__':
    main()
