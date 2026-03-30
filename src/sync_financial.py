#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare财务数据同步脚本 v1.2
- 同步内容：财务指标、利润表、资产负债表、现金流量表
- 限速控制：每分钟30次调用（每2秒1次）
- 分批执行：一次获取一只股票的全部财务数据
- 断点续传：记录进度，失败重试
使用方法:
    python sync_financial.py [token]
    或设置环境变量: TUSHARE_TOKEN
"""
import os
import sys
import time
import sqlite3
import logging
import json
from datetime import datetime

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

# 限速配置：每分钟30次 = 每2秒1次
API_INTERVAL = 2.0  # 秒
BATCH_SIZE = 30  # 每批数量


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
    
    # 进度记录表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_progress (
            id INTEGER PRIMARY KEY,
            last_ts_code TEXT,
            updated_at TEXT
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


def get_last_progress(conn) -> str:
    """获取上次同步进度"""
    try:
        cur = conn.cursor()
        cur.execute("SELECT last_ts_code FROM sync_progress WHERE id=1")
        result = cur.fetchone()
        return result[0] if result else None
    except:
        return None


def save_progress(conn, ts_code: str):
    """保存同步进度"""
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO sync_progress (id, last_ts_code, updated_at)
        VALUES (1, ?, ?)
    """, (ts_code, datetime.now().isoformat()))
    conn.commit()


def sync_single_stock_all(pro, ts_code: str) -> bool:
    """同步单支股票的全部财务数据"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # 财务指标（最新一期）
        try:
            df = pro.fina_indicator(ts_code=ts_code)
            time.sleep(API_INTERVAL)
            if df is not None and not df.empty:
                row = df.iloc[0]
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
        except Exception as e:
            logger.debug(f"财务指标获取失败: {ts_code}, {e}")
        
        # 利润表（最新一期）
        try:
            df = pro.income(ts_code=ts_code)
            time.sleep(API_INTERVAL)
            if df is not None and not df.empty:
                row = df.iloc[0]
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
        except Exception as e:
            logger.debug(f"利润表获取失败: {ts_code}, {e}")
        
        # 资产负债表（最新一期）
        try:
            df = pro.balancesheet(ts_code=ts_code)
            time.sleep(API_INTERVAL)
            if df is not None and not df.empty:
                row = df.iloc[0]
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
        except Exception as e:
            logger.debug(f"资产负债表获取失败: {ts_code}, {e}")
        
        # 现金流量表（最新一期）
        try:
            df = pro.cashflow(ts_code=ts_code)
            time.sleep(API_INTERVAL)
            if df is not None and not df.empty:
                row = df.iloc[0]
                conn.execute("""
                    INSERT OR REPLACE INTO cashflow
                    (ts_code, ann_date, end_date, net_cash_flow, net_cash_flow_2, net_cash_flow_3)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                    row.get('net_cash_flow'), row.get('net_cash_flow_2'), row.get('net_cash_flow_3')
                ))
        except Exception as e:
            logger.debug(f"现金流量表获取失败: {ts_code}, {e}")
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"同步失败: {ts_code}, {e}")
        return False


def sync_all_stocks(pro, stock_codes: list):
    """同步所有股票，带断点续传"""
    conn = sqlite3.connect(DB_PATH)
    last_code = get_last_progress(conn)
    conn.close()
    
    # 从断点继续
    if last_code:
        try:
            idx = stock_codes.index(last_code)
            stock_codes = stock_codes[idx + 1:]
            logger.info(f"从断点继续，上次: {last_code}, 剩余: {len(stock_codes)}")
        except ValueError:
            pass
    
    total = len(stock_codes)
    done = 0
    failed = 0
    
    logger.info(f"开始同步 {total} 支股票（限速: 每分钟30次）...")
    
    for i, code in enumerate(stock_codes):
        success = sync_single_stock_all(pro, code)
        
        if success:
            done += 1
            # 每100个保存一次进度
            if (i + 1) % 100 == 0:
                conn = sqlite3.connect(DB_PATH)
                save_progress(conn, code)
                conn.close()
                elapsed = (i + 1) * 4 * API_INTERVAL / 60
                eta = (total - i - 1) * 4 * API_INTERVAL / 60
                logger.info(f"进度: {i+1}/{total}, 完成: {done}, 失败: {failed}, 耗时: {elapsed:.0f}min, 预计剩余: {eta:.0f}min")
        else:
            failed += 1
    
    # 最终保存进度
    conn = sqlite3.connect(DB_PATH)
    if stock_codes:
        save_progress(conn, stock_codes[-1])
    conn.close()
    
    return done, failed


def main():
    """主函数"""
    token = os.environ.get('TUSHARE_TOKEN') or (sys.argv[1] if len(sys.argv) > 1 else None)
    
    if not token:
        logger.error("请设置TUSHARE_TOKEN环境变量或提供Token参数")
        print("使用方法:")
        print("  python sync_financial.py <your_tushare_token>")
        print("  或设置环境变量: export TUSHARE_TOKEN=your_token")
        sys.exit(1)
    
    logger.info(f"========== Tushare 财务数据同步 v1.2 ==========")
    logger.info(f"限速: 每分钟30次调用 (每2秒1次)")
    
    pro = get_tushare(token)
    if not pro:
        sys.exit(1)
    
    # 确保表存在
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    conn.close()
    
    # 获取所有股票列表
    stock_codes = get_all_stock_codes(pro)
    logger.info(f"获取到 {len(stock_codes)} 支股票")
    
    if not stock_codes:
        logger.error("无法获取股票列表")
        sys.exit(1)
    
    # 预计耗时
    total_calls = len(stock_codes) * 4  # 每股票4个接口
    est_minutes = total_calls / 30
    logger.info(f"预计耗时: {est_minutes:.0f} 分钟 ({est_minutes/60:.1f} 小时)")
    
    # 同步
    done, failed = sync_all_stocks(pro, stock_codes)
    
    logger.info(f"========== 财务数据同步完成 ==========")
    logger.info(f"完成: {done}, 失败: {failed}")


if __name__ == '__main__':
    main()
