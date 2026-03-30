#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare数据同步脚本 v4.0
- 批量同步：使用 pro.daily() 批量接口，一次获取所有股票
- 补偿机制：自动补齐所有缺失的交易日
- 数据就绪探测：Tushare 数据未就绪时等待重试
- 交易日历驱动：使用 trade_cal API
- 指数数据支持
使用方法:
    python sync_tushare.py [token]
    或设置环境变量: TUSHARE_TOKEN
"""
import os
import sys
import time
import sqlite3
import logging
from datetime import datetime, timedelta

# 配置日志
LOG_DIR = os.path.expanduser("~/.openclaw/logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/tushare_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库路径
DB_PATH = os.path.expanduser("~/.openclaw/data/stock.db")


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


def get_db_last_date() -> str:
    """获取数据库中最近有数据的日期"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT MAX(date) FROM stock_daily")
        last_date = cur.fetchone()[0]
        conn.close()
        return last_date if last_date else None
    except Exception as e:
        logger.error(f"获取数据库最后日期失败: {e}")
        return None


def get_trade_days(pro, from_date: str, to_date: str) -> list:
    """使用 trade_cal API 获取 from_date 到 to_date 之间的所有交易日"""
    try:
        cal_df = pro.trade_cal(start_date=from_date, end_date=to_date, is_open='1')
        dates = sorted(cal_df['cal_date'].tolist())
        return dates
    except Exception as e:
        logger.error(f"获取交易日历失败: {e}")
        return []


def get_next_trade_day(pro, from_date: str) -> str:
    """获取 from_date 之后的第一个交易日"""
    try:
        cal_df = pro.trade_cal(start_date=from_date, end_date='20991231', is_open='1')
        dates = sorted(cal_df['cal_date'].tolist())
        try:
            idx = dates.index(from_date)
            if idx + 1 < len(dates):
                return dates[idx + 1]
        except ValueError:
            pass
        for d in dates:
            if d > from_date:
                return d
        return None
    except Exception as e:
        logger.error(f"获取下一交易日失败: {e}")
        return None


def wait_for_data_ready(pro, date: str, max_retries=3, retry_interval=60) -> bool:
    """探测指定日期数据是否就绪（用代表性股票探测）"""
    for attempt in range(max_retries):
        try:
            # 用浦发银行探测当日数据是否返回
            test_df = pro.daily(ts_code='600000.SH', start_date=date, end_date=date)
            if test_df is not None and not test_df.empty:
                logger.info(f"[{date}] 数据已就绪 (探测尝试 {attempt+1}/{max_retries})")
                return True
            logger.warning(f"[{date}] 数据尚未返回，等待 {retry_interval}s... (尝试 {attempt+1}/{max_retries})")
        except Exception as e:
            logger.warning(f"[{date}] 探测失败: {e}，等待 {retry_interval}s...")
        time.sleep(retry_interval)
    logger.error(f"[{date}] 数据探测最终失败，跳过该日期")
    return False


def insert_replace_daily(conn, df):
    """使用INSERT OR REPLACE插入数据"""
    for _, row in df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO stock_daily 
            (stock_code, trade_date, open, high, low, close, volume, amount, ts_code, date, pre_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['ts_code'], row['trade_date'], 
            row.get('open'), row.get('high'), row.get('low'), row.get('close'),
            row.get('vol'), row.get('amount'),
            row['ts_code'], row['trade_date'], row.get('pre_close')
        ))


def sync_stock_daily_batch(pro, trade_date: str) -> tuple:
    """批量同步股票日线数据 - 使用批量接口，一次获取所有股票"""
    try:
        # 批量获取：不需要指定ts_code，一次调用返回所有股票
        df = pro.daily(trade_date=trade_date)
        
        if df is None or df.empty:
            logger.warning(f"[{trade_date}] 无股票数据")
            return 0, 0
        
        synced = len(df)
        
        # 批量写入数据库
        conn = sqlite3.connect(DB_PATH)
        insert_replace_daily(conn, df)
        conn.commit()
        conn.close()
        
        logger.info(f"[{trade_date}] 批量同步成功: {synced} 只股票")
        return synced, 0
        
    except Exception as e:
        logger.error(f"[{trade_date}] 批量同步失败: {e}")
        return 0, 1


def sync_index_daily(pro, trade_date: str):
    """同步指数数据"""
    indices = ['000001.SH', '399001.SZ', '399006.SZ', '000300.SH', '000016.SH']
    
    for index_code in indices:
        try:
            df = pro.index_daily(ts_code=index_code, start_date=trade_date, end_date=trade_date)
            
            if df is not None and not df.empty:
                conn = sqlite3.connect(DB_PATH)
                df['stock_code'] = df['ts_code']
                
                for _, row in df.iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_daily 
                        (stock_code, trade_date, open, high, low, close, volume, amount, ts_code, date, pre_close)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row['stock_code'], row['trade_date'],
                        row.get('open'), row.get('high'), row.get('low'), row.get('close'),
                        row.get('vol'), row.get('amount'),
                        row['ts_code'], row['trade_date'], row.get('pre_close')
                    ))
                conn.commit()
                conn.close()
                logger.info(f"指数同步成功: {index_code}")
            else:
                logger.warning(f"指数无数据: {index_code}")
                
        except Exception as e:
            logger.error(f"指数同步失败: {index_code}, {e}")


def sync_all_missing_days(pro):
    """同步所有缺失的交易日"""
    last_date = get_db_last_date()
    if not last_date:
        logger.error("无法获取数据库最后日期")
        return
    
    today = datetime.now().strftime('%Y%m%d')
    logger.info(f"数据库最后日期: {last_date}，今天: {today}")
    
    # 获取从 last_date 到 today 的所有交易日
    trade_days = get_trade_days(pro, last_date, today)
    
    if not trade_days:
        logger.info("没有需要同步的交易日")
        return
    
    logger.info(f"需要同步的交易日: {trade_days}")
    
    for date in trade_days:
        logger.info(f"========== 开始同步 {date} ==========")
        
        # 数据就绪探测
        if not wait_for_data_ready(pro, date):
            logger.warning(f"[{date}] 数据未就绪，跳过")
            continue
        
        # 批量同步股票
        synced, failed = sync_stock_daily_batch(pro, date)
        
        # 同步指数
        sync_index_daily(pro, date)
        
        # 验证
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM stock_daily WHERE date=?", (date,))
            count = cur.fetchone()[0]
            conn.close()
            logger.info(f"[{date}] 同步后记录数: {count}")
        except Exception as e:
            logger.error(f"[{date}] 验证失败: {e}")
        
        logger.info(f"========== {date} 同步完成 ==========")


def main():
    """主函数"""
    token = os.environ.get('TUSHARE_TOKEN') or (sys.argv[1] if len(sys.argv) > 1 else None)
    
    if not token:
        logger.error("请设置TUSHARE_TOKEN环境变量或提供Token参数")
        print("使用方法:")
        print("  python sync_tushare.py <your_tushare_token>")
        print("  或设置环境变量: export TUSHARE_TOKEN=your_token")
        sys.exit(1)
    
    logger.info(f"========== Tushare 数据同步 v4.0 (批量接口) ==========")
    
    pro = get_tushare(token)
    if not pro:
        sys.exit(1)
    
    sync_all_missing_days(pro)
    
    last_date = get_db_last_date()
    logger.info(f"========== 同步完成，最终日期: {last_date} ==========")


if __name__ == '__main__':
    main()
