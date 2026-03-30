#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare数据同步脚本 v3.3
- 补偿机制：自动补齐所有缺失的交易日
- 数据就绪探测：Tushare 数据未就绪时等待重试
- 交易日历驱动：使用 trade_cal API 而非硬编码周末
- 指数数据支持：修复 pre_close 列问题
- 速率控制：检测到限速后自动等待重试
- 增量同步+完整性检查：
  1. 对于缺失的日期：同步全部股票
  2. 对于已存在但数量不足的日期：增量同步缺失的股票
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

# Tushare 速率限制: 500次/分钟，建议留10%余量
API_INTERVAL = 0.12  # 120ms per call -> ~500/min
RATE_LIMIT_WAIT = 65  # 触发限速后等待秒数（略大于1分钟）

# 限速错误关键字
RATE_LIMIT_ERROR_KEYWORDS = ['每分钟最多访问', '500次', 'rate limit']

# A股股票总数（估算，用于判断是否需要增量同步）
EXPECTED_STOCK_COUNT = 5500


def is_rate_limit_error(e: Exception) -> bool:
    """判断是否为API限速错误"""
    error_msg = str(e).lower()
    return any(kw in error_msg for kw in RATE_LIMIT_ERROR_KEYWORDS)


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


def get_existing_stocks_for_date(trade_date: str) -> set:
    """获取指定日期已存在的股票代码集合"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT ts_code FROM stock_daily WHERE date=?", (trade_date,))
        existing = set(row[0] for row in cur.fetchall())
        conn.close()
        return existing
    except Exception as e:
        logger.error(f"获取已有股票列表失败: {e}")
        return set()


def get_existing_count_for_date(trade_date: str) -> int:
    """获取指定日期已存在的股票数量"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stock_daily WHERE date=?", (trade_date,))
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        logger.error(f"获取已有股票数量失败: {e}")
        return 0


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
        dates = sorted(cal_df['cal_date'].tolist())  # 必须排序，API返回降序
        try:
            idx = dates.index(from_date)
            if idx + 1 < len(dates):
                return dates[idx + 1]
        except ValueError:
            pass
        # from_date 不在日历中，找下一个
        for d in dates:
            if d > from_date:
                return d
        return None
    except Exception as e:
        logger.error(f"获取下一交易日失败: {e}")
        return None


def wait_for_data_ready(pro, date: str, max_retries=3, retry_interval=300) -> bool:
    """探测指定日期数据是否就绪（用代表性股票探测）"""
    for attempt in range(max_retries):
        try:
            # 用浦发银行探测当日数据是否返回
            test_df = pro.daily(ts_code='600000.SH', start_date=date, end_date=date)
            time.sleep(API_INTERVAL)
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
    """使用INSERT OR REPLACE插入数据（修复版：含pre_close）"""
    cols = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']
    stock_col_map = {'vol': 'volume'}  # vol -> volume
    
    for _, row in df.iterrows():
        # 构建字段映射后的值字典
        vals = {}
        for c in cols:
            src = stock_col_map.get(c, c)
            vals[c] = row.get(src) if src in row else None
        
        # pre_close 股票数据为 NULL
        vals['volume'] = row.get('vol') if 'vol' in row else row.get('volume')
        vals['stock_code'] = row['ts_code']
        vals['pre_close'] = row.get('pre_close') if 'pre_close' in row else None
        
        conn.execute("""
            INSERT OR REPLACE INTO stock_daily 
            (stock_code, trade_date, open, high, low, close, volume, amount, ts_code, date, pre_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            vals['stock_code'], vals['trade_date'], vals['open'], vals['high'],
            vals['low'], vals['close'], vals['volume'], vals['amount'],
            vals['ts_code'], vals['trade_date'], vals['pre_close']
        ))


def sync_stock_daily(pro, trade_date: str, existing_stocks: set = None) -> tuple:
    """同步股票日线数据（单日），带速率控制和限速重试
    - existing_stocks: 该日期已存在的股票集合，为None时表示全量同步
    """
    try:
        # 获取A股所有股票
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        time.sleep(API_INTERVAL)
        
        synced = 0
        failed = 0
        no_data = 0
        skipped = 0
        rate_limit_hits = 0
        
        total = len(df)
        is_incremental = existing_stocks is not None
        logger.info(f"[{trade_date}] 开始同步 {total} 只股票... ({'增量' if is_incremental else '全量'})")
        
        if is_incremental:
            logger.info(f"[{trade_date}] 增量模式: 已存在 {len(existing_stocks)} 只股票，将只同步缺失的")
        
        for i, row in df.iterrows():
            ts_code = row['ts_code']
            
            # 增量同步：跳过已存在的股票
            if existing_stocks and ts_code in existing_stocks:
                skipped += 1
                continue
            
            # 限速重试机制
            max_retries = 3
            for retry in range(max_retries):
                try:
                    daily_df = pro.daily(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
                    time.sleep(API_INTERVAL)
                    
                    if daily_df is not None and not daily_df.empty:
                        conn = sqlite3.connect(DB_PATH)
                        insert_replace_daily(conn, daily_df)
                        conn.commit()
                        conn.close()
                        synced += 1
                        if synced % 500 == 0:
                            logger.info(f"[{trade_date}] 进度: {synced}/{total}, 已跳过: {skipped}")
                    else:
                        no_data += 1
                    break  # 成功或无数据，跳出重试循环
                    
                except Exception as e:
                    if is_rate_limit_error(e):
                        rate_limit_hits += 1
                        logger.warning(f"[{trade_date}] 检测到API限速，等待 {RATE_LIMIT_WAIT}s 后重试 ({retry+1}/{max_retries})")
                        time.sleep(RATE_LIMIT_WAIT)
                    else:
                        # 非限速错误，直接记录并跳过
                        failed += 1
                        if failed <= 5:
                            logger.error(f"同步失败: {ts_code}, {e}")
                        break  # 非限速错误不重试
            else:
                # 重试次数用尽仍失败
                failed += 1
                if failed <= 5:
                    logger.error(f"同步失败（重试耗尽）: {ts_code}")
        
        logger.info(f"[{trade_date}] 股票同步完成: 成功 {synced}, 失败 {failed}, 无数据 {no_data}, 跳过(已存在) {skipped}, 限速中断 {rate_limit_hits} 次")
        return synced, failed
        
    except Exception as e:
        logger.error(f"股票同步出错: {e}")
        return 0, 0


def sync_index_daily(pro, trade_date: str):
    """同步指数数据（修复版：INSERT OR REPLACE + 列映射）"""
    indices = ['000001.SH', '399001.SZ', '399006.SZ', '000300.SH', '000016.SH']
    
    for index_code in indices:
        try:
            df = pro.index_daily(ts_code=index_code, start_date=trade_date, end_date=trade_date)
            time.sleep(API_INTERVAL)
            
            if df is not None and not df.empty:
                conn = sqlite3.connect(DB_PATH)
                # 使用 INSERT OR REPLACE，映射列
                cols = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'pre_close']
                insert_df = df[[c for c in cols if c in df.columns]].copy()
                insert_df.rename(columns={'vol': 'volume'}, inplace=True)
                insert_df['stock_code'] = insert_df['ts_code']
                
                for _, row in insert_df.iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_daily 
                        (stock_code, trade_date, open, high, low, close, volume, amount, ts_code, date, pre_close)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row['stock_code'], row['trade_date'], row['open'], row['high'],
                        row['low'], row['close'], row.get('volume'), row.get('amount'),
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
    """同步所有缺失的交易日，并检查已存在日期的完整性"""
    last_date = get_db_last_date()
    if not last_date:
        logger.error("无法获取数据库最后日期")
        return
    
    today = datetime.now().strftime('%Y%m%d')
    logger.info(f"数据库最后日期: {last_date}，今天: {today}")
    
    # 获取从 last_date 的下一交易日起到今天的所有交易日
    next_day = get_next_trade_day(pro, last_date)
    if not next_day:
        logger.warning("无法获取下一交易日")
        return
    
    trade_days = get_trade_days(pro, next_day, today)
    
    if not trade_days:
        logger.info("没有需要同步的日期（检查是否需要补全已存在但数量不足的日期）")
    
    # 对于每个需要检查的日期（从last_date到today）
    all_dates_to_check = trade_days
    
    # 特别处理：也检查今天是否需要补全（即使"最后日期"就是今天）
    if last_date != today:
        all_dates_to_check = get_trade_days(pro, last_date, today)
    
    for date in all_dates_to_check:
        existing_count = get_existing_count_for_date(date)
        
        if existing_count == 0:
            # 日期完全不存在，需要全量同步
            logger.info(f"========== 开始全量同步 {date} ==========")
            logger.info(f"[{date}] 日期不存在，需要全量同步")
            
            if not wait_for_data_ready(pro, date):
                logger.warning(f"[{date}] 数据未就绪，跳过股票同步")
                sync_index_daily(pro, date)
                continue
            
            sync_stock_daily(pro, date, None)  # 全量
            sync_index_daily(pro, date)
            
        elif existing_count < EXPECTED_STOCK_COUNT * 0.5:
            # 日期存在但数量明显不足，需要增量同步
            logger.info(f"========== 开始增量同步 {date} ==========")
            logger.info(f"[{date}] 数据不完整 ({existing_count} 只)，需要增量同步")
            
            existing_stocks = get_existing_stocks_for_date(date)
            
            if not wait_for_data_ready(pro, date):
                logger.warning(f"[{date}] 数据未就绪，跳过股票同步")
                sync_index_daily(pro, date)
                continue
            
            sync_stock_daily(pro, date, existing_stocks)  # 增量
            sync_index_daily(pro, date)
            
        else:
            logger.info(f"[{date}] 数据完整 ({existing_count} 只)，跳过")
            continue
        
        # 验证当日记录数
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
    
    logger.info(f"========== Tushare 数据同步 v3.3 ==========")
    
    pro = get_tushare(token)
    if not pro:
        sys.exit(1)
    
    # 同步所有缺失的日期，并检查已存在日期的完整性
    sync_all_missing_days(pro)
    
    # 最终状态
    last_date = get_db_last_date()
    logger.info(f"========== 同步完成，最终日期: {last_date} ==========")


if __name__ == '__main__':
    main()
