#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare财务数据同步脚本 v1.0
- 同步内容：利润表、资产负债表、现金流量表、财务指标
- 使用批量接口：每类型数据一次API调用获取所有股票
- 增量同步：只同步最新一期的财务数据
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
            fin_revenue REAL,
            operate_income REAL,
            invest_income REAL,
            operate_profit REAL,
            total_profit REAL,
            net_profit REAL,
            total_assets REAL,
            total_liabilities REAL,
            total_holders_equity REAL,
            equity_parent_tot REAL,
            roe REAL,
            roe加权 REAL,
            roa REAL,
            debts_to_assets REAL,
            ar_turn REAL,
            assets_turn REAL,
            gross_profit_rate REAL,
            net_profit_rate REAL,
            debt_to_assets REAL,
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
            diluted_eps REAL,
            total_revenue_yoy REAL,
            net_profit_yoy REAL,
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
            non_curr_assets REAL,
            total_liab_2 REAL,
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


def get_latest_financial_date(conn, table: str) -> str:
    """获取某张财务表的最新日期"""
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(end_date) FROM {table}")
        result = cur.fetchone()[0]
        return result if result else None
    except:
        return None


def sync_financial_indicators(pro, ann_date: str = None) -> int:
    """同步财务指标数据"""
    try:
        if ann_date:
            df = pro.fina_indicator(ann_date=ann_date)
        else:
            df = pro.fina_indicator()
        
        if df is None or df.empty:
            logger.warning("财务指标数据为空")
            return 0
        
        conn = sqlite3.connect(DB_PATH)
        ensure_tables(conn)
        
        synced = 0
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO financial_indicators
                    (ts_code, ann_date, end_date, basic_eps, diluted_eps, total_revenue, revenue,
                     fin_revenue, operate_income, invest_income, operate_profit, total_profit,
                     net_profit, total_assets, total_liabilities, total_holders_equity,
                     equity_parent_tot, roe, roe加权, roa, debts_to_assets, ar_turn,
                     assets_turn, gross_profit_rate, net_profit_rate, debt_to_assets)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                    row.get('basic_eps'), row.get('diluted_eps'), row.get('total_revenue'),
                    row.get('revenue'), row.get('fin_revenue'), row.get('operate_income'),
                    row.get('invest_income'), row.get('operate_profit'), row.get('total_profit'),
                    row.get('net_profit'), row.get('total_assets'), row.get('total_liabilities'),
                    row.get('total_holders_equity'), row.get('equity_parent_tot'),
                    row.get('roe'), row.get('roe加权'), row.get('roa'), row.get('debts_to_assets'),
                    row.get('ar_turn'), row.get('assets_turn'), row.get('gross_profit_rate'),
                    row.get('net_profit_rate'), row.get('debt_to_assets')
                ))
                synced += 1
            except Exception as e:
                pass
        
        conn.commit()
        conn.close()
        logger.info(f"财务指标同步完成: {synced} 条")
        return synced
    except Exception as e:
        logger.error(f"财务指标同步失败: {e}")
        return 0


def sync_income_statement(pro, ann_date: str = None) -> int:
    """同步利润表数据"""
    try:
        if ann_date:
            df = pro.income(ann_date=ann_date)
        else:
            df = pro.income()
        
        if df is None or df.empty:
            logger.warning("利润表数据为空")
            return 0
        
        conn = sqlite3.connect(DB_PATH)
        ensure_tables(conn)
        
        synced = 0
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO income_statement
                    (ts_code, ann_date, end_date, total_revenue, revenue, oper_cost,
                     total_profit, net_profit, net_profit_attr_e, basic_eps, diluted_eps,
                     total_revenue_yoy, net_profit_yoy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                    row.get('total_revenue'), row.get('revenue'), row.get('oper_cost'),
                    row.get('total_profit'), row.get('net_profit'), row.get('net_profit_attr_e'),
                    row.get('basic_eps'), row.get('diluted_eps'),
                    row.get('total_revenue_yoy'), row.get('net_profit_yoy')
                ))
                synced += 1
            except Exception as e:
                pass
        
        conn.commit()
        conn.close()
        logger.info(f"利润表同步完成: {synced} 条")
        return synced
    except Exception as e:
        logger.error(f"利润表同步失败: {e}")
        return 0


def sync_balance_sheet(pro, ann_date: str = None) -> int:
    """同步资产负债表数据"""
    try:
        if ann_date:
            df = pro.balancesheet(ann_date=ann_date)
        else:
            df = pro.balancesheet()
        
        if df is None or df.empty:
            logger.warning("资产负债表数据为空")
            return 0
        
        conn = sqlite3.connect(DB_PATH)
        ensure_tables(conn)
        
        synced = 0
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO balance_sheet
                    (ts_code, ann_date, end_date, total_assets, total_liabilities,
                     total_equity, equity_parent, curr_assets, non_curr_assets, total_liab_2)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                    row.get('total_assets'), row.get('total_liabilities'),
                    row.get('total_equity'), row.get('equity_parent'),
                    row.get('curr_assets'), row.get('non_curr_assets'), row.get('total_liab_2')
                ))
                synced += 1
            except Exception as e:
                pass
        
        conn.commit()
        conn.close()
        logger.info(f"资产负债表同步完成: {synced} 条")
        return synced
    except Exception as e:
        logger.error(f"资产负债表同步失败: {e}")
        return 0


def sync_cashflow(pro, ann_date: str = None) -> int:
    """同步现金流量表数据"""
    try:
        if ann_date:
            df = pro.cashflow(ann_date=ann_date)
        else:
            df = pro.cashflow()
        
        if df is None or df.empty:
            logger.warning("现金流量表数据为空")
            return 0
        
        conn = sqlite3.connect(DB_PATH)
        ensure_tables(conn)
        
        synced = 0
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO cashflow
                    (ts_code, ann_date, end_date, net_cash_flow, net_cash_flow_2, net_cash_flow_3)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    row.get('ts_code'), row.get('ann_date'), row.get('end_date'),
                    row.get('net_cash_flow'), row.get('net_cash_flow_2'), row.get('net_cash_flow_3')
                ))
                synced += 1
            except Exception as e:
                pass
        
        conn.commit()
        conn.close()
        logger.info(f"现金流量表同步完成: {synced} 条")
        return synced
    except Exception as e:
        logger.error(f"现金流量表同步失败: {e}")
        return 0


def main():
    """主函数"""
    token = os.environ.get('TUSHARE_TOKEN') or (sys.argv[1] if len(sys.argv) > 1 else None)
    
    if not token:
        logger.error("请设置TUSHARE_TOKEN环境变量或提供Token参数")
        print("使用方法:")
        print("  python sync_financial.py <your_tushare_token>")
        print("  或设置环境变量: export TUSHARE_TOKEN=your_token")
        sys.exit(1)
    
    logger.info(f"========== Tushare 财务数据同步 v1.0 ==========")
    
    pro = get_tushare(token)
    if not pro:
        sys.exit(1)
    
    total = 0
    total += sync_financial_indicators(pro)
    total += sync_income_statement(pro)
    total += sync_balance_sheet(pro)
    total += sync_cashflow(pro)
    
    logger.info(f"========== 财务数据同步完成，总计: {total} 条 ==========")


if __name__ == '__main__':
    main()
