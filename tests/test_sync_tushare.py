#!/usr/bin/env python3
"""测试 sync_tushare.py 的速率控制逻辑"""
import sys
import os
import tempfile
import sqlite3

# 添加src路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def test_rate_limit_detection():
    """测试限速错误识别"""
    from sync_tushare import is_rate_limit_error
    
    # 测试限速错误关键字
    assert is_rate_limit_error(Exception("抱歉，您每分钟最多访问该接口500次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108"))
    assert is_rate_limit_error(Exception("每分钟最多访问"))
    assert is_rate_limit_error(Exception("rate limit exceeded"))
    
    # 测试非限速错误
    assert not is_rate_limit_error(Exception("连接超时"))
    assert not is_rate_limit_error(Exception("未知错误"))
    assert not is_rate_limit_error(Exception("股票代码不存在"))
    
    print("✅ 限速错误识别测试通过")
    return True

def test_import():
    """测试模块导入"""
    try:
        import sync_tushare
        print("✅ 模块导入成功")
        return True
    except Exception as e:
        print(f"❌ 模块导入失败: {e}")
        return False

def test_constants():
    """测试常量配置"""
    from sync_tushare import API_INTERVAL, RATE_LIMIT_WAIT
    
    assert API_INTERVAL == 0.12, f"API_INTERVAL 应为 0.12，实际: {API_INTERVAL}"
    assert RATE_LIMIT_WAIT == 65, f"RATE_LIMIT_WAIT 应为 65，实际: {RATE_LIMIT_WAIT}"
    
    print(f"✅ 常量配置正确: API_INTERVAL={API_INTERVAL}, RATE_LIMIT_WAIT={RATE_LIMIT_WAIT}")
    return True

def test_get_existing_stocks_for_date():
    """测试获取已存在股票逻辑"""
    import sync_tushare
    
    # 创建临时数据库测试
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE stock_daily (
                ts_code TEXT,
                trade_date TEXT,
                date TEXT,
                close REAL
            )
        """)
        # 插入测试数据
        conn.execute("INSERT INTO stock_daily (ts_code, trade_date, date) VALUES ('000001.SH', '20260330', '20260330')")
        conn.execute("INSERT INTO stock_daily (ts_code, trade_date, date) VALUES ('000002.SZ', '20260330', '20260330')")
        conn.execute("INSERT INTO stock_daily (ts_code, trade_date, date) VALUES ('000003.SZ', '20260330', '20260330')")
        conn.commit()
        conn.close()
        
        # 临时替换DB_PATH
        original_db_path = sync_tushare.DB_PATH
        sync_tushare.DB_PATH = db_path
        
        result = sync_tushare.get_existing_stocks_for_date('20260330')
        
        sync_tushare.DB_PATH = original_db_path
        
        assert len(result) == 3, f"应返回3只股票，实际: {len(result)}"
        assert '000001.SH' in result
        assert '000002.SZ' in result
        
        print(f"✅ 增量同步逻辑测试通过: {len(result)} 只股票已存在")
        return True
    finally:
        os.unlink(db_path)

def main():
    print("=" * 50)
    print("sync_tushare.py 测试")
    print("=" * 50)
    
    tests = [
        ("模块导入", test_import),
        ("常量配置", test_constants),
        ("限速识别", test_rate_limit_detection),
        ("增量同步", test_get_existing_stocks_for_date),
    ]
    
    results = []
    for name, func in tests:
        print(f"\n>>> 测试: {name}")
        try:
            results.append((name, func()))
        except Exception as e:
            print(f"❌ 测试异常: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    print("\n" + "=" * 50)
    print("测试结果汇总:")
    print("=" * 50)
    passed = sum(1 for _, r in results if r)
    total = len(results)
    for name, r in results:
        status = "✅ PASS" if r else "❌ FAIL"
        print(f"  {status} - {name}")
    print(f"\n总计: {passed}/{total} 通过")
    
    return passed == total

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
