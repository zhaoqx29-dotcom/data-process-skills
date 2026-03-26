_ops.py 中的 recommend_strategy 函数是否合理正确
"""

import pandas as pd
import numpy as np
from data_ops import recommend_strategy, assess
import json


def create_test_dataset():
    """创建包含各种缺失值模式的测试数据集"""

    np.random.seed(42)

    # 基础数据：100行
    n_rows = 100

    data = {
        # ========== 场景1: 连续少量缺失 + 随时间缓慢变化 -> 推荐ffill ==========
        "temperature_slow_change": np.linspace(20, 30, n_rows),  # 线性递增
        "humidity_smooth": np.sin(np.linspace(0, 2*np.pi, n_rows)) * 10 + 60,  # 平滑波动

        # ========== 场景2: 连续多行缺失 + 平滑趋势 -> 推荐interpolate ==========
        "smooth_trend_large_missing": np.linspace(100, 200, n_rows),

        # ========== 场景3: 开头缺失 + 缺失块较短 -> 推荐bfill ==========
        "start_missing_data": np.linspace(1, 100, n_rows),

        # ========== 场景4: 类别型 + 众数占比高 -> 推荐mode ==========
        "category_high_mode": ["A"] * 80 + ["B"] * 10 + ["C"] * 10,  # A占80%
        "category_low_mode": ["A"] * 30 + ["B"] * 35 + ["C"] * 25 + ["D"] * 10,  # 分布较均匀

        # ========== 场景5: 正态分布 + 随机缺失 -> 推荐mean ==========
        "normal_distribution": np.random.normal(50, 10, n_rows),

        # ========== 场景6: 偏态分布 + 随机缺失 -> 推荐median ==========
        "skewed_distribution": np.random.exponential(10, n_rows),

        # ========== 场景7: 存在异常值 + 随机缺失 -> 推荐median ==========
        "with_outliers": np.random.normal(100, 15, n_rows),

        # ========== 场景8: 时间序列数据 -> 推荐interpolate或ffill ==========
        "time_series_data": np.sin(np.linspace(0, 4*np.pi, n_rows)) * 50 + 100,

        # ========== 场景9: 高缺失率 (>30%) -> 推荐ask_user ==========
        "high_missing_rate": np.random.normal(50, 5, n_rows),

        # ========== 场景10: 无缺失 -> 推荐no_action ==========
        "no_missing_data": np.random.uniform(30, 70, n_rows),
    }

    df = pd.DataFrame(data)

    # ==================== 添加缺失值 ====================

    # 场景1: 连续少量缺失 (<=3行) + 缺失率 <=5%
    # 处理 temperature_slow_change 和 humidity_smooth
    df.loc[20:22, 'temperature_slow_change'] = np.nan  # 连续3行缺失
    df.loc[50:51, 'humidity_smooth'] = np.nan  # 连续2行缺失

    # 场景2: 连续多行缺失 (>3行)
    df.loc[40:50, 'smooth_trend_large_missing'] = np.nan  # 连续11行缺失

    # 场景3: 开头缺失 + 缺失块较短 (<=5行)
    df.loc[0:3, 'start_missing_data'] = np.nan  # 前4行缺失

    # 场景4: 类别型随机缺失
    category_mode_missing_idx = np.random.choice(df.index, size=10, replace=False)
    df.loc[category_mode_missing_idx, 'category_high_mode'] = np.nan
    category_low_missing_idx = np.random.choice(df.index, size=12, replace=False)
    df.loc[category_low_missing_idx, 'category_low_mode'] = np.nan

    # 场景5: 正态分布随机缺失 (~10%)
    normal_missing_idx = np.random.choice(df.index, size=10, replace=False)
    df.loc[normal_missing_idx, 'normal_distribution'] = np.nan

    # 场景6: 偏态分布随机缺失 (~15%)
    skewed_missing_idx = np.random.choice(df.index, size=15, replace=False)
    df.loc[skewed_missing_idx, 'skewed_distribution'] = np.nan

    # 场景7: 添加异常值 + 随机缺失
    df.loc[10, 'with_outliers'] = 500  # 明显异常值
    df.loc[50, 'with_outliers'] = -100  # 明显异常值
    outlier_missing_idx = np.random.choice(df.index, size=8, replace=False)
    df.loc[outlier_missing_idx, 'with_outliers'] = np.nan

    # 场景8: 时间序列数据 - 连续多行缺失
    df.loc[30:35, 'time_series_data'] = np.nan  # 连续6行缺失

    # 场景9: 高缺失率 (>30%)
    high_missing_idx = np.random.choice(df.index, size=40, replace=False)  # 40%缺失
    df.loc[high_missing_idx, 'high_missing_rate'] = np.nan

    # 场景10: 无缺失
    # no_missing_data 不添加缺失值

    return df


def verify_recommendations(df, strategies):
    """验证推荐策略是否符合预期"""

    # 预期结果映射
    expected_methods = {
        "temperature_slow_change": "fill_missing_ffill",  # 连续少量缺失 + 随时间缓慢变化
        "humidity_smooth": "fill_missing_ffill",  # 连续少量缺失 + 平滑趋势
        "smooth_trend_large_missing": "fill_missing_interpolate",  # 连续多行缺失 + 平滑趋势
        "start_missing_data": "fill_missing_interpolate",  # 开头4行缺失 + 平滑趋势 -> interpolate更合理
        "category_high_mode": "fill_missing_mode",  # 类别型 + 众数占比高
        "category_low_mode": "fill_missing_mode",  # 类别型 (即使众数占比不高)
        "normal_distribution": "fill_missing_mean",  # 正态分布
        "skewed_distribution": "fill_missing_median",  # 偏态分布
        "with_outliers": "fill_missing_median",  # 存在显著异常值
        "time_series_data": "fill_missing_interpolate",  # 连续多行缺失 + 时间序列
        "high_missing_rate": "ask_user",  # 高缺失率
        "no_missing_data": "no_action",  # 无缺失
    }

    # 验证结果
    results = {
        "total_tested": len(expected_methods),
        "passed": 0,
        "failed": 0,
        "details": []
    }

    for col, expected_method in expected_methods.items():
        actual_method = strategies.get(col, {}).get("recommended_method", "NOT_FOUND")
        actual_reason = strategies.get(col, {}).get("reason", "")
        missing_rate = strategies.get(col, {}).get("missing_rate", 0)
        data_type = strategies.get(col, {}).get("data_type", "")

        passed = actual_method == expected_method
        if passed:
            results["passed"] += 1
        else:
            results["failed"] += 1

        results["details"].append({
            "column": col,
            "expected": expected_method,
            "actual": actual_method,
            "missing_rate": missing_rate,
            "data_type": data_type,
            "reason": actual_reason,
            "passed": passed
        })

    return results


def main():
    """主测试函数"""

    print("="*70)
    print("缺失值推荐策略测试")
    print("="*70)
    print()

    # 创建测试数据集
    print("1. 创建测试数据集...")
    df = create_test_dataset()
    print(f"   数据集形状: {df.shape}")
    print()

    # 获取评估报告
    print("2. 数据评估报告...")
    report = assess(df)
    print(f"   总列数: {len(df.columns)}")
    print(f"   总行数: {df.shape[0]}")
    print()

    # 测试推荐策略
    print("3. 测试推荐策略...")
    strategies = recommend_strategy(df, list(df.columns), process_type="missing")

    # 打印每个列的推荐策略
    print("\n" + "="*70)
    print("推荐策略详情:")
    print("="*70)
    for col, info in strategies.items():
        print(f"\n列名: {col}")
        print(f"  数据类型: {info['data_type']}")
        print(f"  缺失率: {info['missing_rate']}%")
        print(f"  推荐方法: {info['recommended_method']}")
        print(f"  推荐理由: {info['reason']}")
        print(f"  数据特征: {', '.join(info['characteristics'])}")

    # 验证结果
    print("\n" + "="*70)
    print("验证结果:")
    print("="*70)
    verification = verify_recommendations(df, strategies)

    print(f"\n总测试数: {verification['total_tested']}")
    print(f"通过: {verification['passed']}")
    print(f"失败: {verification['failed']}")
    print(f"通过率: {verification['passed']/verification['total_tested']*100:.1f}%")

    # 打印详细结果
    if verification['failed'] > 0:
        print("\n" + "-"*70)
        print("失败详情:")
        print("-"*70)
        for detail in verification['details']:
            if not detail['passed']:
                print(f"\n列: {detail['column']}")
                print(f"  预期: {detail['expected']}")
                print(f"  实际: {detail['actual']}")
                print(f"  理由: {detail['reason']}")
    else:
        print("\n所有测试通过！")

    # 保存详细结果到文件
    output_file = r"C:\Users\z00951953\.claude\skills\data-engineering\scripts\test_results_missing_strategy.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(verification, f, ensure_ascii=False, indent=2)
    print(f"\n详细结果已保存到: {output_file}")

    # 保存测试数据集
    data_file = r"C:\Users\z00951953\.claude\skills\data-engineering\scripts\test_dataset_missing_values.csv"
    df.to_csv(data_file, index=False, encoding='utf-8')
    print(f"测试数据集已保存到: {data_file}")


if __name__ == "__main__":
    main()
