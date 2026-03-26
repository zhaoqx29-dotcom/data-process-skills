mport sys
import numpy as np
import pandas as pd

# 添加当前目录到路径以导入 data_ops
sys.path.insert(0, __file__.rsplit('\\', 1)[0] if '\\' in __file__ else __file__.rsplit('/', 1)[0])
from data_ops import recommend_strategy, assess

def test_median_filter_recommendation():
    """测试中值滤波推荐：检测到显著异常值（孤立尖峰/脉冲噪声）"""
    print("=" * 60)
    print("测试 1: 中值滤波推荐 - 显著异常值（脉冲噪声）")
    print("=" * 60)

    np.random.seed(42)
    n = 100

    # 创建带有脉冲噪声的数据（恒定基线 + 多个孤立尖峰）
    # 使用恒定基线而非随机趋势，确保不被识别为"平稳数据"导致混淆
    base_signal = np.full(n, 50.0)  # 恒定基线
    noise = np.random.normal(0, 1, n)
    data = base_signal + noise

    # 添加多个孤立的尖峰噪声（脉冲噪声），使其超过5%阈值
    outlier_indices = [5, 15, 25, 35, 45, 55, 65, 75, 85, 95]  # 10个点 = 10%
    for idx in outlier_indices:
        data[idx] = data[idx] + 30 if idx % 2 == 0 else data[idx] - 25

    df = pd.DataFrame({'signal': data})

    # 检查数据分析结果
    report = assess(df)
    print("\n数据分析结果:")
    print(f"  数据类型: {report['column_analysis']['signal']['dtype']}")
    print(f"  数据特征: {report['column_analysis']['signal']['characteristics']}")

    # 推荐滤波策略
    strategies = recommend_strategy(df, ['signal'], business_context='sensor_data', process_type='filter')
    print("\n推荐结果:")
    print(f"  推荐方法: {strategies['signal']['recommended_method']}")
    print(f"  推荐理由: {strategies['signal']['reason']}")

    # 验证
    expected_method = "filter_median"
    if strategies['signal']['recommended_method'] == expected_method:
        print(f"\n[OK] 测试通过！正确推荐 {expected_method}")
        return True
    else:
        print(f"\n[FAIL] 测试失败！期望 {expected_method}，实际 {strategies['signal']['recommended_method']}")
        return False


def test_moving_avg_filter_recommendation():
    """测试移动平均滤波推荐：随机小幅波动噪声（中等波动）"""
    print("\n" + "=" * 60)
    print("测试 2: 移动平均滤波推荐 - 随机小幅波动噪声（中等波动）")
    print("=" * 60)

    np.random.seed(123)
    n = 100

    # 创建带有随机小幅波动噪声的平稳数据
    base_signal = np.ones(n) * 50  # 平稳基线
    noise = np.random.normal(0, 3, n)  # 小幅随机噪声
    data = base_signal + noise

    df = pd.DataFrame({'sensor_value': data})

    # 检查数据分析结果
    report = assess(df)
    print("\n数据分析结果:")
    print(f"  数据类型: {report['column_analysis']['sensor_value']['dtype']}")
    print(f"  数据特征: {report['column_analysis']['sensor_value']['characteristics']}")

    # 推荐滤波策略
    strategies = recommend_strategy(df, ['sensor_value'], business_context='sensor_data', process_type='filter')
    print("\n推荐结果:")
    print(f"  推荐方法: {strategies['sensor_value']['recommended_method']}")
    print(f"  推荐理由: {strategies['sensor_value']['reason']}")

    # 验证
    expected_method = "filter_moving_avg"
    if strategies['sensor_value']['recommended_method'] == expected_method:
        print(f"\n[OK] 测试通过！正确推荐 {expected_method}")
        return True
    else:
        print(f"\n[FAIL] 测试失败！期望 {expected_method}，实际 {strategies['sensor_value']['recommended_method']}")
        return False


def test_fourier_filter_recommendation():
    """测试傅里叶变换滤波推荐：周期性强的数据"""
    print("\n" + "=" * 60)
    print("测试 3: 傅里叶变换滤波推荐 - 周期性强的数据")
    print("=" * 60)

    np.random.seed(456)
    n = 200

    # 创建周期性数据（正弦波叠加）
    t = np.linspace(0, 4*np.pi, n)
    signal1 = np.sin(5*t) * 2  # 高频周期分量
    signal2 = np.sin(t) * 5      # 低频周期分量
    data = signal1 + signal2

    df = pd.DataFrame({'vibration': data})

    # 检查数据分析结果
    report = assess(df)
    print("\n数据分析结果:")
    print(f"  数据类型: {report['column_analysis']['vibration']['dtype']}")
    print(f"  数据特征: {report['column_analysis']['vibration']['characteristics']}")

    # 推荐滤波策略
    strategies = recommend_strategy(df, ['vibration'], business_context='time_series', process_type='filter')
    print("\n推荐结果:")
    print(f"  推荐方法: {strategies['vibration']['recommended_method']}")
    print(f"  推荐理由: {strategies['vibration']['reason']}")

    # 验证
    expected_method = "filter_fourier"
    if strategies['vibration']['recommended_method'] == expected_method:
        print(f"\n[OK] 测试通过！正确推荐 {expected_method}")
        return True
    else:
        print(f"\n[FAIL] 测试失败！期望 {expected_method}，实际 {strategies['vibration']['recommended_method']}")
        return False


def test_smooth_trend_moving_avg():
    """测试平滑趋势 + 平稳数据推荐移动平均滤波"""
    print("\n" + "=" * 60)
    print("测试 4: 移动平均滤波推荐 - 平稳数据 + 平滑趋势")
    print("=" * 60)

    np.random.seed(789)
    n = 100

    # 创建平稳且具有平滑趋势的数据
    data = np.linspace(20, 30, n)  # 平滑的线性增长趋势
    noise = np.random.normal(0, 0.3, n)  # 极小的噪声
    data = data + noise

    df = pd.DataFrame({'temperature': data})

    # 检查数据分析结果
    report = assess(df)
    print("\n数据分析结果:")
    print(f"  数据类型: {report['column_analysis']['temperature']['dtype']}")
    print(f"  数据特征: {report['column_analysis']['temperature']['characteristics']}")

    # 推荐滤波策略
    strategies = recommend_strategy(df, ['temperature'], business_context='sensor_data', process_type='filter')
    print("\n推荐结果:")
    print(f"  推荐方法: {strategies['temperature']['recommended_method']}")
    print(f"  推荐理由: {strategies['temperature']['reason']}")

    # 验证
    expected_method = "filter_moving_avg"
    if strategies['temperature']['recommended_method'] == expected_method:
        print(f"\n[OK] 测试通过！正确推荐 {expected_method}")
        return True
    else:
        print(f"\n[FAIL] 测试失败！期望 {expected_method}，实际 {strategies['temperature']['recommended_method']}")
        return False


def main():
    print("\n" + "=" * 60)
    print("开始测试滤波降噪推荐逻辑")
    print("=" * 60)

    results = []

    # 运行所有测试
    results.append(test_median_filter_recommendation())
    results.append(test_moving_avg_filter_recommendation())
    results.append(test_fourier_filter_recommendation())
    results.append(test_smooth_trend_moving_avg())

    # 总结结果
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"通过: {passed}/{total}")
    print(f"失败: {total - passed}/{total}")

    if passed == total:
        print("\n[OK] 所有测试通过！")
    else:
        print(f"\n[FAIL] 有 {total - passed} 个测试失败")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
