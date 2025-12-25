# flows/etl_flow.py
import os
import sys
import time
import pandas as pd
import dask.dataframe as dd
#import polars as pl
from prefect import flow, task, get_run_logger
from flows.database import get_engine

from dotenv import load_dotenv
load_dotenv()  # загружает .env автоматически

# Добавляем корень проекта в путь (для запуска из подпапки)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Параметры по умолчанию
#RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/Social_Media_Users.csv")
#TABLE_NAME = os.getenv("TABLE_NAME", "platform_analytics")
from prefect import variables

RAW_DATA_PATH = variables.get("raw_data_path", default="data/Social_Media_Users.csv")
TABLE_NAME = variables.get("table_name", default="platform_analytics")
DB_TYPE = variables.get("db_type", default="sqlite")



def preprocess_df(df):
    """Преобразует данные независимо от библиотеки."""
    if isinstance(df, pd.DataFrame):
        df = df.copy()
        df['Verified_Account_Num'] = df['Verified Account'].map({'Yes': 1, 'No': 0})
        df['Daily Time Spent (min)'] = pd.to_numeric(df['Daily Time Spent (min)'], errors='coerce')

    return df


def aggregate_pandas_like(df):
    """Агрегация для pandas/Dask - возвращает pandas DataFrame с правильными колонками."""
    result = df.groupby('Platform').agg(
        Avg_Daily_Usage_Minutes=('Daily Time Spent (min)', 'mean'),
        Verified_Account_Ratio=('Verified_Account_Num', 'mean'),
        User_Count=('Verified_Account_Num', 'size')
    ).reset_index()
    result['Avg_Daily_Usage_Hours'] = result['Avg_Daily_Usage_Minutes'] / 60.0
    final_df = result[['Platform', 'Avg_Daily_Usage_Hours', 'Verified_Account_Ratio', 'User_Count']]
    final_df = final_df.rename(columns={'Platform': 'Most_Used_Platform'})
    return final_df

'''
def aggregate_polars(df):
    """Агрегация для Polars."""
    result = df.group_by("Platform").agg([
        pl.col("Daily Time Spent (min)").mean().alias("Avg_Daily_Usage_Minutes"),
        pl.col("Verified_Account_Num").mean().alias("Verified_Account_Ratio"),
        pl.col("Verified_Account_Num").count().alias("User_Count")
    ]).sort("Platform")
    result = result.with_columns(
        (pl.col("Avg_Daily_Usage_Minutes") / 60.0).alias("Avg_Daily_Usage_Hours")
    )
    final = result.select([
        pl.col("Platform").alias("Most_Used_Platform"),
        pl.col("Avg_Daily_Usage_Hours"),
        pl.col("Verified_Account_Ratio"),
        pl.col("User_Count")
    ])
    return final.to_pandas()  # для совместимости с загрузкой
'''


@task
def extract_pandas(path: str):
    logger = get_run_logger()
    start = time.time()
    logger.info("Извлечение данных: [Pandas]")
    df = pd.read_csv(path)
    duration = time.time() - start
    logger.info(f"Pandas чтение завершено за {duration:.3f} сек")
    return df, duration


@task
def extract_dask(path: str):
    logger = get_run_logger()
    start = time.time()
    logger.info("Извлечение данных: [Dask]")
    ddf = dd.read_csv(path)
    df = ddf.compute()
    duration = time.time() - start
    logger.info(f"Dask чтение завершено за {duration:.3f} сек")
    return df, duration

'''
@task
def extract_polars(path: str):
    logger = get_run_logger()
    start = time.time()
    logger.info("Извлечение данных: [Polars]")
    df = pl.read_csv(path)
    duration = time.time() - start
    logger.info(f"Polars чтение завершено за {duration:.3f} сек")
    return df, duration
'''

@task
def transform_pandas_mode(df, method_name: str):
    logger = get_run_logger()
    start = time.time()
    logger.info(f"Преобразование данных: [{method_name}]")
    df = preprocess_df(df)
    result = aggregate_pandas_like(df)
    duration = time.time() - start
    logger.info(f"{method_name} обработка завершена за {duration:.3f} сек")
    return result, duration

'''
@task
def transform_polars_mode(df):
    logger = get_run_logger()
    start = time.time()
    logger.info("Преобразование данных: [Polars]")
    df = preprocess_df(df)
    result = aggregate_polars(df)
    duration = time.time() - start
    logger.info(f"Polars обработка завершена за {duration:.3f} сек")
    return result, duration
'''

@task
def load_data_to_db(df, table_name: str, suffix: str = ""):
    logger = get_run_logger()
    start = time.time()
    full_table = f"{table_name}_{suffix}" if suffix else table_name
    logger.info(f"Сохранение в таблицу '{full_table}'...")
    engine = get_engine()
    df.to_sql(full_table, con=engine, if_exists="replace", index=False)
    duration = time.time() - start
    logger.info(f"Загрузка завершена за {duration:.3f} сек")
    return full_table, duration



#@flow(name="Social Media Analytics ETL - Сравнение производительности")
@flow(name="Social Media Analytics ETL - Performance Comparison")
def etl_comparison_flow(input_path: str = RAW_DATA_PATH, table_name: str = TABLE_NAME):
    logger = get_run_logger()
    results = {}

    # Pandas only
    logger.info("\n=== Режим 1: Pandas (чтение + обработка) ===")
    df_pd, t_read = extract_pandas(input_path)
    df_pd, t_proc = transform_pandas_mode(df_pd, "Pandas")
    _, t_load = load_data_to_db(df_pd, table_name, "pandas")
    results["Pandas"] = {"read": t_read, "process": t_proc, "load": t_load, "total": t_read + t_proc + t_load}

    # Pandas read + Dask process
    logger.info("\n=== Режим 2: Pandas + Dask ===")
    df_pd2, t_read = extract_pandas(input_path)
    ddf = dd.from_pandas(preprocess_df(df_pd2), npartitions=2)
    start = time.time()
    #agg = ddf.groupby('Platform').agg(
    #    Avg_Daily_Usage_Minutes=('Daily Time Spent (min)', 'mean'),
    #    Verified_Account_Ratio=('Verified_Account_Num', 'mean'),
    #    User_Count=('Verified_Account_Num', 'size')
    #).compute()
    # Явно выбираем только нужные столбцы перед агрегацией
    ddf_subset = ddf[['Platform', 'Daily Time Spent (min)', 'Verified_Account_Num']]

    agg = ddf_subset.groupby('Platform').agg(
        Avg_Daily_Usage_Minutes=('Daily Time Spent (min)', 'mean'),
        Verified_Account_Ratio=('Verified_Account_Num', 'mean'),
        User_Count=('Verified_Account_Num', 'size')
    ).compute()

    agg = agg.reset_index()
    agg['Avg_Daily_Usage_Hours'] = agg['Avg_Daily_Usage_Minutes'] / 60.0
    final = agg[['Platform', 'Avg_Daily_Usage_Hours', 'Verified_Account_Ratio', 'User_Count']].rename(
        columns={'Platform': 'Most_Used_Platform'}
    )
    t_proc = time.time() - start
    _, t_load = load_data_to_db(final, table_name, "pandas_dask")
    results["Pandas+Dask"] = {"read": t_read, "process": t_proc, "load": t_load, "total": t_read + t_proc + t_load}

    # Full Dask
    logger.info("\n=== Режим 3: Dask (чтение + обработка) ===")
    df_dask, t_read = extract_dask(input_path)
    df_dask, t_proc = transform_pandas_mode(df_dask, "Dask")
    _, t_load = load_data_to_db(df_dask, table_name, "dask")
    results["Dask"] = {"read": t_read, "process": t_proc, "load": t_load, "total": t_read + t_proc + t_load}
    '''
    # Polars 
    logger.info("\n=== Режим 4: Polars (чтение + обработка) ===")
    df_pl, t_read = extract_polars(input_path)
    df_pl, t_proc = transform_polars_mode(df_pl)
    _, t_load = load_data_to_db(df_pl, table_name, "polars")
    results["Polars"] = {"read": t_read, "process": t_proc, "load": t_load, "total": t_read + t_proc + t_load}
    '''

    logger.info("\n" + "=" * 60)
    logger.info("СВОДКА ПРОИЗВОДИТЕЛЬНОСТИ")
    logger.info("=" * 60)
    for name, times in results.items():
        logger.info(
            f"{name:15} | Чтение: {times['read']:6.3f}с | "
            f"Обработка: {times['process']:6.3f}с | "
            f"Загрузка: {times['load']:6.3f}с | "
            f"Итого: {times['total']:6.3f}с"
        )
    logger.info("=" * 60)

    best = min(results.items(), key=lambda x: x[1]['total'])
    logger.info(f"Самый быстрый режим: {best[0]} ({best[1]['total']:.3f} сек)")
    return best[0]

if __name__ == "__main__":
    import os
    os.environ["PREFECT_API_URL"] = ""

    try:
        result = etl_comparison_flow()
        print(f"\n Лучший режим: {result}")
    except Exception as e:
        print("КРИТИЧЕСКАЯ ОШИБКА:", e)
        import traceback
        traceback.print_exc()
