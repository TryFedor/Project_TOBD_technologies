# dashboards/visualize.py
import os
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flows.database import get_engine

TABLE_NAME = os.getenv("TABLE_NAME", "platform_analytics")


def visualize():
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", con=engine)
    print("Данные загружены:")
    print(df.round(2))

    fig1 = px.bar(
        df,
        x='Most_Used_Platform',
        y='Avg_Daily_Usage_Hours',
        title='Среднее время использования по платформам (часы/день)',
        color='Verified_Account_Ratio',
        color_continuous_scale='Blues',
        labels={
            'Most_Used_Platform': 'Платформа',
            'Avg_Daily_Usage_Hours': 'Среднее время (часы)',
            'Verified_Account_Ratio': 'Доля верифицированных аккаунтов'
        },
        text=df['Avg_Daily_Usage_Hours'].round(1).astype(str) + ' ч'
    )
    fig1.update_traces(textposition='outside')
    fig1.update_layout(height=500)
    fig1.show()

    fig2 = px.scatter(
        df,
        x='Avg_Daily_Usage_Hours',
        y='Verified_Account_Ratio',
        size='User_Count',
        color='Most_Used_Platform',
        hover_name='Most_Used_Platform',
        title='Соотношение времени использования и верификации аккаунтов',
        labels={
            'Avg_Daily_Usage_Hours': 'Среднее время использования (часы)',
            'Verified_Account_Ratio': 'Доля верифицированных аккаунтов',
            'User_Count': 'Количество пользователей'
        },
        size_max=60
    )
    fig2.update_layout(
        height=600,
        xaxis_title='Среднее время использования (часы)',
        yaxis_title='Доля верифицированных аккаунтов',
        hoverlabel=dict(bgcolor="white", font_size=12)
    )
    fig2.add_hline(y=0.5, line_dash="dash", line_color="red")
    fig2.add_vline(x=2.0, line_dash="dash", line_color="green")
    fig2.show()

    features = df[['Avg_Daily_Usage_Hours', 'Verified_Account_Ratio']].copy()
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    kmeans = KMeans(n_clusters=5, random_state=42)
    df['Cluster'] = kmeans.fit_predict(features_scaled)

    fig3 = px.scatter(
        df,
        x='Avg_Daily_Usage_Hours',
        y='Verified_Account_Ratio',
        color=df['Cluster'].astype(str),
        size='User_Count',
        hover_name='Most_Used_Platform',
        title='Кластеризация платформ: время vs верификация',
        labels={
            'Avg_Daily_Usage_Hours': 'Среднее время (часы)',
            'Verified_Account_Ratio': 'Доля верифицированных',
            'color': 'Кластер'
        },
        size_max=60
    )
    fig3.update_layout(height=600)
    fig3.show()

    fig4 = px.bar(
        df.sort_values('User_Count', ascending=False),
        x='Most_Used_Platform',
        y='User_Count',
        title='Популярность платформ (по числу пользователей)',
        labels={'Most_Used_Platform': 'Платформа', 'User_Count': 'Число пользователей'},
        text='User_Count'
    )
    fig4.update_traces(textposition='outside')
    fig4.update_layout(height=500)
    fig4.show()

    corr = df[['Avg_Daily_Usage_Hours', 'Verified_Account_Ratio', 'User_Count']].corr()
    fig5 = px.imshow(
        corr,
        text_auto=True,
        color_continuous_scale='RdBu',
        title='Корреляция между метриками платформ'
    )
    fig5.update_layout(height=400)
    fig5.show()

    # Эффективность платформы: время на пользователя
    df['Hours_per_1k_Users'] = df['Avg_Daily_Usage_Hours'] / (df['User_Count'] / 1000)
    fig6 = px.bar(
        df.sort_values('Hours_per_1k_Users', ascending=False),
        x='Most_Used_Platform',
        y='Hours_per_1k_Users',
        title='Эффективность платформы: часы использования на 1000 пользователей',
        labels={'Most_Used_Platform': 'Платформа', 'Hours_per_1k_Users': 'Часы / 1000 пользователей'},
        text=df['Hours_per_1k_Users'].round(2).astype(str)
    )
    fig6.update_traces(textposition='outside')
    fig6.update_layout(height=500)
    fig6.show()

    cluster_summary = df.groupby('Cluster').agg(
        Avg_Usage=('Avg_Daily_Usage_Hours', 'mean'),
        Avg_Verified=('Verified_Account_Ratio', 'mean'),
        Total_Users=('User_Count', 'sum'),
        Platforms=('Most_Used_Platform', lambda x: ', '.join(x))
    ).round(2)
    print("\nСводка по кластерам:")
    print(cluster_summary)


if __name__ == "__main__":
    visualize()
