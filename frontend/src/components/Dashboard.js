import React, { useState, useEffect } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
} from 'chart.js';
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import './Dashboard.css';
import VirtualClock from './VirtualClock';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement
);

const Dashboard = () => {
  const [analyticsData, setAnalyticsData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedTimeRange, setSelectedTimeRange] = useState('24h');
  const [selectedNews, setSelectedNews] = useState('');
  const [selectedUser, setSelectedUser] = useState('');

  const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

  useEffect(() => {
    fetchAnalyticsOverview();
    // Set up auto-refresh every 30 seconds
    const interval = setInterval(fetchAnalyticsOverview, 30000);
    return () => clearInterval(interval);
  }, [selectedTimeRange]);

  const fetchAnalyticsOverview = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE_URL}/api/analytics/overview`);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      setAnalyticsData(data);
      setError(null);
    } catch (err) {
      setError(err.message);
      console.error('Error fetching analytics data:', err);
    } finally {
      setLoading(false);
    }
  };

  const fetchNewsLifecycle = async (newsId) => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/analytics/news-lifecycle/${newsId}`);
      const data = await response.json();
      return data;
    } catch (err) {
      console.error('Error fetching news lifecycle:', err);
      return null;
    }
  };

  const fetchUserInterests = async (userId) => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/analytics/user-interests?user_id=${userId}`);
      const data = await response.json();
      return data;
    } catch (err) {
      console.error('Error fetching user interests:', err);
      return null;
    }
  };

  const fetchCategoryTrends = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/analytics/category-trends`);
      const data = await response.json();
      return data;
    } catch (err) {
      console.error('Error fetching category trends:', err);
      return null;
    }
  };

  if (loading && !analyticsData) {
    return (
      <div className="dashboard-loading">
        <div className="spinner"></div>
        <p>Loading analytics dashboard...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="dashboard-error">
        <h2>Error Loading Dashboard</h2>
        <p>{error}</p>
        <button onClick={fetchAnalyticsOverview}>Retry</button>
      </div>
    );
  }

  const renderOverviewCards = () => {
    if (!analyticsData?.overview) return null;

    const { summary } = analyticsData.overview;
    
    return (
      <div className="overview-cards">
        <div className="metric-card">
          <h3>Total Impressions</h3>
          <div className="metric-value">{summary.total_impressions?.toLocaleString() || 0}</div>
        </div>
        <div className="metric-card">
          <h3>Total Clicks</h3>
          <div className="metric-value">{summary.total_clicks?.toLocaleString() || 0}</div>
        </div>
        <div className="metric-card">
          <h3>Click-Through Rate</h3>
          <div className="metric-value">{(summary.overall_ctr * 100)?.toFixed(2) || 0}%</div>
        </div>
        <div className="metric-card">
          <h3>Categories</h3>
          <div className="metric-value">{summary.total_categories || 0}</div>
        </div>
        <div className="metric-card">
          <h3>Trending News</h3>
          <div className="metric-value">{summary.trending_news_count || 0}</div>
        </div>
      </div>
    );
  };

  const renderCategoryTrends = () => {
    if (!analyticsData?.category_trends?.length) return null;

    const categoryData = analyticsData.category_trends.reduce((acc, trend) => {
      if (!acc[trend.category]) {
        acc[trend.category] = { impressions: 0, clicks: 0 };
      }
      acc[trend.category].impressions += trend.impressions;
      acc[trend.category].clicks += trend.clicks;
      return acc;
    }, {});

    const categories = Object.keys(categoryData);
    const impressions = categories.map(cat => categoryData[cat].impressions);
    const clicks = categories.map(cat => categoryData[cat].clicks);

    const chartData = {
      labels: categories,
      datasets: [
        {
          label: 'Impressions',
          data: impressions,
          backgroundColor: 'rgba(54, 162, 235, 0.6)',
          borderColor: 'rgba(54, 162, 235, 1)',
          borderWidth: 1,
        },
        {
          label: 'Clicks',
          data: clicks,
          backgroundColor: 'rgba(255, 99, 132, 0.6)',
          borderColor: 'rgba(255, 99, 132, 1)',
          borderWidth: 1,
        },
      ],
    };

    const options = {
      responsive: true,
      plugins: {
        legend: {
          position: 'top',
        },
        title: {
          display: true,
          text: 'Category Performance',
        },
      },
      scales: {
        y: {
          beginAtZero: true,
        },
      },
    };

    return (
      <div className="chart-container">
        <Bar data={chartData} options={options} />
      </div>
    );
  };

  const renderHotNews = () => {
    if (!analyticsData?.hot_news?.length) return null;

    return (
      <div className="hot-news-section">
        <h3>🔥 Trending News</h3>
        <div className="hot-news-list">
          {analyticsData.hot_news.slice(0, 10).map((news, index) => (
            <div key={news.news_id} className="hot-news-item">
              <div className="news-rank">#{index + 1}</div>
              <div className="news-info">
                <div className="news-headline">{news.headline}</div>
                <div className="news-category">{news.category} • {news.topic}</div>
                <div className="news-metrics">
                  <span>👁 {news.impressions}</span>
                  <span>👆 {news.clicks}</span>
                  <span>📈 {(news.click_rate * 100).toFixed(1)}%</span>
                  <span>🔥 {news.hotness_score?.toFixed(2)}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  };

  const renderPerformanceStats = () => {
    if (!analyticsData?.performance?.length) return null;

    const performanceData = analyticsData.performance;
    const labels = performanceData.map(stat => stat.query_type);
    const avgTimes = performanceData.map(stat => stat.avg_execution_time * 1000); // Convert to ms
    const successRates = performanceData.map(stat => stat.success_rate);

    const chartData = {
      labels: labels,
      datasets: [
        {
          label: 'Avg Response Time (ms)',
          data: avgTimes,
          backgroundColor: 'rgba(75, 192, 192, 0.6)',
          borderColor: 'rgba(75, 192, 192, 1)',
          borderWidth: 1,
          yAxisID: 'y',
        },
        {
          label: 'Success Rate (%)',
          data: successRates,
          backgroundColor: 'rgba(153, 102, 255, 0.6)',
          borderColor: 'rgba(153, 102, 255, 1)',
          borderWidth: 1,
          yAxisID: 'y1',
        },
      ],
    };

    const options = {
      responsive: true,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      scales: {
        y: {
          type: 'linear',
          display: true,
          position: 'left',
          beginAtZero: true,
          title: {
            display: true,
            text: 'Response Time (ms)',
          },
        },
        y1: {
          type: 'linear',
          display: true,
          position: 'right',
          beginAtZero: true,
          max: 100,
          title: {
            display: true,
            text: 'Success Rate (%)',
          },
          grid: {
            drawOnChartArea: false,
          },
        },
      },
      plugins: {
        title: {
          display: true,
          text: 'Query Performance Metrics',
        },
      },
    };

    return (
      <div className="chart-container">
        <Bar data={chartData} options={options} />
      </div>
    );
  };

  return (
    <div className="dashboard">      <header className="dashboard-header">
        <h1>📊 Real-Time News Analytics Dashboard</h1>
        <div className="dashboard-controls">
          <select 
            value={selectedTimeRange} 
            onChange={(e) => setSelectedTimeRange(e.target.value)}
            className="time-range-select"
          >
            <option value="1h">Last Hour</option>
            <option value="6h">Last 6 Hours</option>
            <option value="24h">Last 24 Hours</option>
            <option value="7d">Last 7 Days</option>
          </select>
          <button onClick={fetchAnalyticsOverview} disabled={loading} className="refresh-btn">
            {loading ? '🔄' : '↻'} Refresh
          </button>
        </div>
      </header>

      <main className="dashboard-content">
        {/* Overview Metrics */}
        <section className="overview-section">
          <h2>📈 Overview</h2>
          {renderOverviewCards()}
        </section>

        {/* Charts Section */}
        <div className="charts-grid">
          <section className="chart-section">
            <h2>📊 Category Performance</h2>
            {renderCategoryTrends()}
          </section>

          <section className="chart-section">
            <h2>⚡ System Performance</h2>
            {renderPerformanceStats()}
          </section>
        </div>

        {/* Hot News Section */}
        <section className="trending-section">
          {renderHotNews()}
        </section>

        {/* Interactive Analysis Tools */}
        <section className="analysis-tools">
          <h2>🔍 Interactive Analysis</h2>
          <div className="tools-grid">
            <div className="tool-card">
              <h3>News Lifecycle Analysis</h3>
              <input
                type="text"
                placeholder="Enter News ID"
                value={selectedNews}
                onChange={(e) => setSelectedNews(e.target.value)}
              />
              <button 
                onClick={() => fetchNewsLifecycle(selectedNews)}
                disabled={!selectedNews}
              >
                Analyze Lifecycle
              </button>
            </div>

            <div className="tool-card">
              <h3>User Interest Analysis</h3>
              <input
                type="text"
                placeholder="Enter User ID"
                value={selectedUser}
                onChange={(e) => setSelectedUser(e.target.value)}
              />
              <button 
                onClick={() => fetchUserInterests(selectedUser)}
                disabled={!selectedUser}
              >
                Analyze Interests
              </button>
            </div>
          </div>
        </section>
        {/* Virtual Clock */}
        {/* <VirtualClock /> */}

        {/* Footer with real-time status */}
        {/* Real-time Status */}
        <footer className="dashboard-footer">
          <div className="status-indicator">
            <span className="status-dot active"></span>
            {/* Most recent time that page rendered */}
            <span>Live Data • Last Updated: {new Date().toLocaleTimeString()}</span>
          </div>
          <div className="data-info">
            {analyticsData?.query_timestamp && 
              `Data as of: ${new Date(analyticsData.query_timestamp).toLocaleString()}`
            }
          </div>
        </footer>
      </main>
    </div>
  );
};

export default Dashboard;
