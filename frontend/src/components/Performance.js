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
import { Bar, Line, Doughnut } from 'react-chartjs-2';
import './Performance.css';

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

const Performance = () => {
  const [performanceData, setPerformanceData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [timeRange, setTimeRange] = useState('24h');
  const [selectedMetric, setSelectedMetric] = useState('execution_time');

  const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

  useEffect(() => {
    fetchPerformanceData();
    // Set up auto-refresh every 60 seconds
    const interval = setInterval(fetchPerformanceData, 60000);
    return () => clearInterval(interval);
  }, [timeRange]);

  const fetchPerformanceData = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE_URL}/api/analytics/performance?time_range=${timeRange}`);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      setPerformanceData(data);
      setError(null);
    } catch (err) {
      setError(err.message);
      console.error('Error fetching performance data:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleTimeRangeChange = (event) => {
    setTimeRange(event.target.value);
  };

  const handleMetricChange = (event) => {
    setSelectedMetric(event.target.value);
  };

  if (loading && !performanceData) {
    return (
      <div className="performance-loading">
        <div className="spinner"></div>
        <p>Loading performance metrics...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="performance-error">
        <h2>Error Loading Performance Data</h2>
        <p>{error}</p>
        <button onClick={fetchPerformanceData}>Retry</button>
      </div>
    );
  }

  const renderPerformanceStats = () => {
    if (!performanceData?.query_metrics?.length) return null;

    const metrics = performanceData.query_metrics;
    const labels = metrics.map(stat => stat.query_type);
    const avgTimes = metrics.map(stat => stat.avg_execution_time * 1000); // Convert to ms
    const maxTimes = metrics.map(stat => stat.max_execution_time * 1000); // Convert to ms
    const successRates = metrics.map(stat => stat.success_rate);

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
          label: 'Max Response Time (ms)',
          data: maxTimes,
          backgroundColor: 'rgba(255, 159, 64, 0.6)',
          borderColor: 'rgba(255, 159, 64, 1)',
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
      <div className="chart-container performance-chart">
        <Bar data={chartData} options={options} />
      </div>
    );
  };

  const renderTimeHistogram = () => {
    if (!performanceData?.time_distribution?.length) return null;

    const distribution = performanceData.time_distribution;
    const labels = distribution.map(item => `${item.range_start}-${item.range_end}ms`);
    const counts = distribution.map(item => item.count);

    const chartData = {
      labels: labels,
      datasets: [
        {
          label: 'Query Count',
          data: counts,
          backgroundColor: 'rgba(54, 162, 235, 0.6)',
          borderColor: 'rgba(54, 162, 235, 1)',
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
          text: 'Query Execution Time Distribution',
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: 'Count',
          },
        },
        x: {
          title: {
            display: true,
            text: 'Time Range',
          },
        },
      },
    };

    return (
      <div className="chart-container time-histogram">
        <Bar data={chartData} options={options} />
      </div>
    );
  };

  const renderStatusDistribution = () => {
    if (!performanceData?.status_distribution) return null;

    const statusData = performanceData.status_distribution;
    const statuses = Object.keys(statusData);
    const counts = statuses.map(status => statusData[status]);
    
    const chartData = {
      labels: statuses,
      datasets: [
        {
          data: counts,
          backgroundColor: [
            'rgba(75, 192, 192, 0.6)',
            'rgba(255, 99, 132, 0.6)',
            'rgba(255, 159, 64, 0.6)',
            'rgba(153, 102, 255, 0.6)',
          ],
          borderColor: [
            'rgba(75, 192, 192, 1)',
            'rgba(255, 99, 132, 1)',
            'rgba(255, 159, 64, 1)',
            'rgba(153, 102, 255, 1)',
          ],
          borderWidth: 1,
        },
      ],
    };

    const options = {
      responsive: true,
      plugins: {
        legend: {
          position: 'right',
        },
        title: {
          display: true,
          text: 'Query Status Distribution',
        },
      },
    };

    return (
      <div className="chart-container status-distribution">
        <Doughnut data={chartData} options={options} />
      </div>
    );
  };

  const renderTimeSeriesTrends = () => {
    if (!performanceData?.time_series?.length) return null;

    const timeSeries = performanceData.time_series;
    const labels = timeSeries.map(item => {
      const date = new Date(item.timestamp);
      return date.toLocaleTimeString();
    });
    const avgTimes = timeSeries.map(item => item.avg_execution_time * 1000); // Convert to ms
    const queryCount = timeSeries.map(item => item.query_count);

    const chartData = {
      labels: labels,
      datasets: [
        {
          label: 'Avg Response Time (ms)',
          data: avgTimes,
          borderColor: 'rgba(75, 192, 192, 1)',
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
          yAxisID: 'y',
          tension: 0.4,
        },
        {
          label: 'Query Count',
          data: queryCount,
          borderColor: 'rgba(153, 102, 255, 1)',
          backgroundColor: 'rgba(153, 102, 255, 0.2)',
          yAxisID: 'y1',
          tension: 0.4,
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
        x: {
          title: {
            display: true,
            text: 'Time',
          },
        },
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
          title: {
            display: true,
            text: 'Query Count',
          },
          grid: {
            drawOnChartArea: false,
          },
        },
      },
      plugins: {
        title: {
          display: true,
          text: 'Performance Trends Over Time',
        },
      },
    };

    return (
      <div className="chart-container time-series-chart">
        <Line data={chartData} options={options} />
      </div>
    );
  };

  const renderOverviewCards = () => {
    if (!performanceData?.summary) return null;

    const { summary } = performanceData;
    
    return (
      <div className="overview-cards">
        <div className="metric-card">
          <h3>Total Queries</h3>
          <div className="metric-value">{summary.total_queries?.toLocaleString() || 0}</div>
        </div>
        <div className="metric-card">
          <h3>Avg Response Time</h3>
          <div className="metric-value">{(summary.avg_execution_time * 1000)?.toFixed(2) || 0} ms</div>
        </div>
        <div className="metric-card">
          <h3>Max Response Time</h3>
          <div className="metric-value">{(summary.max_execution_time * 1000)?.toFixed(2) || 0} ms</div>
        </div>
        <div className="metric-card">
          <h3>Success Rate</h3>
          <div className="metric-value">{(summary.success_rate * 100)?.toFixed(2) || 0}%</div>
        </div>
        <div className="metric-card">
          <h3>Errors</h3>
          <div className="metric-value">{summary.error_count || 0}</div>
        </div>
      </div>
    );
  };

  return (
    <div className="performance-container">
      <div className="performance-header">
        <h1>System Performance Analytics</h1>
        <div className="filters">
          <div className="filter">
            <label htmlFor="timeRange">Time Range:</label>
            <select
              id="timeRange"
              value={timeRange}
              onChange={handleTimeRangeChange}
            >
              <option value="1h">Last Hour</option>
              <option value="6h">Last 6 Hours</option>
              <option value="24h">Last 24 Hours</option>
              <option value="7d">Last 7 Days</option>
              <option value="30d">Last 30 Days</option>
            </select>
          </div>
          <div className="filter">
            <label htmlFor="metricType">Metric:</label>
            <select
              id="metricType"
              value={selectedMetric}
              onChange={handleMetricChange}
            >
              <option value="execution_time">Execution Time</option>
              <option value="success_rate">Success Rate</option>
              <option value="query_count">Query Count</option>
            </select>
          </div>
          <button className="refresh-btn" onClick={fetchPerformanceData}>
            Refresh
          </button>
        </div>
      </div>

      {renderOverviewCards()}

      <div className="performance-grid">
        <div className="grid-item full-width">
          {renderTimeSeriesTrends()}
        </div>
        <div className="grid-item">
          {renderPerformanceStats()}
        </div>
        <div className="grid-item">
          {renderTimeHistogram()}
        </div>
        <div className="grid-item">
          {renderStatusDistribution()}
        </div>
      </div>

      <div className="performance-insights">
        <h2>Performance Insights</h2>
        <div className="insights-container">
          <div className="insight-card">
            <h3>Slow Queries</h3>
            <p>Top queries with the highest execution times</p>
            <ul className="insight-list">
              {performanceData?.slow_queries?.map((query, index) => (
                <li key={index}>
                  <span className="query-type">{query.query_type}</span>
                  <span className="query-time">{(query.execution_time * 1000).toFixed(2)} ms</span>
                </li>
              )) || <li>No slow queries detected</li>}
            </ul>
          </div>
          <div className="insight-card">
            <h3>Error Analysis</h3>
            <p>Most common error types</p>
            <ul className="insight-list">
              {performanceData?.error_types?.map((error, index) => (
                <li key={index}>
                  <span className="error-type">{error.type}</span>
                  <span className="error-count">{error.count}</span>
                </li>
              )) || <li>No errors detected</li>}
            </ul>
          </div>
          <div className="insight-card">
            <h3>Optimization Suggestions</h3>
            <ul className="insight-list">
              {performanceData?.optimization_suggestions?.map((suggestion, index) => (
                <li key={index}>{suggestion}</li>
              )) || <li>No optimization suggestions available</li>}
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Performance;
