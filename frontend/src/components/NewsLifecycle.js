import React, { useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import { fetchNewsLifecycle, fetchNewsData } from '../services/api';
import './NewsLifecycle.css';

const NewsLifecycle = () => {
  const [newsArticles, setNewsArticles] = useState([]);
  const [selectedNewsId, setSelectedNewsId] = useState('');
  const [lifecycleData, setLifecycleData] = useState(null);
  const [dateRange, setDateRange] = useState({ startDate: '', endDate: '' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Get news articles on component mount
  useEffect(() => {
    const getNewsArticles = async () => {
      try {
        const data = await fetchNewsData();
        setNewsArticles(data);
      } catch (err) {
        setError('Failed to load news articles');
        console.error('Error fetching news articles:', err);
      }
    };

    getNewsArticles();
  }, []);

  const handleNewsSelect = async (e) => {
    const newsId = e.target.value;
    setSelectedNewsId(newsId);
    
    if (newsId) {
      await getLifecycleData(newsId);
    } else {
      setLifecycleData(null);
    }
  };

  const getLifecycleData = async (newsId) => {
    try {
      setLoading(true);
      setError(null);
      const { startDate, endDate } = dateRange;
      const data = await fetchNewsLifecycle(newsId, startDate, endDate);
      setLifecycleData(data);
    } catch (err) {
      setError('Failed to load lifecycle data');
      console.error('Error fetching lifecycle data:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleDateChange = (e) => {
    const { name, value } = e.target;
    setDateRange(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const renderLifecycleChart = () => {
    if (!lifecycleData || !lifecycleData.lifecycle_data || lifecycleData.lifecycle_data.length === 0) {
      return (
        <div className="no-data">
          <p>No lifecycle data available for this news article</p>
        </div>
      );
    }

    const lifecyclePoints = lifecycleData.lifecycle_data;
    
    // Sort data by timestamp
    const sortedData = [...lifecyclePoints].sort((a, b) => 
      new Date(a.timestamp) - new Date(b.timestamp)
    );

    // Extract chart data
    const labels = sortedData.map(point => {
      const date = new Date(point.timestamp);
      return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    });
    
    const impressions = sortedData.map(point => point.impressions || 0);
    const clicks = sortedData.map(point => point.clicks || 0);
    const ctr = sortedData.map(point => (point.clicks / point.impressions) * 100 || 0);
    
    const chartData = {
      labels,
      datasets: [
        {
          label: 'Impressions',
          data: impressions,
          borderColor: 'rgba(53, 162, 235, 1)',
          backgroundColor: 'rgba(53, 162, 235, 0.2)',
          yAxisID: 'y',
          tension: 0.4,
          fill: true,
        },
        {
          label: 'Clicks',
          data: clicks,
          borderColor: 'rgba(255, 99, 132, 1)',
          backgroundColor: 'rgba(255, 99, 132, 0.2)',
          yAxisID: 'y',
          tension: 0.4,
          fill: true,
        },
        {
          label: 'CTR (%)',
          data: ctr,
          borderColor: 'rgba(75, 192, 192, 1)',
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
          yAxisID: 'y1',
          tension: 0.4,
          borderDash: [5, 5],
        },
      ],
    };

    const options = {
      responsive: true,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      stacked: false,
      plugins: {
        title: {
          display: true,
          text: 'News Article Lifecycle',
          font: {
            size: 16,
            weight: 'bold',
          },
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              let label = context.dataset.label || '';
              if (label) {
                label += ': ';
              }
              if (context.parsed.y !== null) {
                if (label.includes('CTR')) {
                  label += context.parsed.y.toFixed(2) + '%';
                } else {
                  label += context.parsed.y;
                }
              }
              return label;
            }
          }
        }
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
          title: {
            display: true,
            text: 'Count',
          },
          beginAtZero: true,
        },
        y1: {
          type: 'linear',
          display: true,
          position: 'right',
          title: {
            display: true,
            text: 'CTR (%)',
          },
          beginAtZero: true,
          max: 100,
          grid: {
            drawOnChartArea: false,
          },
        },
      },
    };

    return <Line data={chartData} options={options} />;
  };

  const getSelectedNewsDetails = () => {
    if (!selectedNewsId) return null;
    
    const article = newsArticles.find(article => article.news_id === selectedNewsId);
    if (!article) return null;
    
    return (
      <div className="news-details">
        <h3>{article.headline}</h3>
        <div className="news-meta">
          <span className="category-badge">{article.category}</span>
          {article.topic && <span className="topic-badge">{article.topic}</span>}
        </div>
        <div className="news-body">{article.news_body}</div>
      </div>
    );
  };

  return (
    <div className="news-lifecycle-page">
      <div className="page-header">
        <h1 className="page-title">News Lifecycle Analysis</h1>
      </div>

      <div className="filter-section">
        <div className="form-group">
          <label htmlFor="newsId">Select News Article</label>
          <select 
            id="newsId" 
            value={selectedNewsId} 
            onChange={handleNewsSelect}
            className="select-input"
          >
            <option value="">Select an article</option>
            {newsArticles.map(article => (
              <option key={article.news_id} value={article.news_id}>
                {article.headline.substring(0, 60)}...
              </option>
            ))}
          </select>
        </div>

        <div className="date-filters">
          <div className="form-group">
            <label htmlFor="startDate">Start Date</label>
            <input
              type="date"
              id="startDate"
              name="startDate"
              value={dateRange.startDate}
              onChange={handleDateChange}
              className="date-input"
            />
          </div>
          
          <div className="form-group">
            <label htmlFor="endDate">End Date</label>
            <input
              type="date"
              id="endDate"
              name="endDate"
              value={dateRange.endDate}
              onChange={handleDateChange}
              className="date-input"
            />
          </div>

          <button 
            onClick={() => getLifecycleData(selectedNewsId)}
            disabled={!selectedNewsId || loading}
            className="btn-apply"
          >
            {loading ? 'Loading...' : 'Apply Filters'}
          </button>
        </div>
      </div>

      {error && <div className="error-message">{error}</div>}

      {selectedNewsId && (
        <div className="lifecycle-content">
          <div className="news-info-section">
            {getSelectedNewsDetails()}
          </div>
          
          <div className="chart-section">
            {loading ? (
              <div className="loading">
                <div className="loading-spinner"></div>
                <span>Loading lifecycle data...</span>
              </div>
            ) : (
              renderLifecycleChart()
            )}
          </div>

          {lifecycleData && lifecycleData.lifecycle_data && lifecycleData.lifecycle_data.length > 0 && (
            <div className="lifecycle-metrics">
              <h3>Lifecycle Analytics</h3>
              <div className="metrics-grid">
                <div className="metric-box">
                  <div className="metric-value">
                    {lifecycleData.lifecycle_data.reduce((sum, point) => sum + (point.impressions || 0), 0).toLocaleString()}
                  </div>
                  <div className="metric-label">Total Impressions</div>
                </div>
                
                <div className="metric-box">
                  <div className="metric-value">
                    {lifecycleData.lifecycle_data.reduce((sum, point) => sum + (point.clicks || 0), 0).toLocaleString()}
                  </div>
                  <div className="metric-label">Total Clicks</div>
                </div>
                
                <div className="metric-box">
                  <div className="metric-value">
                    {(
                      (lifecycleData.lifecycle_data.reduce((sum, point) => sum + (point.clicks || 0), 0) / 
                      lifecycleData.lifecycle_data.reduce((sum, point) => sum + (point.impressions || 0), 0) * 100) || 0
                    ).toFixed(2)}%
                  </div>
                  <div className="metric-label">Overall CTR</div>
                </div>
                
                <div className="metric-box">
                  <div className="metric-value">
                    {Math.max(...lifecycleData.lifecycle_data.map(point => point.impressions || 0)).toLocaleString()}
                  </div>
                  <div className="metric-label">Peak Impressions</div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default NewsLifecycle;
