import React, { useState, useEffect } from 'react';
import { Bar } from 'react-chartjs-2';
import { fetchHotNews } from '../services/api';
import './HotNews.css';

const HotNews = () => {
  const [hotNewsData, setHotNewsData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filters, setFilters] = useState({
    hoursAhead: 24,
    minImpressions: 100
  });
  const [selectedNewsId, setSelectedNewsId] = useState(null);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);
      const { hoursAhead, minImpressions } = filters;
      const data = await fetchHotNews(hoursAhead, minImpressions);
      setHotNewsData(data);
      setSelectedNewsId(null);
    } catch (err) {
      console.error('Error fetching hot news data:', err);
      setError('Failed to load hot news data');
    } finally {
      setLoading(false);
    }
  };

  const handleFilterChange = (e) => {
    const { name, value } = e.target;
    setFilters(prev => ({ ...prev, [name]: parseInt(value, 10) }));
  };

  const applyFilters = () => {
    fetchData();
  };

  const handleNewsSelect = (newsId) => {
    setSelectedNewsId(newsId === selectedNewsId ? null : newsId);
  };

  const renderBarChart = () => {
    if (!hotNewsData || !hotNewsData.hot_news || hotNewsData.hot_news.length === 0) {
      return (
        <div className="no-data">
          <p>No hot news data available</p>
        </div>
      );
    }

    // Use top 10 articles for the chart
    const topArticles = hotNewsData.hot_news.slice(0, 10);
    
    // Prepare data for the chart
    const chartData = {
      labels: topArticles.map(news => news.headline.substring(0, 30) + '...'),
      datasets: [
        {
          label: 'Predicted Popularity Score',
          data: topArticles.map(news => news.hotness_score || 0),
          backgroundColor: topArticles.map((_, index) => {
            const hue = (index * 20) % 360;
            return `hsla(${hue}, 85%, 60%, 0.7)`;
          }),
          borderColor: topArticles.map((_, index) => {
            const hue = (index * 20) % 360;
            return `hsla(${hue}, 85%, 60%, 1)`;
          }),
          borderWidth: 1,
        },
      ],
    };

    const options = {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
        title: {
          display: true,
          text: 'Hot News Prediction Scores',
          font: {
            size: 16,
            weight: 'bold',
          },
        },
        tooltip: {
          callbacks: {
            title: function(context) {
              const index = context[0].dataIndex;
              return topArticles[index].headline;
            },
            label: function(context) {
              const index = context.dataIndex;
              return [
                `Score: ${context.formattedValue}`,
                `Category: ${topArticles[index].category || 'Unknown'}`,
                `Predicted Impressions: ${topArticles[index].predicted_impressions?.toLocaleString() || 'Unknown'}`
              ];
            }
          }
        }
      },
      scales: {
        x: {
          title: {
            display: true,
            text: 'Hotness Score'
          },
          min: 0,
        },
      },
    };

    return (
      <div className="chart-container">
        <Bar data={chartData} options={options} />
      </div>
    );
  };

  const getHotNewsDetails = () => {
    if (!selectedNewsId || !hotNewsData || !hotNewsData.hot_news) return null;
    
    const newsItem = hotNewsData.hot_news.find(item => item.news_id === selectedNewsId);
    if (!newsItem) return null;
    
    return (
      <div className="news-detail-modal">
        <div className="modal-content">
          <div className="modal-header">
            <h3>{newsItem.headline}</h3>
            <button className="close-button" onClick={() => setSelectedNewsId(null)}>×</button>
          </div>
          
          <div className="modal-body">
            <div className="news-meta">
              <span className="category-badge">{newsItem.category}</span>
              {newsItem.topic && <span className="topic-badge">{newsItem.topic}</span>}
            </div>
            
            <p className="news-body">{newsItem.news_body}</p>
            
            <div className="prediction-metrics">
              <div className="metric-item">
                <div className="metric-label">Hotness Score</div>
                <div className="metric-value hot-score">{newsItem.hotness_score?.toFixed(2) || 'N/A'}</div>
              </div>
              
              <div className="metric-item">
                <div className="metric-label">Impressions</div>
                <div className="metric-value">{newsItem.impressions?.toLocaleString() || 'N/A'}</div>
              </div>
              
              <div className="metric-item">
                <div className="metric-label">Clicks</div>
                <div className="metric-value">{newsItem.clicks?.toLocaleString() || 'N/A'}</div>
              </div>
              
              <div className="metric-item">
                <div className="metric-label">CTR</div>
                <div className="metric-value">{newsItem.click_rate ? (newsItem.click_rate * 100).toFixed(2) + '%' : 'N/A'}</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="hot-news-page">
      <div className="page-header">
        <h1 className="page-title">Hot News Prediction</h1>
      </div>

      <div className="filter-section">
        <div className="filter-row">
          <div className="form-group">
            <label htmlFor="hoursAhead">Prediction Window (hours)</label>
            <select 
              id="hoursAhead" 
              name="hoursAhead" 
              value={filters.hoursAhead} 
              onChange={handleFilterChange}
              className="select-input"
            >
              <option value="6">6 Hours</option>
              <option value="12">12 Hours</option>
              <option value="24">24 Hours</option>
              <option value="48">48 Hours</option>
              <option value="72">72 Hours</option>
            </select>
          </div>
          
          <div className="form-group">
            <label htmlFor="minImpressions">Minimum Impressions</label>
            <select 
              id="minImpressions" 
              name="minImpressions" 
              value={filters.minImpressions} 
              onChange={handleFilterChange}
              className="select-input"
            >
              <option value="50">50+</option>
              <option value="100">100+</option>
              <option value="200">200+</option>
              <option value="500">500+</option>
              <option value="1000">1000+</option>
            </select>
          </div>
          
          <button 
            onClick={applyFilters}
            disabled={loading}
            className="btn-apply"
          >
            {loading ? 'Loading...' : 'Apply Filters'}
          </button>
        </div>
      </div>

      {error && <div className="error-message">{error}</div>}

      <div className="hot-news-content">
        <div className="chart-section">
          {loading ? (
            <div className="loading">
              <div className="loading-spinner"></div>
              <span>Loading hot news predictions...</span>
            </div>
          ) : (
            renderBarChart()
          )}
        </div>

        <div className="news-list-section">
          <h2 className="section-title">Top Trending Articles</h2>
          
          {loading ? (
            <div className="loading">
              <div className="loading-spinner"></div>
              <span>Loading hot news predictions...</span>
            </div>
          ) : hotNewsData && hotNewsData.hot_news && hotNewsData.hot_news.length > 0 ? (
            <div className="hot-news-list">
              {hotNewsData.hot_news.map((news, index) => (
                <div 
                  key={news.news_id} 
                  className={`hot-news-item ${selectedNewsId === news.news_id ? 'selected' : ''}`}
                  onClick={() => handleNewsSelect(news.news_id)}
                >
                  <div className="news-rank">{index + 1}</div>
                  <div className="news-content">
                    <h3 className="news-headline">{news.headline}</h3>
                    <div className="news-meta">
                      <span className="category-tag">{news.category}</span>
                      {news.topic && <span className="topic-tag">{news.topic}</span>}
                    </div>
                    <div className="metrics-row">
                      <div className="metric">
                        <span className="metric-icon">🔥</span>
                        <span className="metric-value">{news.hotness_score?.toFixed(2) || 'N/A'}</span>
                      </div>
                      <div className="metric">
                        <span className="metric-icon">👁️</span>
                        <span className="metric-value">{news.impressions?.toLocaleString() || 'N/A'}</span>
                      </div>
                      <div className="metric">
                        <span className="metric-icon">👆</span>
                        <span className="metric-value">{news.clicks?.toLocaleString() || 'N/A'}</span>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="no-data">
              <p>No hot news predictions available</p>
            </div>
          )}
        </div>
      </div>

      {selectedNewsId && getHotNewsDetails()}
    </div>
  );
};

export default HotNews;
