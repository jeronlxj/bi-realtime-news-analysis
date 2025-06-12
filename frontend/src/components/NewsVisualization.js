import React, { useEffect, useState } from 'react';
import { 
  fetchNewsData, 
  fetchNewsLifecycle, 
  fetchUserRecommendations,
  fetchRealTimeInsights 
} from '../services/api';
import './NewsVisualization.css';

const NewsVisualization = () => {
  const [newsData, setNewsData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedNews, setSelectedNews] = useState(null);
  const [lifecycleData, setLifecycleData] = useState(null);
  const [recommendations, setRecommendations] = useState([]);
  const [realTimeInsights, setRealTimeInsights] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterCategory, setFilterCategory] = useState('');
  const [selectedUserId, setSelectedUserId] = useState('');

  useEffect(() => {
    fetchInitialData();
    
    // Set up real-time data refresh
    const interval = setInterval(() => {
      fetchRealTimeData();
    }, 15000); // Refresh every 15 seconds
    
    return () => clearInterval(interval);
  }, []);

  const fetchInitialData = async () => {
    try {
      setLoading(true);
      const data = await fetchNewsData();
      setNewsData(data);
      await fetchRealTimeData();
    } catch (err) {
      setError(err.message);
      console.error('Error fetching initial data:', err);
    } finally {
      setLoading(false);
    }
  };

  const fetchRealTimeData = async () => {
    try {
      const insights = await fetchRealTimeInsights('overview');
      setRealTimeInsights(insights);
    } catch (err) {
      console.error('Error fetching real-time insights:', err);
    }
  };

  const handleNewsSelect = async (news) => {
    setSelectedNews(news);
    try {
      const lifecycle = await fetchNewsLifecycle(news.news_id);
      setLifecycleData(lifecycle);
    } catch (err) {
      console.error('Error fetching news lifecycle:', err);
    }
  };

  const handleUserRecommendations = async () => {
    if (!selectedUserId) return;
    
    try {
      const recs = await fetchUserRecommendations(selectedUserId, 10);
      setRecommendations(recs.recommendations || []);
    } catch (err) {
      console.error('Error fetching recommendations:', err);
    }
  };

  const filteredNews = newsData.filter(news => {
    const matchesSearch = news.headline?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         news.news_body?.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesCategory = !filterCategory || news.category === filterCategory;
    return matchesSearch && matchesCategory;
  });

  const uniqueCategories = [...new Set(newsData.map(news => news.category))].filter(Boolean);

  if (loading) {
    return (
      <div className="news-loading">
        <div className="loading-spinner"></div>
        <p>Loading news data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="news-error">
        <h2>Error Loading News</h2>
        <p>{error}</p>
        <button onClick={fetchInitialData}>Retry</button>
      </div>
    );
  }

  return (
    <div className="news-visualization">
      <header className="news-header">
        <h1>📰 News Analysis & Visualization</h1>
        <div className="header-controls">
          <button onClick={() => window.history.back()}>← Back to Dashboard</button>
        </div>
      </header>

      {/* Real-time Insights Panel */}
      {realTimeInsights && (
        <div className="insights-panel">
          <h3>⚡ Real-Time Insights</h3>
          <div className="insights-grid">
            <div className="insight-card">
              <span className="insight-label">Total Impressions</span>
              <span className="insight-value">
                {realTimeInsights.insights?.total_impressions?.toLocaleString() || 0}
              </span>
            </div>
            <div className="insight-card">
              <span className="insight-label">Total Clicks</span>
              <span className="insight-value">
                {realTimeInsights.insights?.total_clicks?.toLocaleString() || 0}
              </span>
            </div>
            <div className="insight-card">
              <span className="insight-label">CTR</span>
              <span className="insight-value">
                {realTimeInsights.insights?.overall_ctr?.toFixed(2) || 0}%
              </span>
            </div>
            <div className="insight-card">
              <span className="insight-label">Active Users</span>
              <span className="insight-value">
                {realTimeInsights.insights?.unique_users?.toLocaleString() || 0}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Filters and Search */}
      <div className="news-controls">
        <div className="search-section">
          <input
            type="text"
            placeholder="Search news articles..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="search-input"
          />
          <select 
            value={filterCategory} 
            onChange={(e) => setFilterCategory(e.target.value)}
            className="category-filter"
          >
            <option value="">All Categories</option>
            {uniqueCategories.map(category => (
              <option key={category} value={category}>{category}</option>
            ))}
          </select>
        </div>

        <div className="recommendation-section">
          <input
            type="text"
            placeholder="Enter User ID for recommendations"
            value={selectedUserId}
            onChange={(e) => setSelectedUserId(e.target.value)}
            className="user-input"
          />
          <button 
            onClick={handleUserRecommendations}
            disabled={!selectedUserId}
            className="recommend-btn"
          >
            Get Recommendations
          </button>
        </div>
      </div>

      <div className="news-content">
        {/* News List */}
        <div className="news-list-section">
          <h2>📋 News Articles ({filteredNews.length})</h2>
          <div className="news-list">
            {filteredNews.map((newsItem) => (
              <div 
                key={newsItem.news_id} 
                className={`news-item ${selectedNews?.news_id === newsItem.news_id ? 'selected' : ''}`}
                onClick={() => handleNewsSelect(newsItem)}
              >
                <div className="news-meta">
                  <span className="news-category">{newsItem.category}</span>
                  <span className="news-topic">{newsItem.topic}</span>
                </div>
                <h3 className="news-title">{newsItem.headline}</h3>
                <p className="news-excerpt">
                  {newsItem.news_body?.substring(0, 150)}...
                </p>
                <div className="news-id">ID: {newsItem.news_id}</div>
              </div>
            ))}
            
            {filteredNews.length === 0 && (
              <div className="no-results">
                <p>No news articles found matching your criteria.</p>
              </div>
            )}
          </div>
        </div>

        {/* News Details */}
        {selectedNews && (
          <div className="news-details-section">
            <h2>📊 News Analysis</h2>
            <div className="news-details">
              <div className="news-header-info">
                <h3>{selectedNews.headline}</h3>
                <div className="news-metadata">
                  <span className="metadata-item">
                    <strong>Category:</strong> {selectedNews.category}
                  </span>
                  <span className="metadata-item">
                    <strong>Topic:</strong> {selectedNews.topic}
                  </span>
                  <span className="metadata-item">
                    <strong>ID:</strong> {selectedNews.news_id}
                  </span>
                </div>
              </div>

              <div className="news-body">
                <h4>Article Content:</h4>
                <p>{selectedNews.news_body}</p>
              </div>

              {/* Lifecycle Data */}
              {lifecycleData && lifecycleData.lifecycle_data && (
                <div className="lifecycle-section">
                  <h4>📈 Lifecycle Analysis</h4>
                  {lifecycleData.lifecycle_data.length > 0 ? (
                    <div className="lifecycle-data">
                      <div className="lifecycle-summary">
                        <p>Total time periods: {lifecycleData.lifecycle_data.length}</p>
                      </div>
                      <div className="lifecycle-timeline">
                        {lifecycleData.lifecycle_data.map((period, index) => (
                          <div key={index} className="timeline-item">
                            <div className="timeline-time">
                              {period.timestamp ? new Date(period.timestamp).toLocaleString() : 'N/A'}
                            </div>
                            <div className="timeline-metrics">
                              <span>👁 {period.total_impressions}</span>
                              <span>👆 {period.total_clicks}</span>
                              <span>📈 {(period.click_rate * 100).toFixed(1)}%</span>
                              <span>👥 {period.unique_users}</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : (
                    <p>No lifecycle data available for this article.</p>
                  )}
                </div>
              )}
            </div>
          </div>
        )}

        {/* Recommendations */}
        {recommendations.length > 0 && (
          <div className="recommendations-section">
            <h2>🎯 Recommendations for User {selectedUserId}</h2>
            <div className="recommendations-list">
              {recommendations.map((rec, index) => (
                <div key={rec.news_id || index} className="recommendation-item">
                  <div className="rec-header">
                    <h4>{rec.headline}</h4>
                    <span className="rec-score">
                      Interest: {(rec.predicted_interest * 100).toFixed(1)}%
                    </span>
                  </div>
                  <div className="rec-meta">
                    <span>{rec.category}</span>
                    <span>{rec.topic}</span>
                    <span>Popularity: {rec.popularity}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default NewsVisualization;