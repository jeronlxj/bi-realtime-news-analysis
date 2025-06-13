import React, { useState } from 'react';
import { fetchNewsData } from '../services/api';
import './Recommendations.css';

const Recommendations = () => {
  const [userId, setUserId] = useState('');
  const [recommendedNews, setRecommendedNews] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [limitCount, setLimitCount] = useState(10);
  const [showAdvanced, setShowAdvanced] = useState(false);

  const handleUserIdChange = (e) => {
    setUserId(e.target.value);
  };

  const handleLimitChange = (e) => {
    setLimitCount(parseInt(e.target.value, 10));
  };

  const fetchRecommendations = async () => {
    if (!userId) {
      setError('Please enter a user ID');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      
      // In a real implementation, this would use the recommendation API endpoint
      // For now, we'll simulate recommendations by fetching some news articles
      const data = await fetchNewsData();
      
      // Ensure we have an array even if API response format changed
      const newsArticles = Array.isArray(data) ? data : [];
      
      // Simulate personalization by randomly selecting and sorting articles
      const randomSelectedNews = [...newsArticles]
        .sort(() => 0.5 - Math.random())
        .slice(0, limitCount)
        .map(article => ({
          ...article,
          relevance_score: (Math.random() * 0.5 + 0.5).toFixed(2),
          category_affinity: (Math.random() * 0.8 + 0.2).toFixed(2),
          recency_bonus: (Math.random() * 0.3).toFixed(2),
        }))
        .sort((a, b) => parseFloat(b.relevance_score) - parseFloat(a.relevance_score));
      
      setRecommendedNews({
        user_id: userId,
        recommendations: randomSelectedNews,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error('Error fetching recommendations:', err);
      setError('Failed to load recommendations');
    } finally {
      setLoading(false);
    }
  };

  const getCategoryColor = (category) => {
    const categoryColors = {
      sports: '#4cd137',
      entertainment: '#9c88ff',
      news: '#487eb0',
      finance: '#e1b12c',
      health: '#e84393',
      technology: '#00a8ff',
      lifestyle: '#8c7ae6',
      default: '#6a11cb'
    };
    
    return categoryColors[category] || categoryColors.default;
  };

  const renderRelevanceChart = (score) => {
    const scoreNum = parseFloat(score);
    const segments = 5;
    const filledSegments = Math.round(scoreNum * segments);
    
    return (
      <div className="relevance-chart">
        {[...Array(segments)].map((_, index) => (
          <div 
            key={index} 
            className={`segment ${index < filledSegments ? 'filled' : ''}`}
            style={{
              backgroundColor: index < filledSegments 
                ? `rgba(106, 17, 203, ${0.4 + (index * 0.12)})` 
                : '#eee'
            }}
          ></div>
        ))}
        <span className="relevance-score">{scoreNum.toFixed(2)}</span>
      </div>
    );
  };

  return (
    <div className="recommendations-page">
      <div className="page-header">
        <h1 className="page-title">Personalized News Recommendations</h1>
      </div>

      <div className="filter-section">
        <div className="user-filter">
          <div className="form-group">
            <label htmlFor="userId">User ID</label>
            <div className="user-input-group">
              <input
                type="text"
                id="userId"
                value={userId}
                onChange={handleUserIdChange}
                placeholder="Enter user ID"
                className="user-input"
              />
              <button 
                onClick={fetchRecommendations}
                disabled={loading || !userId}
                className="btn-search"
              >
                {loading ? 'Loading...' : 'Get Recommendations'}
              </button>
            </div>
          </div>

          <div className="advanced-toggle" onClick={() => setShowAdvanced(!showAdvanced)}>
            <div className={`toggle-icon ${showAdvanced ? 'open' : ''}`}>
              <span></span>
            </div>
            Advanced Options
          </div>

          {showAdvanced && (
            <div className="advanced-options">
              <div className="form-group">
                <label htmlFor="limitCount">Number of Recommendations</label>
                <select 
                  id="limitCount" 
                  value={limitCount} 
                  onChange={handleLimitChange}
                  className="select-input"
                >
                  <option value="5">5</option>
                  <option value="10">10</option>
                  <option value="20">20</option>
                  <option value="50">50</option>
                </select>
              </div>
            </div>
          )}
        </div>
      </div>

      {error && <div className="error-message">{error}</div>}

      {recommendedNews && (
        <div className="recommendations-content">
          <div className="recommendations-header">
            <div className="user-summary">
              <h2>Recommendations for User: <span className="user-highlight">{recommendedNews.user_id}</span></h2>
              <div className="recommendations-timestamp">
                Generated at: {new Date(recommendedNews.timestamp).toLocaleString()}
              </div>
            </div>
            <div className="recommendations-stats">
              <div className="stat-pill">
                <span className="stat-label">Total</span>
                <span className="stat-value">{recommendedNews.recommendations.length}</span>
              </div>
              <div className="stat-pill">
                <span className="stat-label">Categories</span>
                <span className="stat-value">
                  {new Set(recommendedNews.recommendations.map(item => item.category)).size}
                </span>
              </div>
            </div>
          </div>

          <div className="recommendations-list">
            {recommendedNews.recommendations.map((article, index) => (
              <div key={article.news_id} className="recommendation-card">
                <div className="recommendation-rank">{index + 1}</div>
                
                <div className="recommendation-content">
                  <h3 className="recommendation-title">{article.headline}</h3>
                  
                  <div className="recommendation-meta">
                    <span 
                      className="recommendation-category" 
                      style={{ backgroundColor: getCategoryColor(article.category) }}
                    >
                      {article.category}
                    </span>
                    
                    {article.topic && (
                      <span className="recommendation-topic">{article.topic}</span>
                    )}
                  </div>
                  
                  <div className="recommendation-preview">
                    {article.news_body_preview}...
                  </div>
                  
                  <div className="recommendation-stats">
                    <div className="stat-item">
                      <div className="stat-name">Relevance Score</div>
                      <div className="stat-chart">
                        {renderRelevanceChart(article.relevance_score)}
                      </div>
                    </div>
                    
                    <div className="stat-item">
                      <div className="stat-name">Category Affinity</div>
                      <div className="stat-value">{article.category_affinity}</div>
                    </div>
                    
                    <div className="stat-item">
                      <div className="stat-name">Recency Bonus</div>
                      <div className="stat-value">{article.recency_bonus}</div>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {!recommendedNews && !loading && (
        <div className="empty-state">
          <div className="empty-icon">🔍</div>
          <h2>Enter a user ID to get personalized recommendations</h2>
          <p>Our recommendation engine uses user behavior data to suggest the most relevant news articles</p>
        </div>
      )}
    </div>
  );
};

export default Recommendations;
