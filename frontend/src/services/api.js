import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

// Create axios instance with default config
const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000, // 30 seconds timeout
  headers: {
    'Content-Type': 'application/json',
  },
});

// Basic news data endpoints
export const fetchNewsData = async () => {
  try {
    const response = await api.get('/api/news');
    // Extract the data array from the response object
    return response.data.data || [];
  } catch (error) {
    console.error('Error fetching news data:', error);
    throw error;
  }
};

export const fetchNewsAnalysis = async (query) => {
  try {
    const response = await api.get('/api/analyze', {
      params: { query }
    });
    return response.data;
  } catch (error) {
    console.error('Error fetching news analysis:', error);
    throw error;
  }
};

// Analytics endpoints
export const fetchAnalyticsOverview = async () => {
  try {
    const response = await api.get('/api/analytics/overview');
    return response.data;
  } catch (error) {
    console.error('Error fetching analytics overview:', error);
    throw error;
  }
};

export const fetchNewsLifecycle = async (newsId, startDate = null, endDate = null) => {
  try {
    const params = {};
    if (startDate) params.start_date = startDate;
    if (endDate) params.end_date = endDate;
    
    const response = await api.get(`/api/analytics/news-lifecycle/${newsId}`, { params });
    return response.data;
  } catch (error) {
    console.error('Error fetching news lifecycle:', error);
    throw error;
  }
};

export const fetchCategoryTrends = async (startDate = null, endDate = null) => {
  try {
    const params = {};
    if (startDate) params.start_date = startDate;
    if (endDate) params.end_date = endDate;
    
    const response = await api.get('/api/analytics/category-trends', { params });
    return response.data;
  } catch (error) {
    console.error('Error fetching category trends:', error);
    throw error;
  }
};

export const fetchUserInterests = async (userId = null, startDate = null, endDate = null) => {
  try {
    const params = {};
    if (userId) params.user_id = userId;
    if (startDate) params.start_date = startDate;
    if (endDate) params.end_date = endDate;
    
    const response = await api.get('/api/analytics/user-interests', { params });
    return response.data;
  } catch (error) {
    console.error('Error fetching user interests:', error);
    throw error;
  }
};

export const fetchHotNews = async (hoursAhead = 24, minImpressions = 50) => {
  try {
    const response = await api.get('/api/analytics/hot-news', {
      params: {
        hours_ahead: hoursAhead,
        min_impressions: minImpressions
      }
    });
    return response.data;
  } catch (error) {
    console.error('Error fetching hot news:', error);
    throw error;
  }
};

export const fetchUserRecommendations = async (userId, limit = 10) => {
  try {
    const response = await api.get(`/api/analytics/recommendations/${userId}`, {
      params: { limit }
    });
    return response.data;
  } catch (error) {
    console.error('Error fetching user recommendations:', error);
    throw error;
  }
};

export const fetchPerformanceStats = async (hours = 24) => {
  try {
    const response = await api.get('/api/analytics/performance', {
      params: { hours }
    });
    return response.data;
  } catch (error) {
    console.error('Error fetching performance stats:', error);
    throw error;
  }
};

// Spark Streaming endpoints
export const startTrendAnalysis = async () => {
  try {
    const response = await api.post('/api/spark/start-trend-analysis');
    return response.data;
  } catch (error) {
    console.error('Error starting trend analysis:', error);
    throw error;
  }
};

export const startUserBehaviorAnalysis = async () => {
  try {
    const response = await api.post('/api/spark/start-user-behavior');
    return response.data;
  } catch (error) {
    console.error('Error starting user behavior analysis:', error);
    throw error;
  }
};

export const startAnomalyDetection = async () => {
  try {
    const response = await api.post('/api/spark/start-anomaly-detection');
    return response.data;
  } catch (error) {
    console.error('Error starting anomaly detection:', error);
    throw error;
  }
};

export const fetchRealTimeInsights = async (type = 'overview') => {
  try {
    const response = await api.get('/api/spark/real-time-insights', {
      params: { type }
    });
    return response.data;
  } catch (error) {
    console.error('Error fetching real-time insights:', error);
    throw error;
  }
};

export const fetchActiveSparkQueries = async () => {
  try {
    const response = await api.get('/api/spark/queries');
    return response.data;
  } catch (error) {
    console.error('Error fetching active Spark queries:', error);
    throw error;
  }
};

export const stopSparkQuery = async (queryId) => {
  try {
    const response = await api.delete(`/api/spark/queries/${queryId}`);
    return response.data;
  } catch (error) {
    console.error('Error stopping Spark query:', error);
    throw error;
  }
};

export const stopAllSparkQueries = async () => {
  try {
    const response = await api.post('/api/spark/stop-all');
    return response.data;
  } catch (error) {
    console.error('Error stopping all Spark queries:', error);
    throw error;
  }
};

// ETL control endpoints
export const runETLPipeline = async () => {
  try {
    const response = await api.post('/api/run_etl');
    return response.data;
  } catch (error) {
    console.error('Error running ETL pipeline:', error);
    throw error;
  }
};

// Virtual time endpoint
export const fetchVirtualTime = async () => {
  try {
    const response = await api.get('/api/time/current');
    return response.data;
  } catch (error) {
    console.error('Error fetching virtual time:', error);
    throw error;
  }
};

// Utility functions
export const formatDate = (date) => {
  if (!date) return null;
  return date instanceof Date ? date.toISOString() : date;
};

export const createDateRange = (days) => {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - days);
  return {
    start_date: formatDate(start),
    end_date: formatDate(end)
  };
};

export default api;