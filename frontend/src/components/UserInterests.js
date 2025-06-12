import React, { useState, useEffect } from 'react';
import { Line, Radar } from 'react-chartjs-2';
import { fetchUserInterests } from '../services/api';
import './UserInterests.css';

const UserInterests = () => {
  const [userId, setUserId] = useState('');
  const [userData, setUserData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [dateRange, setDateRange] = useState({
    startDate: '',
    endDate: ''
  });
  const [chartType, setChartType] = useState('line'); // 'line' or 'radar'
  const [activeTab, setActiveTab] = useState('chart'); // 'chart' or 'table'

  const handleUserIdChange = (e) => {
    setUserId(e.target.value);
  };

  const handleDateChange = (e) => {
    const { name, value } = e.target;
    setDateRange(prev => ({ ...prev, [name]: value }));
  };

  const fetchData = async () => {
    if (!userId) {
      setError('Please enter a user ID');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      const { startDate, endDate } = dateRange;
      const data = await fetchUserInterests(userId, startDate || null, endDate || null);
      setUserData(data);
    } catch (err) {
      console.error('Error fetching user interests:', err);
      setError('Failed to load user interest data');
    } finally {
      setLoading(false);
    }
  };

  const renderChart = () => {
    if (!userData || !userData.user_interests || userData.user_interests.length === 0) {
      return (
        <div className="no-data">
          <p>No interest data available for this user</p>
        </div>
      );
    }

    // Group data by timestamp to show evolution over time
    const interestsByTime = userData.user_interests.reduce((acc, entry) => {
      const timestamp = new Date(entry.timestamp).toLocaleDateString();
      
      if (!acc[timestamp]) {
        acc[timestamp] = {};
      }
      
      // Merge categories for this timestamp
      Object.entries(entry.category_interests || {}).forEach(([category, value]) => {
        acc[timestamp][category] = (acc[timestamp][category] || 0) + value;
      });
      
      return acc;
    }, {});

    // Get all unique categories and timestamps
    const allCategories = new Set();
    Object.values(interestsByTime).forEach(interests => {
      Object.keys(interests).forEach(category => allCategories.add(category));
    });
    
    const categories = Array.from(allCategories);
    const timestamps = Object.keys(interestsByTime).sort((a, b) => 
      new Date(a) - new Date(b)
    );

    // Generate colors for each category
    const categoryColors = categories.reduce((acc, category, index) => {
      const hue = (index * 25) % 360;
      acc[category] = {
        borderColor: `hsla(${hue}, 85%, 60%, 1)`,
        backgroundColor: `hsla(${hue}, 85%, 60%, 0.2)`
      };
      return acc;
    }, {});

    if (chartType === 'radar') {
      // For radar chart - use the latest timestamp data
      const latestTimestamp = timestamps[timestamps.length - 1];
      const latestInterests = interestsByTime[latestTimestamp] || {};
      
      const radarData = {
        labels: categories,
        datasets: [
          {
            label: `User Interests (${latestTimestamp})`,
            data: categories.map(category => latestInterests[category] || 0),
            backgroundColor: 'rgba(106, 17, 203, 0.2)',
            borderColor: 'rgba(106, 17, 203, 1)',
            borderWidth: 2,
            pointBackgroundColor: 'rgba(106, 17, 203, 1)',
            pointBorderColor: '#fff',
            pointHoverBackgroundColor: '#fff',
            pointHoverBorderColor: 'rgba(106, 17, 203, 1)'
          }
        ]
      };

      const radarOptions = {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          r: {
            beginAtZero: true,
            ticks: {
              stepSize: 1
            }
          }
        },
        plugins: {
          title: {
            display: true,
            text: `User ${userId} Interest Profile`,
            font: {
              size: 16,
              weight: 'bold',
            },
          },
          legend: {
            display: false
          }
        }
      };

      return <Radar data={radarData} options={radarOptions} />;
    } else {
      // For line chart - show evolution over time
      const lineData = {
        labels: timestamps,
        datasets: categories.map(category => ({
          label: category,
          data: timestamps.map(timestamp => interestsByTime[timestamp][category] || 0),
          borderColor: categoryColors[category].borderColor,
          backgroundColor: categoryColors[category].backgroundColor,
          borderWidth: 2,
          tension: 0.3,
          fill: false,
        }))
      };

      const lineOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: `User ${userId} Interest Evolution Over Time`,
            font: {
              size: 16,
              weight: 'bold',
            },
          },
          tooltip: {
            mode: 'index',
            intersect: false,
          }
        },
        scales: {
          x: {
            title: {
              display: true,
              text: 'Date'
            }
          },
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: 'Interest Level'
            }
          }
        },
      };

      return <Line data={lineData} options={lineOptions} />;
    }
  };

  const renderUserStats = () => {
    if (!userData || !userData.user_interests || userData.user_interests.length === 0) {
      return null;
    }

    // Aggregate all interests and calculate statistics
    const allInterests = userData.user_interests.reduce((acc, entry) => {
      Object.entries(entry.category_interests || {}).forEach(([category, value]) => {
        if (!acc[category]) {
          acc[category] = {
            totalInterest: 0,
            occurrences: 0,
            trend: 0
          };
        }
        
        acc[category].totalInterest += value;
        acc[category].occurrences += 1;
      });
      
      return acc;
    }, {});

    // Calculate average interest and identify trends
    Object.keys(allInterests).forEach(category => {
      allInterests[category].averageInterest = 
        allInterests[category].totalInterest / allInterests[category].occurrences;
        
      // Simple trend calculation (positive change over time)
      const interestsOverTime = userData.user_interests.map(entry => 
        entry.category_interests?.[category] || 0
      );
      
      if (interestsOverTime.length > 1) {
        const firstValue = interestsOverTime[0];
        const lastValue = interestsOverTime[interestsOverTime.length - 1];
        allInterests[category].trend = lastValue - firstValue;
      }
    });

    // Find top interests
    const sortedInterests = Object.entries(allInterests)
      .sort((a, b) => b[1].averageInterest - a[1].averageInterest);
    
    const topInterests = sortedInterests.slice(0, 5);
    const risingInterests = [...sortedInterests]
      .sort((a, b) => b[1].trend - a[1].trend)
      .filter(item => item[1].trend > 0)
      .slice(0, 3);
    
    return (
      <div className="user-stats">
        <div className="stats-grid">
          <div className="stat-card">
            <h3>Total Categories</h3>
            <div className="stat-value">{Object.keys(allInterests).length}</div>
          </div>
          
          <div className="stat-card">
            <h3>Data Points</h3>
            <div className="stat-value">{userData.user_interests.length}</div>
          </div>
          
          <div className="stat-card">
            <h3>Primary Interest</h3>
            <div className="stat-value">{topInterests[0]?.[0] || '-'}</div>
          </div>
          
          <div className="stat-card">
            <h3>Rising Interest</h3>
            <div className="stat-value">{risingInterests[0]?.[0] || '-'}</div>
          </div>
        </div>

        <div className="interest-details">
          <h3>Top Interests</h3>
          <div className="interest-bars">
            {topInterests.map(([category, data]) => (
              <div key={category} className="interest-bar-item">
                <div className="interest-label">{category}</div>
                <div className="interest-bar-container">
                  <div 
                    className="interest-bar-fill" 
                    style={{ 
                      width: `${Math.min(data.averageInterest * 10, 100)}%`,
                      backgroundColor: data.trend > 0 ? 'var(--success-color)' : 
                                       data.trend < 0 ? 'var(--accent-color)' : 
                                       'var(--secondary-color)'
                    }}
                  ></div>
                </div>
                <div className="interest-value">{data.averageInterest.toFixed(2)}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  };

  const renderTable = () => {
    if (!userData || !userData.user_interests || userData.user_interests.length === 0) {
      return (
        <div className="no-data">
          <p>No interest data available for this user</p>
        </div>
      );
    }

    // Aggregate all categories from all entries
    const allCategories = new Set();
    userData.user_interests.forEach(entry => {
      Object.keys(entry.category_interests || {}).forEach(category => {
        allCategories.add(category);
      });
    });

    // Sort entries by timestamp
    const sortedEntries = [...userData.user_interests].sort((a, b) => 
      new Date(a.timestamp) - new Date(b.timestamp)
    );

    return (
      <div className="interests-table-container">
        <h3>User Interest Data</h3>
        <div className="table-wrapper">
          <table className="interests-table">
            <thead>
              <tr>
                <th>Date</th>
                {Array.from(allCategories).map(category => (
                  <th key={category}>{category}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedEntries.map((entry, index) => (
                <tr key={index}>
                  <td>{new Date(entry.timestamp).toLocaleDateString()}</td>
                  {Array.from(allCategories).map(category => (
                    <td key={category}>
                      {entry.category_interests?.[category] || 0}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <div className="user-interests-page">
      <div className="page-header">
        <h1 className="page-title">User Interest Analysis</h1>
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
                onClick={fetchData}
                disabled={loading || !userId}
                className="btn-search"
              >
                {loading ? 'Loading...' : 'Search'}
              </button>
            </div>
          </div>

          <div className="date-range-group">
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
          </div>
        </div>

        {userData && (
          <div className="visualization-controls">
            <div className="tabs">
              <button 
                className={`tab-button ${activeTab === 'chart' ? 'active' : ''}`} 
                onClick={() => setActiveTab('chart')}
              >
                Chart View
              </button>
              <button 
                className={`tab-button ${activeTab === 'table' ? 'active' : ''}`} 
                onClick={() => setActiveTab('table')}
              >
                Table View
              </button>
            </div>

            {activeTab === 'chart' && (
              <div className="chart-type-selector">
                <button 
                  className={`chart-type-btn ${chartType === 'line' ? 'active' : ''}`} 
                  onClick={() => setChartType('line')}
                >
                  Timeline
                </button>
                <button 
                  className={`chart-type-btn ${chartType === 'radar' ? 'active' : ''}`} 
                  onClick={() => setChartType('radar')}
                >
                  Radar
                </button>
              </div>
            )}
          </div>
        )}
      </div>

      {error && <div className="error-message">{error}</div>}

      {userData && (
        <div className="interests-content">
          {renderUserStats()}
          
          <div className="visualization-section">
            {activeTab === 'chart' ? (
              <div className="chart-section">
                {loading ? (
                  <div className="loading">
                    <div className="loading-spinner"></div>
                    <span>Loading interest data...</span>
                  </div>
                ) : (
                  renderChart()
                )}
              </div>
            ) : (
              <div className="table-section">
                {loading ? (
                  <div className="loading">
                    <div className="loading-spinner"></div>
                    <span>Loading interest data...</span>
                  </div>
                ) : (
                  renderTable()
                )}
              </div>
            )}
          </div>
        </div>
      )}

      {!userData && !loading && (
        <div className="empty-state">
          <div className="empty-icon">🔍</div>
          <h2>Search for a user</h2>
          <p>Enter a user ID to analyze their interest patterns over time</p>
        </div>
      )}
    </div>
  );
};

export default UserInterests;
