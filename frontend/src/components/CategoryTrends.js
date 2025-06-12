import React, { useState, useEffect } from 'react';
import { Bar, Doughnut, Line } from 'react-chartjs-2';
import { fetchCategoryTrends } from '../services/api';
import './CategoryTrends.css';

const CategoryTrends = () => {
  const [categoryData, setCategoryData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [dateRange, setDateRange] = useState({
    startDate: '',
    endDate: ''
  });
  const [chartType, setChartType] = useState('bar'); // 'bar', 'doughnut', 'line'
  const [metric, setMetric] = useState('impressions'); // 'impressions', 'clicks', 'ctr'

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      setLoading(true);
      const { startDate, endDate } = dateRange;
      const data = await fetchCategoryTrends(startDate || null, endDate || null);
      setCategoryData(data);
      setError(null);
    } catch (err) {
      console.error('Error fetching category trends:', err);
      setError('Failed to load category trends data');
    } finally {
      setLoading(false);
    }
  };

  const handleDateChange = (e) => {
    const { name, value } = e.target;
    setDateRange(prev => ({ ...prev, [name]: value }));
  };

  const applyFilters = () => {
    fetchData();
  };

  const processChartData = () => {
    if (!categoryData || !categoryData.trends || categoryData.trends.length === 0) {
      return null;
    }

    // Group by category
    const categoryGroups = categoryData.trends.reduce((acc, item) => {
      if (!acc[item.category]) {
        acc[item.category] = {
          impressions: 0,
          clicks: 0,
          ctr: 0,
          count: 0
        };
      }
      
      acc[item.category].impressions += item.impressions || 0;
      acc[item.category].clicks += item.clicks || 0;
      acc[item.category].count += 1;
      
      return acc;
    }, {});

    // Calculate CTR for each category
    Object.keys(categoryGroups).forEach(category => {
      const { impressions, clicks } = categoryGroups[category];
      categoryGroups[category].ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
    });

    // Sort categories by the selected metric
    const sortedCategories = Object.keys(categoryGroups).sort((a, b) => 
      categoryGroups[b][metric] - categoryGroups[a][metric]
    );

    // Prepare chart data
    const backgroundColor = sortedCategories.map((_, index) => {
      const hue = (index * 25) % 360;
      return `hsla(${hue}, 85%, 60%, 0.7)`;
    });
    
    const borderColor = backgroundColor.map(color => color.replace('0.7', '1'));

    const data = {
      labels: sortedCategories,
      datasets: [
        {
          label: metric === 'impressions' ? 'Impressions' : 
                 metric === 'clicks' ? 'Clicks' : 'CTR (%)',
          data: sortedCategories.map(cat => 
            metric === 'ctr' ? 
              categoryGroups[cat].ctr.toFixed(2) : 
              categoryGroups[cat][metric]
          ),
          backgroundColor,
          borderColor,
          borderWidth: 1,
        },
      ],
    };

    return data;
  };

  const renderChart = () => {
    const chartData = processChartData();
    
    if (!chartData) {
      return (
        <div className="no-data">
          <p>No category data available</p>
        </div>
      );
    }

    const options = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'top',
        },
        title: {
          display: true,
          text: metric === 'impressions' ? 'Category Impressions' : 
                metric === 'clicks' ? 'Category Clicks' : 'Category Click-Through Rates',
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
                if (metric === 'ctr') {
                  label += context.parsed.y + '%';
                } else {
                  label += new Intl.NumberFormat().format(context.parsed.y);
                }
              }
              return label;
            }
          }
        }
      },
      scales: chartType !== 'doughnut' ? {
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: metric === 'impressions' ? 'Impressions Count' : 
                  metric === 'clicks' ? 'Clicks Count' : 'CTR (%)',
          },
          ticks: {
            callback: function(value) {
              if (metric === 'ctr') {
                return value + '%';
              }
              return new Intl.NumberFormat().format(value);
            }
          }
        },
      } : undefined,
    };

    switch (chartType) {
      case 'doughnut':
        return <Doughnut data={chartData} options={options} />;
      case 'line':
        return <Line data={chartData} options={options} />;
      default:
        return <Bar data={chartData} options={options} />;
    }
  };

  const renderCategoryTable = () => {
    if (!categoryData || !categoryData.trends || categoryData.trends.length === 0) {
      return null;
    }

    // Group by category
    const categoryGroups = categoryData.trends.reduce((acc, item) => {
      if (!acc[item.category]) {
        acc[item.category] = {
          impressions: 0,
          clicks: 0,
          ctr: 0
        };
      }
      
      acc[item.category].impressions += item.impressions || 0;
      acc[item.category].clicks += item.clicks || 0;
      
      return acc;
    }, {});

    // Calculate CTR for each category
    Object.keys(categoryGroups).forEach(category => {
      const { impressions, clicks } = categoryGroups[category];
      categoryGroups[category].ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
    });

    // Sort categories by the selected metric
    const sortedCategories = Object.keys(categoryGroups).sort((a, b) => 
      categoryGroups[b][metric] - categoryGroups[a][metric]
    );

    return (
      <div className="category-table-container">
        <h3>Category Performance Data</h3>
        <div className="table-wrapper">
          <table className="category-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Category</th>
                <th>Impressions</th>
                <th>Clicks</th>
                <th>CTR (%)</th>
              </tr>
            </thead>
            <tbody>
              {sortedCategories.map((category, index) => {
                const data = categoryGroups[category];
                return (
                  <tr key={category} className={metric === 'impressions' && index === 0 ? 'top-row' : ''}>
                    <td>{index + 1}</td>
                    <td>
                      <span className="category-name">{category}</span>
                    </td>
                    <td>{data.impressions.toLocaleString()}</td>
                    <td>{data.clicks.toLocaleString()}</td>
                    <td>{data.ctr.toFixed(2)}%</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <div className="category-trends-page">
      <div className="page-header">
        <h1 className="page-title">Category Trends Analysis</h1>
      </div>

      <div className="filter-section">
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
            onClick={applyFilters}
            disabled={loading}
            className="btn-apply"
          >
            {loading ? 'Loading...' : 'Apply Filters'}
          </button>
        </div>

        <div className="chart-controls">
          <div className="form-group">
            <label>Chart Type</label>
            <div className="button-group">
              <button 
                className={`btn-chart-type ${chartType === 'bar' ? 'active' : ''}`} 
                onClick={() => setChartType('bar')}
              >
                Bar
              </button>
              <button 
                className={`btn-chart-type ${chartType === 'line' ? 'active' : ''}`} 
                onClick={() => setChartType('line')}
              >
                Line
              </button>
              <button 
                className={`btn-chart-type ${chartType === 'doughnut' ? 'active' : ''}`} 
                onClick={() => setChartType('doughnut')}
              >
                Doughnut
              </button>
            </div>
          </div>

          <div className="form-group">
            <label>Metric</label>
            <div className="button-group">
              <button 
                className={`btn-metric ${metric === 'impressions' ? 'active' : ''}`} 
                onClick={() => setMetric('impressions')}
              >
                Impressions
              </button>
              <button 
                className={`btn-metric ${metric === 'clicks' ? 'active' : ''}`} 
                onClick={() => setMetric('clicks')}
              >
                Clicks
              </button>
              <button 
                className={`btn-metric ${metric === 'ctr' ? 'active' : ''}`} 
                onClick={() => setMetric('ctr')}
              >
                CTR
              </button>
            </div>
          </div>
        </div>
      </div>

      {error && <div className="error-message">{error}</div>}

      <div className="trends-content">
        <div className="chart-section">
          {loading ? (
            <div className="loading">
              <div className="loading-spinner"></div>
              <span>Loading category data...</span>
            </div>
          ) : (
            renderChart()
          )}
        </div>

        <div className="table-section">
          {loading ? (
            <div className="loading">
              <div className="loading-spinner"></div>
              <span>Loading data table...</span>
            </div>
          ) : (
            renderCategoryTable()
          )}
        </div>
      </div>
    </div>
  );
};

export default CategoryTrends;
