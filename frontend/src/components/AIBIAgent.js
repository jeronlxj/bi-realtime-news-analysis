import React, { useState } from 'react';
import { queryAIBIAgent } from '../services/api';
import './AIBIAgent.css';

const AIBIAgent = () => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showSql, setShowSql] = useState(false);

  // Example queries to help users
  const exampleQueries = [
    "Show me the most popular news articles from last week with their headlines",
    "Which 3 news categories had the highest click-through rate?",
    "What are the trending headlines in entertainment news?",
    "Show me user engagement statistics by category",
    "List the headlines of the top 5 articles with the longest dwell time"
  ];

  const handleQuerySubmit = async (e) => {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setError(null);
    setResults(null);

    try {
      const response = await queryAIBIAgent(query);
      setResults(response);
    } catch (err) {
      console.error('Error querying AI/BI agent:', err);
      setError(err.response?.data?.error || 'An error occurred while processing your query');
    } finally {
      setLoading(false);
    }
  };

  const handleExampleClick = (exampleQuery) => {
    setQuery(exampleQuery);
  };

  const renderResultsTable = () => {
    if (!results || !results.columns || !results.results || results.results.length === 0) {
      return (
        <div className="aibi-no-results">
          No results found for this query. Try a different question.
        </div>
      );
    }

    return (
      <div className="aibi-table-container">
        <table className="aibi-table">
          <thead>
            <tr>
              {results.columns.map((column, index) => (
                <th key={index}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {results.results.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {results.columns.map((column, colIndex) => (
                  <td key={`${rowIndex}-${colIndex}`}>
                    {formatCellValue(row[column])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  const formatCellValue = (value) => {
    if (value === null || value === undefined) return '-';
    if (typeof value === 'object') return JSON.stringify(value);
    return value.toString();
  };

  return (
    <div className="aibi-container">
      <div className="aibi-header">
        <h2 className="aibi-title">AI/BI Query Assistant</h2>
      </div>

      <p>
        Ask questions about news data in natural language. The AI will translate your query into SQL
        and return results from the database.
      </p>

      <form className="aibi-form" onSubmit={handleQuerySubmit}>
        <div className="aibi-input-container">
          <input
            type="text"
            className="aibi-input"
            placeholder="Ask a question about the news data..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <button
            type="submit"
            className="aibi-button"
            disabled={loading || !query.trim()}
          >
            {loading ? 'Processing...' : 'Ask'}
          </button>
        </div>
      </form>

      <div className="aibi-example-queries">
        <p>Try asking:</p>
        {exampleQueries.map((exampleQuery, index) => (
          <div
            key={index}
            className="aibi-example-query"
            onClick={() => handleExampleClick(exampleQuery)}
          >
            {exampleQuery}
          </div>
        ))}
      </div>

      {loading && (
        <div className="aibi-loading">
          <span>Analyzing your query...</span>
        </div>
      )}

      {error && (
        <div className="aibi-error">
          <strong>Error:</strong> {error}
        </div>
      )}

      {results && !loading && (
        <div className="aibi-results">
          <h3>Results for: "{query}"</h3>
          {renderResultsTable()}

          <div>
            <button
              className="aibi-button"
              style={{ backgroundColor: '#6c757d', marginTop: '10px' }}
              onClick={() => setShowSql(!showSql)}
            >
              {showSql ? 'Hide SQL Query' : 'Show SQL Query'}
            </button>
            
            {showSql && (
              <div className="aibi-sql">
                {results.sql}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default AIBIAgent;
