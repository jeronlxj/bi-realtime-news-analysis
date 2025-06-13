import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Dashboard from './components/Dashboard';
import NewsLifecycle from './components/NewsLifecycle';
import CategoryTrends from './components/CategoryTrends';
import UserInterests from './components/UserInterests';
import HotNews from './components/HotNews';
import Recommendations from './components/Recommendations';
import Performance from './components/Performance';
import Navbar from './components/Navbar';
import './styles/App.css';

function App() {
  return (
    <Router>
      <div className="App">
        <Navbar />
        <main className="content">
          <Routes>
            <Route path="/" element={<Navigate to="/dashboard" replace />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/news-lifecycle" element={<NewsLifecycle />} />
            <Route path="/category-trends" element={<CategoryTrends />} />
            <Route path="/user-interests" element={<UserInterests />} />
            <Route path="/hot-news" element={<HotNews />} />
            <Route path="/recommendations" element={<Recommendations />} />
            <Route path="/performance" element={<Performance />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
