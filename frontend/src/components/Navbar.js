import React, { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import './Navbar.css';

const Navbar = () => {
  const location = useLocation();
  const [isOpen, setIsOpen] = useState(false);

  const toggleMenu = () => {
    setIsOpen(!isOpen);
  };

  const navLinks = [
    { path: '/dashboard', label: 'Dashboard' },
    { path: '/news-lifecycle', label: 'News Lifecycle' },
    { path: '/category-trends', label: 'Category Trends' },
    { path: '/user-interests', label: 'User Interests' },
    { path: '/hot-news', label: 'Hot News' },
    { path: '/recommendations', label: 'Recommendations' },
    { path: '/performance', label: 'Performance' },
  ];

  return (
    <nav className="navbar">
      <div className="navbar-logo">
        <Link to="/">
          <span className="logo-text">NewsFlash</span>
          <span className="logo-accent">Analytics</span>
        </Link>
      </div>

      <div className={`navbar-links ${isOpen ? 'active' : ''}`}>
        {navLinks.map((link) => (
          <Link
            key={link.path}
            to={link.path}
            className={location.pathname === link.path ? 'active' : ''}
            onClick={() => setIsOpen(false)}
          >
            {link.label}
          </Link>
        ))}
      </div>

      <div className="navbar-menu-icon" onClick={toggleMenu}>
        <div className={`bar ${isOpen ? 'change' : ''}`}></div>
        <div className={`bar ${isOpen ? 'change' : ''}`}></div>
        <div className={`bar ${isOpen ? 'change' : ''}`}></div>
      </div>
    </nav>
  );
};

export default Navbar;
