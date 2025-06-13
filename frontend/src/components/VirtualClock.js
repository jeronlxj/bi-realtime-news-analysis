import React, { useState, useEffect, useCallback } from 'react';
import './VirtualClock.css';
import { fetchVirtualTime } from '../services/api';

const VirtualClock = () => {
  const [timeData, setTimeData] = useState({
    virtualTime: '',
    realTime: '',
  });
  const [loading, setLoading] = useState(true);

  // Fetch time data from the backend with useCallback to prevent re-creating on every render
  const fetchTimeData = useCallback(async () => {
    try {
      const data = await fetchVirtualTime();
      setTimeData({
        virtualTime: data.virtual_time,
        realTime: data.real_time,
      });
      setLoading(false);
    } catch (error) {
      console.error('Error fetching time data:', error);
      setLoading(false);
    }
  }, []); // Empty dependency array means this function won't change

  // Fetch time initially and every minute
  useEffect(() => {
    fetchTimeData();
    const interval = setInterval(fetchTimeData, 60000);
    return () => clearInterval(interval);
  }, [fetchTimeData]);

  if (loading) {
    return <div className="virtual-clock loading">Loading time data...</div>;
  }

  return (
    <div className="virtual-clock">
      <div className="virtual-time">
        <h3>Virtual Time</h3>
        <div className="time-display">{timeData.virtualTime}</div>
        <div className="time-label">Analysis based on this time range</div>
      </div>
      <div className="real-time">
        <h3>Real Time</h3>
        <div className="time-display small">{timeData.realTime}</div>
      </div>
    </div>
  );
};

export default VirtualClock;
