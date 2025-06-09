import React, { useEffect, useState } from 'react';
import axios from 'axios';

const NewsVisualization = () => {
    const [newsData, setNewsData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchNewsData = async () => {
            try {
                const response = await axios.get('/api/news'); // Adjust the endpoint as necessary
                setNewsData(response.data);
            } catch (err) {
                setError(err);
            } finally {
                setLoading(false);
            }
        };

        fetchNewsData();
    }, []);

    if (loading) {
        return <div>Loading...</div>;
    }

    if (error) {
        return <div>Error fetching news data: {error.message}</div>;
    }

    return (
        <div>
            <h1>News Visualization</h1>
            <ul>
                {newsData.map((newsItem, index) => (
                    <li key={index}>
                        <h2>{newsItem.title}</h2>
                        <p>{newsItem.description}</p>
                        <p><strong>Published at:</strong> {newsItem.publishedAt}</p>
                    </li>
                ))}
            </ul>
        </div>
    );
};

export default NewsVisualization;