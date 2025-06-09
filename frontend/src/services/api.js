import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000'; // Adjust the base URL as needed

export const fetchNewsData = async () => {
    try {
        const response = await axios.get(`${API_BASE_URL}/news`);
        return response.data;
    } catch (error) {
        console.error('Error fetching news data:', error);
        throw error;
    }
};

export const fetchNewsAnalysis = async (query) => {
    try {
        const response = await axios.get(`${API_BASE_URL}/analyze`, {
            params: { query }
        });
        return response.data;
    } catch (error) {
        console.error('Error fetching news analysis:', error);
        throw error;
    }
};