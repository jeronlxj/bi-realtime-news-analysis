# Frontend Real-Time News BI Analysis

This project is a real-time dynamic change analysis system for news topics, implemented using React for the frontend. It interacts with a backend API to fetch and visualize news data.

## Project Structure

- **src/**: Contains the source code for the frontend application.
  - **App.js**: The main entry point for the React application.
  - **components/**: Contains React components for the application.
    - **NewsVisualization.js**: Component responsible for visualizing news data.
  - **services/**: Contains service files for API interactions.
    - **api.js**: Functions to interact with the backend API.

## Setup Instructions

1. **Clone the repository**:
   ```
   git clone <repository-url>
   cd realtime-news-bi-analysis/frontend
   ```

2. **Install dependencies**:
   ```
   npm install
   ```

3. **Run the application**:
   ```
   npm start
   ```

4. **Access the application**:
   Open your browser and navigate to `http://localhost:3000`.

## Component Descriptions

- **App.js**: Initializes the React application and sets up routing for different components.
- **NewsVisualization.js**: Fetches news data from the backend and displays it in a user-friendly format.

## Additional Information

For more details on the backend API, refer to the `README.md` file located in the `api` directory.