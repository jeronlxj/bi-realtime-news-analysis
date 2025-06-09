import React from 'react';
import { BrowserRouter as Router, Route, Switch } from 'react-router-dom';
import NewsVisualization from './components/NewsVisualization';

function App() {
  return (
    <Router>
      <div className="App">
        <Switch>
          <Route path="/" exact component={NewsVisualization} />
          {/* Additional routes can be added here */}
        </Switch>
      </div>
    </Router>
  );
}

export default App;