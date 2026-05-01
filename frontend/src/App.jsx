import { useState } from 'react';
import './App.css';

function App() {
  const [inputData, setInputData] = useState('{\n  "feature_1": 0,\n  "feature_2": 1\n}');
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const handlePredict = async () => {
    try {
      setError(null);
      setResult(null);
      setLoading(true);

      let parsedData = {};
      try {
        parsedData = JSON.parse(inputData);
      } catch (e) {
        throw new Error("Invalid JSON format in the input field.", { cause: e });
      }

      const response = await fetch('http://localhost:5050/api/predict', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(parsedData)
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || "An error occurred");
      }

      setResult(data);
    } catch (err) {
      setError(err.message || "An error occurred");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container">
      <header className="header">
        <h1>Predict Customer Churn</h1>
      </header>

      <main className="main-content">
        <div className="input-section">
          <h3>Input Data (JSON)</h3>
          <textarea
            value={inputData}
            onChange={(e) => setInputData(e.target.value)}
            rows={10}
          />
          <button onClick={handlePredict} disabled={loading}>
            {loading ? 'Predicting...' : 'Get Prediction'}
          </button>
        </div>

        <div className="result-section">
          <h3>Results</h3>
          {error && <div className="error-box"><strong>Error:</strong> {error}</div>}
          {result && (
            <div className="success-box">
              <pre>{JSON.stringify(result, null, 2)}</pre>
            </div>
          )}
          {!error && !result && <div className="placeholder">Results will appear here...</div>}
        </div>
      </main>
    </div>
  );
}

export default App;
