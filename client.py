from flask import Flask, render_template, request, jsonify
import requests
import logging
import json

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Client - %(levelname)s - %(message)s')

app = Flask(__name__)
MASTER_URL = "http://localhost:5001"

# Create templates directory and HTML files
import os
if not os.path.exists('templates'):
    os.makedirs('templates')

# Create HTML template files
with open('templates/index.html', 'w') as f:
    f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>Web Crawler Search Interface</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding: 20px; background-color: #ffffff; color: #333333; }
        .search-results { margin-top: 20px; }
        .alert { margin-top: 10px; }
        .card { background-color: #ffffff; border: 1px solid #dee2e6; }
        .card-title { color: #333333; }
        .list-group-item {
            background-color: #ffffff;
            border: 1px solid #dee2e6;
            color: #333333;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
        }
        .list-group-item a {
            color: #0056b3;
            text-decoration: none;
        }
        .list-group-item a:hover {
            color: #003d82;
            text-decoration: underline;
        }
        .badge {
            background-color: #6c757d !important;
            color: #ffffff;
        }
        .form-control {
            background-color: #ffffff;
            border: 1px solid #ced4da;
            color: #333333;
        }
        .form-control:focus {
            background-color: #ffffff;
            border-color: #80bdff;
            color: #333333;
            box-shadow: 0 0 0 0.25rem rgba(0, 123, 255, 0.25);
        }
        .btn-primary {
            background-color: #0056b3;
            border-color: #0056b3;
        }
        .btn-primary:hover {
            background-color: #003d82;
            border-color: #003d82;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="mb-4">Web Crawler Search Interface</h1>
        
        <!-- Search Form -->
        <div class="card mb-4">
            <div class="card-body">
                <h5 class="card-title">Search</h5>
                <form id="searchForm" class="mb-3">
                    <div class="input-group">
                        <input type="text" id="searchQuery" class="form-control" placeholder="Enter search query">
                        <button type="submit" class="btn btn-primary">Search</button>
                    </div>
                </form>
                <div id="searchResults"></div>
            </div>
        </div>

        <!-- System Status -->
        <div class="card mb-4">
            <div class="card-body">
                <h5 class="card-title">System Status</h5>
                <div id="statusContent">
                    <div class="text-center">
                        <div class="spinner-border text-primary" role="status">
                            <span class="visually-hidden">Loading...</span>
                        </div>
                        <p>Loading system status...</p>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Function to update status
        function updateStatus() {
            fetch('/status')
                .then(response => response.json())
                .then(data => {
                    if (Object.keys(data).length === 0) {
                        document.getElementById('statusContent').innerHTML = `
                            <div class="alert alert-warning">
                                <h6>System Status Unavailable</h6>
                                <p>Please ensure that:</p>
                                <ul>
                                    <li>The master node is running (python master_node.py)</li>
                                    <li>At least one crawler is running (python crawler_node.py)</li>
                                    <li>The indexer is running (python indexer_node.py)</li>
                                </ul>
                            </div>
                        `;
                        return;
                    }
                    
                    let html = `
                        <div class="row">
                            <div class="col-md-6">
                                <div class="alert alert-info">
                                    <h6>Crawler Status</h6>
                                    <p><strong>Active Crawlers:</strong> ${data.active_crawlers || 0}/2</p>
                                    <p><strong>URLs Crawled:</strong> ${data.urls_crawled || 0}</p>
                                    <p><strong>URLs in Queue:</strong> ${data.urls_in_queue || 0}</p>
                                    <p><strong>Failed URLs:</strong> ${data.failed_urls || 0}</p>
                                    <p><strong>Error Rate:</strong> ${((data.failed_urls || 0) / (data.urls_crawled || 1) * 100).toFixed(2)}%</p>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div class="alert alert-info">
                                    <h6>Indexer Status</h6>
                                    <p><strong>URLs Indexed:</strong> ${data.urls_indexed || 0}</p>
                                    <p><strong>Indexing Progress:</strong> ${((data.urls_indexed || 0) / (data.urls_crawled || 1) * 100).toFixed(2)}%</p>
                                    <p><strong>Total Links Found:</strong> ${data.total_links_found || 0}</p>
                                    <p><strong>Average Links/Page:</strong> ${(data.average_links_per_page || 0).toFixed(2)}</p>
                                </div>
                            </div>
                        </div>
                        <div class="row mt-3">
                            <div class="col-12">
                                <div class="alert alert-secondary">
                                    <h6>System Overview</h6>
                                    <p><strong>Domains Crawled:</strong> ${(data.domains_crawled || []).length}</p>
                                    <p><strong>Filtered URLs:</strong> ${data.filtered_urls || 0}</p>
                                    <p><strong>URLs in Progress:</strong> ${(data.urls_in_progress || []).length}</p>
                                </div>
                            </div>
                        </div>
                    `;
                    document.getElementById('statusContent').innerHTML = html;
                })
                .catch(error => {
                    document.getElementById('statusContent').innerHTML = `
                        <div class="alert alert-danger">
                            <h6>Error</h6>
                            <p>Failed to fetch system status. Please ensure the master node is running.</p>
                        </div>
                    `;
                });
        }

        // Update status every 5 seconds
        setInterval(updateStatus, 5000);
        updateStatus();  // Initial update

        // Handle search form
        document.getElementById('searchForm').onsubmit = function(e) {
            e.preventDefault();
            const query = document.getElementById('searchQuery').value;
            if (!query.trim()) {
                document.getElementById('searchResults').innerHTML = `
                    <div class="alert alert-warning">Please enter a search query</div>
                `;
                return;
            }
            
            document.getElementById('searchResults').innerHTML = `
                <div class="text-center">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Searching...</span>
                    </div>
                </div>
            `;
            
            fetch(`/search?q=${encodeURIComponent(query)}`)
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById('searchResults').innerHTML = `
                            <div class="alert alert-warning">${data.error}</div>
                        `;
                        return;
                    }
                    
                    if (!Array.isArray(data) || data.length === 0) {
                        document.getElementById('searchResults').innerHTML = `
                            <div class="alert alert-info">No results found for "${query}"</div>
                        `;
                        return;
                    }
                    
                    let html = '<h6 class="mt-3">Search Results:</h6><div class="list-group">';
                    data.forEach(result => {
                        // Handle both array format [url, score] and object format {url, score}
                        const url = Array.isArray(result) ? result[0] : result.url;
                        const score = Array.isArray(result) ? result[1] : result.score;
                        
                        html += `<div class="list-group-item">
                            <div class="d-flex justify-content-between align-items-center">
                                <a href="${url}" target="_blank" class="text-break" style="word-wrap: break-word;">${url}</a>
                                <span class="badge bg-secondary ms-2">score: ${Number(score).toFixed(9)}</span>
                            </div>
                        </div>`;
                    });
                    html += '</div>';
                    document.getElementById('searchResults').innerHTML = html;
                })
                .catch(error => {
                    console.error('Search error:', error);
                    document.getElementById('searchResults').innerHTML = `
                        <div class="alert alert-danger">Error performing search. Please try again.</div>
                    `;
                });
        };
    </script>
</body>
</html>
    """)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/test')
def test():
    response = requests.get(f"{MASTER_URL}/test")
    return (response.text, response.status_code, response.headers.items())

@app.route('/search')
def search():
    query = request.args.get('q', '')
    if not query.strip():
        logging.warning("Empty search query received")
        return jsonify([])
    
    try:
        logging.info(f"Sending search request to master for query: {query}")
        response = requests.get(f"{MASTER_URL}/search", params={"q": query}, timeout=15)
        logging.info(f"Received response from master: Status {response.status_code}")
        
        try:
            response_data = response.json()
            logging.info(f"Response data: {json.dumps(response_data)}")
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON response: {e}")
            return jsonify({"error": "Invalid response from search service"}), 500
        
        if response.status_code == 200:
            if isinstance(response_data, list):
                logging.info(f"Received {len(response_data)} search results from master")
                return jsonify(response_data)
            else:
                logging.warning(f"Unexpected response format: {type(response_data)}")
                return jsonify({"error": "Invalid response format"}), 500
        else:
            logging.error(f"Master node returned error status: {response.status_code}")
            error_msg = response_data.get('error', 'Unknown error') if isinstance(response_data, dict) else 'Unknown error'
            return jsonify({"error": error_msg}), response.status_code
            
    except requests.exceptions.Timeout:
        logging.error("Request to master node timed out")
        return jsonify({"error": "Search request timed out"}), 504
    except requests.exceptions.ConnectionError:
        logging.error("Could not connect to master node")
        return jsonify({"error": "Could not connect to search service"}), 503
    except Exception as e:
        logging.error(f"Error searching: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/status')
def status():
    try:
        response = requests.get(f"{MASTER_URL}/status", timeout=10)
        return (response.text, response.status_code, response.headers.items())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logging.info("Starting client application")
    # Run on a different port than the master node
    app.run(host='0.0.0.0', port=5002, debug=True) 