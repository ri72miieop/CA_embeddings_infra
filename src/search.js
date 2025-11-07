document.addEventListener('DOMContentLoaded', () => {
    const searchForm = document.getElementById('searchForm');
    const searchButton = document.getElementById('searchButton');
    const searchQuery = document.getElementById('searchQuery');
    const resultCount = document.getElementById('resultCount');
    const threshold = document.getElementById('threshold');
    // Metadata filter elements (commented out for now)
    // const filterText = document.getElementById('filterText');
    // const filterHasQuotes = document.getElementById('filterHasQuotes');
    // const filterTokensOperator = document.getElementById('filterTokensOperator');
    // const filterTokensValue = document.getElementById('filterTokensValue');
    const resultsSection = document.getElementById('resultsSection');
    const resultsContainer = document.getElementById('resultsContainer');
    const resultsCount = document.getElementById('resultsCount');

    // Enable/disable tokens value input based on operator selection (commented out)
    // filterTokensOperator.addEventListener('change', () => {
    //     filterTokensValue.disabled = !filterTokensOperator.value;
    //     if (!filterTokensOperator.value) {
    //         filterTokensValue.value = '';
    //     }
    // });

    // Handle Enter key in form fields
    searchForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        e.stopPropagation();
        await performSearch();
        return false;
    });

    // Handle button click
    searchButton.addEventListener('click', async (e) => {
        e.preventDefault();
        e.stopPropagation();
        await performSearch();
        return false;
    });

    async function performSearch() {
        const query = searchQuery.value.trim();
        const k = parseInt(resultCount.value);
        const thresholdValue = threshold.value ? parseFloat(threshold.value) : undefined;

        // Validate inputs
        if (!query) {
            showError('Please enter a search query');
            return;
        }

        if (k < 1 || k > 1000) {
            showError('Result count must be between 1 and 1000');
            return;
        }

        if (thresholdValue !== undefined && (thresholdValue < 0 || thresholdValue > 1)) {
            showError('Threshold must be between 0.0 and 1.0');
            return;
        }

            // Build metadata filter object from form fields (commented out for now)
            // const filterObject = {};
            // let hasFilters = false;

            // // Text filter
            // if (filterText.value.trim()) {
            //     filterObject.text = filterText.value.trim();
            //     hasFilters = true;
            // }

            // // Has quotes filter (only add if checked)
            // if (filterHasQuotes.checked) {
            //     filterObject.has_quotes = true;
            //     hasFilters = true;
            // }

            // // Tokens filter
            // const tokensOp = filterTokensOperator.value;
            // const tokensVal = filterTokensValue.value;
            // if (tokensOp && tokensVal) {
            //     const tokenValue = parseInt(tokensVal);
            //     if (isNaN(tokenValue) || tokenValue < 0) {
            //         showError('Token value must be a positive number');
            //         return;
            //     }

            //     // Build the filter based on operator
            //     if (tokensOp === 'gte') {
            //         filterObject.tokens = { $gte: tokenValue };
            //     } else if (tokensOp === 'lte') {
            //         filterObject.tokens = { $lte: tokenValue };
            //     } else if (tokensOp === 'eq') {
            //         filterObject.tokens = tokenValue;
            //     }
            //     hasFilters = true;
            // }

        // Show loading state
        searchButton.disabled = true;
        searchButton.textContent = 'Searching...';
        resultsSection.classList.remove('hidden');
        resultsContainer.innerHTML = '<div class="loading"><div class="spinner"></div><p>Searching...</p></div>';

        try {
            const requestBody = {
                searchTerm: query,
                k: k
            };

                if (thresholdValue !== undefined) {
                    requestBody.threshold = thresholdValue;
                }

                // Metadata filters (commented out for now)
                // if (hasFilters) {
                //     requestBody.filter = filterObject;
                // }

            const response = await fetch('/embeddings/search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestBody)
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Search failed');
            }

            displayResults(data.results || [], data.count || 0);
        } catch (error) {
            showError(error.message || 'An error occurred while searching');
        } finally {
            searchButton.disabled = false;
            searchButton.textContent = 'Search';
        }
    }

    function displayResults(results, count) {
        resultsCount.textContent = `${count} result${count !== 1 ? 's' : ''}`;

        if (results.length === 0) {
            resultsContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">🔍</div>
                    <p>No results found</p>
                </div>
            `;
            return;
        }

        resultsContainer.innerHTML = results.map((result, index) => {
            const text = result.metadata?.text || 'No text available';
            const truncatedText = text.length > 200 ? text.substring(0, 200) + '...' : text;
            
            // Format metadata as JSON but make text value bold
            const metadataJson = JSON.stringify(result.metadata, null, 2);
            // Replace the text value in JSON with a bold version
            const textValue = result.metadata?.text || '';
            const textJsonValue = JSON.stringify(textValue);
            const highlightedJson = metadataJson.replace(
                `"text": ${textJsonValue}`,
                `"text": <strong>${escapeHtml(textJsonValue)}</strong>`
            );

            return `
                <div class="result-card">
                    <div class="result-main">
                        <div class="result-header-line">
                            <div class="result-key-badge">${escapeHtml(result.key)}</div>
                            <div class="result-score-badge">
                                ${result.distance.toFixed(4)}
                            </div>
                        </div>
                        <div class="result-text">
                            ${escapeHtml(truncatedText)}
                        </div>
                    </div>
                    <button class="toggle-details-btn" data-index="${index}">
                        <span class="toggle-icon">▼</span>
                        Show Details
                    </button>
                    <div class="result-details hidden" id="details-${index}">
                        <div class="detail-section">
                            <h4>Metadata</h4>
                            <pre class="metadata-json">${highlightedJson}</pre>
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        // Add event listeners to toggle buttons using event delegation
        resultsContainer.querySelectorAll('.toggle-details-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                const index = this.getAttribute('data-index');
                const details = document.getElementById(`details-${index}`);
                const icon = this.querySelector('.toggle-icon');
                
                if (details.classList.contains('hidden')) {
                    details.classList.remove('hidden');
                    icon.textContent = '▲';
                    this.innerHTML = '<span class="toggle-icon">▲</span> Hide Details';
                } else {
                    details.classList.add('hidden');
                    icon.textContent = '▼';
                    this.innerHTML = '<span class="toggle-icon">▼</span> Show Details';
                }
            });
        });
    }

    function showError(message) {
        resultsSection.classList.remove('hidden');
        resultsContainer.innerHTML = `
            <div class="error-message">
                <strong>Error:</strong> ${escapeHtml(message)}
            </div>
        `;
    }

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
});

