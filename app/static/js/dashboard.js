/**
 * UK Home Dashboard — Common JavaScript utilities
 */

// Score to colour helper (used in templates too)
function scoreColor(score) {
    if (score >= 70) return '#198754';  // green
    if (score >= 55) return '#0d6efd';  // blue
    if (score >= 40) return '#ffc107';  // yellow
    return '#dc3545';                    // red
}

// Format currency
function formatGBP(value) {
    return new Intl.NumberFormat('en-GB', {
        style: 'currency',
        currency: 'GBP',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
    }).format(value);
}

// Format number with commas
function formatNumber(value) {
    return new Intl.NumberFormat('en-GB').format(value);
}

// Debounce helper for sliders
function debounce(fn, delay) {
    let timer;
    return function (...args) {
        clearTimeout(timer);
        timer = setTimeout(() => fn.apply(this, args), delay);
    };
}

// API helper
async function fetchJSON(url) {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp.json();
}

// Initialize tooltips
document.addEventListener('DOMContentLoaded', () => {
    // Bootstrap tooltips
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tooltipTriggerList.forEach(el => new bootstrap.Tooltip(el));
});
