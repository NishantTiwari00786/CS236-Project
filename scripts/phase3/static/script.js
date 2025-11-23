// mapping the dataset columns to make the dropdown for the website
const columnsByDataset = {
    customer_reservations: [
        "booking_id",  // Changed from Booking_ID
        "stays_in_weekend_nights",
        "stays_in_week_nights",
        "lead_time",
        "arrival_year",
        "arrival_month",
        "arrival_date",
        "market_segment_type",
        "avg_price_per_room",
        "booking_status"
    ],
    hotel_bookings: [
        "hotel",
        "booking_status",
        "lead_time",
        "arrival_year",
        "arrival_month",
        "arrival_date_week_number",
        "arrival_date_day_of_month",
        "stays_in_weekend_nights",
        "stays_in_week_nights",
        "market_segment_type",
        "country",
        "avg_price_per_room",
        "email"
    ],
    unified_bookings: [
        "booking_id",
        "hotel",
        "arrival_year",
        "arrival_month",
        "arrival_day_of_month",
        "stays_in_week_nights",
        "stays_in_weekend_nights",
        "avg_price_per_room",
        "country",
        "market_segment_type",
        "booking_status",
        "email",
        "lead_time",
        "arrival_date_week_number"
    ]
};

// when user changes the dataset, this function updates the dropdown
function updateFilterColumns() {
    const dataset = document.getElementById("dataset").value;
    const filterDropdown = document.getElementById("filter_column");

    filterDropdown.innerHTML = "";

    columnsByDataset[dataset].forEach(col => {
        const opt = document.createElement("option");
        opt.value = col;
        opt.textContent = col;
        filterDropdown.appendChild(opt);
    });

    // Also update values for the first column
    updateFilterValues();
}


// when user changes the filter column, this function updates the values in them 
async function updateFilterValues() {
    const dataset = document.getElementById("dataset").value;
    const column = document.getElementById("filter_column").value;
    const valueDropdown = document.getElementById("filter_value");

    valueDropdown.innerHTML = "";
    // Default "All values" option
    const allOpt = document.createElement("option");
    allOpt.value = "";
    allOpt.textContent = "All values";
    valueDropdown.appendChild(allOpt);

    if (!dataset || !column) return;

    try {
        const response = await fetch(`/api/distinct_values?dataset=${dataset}&column=${column}`);
        const values = await response.json();

        values.forEach(v => {
            const opt = document.createElement("option");
            opt.value = v;
            opt.textContent = v;
            valueDropdown.appendChild(opt);
        });
    } catch (err) {
        console.error("Error loading distinct values:", err);
    }
}

// when user clickes on fetch data, this function gets the data from backend
async function fetchData() {
    const dataset = document.getElementById("dataset").value;
    const filterColumn = document.getElementById("filter_column").value;
    const filterValue = document.getElementById("filter_value").value;

    let url = `/api/bookings?dataset=${dataset}`;
    if (filterColumn && filterValue) {
        url += `&filter_column=${filterColumn}&filter_value=${encodeURIComponent(filterValue)}`;
    }

    try {
        const response = await fetch(url);
        const data = await response.json();
        populateTable(data);
    } catch (error) {
        console.error("Error fetching data:", error);
        alert("Failed to fetch data. Check console for details.");
    }
}

// it populates the table with data received from backend, it is limited to 1000 rows, to avoid crashing in the browser
function populateTable(data) {
    const tableHead = document.getElementById("tableHead");
    const tableBody = document.getElementById("tableBody");

    tableHead.innerHTML = "";
    tableBody.innerHTML = "";

    if (!data || data.length === 0) {
        tableBody.innerHTML = "<tr><td colspan='100%'>No data found</td></tr>";
        return;
    }

    // change the 10000 to whatever limit 
    const displayData = data.slice(0, 1000); 
    if (data.length > 1000) {
        alert(`Showing first 1000 rows out of ${data.length}. Please filter to see specific results.`);
    }

    const headers = Object.keys(displayData[0]);

    // Build Header
    let headerRow = "<tr>";
    headers.forEach(h => headerRow += `<th>${h}</th>`);
    headerRow += "</tr>";
    tableHead.innerHTML = headerRow;

    
    let allRows = "";
    displayData.forEach(row => {
        allRows += "<tr>";
        headers.forEach(h => {
            allRows += `<td>${row[h]}</td>`;
        });
        allRows += "</tr>";
    });
    
    tableBody.innerHTML = allRows;
}

// iniatialize the filter columns on page load
window.onload = function () {
    updateFilterColumns();
};
