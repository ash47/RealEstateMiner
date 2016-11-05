var fs = require('fs');
var http = require('http');
var querystring = require("querystring");
var path = require('path');

// When splitting regions, this is the distance between the two parts
var splitDifference = 0.0000000001;

var maxDataPullSize = 4000;
var maxResultsPerPage = 200;

// The max number of operations allowed at once (this is per operation type)
var maxConcurrentOperations = 1;

// The website hosting the endpoint
var endpointWebsite = 'services.realestate.com.au';

// All the APIs we can hit
var apiSummary = '/services/listings/summaries/search';
var apiResults = '/services/listings/search';

// The channel to search
var channel = 'buy';

// Where to store the data into
var dataStore = './cache/';

// The custom headers to push during requests
var browserHeaders = {
	Origin: 'http://www.realestate.com.au',
	Referer: 'http://www.realestate.com.au',
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36'
};

// Ensure the datastore exist
function ensureExists(path) {
	try {
		fs.mkdirSync(path);
	} catch(e) {
		// do nothing
	}
}

ensureExists(dataStore);

function queryAPI(apiPath, query, callback) {
	// Build the full URL
	var fullUrl = apiPath + '?query=' + querystring.escape(JSON.stringify(query));

	http.get({
		host: endpointWebsite,
		path: fullUrl,
		headers: browserHeaders
	}, function(res) {
		var responseData = '';

		res.on('data', function(data) {
			responseData += data;
		});

		res.on('end', function() {
			try {
				// Attempt to parse the json data
				var jsonData = JSON.parse(responseData);

				// All good, run the callback
				callback(null, jsonData)
			} catch(e) {
				// Failed to parse, must be invalid
				callback(e, res, responseData);
			}
		});
	});
}

// An array of areas left to search
var regionQueryPool = [];
var pullDataPool = [];

// Stores world coords
function worldCords(latLow, longLow, latHigh, longHigh) {
	// Store coords
	this.latLow = latLow;
	this.longLow = longLow;
	this.latHigh = latHigh;
	this.longHigh = longHigh;
}

// Return this as an array for the website
worldCords.prototype.asArray = function() {
	return [
		this.latLow,
		this.longLow,
		this.latHigh,
		this.longHigh
	];
}

// Splits this world coords in two new word coords, exactly in half, no overlap, splits in the direction that is largest
worldCords.prototype.split = function() {
	var latDist = Math.abs(this.latHigh - this.latLow);
	var longDist = Math.abs(this.longHigh - this.longLow);

	// Decide which direction to do the split
	if(latDist > longDist) {
		// Split based on latitude
		var splitDistance = latDist / 2;
		var midPointLeft = this.latLow + splitDistance;
		var midPointRight = midPointLeft + splitDifference;

		if(midPointLeft == midPointRight) {
			console.log('There was an accuracy error splitting ' + this.asArray);
		}

		return [
			new worldCords(this.latLow, this.longLow, midPointLeft, this.longHigh),
			new worldCords(midPointRight, this.longLow, this.latHigh, this.longHigh)
		];
	} else {
		// Split based on longitude
		var splitDistance = longDist / 2;
		var midPointLeft = this.longLow + splitDistance;
		var midPointRight = midPointLeft + splitDifference;

		if(midPointLeft == midPointRight) {
			console.log('There was an accuracy error splitting ' + this.asArray);
		}

		return [
			new worldCords(this.latLow, this.longLow, this.latHigh, midPointLeft),
			new worldCords(this.latLow, midPointRight, this.latHigh, this.longHigh)
		];
	}
}

// Push the entire world to search
regionQueryPool.push(new worldCords(-90, -180, 90, 180));

var activeCheckRegionProcesses = 0;
function checkRegionResultSize() {
	// Ensure there is work to do, and we are allowed to do it
	if(regionQueryPool.length <= 0) return;
	if(activeCheckRegionProcesses >= maxConcurrentOperations) return;

	// There is now an active process
	++activeCheckRegionProcesses;

	// Grab a region
	var thisRegion = regionQueryPool.pop();

	queryAPI(apiSummary, {
	    "channel": channel,
	    "filters": {
	        "geoPrecision": "address"
	    },
	    "pageSize": "1",
	    "boundingBoxSearch": thisRegion.asArray()
	}, function(err, res) {
		if(err) {
			console.log(err);

			// This process has ended
			--activeCheckRegionProcesses;

			// Try to continue
			checkRegionResultSize();
			return;
		}

		var totalResultsCount = res.totalResultsCount;

		// This process has now ended
		--activeCheckRegionProcesses;

		// Do we have too much data?
		if(totalResultsCount > maxDataPullSize) {
			// Split the section
			var newParts = thisRegion.split();

			// Store the new parts
			regionQueryPool.push(newParts[0], newParts[1]);

			// Try to open a parellel connection
			checkRegionResultSize();
		} else if(totalResultsCount > 0) {
			// Allow us to pull the data for this region
			pullDataPool.push(thisRegion);

			// Attempt to pull data
			tryToPullData();
		}

		// Run another query
		checkRegionResultSize();
	});
}

// Tries to pull data, if there is any
var activePullingData = 0;
var totalDiscovered = 0;
function tryToPullData() {
	// Ensure there is work to do, and we are allowed to do it
	if(pullDataPool.length <= 0) return;
	if(activePullingData >= maxConcurrentOperations) return;

	// We are now doing work
	++activePullingData;

	// Grab a region
	var thisRegion = pullDataPool.pop();

	var pageNumber = 1;

	var pullNextPage = function() {
		queryAPI(apiResults, {
		    "channel": channel,
		    "filters": {
		        "geoPrecision": "address"
		    },
		    "pageSize": maxResultsPerPage,
		    "boundingBoxSearch": thisRegion.asArray(),
		    "page": pageNumber
		}, function(err, res) {
			if(err) {
				console.log(err);

				// This process has ended
				--activePullingData;

				// Try to continue
				tryToPullData();
				return;
			}

			var totalResultsCount = res.totalResultsCount;
			var totalPages = Math.ceil(totalResultsCount / maxResultsPerPage);

			// Check if we got anything:
			var tieredResults = res.tieredResults;
			if(tieredResults) {
				for(var i=0; i<tieredResults.length; ++i) {
					var tieredResult = tieredResults[i];

					var results = tieredResult.results;
					if(results) {
						totalDiscovered += results.length;
						console.log('Found ' + results.length + ' new listings - ' + totalDiscovered + ' so far! ' + pullDataPool.length + ' | ' + regionQueryPool.length);

						for(var j=0; j<results.length; ++j) {
							(function(result) {
								var listingId = result.listingId;

								var fileName = path.join(dataStore, listingId + '.json');
								fs.exists(fileName, function(exists) {
									if(!exists) {
										// Store it
										fs.writeFile(fileName, JSON.stringify(result), function(err) {
											if(err) console.log(err);
										});
									}
								});
							})(results[j]);
						}
					}
				}
			}

			if(pageNumber < totalPages) {
				// There is another page to grab
				++pageNumber;

				// Pull the next page
				pullNextPage();
				return;
			}

			// This process has now ended
			--activePullingData;

			// Try to pull more data
			tryToPullData();
		});
	}

	// Start pulling pages
	pullNextPage();
}

//pullDataPool.push(new worldCords(-22.49999999985,112.50000000021251,0,135.000000000075));

// Try to pull data
//tryToPullData();

// Start searching
checkRegionResultSize();

/*queryAPI(apiSummary, {
    "channel": "rent",
    "filters": {
        "surroundingSuburbs": "true",
        "excludeTier2": "true",
        "geoPrecision": "address",
        "excludeAddressHidden": "true",
        "localities": []
    },
    "pageSize": "1",
    "boundingBoxSearch": [-36.78711646513089, 142.28154449218755, -36.71780856589277, 147.26384429687505]
}, function(err, res) {
	if(err) {
		console.log(err);
		return;
	}

	console.log(res);
});*/
