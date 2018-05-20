var fs = require('fs');
var path = require('path');
var MongoClient = require('mongodb').MongoClient;

//var propertyType = 'rent';
var propertyType = 'buy';
var dirToPrepare = './cache_' + propertyType + '/';

// The max number of files to process at once
var maxFilesAtOnce = 50;

// Converts dumb units to real numbers
function dumbNumberToRealNumber(oldNumber) {
	// Ensure we are dealing with a string
	oldNumber = '' + oldNumber;
	oldNumber = oldNumber.trim();
	oldNumber = oldNumber.replace(/,/g, '');

	var niceNumber = 0;
	try {
		niceNumber = parseInt(oldNumber);
	} catch(e) {
		console.log('Failed to convert: ' + oldNumber);
		console.log(e);
		return 0;
	}

	if(oldNumber.indexOf('k') != -1) {
		niceNumber = niceNumber * 1000;
	} else if(oldNumber.indexOf('m') != -1) {
		niceNumber = niceNumber * 1000000
	}

	return niceNumber;
}

// Process a file
function processFile(data, callback) {
	// Ignore all projects
	if(data.isProject) {
		callback();
		return;
	}

	var lowerRange = -1;
	var upperRange = -1

	if(data.advertising && data.advertising.priceRange) {
		var rawPriceRange = data.advertising.priceRange.split('_');

		var rawPriceRangeLeft = rawPriceRange[0];
		var rawPriceRangeRight = rawPriceRange[1];

		if(rawPriceRangeRight == null) {
			rawPriceRangeRight = rawPriceRangeLeft;
		}

		var unit = '';

		// We are going to assume left and right are in the same unit
		if(rawPriceRangeLeft.indexOf('pw') != -1) {
			unit = 'pw';
			rawPriceRangeLeft = rawPriceRangeLeft.replace(/pw/g, '');
			rawPriceRangeRight = rawPriceRangeRight.replace(/pw/g, '');
		} else if(rawPriceRangeLeft.indexOf('pcm') != -1) {
			unit = 'pcm';
			rawPriceRangeLeft = rawPriceRangeLeft.replace(/pcm/g, '');
			rawPriceRangeRight = rawPriceRangeRight.replace(/pcm/g, '');
		}

		lowerRange = dumbNumberToRealNumber(rawPriceRangeLeft);
		upperRange = dumbNumberToRealNumber(rawPriceRangeRight);

		// Convert back to per week
		if(unit == 'pcm') {
			lowerRange = lowerRange * 12 / 52;
			upperRange = upperRange * 12 / 52;
			unit = 'pw';
		}

		data.priceRangeLower = lowerRange;
		data.priceRangeUpper = upperRange;
		data.priceRangeUnit = unit;
	}

	if(data.price && data.price.display) {
		var originalDisplayNumber = data.price.display
		var displayNumber = originalDisplayNumber;

		// -1 will stand for, they are idiots and dont know what to sell for
		var finalNumber = -1;
		var finalNumberUpper = -1;

		// Start by replace all the $ and , -- they dont add anything
		displayNumber = displayNumber.replace(/\$/, '');
		displayNumber = displayNumber.replace(/,/, '');

		// The unit the price is in
		var unit = '';

		// Are we offerd multiple kinds of units?
		if(displayNumber.indexOf('/') != -1) {
			var displayNumberSplit = displayNumber.split('/');

			// Default to the first part
			displayNumber = displayNumberSplit[0];

			for(var i=0; i<displayNumberSplit.length; ++i) {
				var thisDisplayNumber = displayNumberSplit[i];

				if(thisDisplayNumber.indexOf('pw') != -1) {
					// Lets use this unit
					displayNumber = thisDisplayNumber;
				}
			}
		}

		// Check for units
		if(displayNumber.indexOf('pw') != -1 || displayNumber.indexOf('per week') != -1) {
			unit = 'pw';
		} else if(displayNumber.indexOf('pcm') != -1) {
			unit = 'pcm';
		}

		// Are there any numbers at all?
		if(/\d/.test(displayNumber)) {
			// There are some numbers, let's see if we can work this out

			// Remove cents
			displayNumber = displayNumber.replace(/\.\d\d/g, '');

			// Try to split on -
			var displayNumberParts = displayNumber.split('-');
			if(displayNumberParts.length > 1) {
				// We are dealing with a range

				var onlyNumbersA = displayNumberParts[0].replace(/[^\d]/g, '');
				var onlyNumbersB = displayNumberParts[1].replace(/[^\d]/g, '');

				try {
					finalNumber = parseInt(onlyNumbersA);
					finalNumberUpper = parseInt(onlyNumbersB);
				} catch(e) {
					// do nothing, this property is too hard to work out
					console.log('Unable to deal parse: ' + originalDisplayNumber);
				}
			} else {
				// We are dealing with a single figure
				var onlyNumbers = displayNumber.replace(/[^\d]/g, '');

				try {
					finalNumber = parseInt(onlyNumbers);
				} catch(e) {
					// do nothing, this property is too hard to work out
					console.log('Unable to deal parse: ' + originalDisplayNumber);
				}
			}
		}

		// Can we convert the unit?
		if(unit == 'pcm') {
			if(finalNumber != -1) {
				finalNumber = finalNumber * 12 / 52;
			}

			if(finalNumberUpper != -1) {
				finalNumberUpper = finalNumberUpper * 12 / 52;
			}

			unit = 'pw';
		}

		// Ensure we have numbers
		if(isNaN(finalNumber)) {
			finalNumber = -1;
		}

		if(isNaN(finalNumberUpper)) {
			finalNumberUpper = -1;
		}

		// Sanity check
		if(lowerRange != -1 && finalNumber != -1) {
			if(finalNumber < lowerRange) {
				// We only care if the value is REALLY small
				if(finalNumber < 50) {
					finalNumber = lowerRange;
					finalNumberUpper = -1;
				}
			}
		}

		if(upperRange != -1) {
			if(finalNumber != -1) {
				if(finalNumber > upperRange) {
					// Something funky going on, limit it
					finalNumber = upperRange;
					finalNumberUpper = -1;
				}
			} else {
				finalNumber = upperRange;
			}
		}

		// Is there an exact value sitting there?
		if(data.price.value) {
			finalNumber = data.price.value;
			finalNumberUpper = -1;
		}

		data.priceReal = finalNumber;
		data.priceRealUpper = finalNumberUpper;
		data.priceRealUnit = unit;
	}

	// All done!
	callback(data);
}

function processAllFiles(callback, onGetData) {
	fs.readdir(dirToPrepare, function(err, files) {
		if(err) throw err;

		var concurrentConnections = 0;

		var finishedProcessingFile = function() {
			// We are done with this connection
			--concurrentConnections;

			// Are there any files left to process?
			if(files.length <= 0 && concurrentConnections == 0) {
				// We are done processing!
				callback();
			} else {
				// Try to spin up another connection
				continueReading();
			}
		}

		var continueReading = function() {
			if(concurrentConnections >= maxFilesAtOnce) return;
			if(files.length <= 0) return;

			// We are now using a connection
			++concurrentConnections;

			var filesLeft = files.length;
			if(filesLeft % 1000 == 0) {
				console.log(filesLeft + ' files left to process!');
			}

			// Grab a file
			var file = files.pop();
			var filePath = path.join(dirToPrepare, file);

			fs.readFile(filePath, function(err, data) {
				// Did we read successfully?
				if(err) {
					console.log(err);
					finishedProcessingFile();
					return;
				}

				// Attempt to parse it
				try {
					var niceData = JSON.parse(data);

					// Process this file
					processFile(niceData, function(data) {
						// Did we get some data?
						if(data != null) {
							// Push the data
							onGetData(data);
						}

						// We are now done
						finishedProcessingFile();
					});
				} catch(e) {
					// Failed to parse it
					console.log('--- error: ' + file + ' ---')
					console.log(e);
					finishedProcessingFile();
					return;
				}
			})
		}

		// Start reading
		for(var i=0; i<maxFilesAtOnce; ++i) {
			continueReading();
		}
	})
}

MongoClient.connect("mongodb://localhost:27017/re", function(err, db) {
	if(err) throw err;

	db.createCollection(propertyType, {w:1}, function(err, collection) {
		if(err) throw err;

		console.log('Successfully connected to DB server!');

		var activeCommits = 0;
		var allDone = false;

		var checkIfAllDone = function() {
			if(allDone && activeCommits == 0) {
				// We are officially complete
				console.log('Done commiting to DB!');

				// Close the DB
				db.close();
			}
		}

		processAllFiles(function() {
			console.log('Done reading data!');

			// Check if we are done
			allDone = true;
			checkIfAllDone();
		}, function(data) {
			// We now have an active commit
			++activeCommits;

			// We got some data
			collection.insert(data, {w:1}, function(err, result) {
				if(err) throw err;

				// Commit has finished
				--activeCommits;

				// Check if we are done
				checkIfAllDone();
			});
		});
	});
});
