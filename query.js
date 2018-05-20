var fs = require('fs');
var path = require('path');
var MongoClient = require('mongodb').MongoClient;

// The type of property to query for
var propertyType = 'rent';
//var propertyType = 'buy';

MongoClient.connect("mongodb://localhost:27017/re", function(err, db) {
	db.collection(propertyType, {strict:true}, function(err, collection) {
		var stream = collection.find({
			//priceReal: {$gt: 0, $lt: 300},
			//"address.state": {$eq: 'Vic'},
			//"address.postCode": {$eq: "3000"},
			//propertyType: 'house'
		}, {
			limit: 10,
			sort: [['priceReal', 'desc']]
		}).stream();

		stream.on('data', function(item) {
			console.log('---- ' + item.prettyUrl + ' ----');
			console.log(item.priceReal);
			console.log(item.advertising);
			console.log(item.price);
		});
    	stream.on('end', function() {
    		console.log('All done!');

    		// Close the db
    		db.close();
    	});
	});
});
