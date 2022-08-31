const os = require('os')
const dns = require('dns').promises
const { program: optionparser } = require('commander')
const { Kafka } = require('kafkajs')
const mariadb = require('mariadb')
const MemcachePlus = require('memcache-plus')
const express = require('express')
const { exec } = require('child_process')

const app = express()
const cacheTimeSecs = 30
const numberOfTitles = 5332

// -------------------------------------------------------
// Command-line options
// -------------------------------------------------------

let options = optionparser
	.storeOptionsAsProperties(true)
	// Web server
	.option('--port <port>', "Web server port", 3000)
	// Kafka options
	.option('--kafka-broker <host:port>', "Kafka bootstrap host:port", "my-cluster-kafka-bootstrap:9092")
	.option('--kafka-topic-tracking <topic>', "Kafka topic to tracking data send to", "tracking-data")
	.option('--kafka-client-id < id > ', "Kafka client ID", "tracker-" + Math.floor(Math.random() * 100000))
	// Memcached options
	.option('--memcached-hostname <hostname>', 'Memcached hostname (may resolve to multiple IPs)', 'my-memcached-service')
	.option('--memcached-port <port>', 'Memcached port', 11211)
	.option('--memcached-update-interval <ms>', 'Interval to query DNS for memcached IPs', 5000)
	// Database options
	.option('--mariadb-host <host>', 'MariaDB host', 'my-app-mariadb-service')
	.option('--mariadb-port <port>', 'MariaDB port', 3306)
	.option('--mariadb-schema <db>', 'MariaDB Schema/database', 'netflix_titles')
	.option('--mariadb-username <username>', 'MariaDB username', 'root')
	.option('--mariadb-password <password>', 'MariaDB password', 'mysecretpw')
	// Misc
	.addHelpCommand()
	.parse()
	.opts()

// -------------------------------------------------------
// Database Configuration
// -------------------------------------------------------

const pool = mariadb.createPool({
	host: options.mariadbHost,
	port: options.mariadbPort,
	database: options.mariadbSchema,
	user: options.mariadbUsername,
	password: options.mariadbPassword,
	connectionLimit: 5
})

async function executeQuery(query, data) {
	let connection
	try {
		connection = await pool.getConnection()
		console.log("Executing query ", query)
		let res = await connection.query({ rowsAsArray: true, sql: query }, data)
		return res
	} catch {
		console.log("Could not connect to database to execute query")
	} finally {
		if (connection)
			connection.end()
	}
}

// -------------------------------------------------------
// Memcache Configuration
// -------------------------------------------------------

//Connect to the memcached instances
let memcached = null
let memcachedServers = []

async function getMemcachedServersFromDns() {
	try {
		// Query all IP addresses for this hostname
		let queryResult = await dns.lookup(options.memcachedHostname, { all: true })

		// Create IP:Port mappings
		let servers = queryResult.map(el => el.address + ":" + options.memcachedPort)

		// Check if the list of servers has changed
		// and only create a new object if the server list has changed
		if (memcachedServers.sort().toString() !== servers.sort().toString()) {
			console.log("Updated memcached server list to ", servers)
			memcachedServers = servers

			//Disconnect an existing client
			if (memcached)
				await memcached.disconnect()

			memcached = new MemcachePlus(memcachedServers);
		}
	} catch (e) {
		console.log("Unable to get memcache servers", e)
	}
}

//Initially try to connect to the memcached servers, then each 5s update the list
getMemcachedServersFromDns()
setInterval(() => getMemcachedServersFromDns(), options.memcachedUpdateInterval)

//Get data from cache if a cache exists yet
async function getFromCache(key) {
	if (!memcached) {
		console.log(`No memcached instance available, memcachedServers = ${memcachedServers}`)
		return null;
	}
	return await memcached.get(key);
}

// -------------------------------------------------------
// Kafka Configuration
// -------------------------------------------------------

// Kafka connection
const kafka = new Kafka({
	clientId: options.kafkaClientId,
	brokers: [options.kafkaBroker],
	retry: {
		retries: 0
	}
})

const producer = kafka.producer()
// End

// Send tracking message to Kafka
async function sendTrackingMessage(data) {
	//Ensure the producer is connected
	await producer.connect()

	//Send message
	let result = await producer.send({
		topic: options.kafkaTopicTracking,
		messages: [
			{ value: JSON.stringify(data) } // @ how does the data look like?
		]
	})

	console.log("Sent result to kafka:", result)
	return result
}
// End

// -------------------------------------------------------
// HTML helper to send a response to the client
// -------------------------------------------------------

function sendResponse(res, html, cachedResult) { // @ title fetch working? only number in url
	res.send(`<!DOCTYPE html>
		<html lang="en">
		<head>
			<meta charset="UTF-8">
			<meta name="viewport" content="width=device-width, initial-scale=1.0">
			<title>Big Data Netflix Ratings</title>
			<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/mini.css/3.0.1/mini-default.min.css">
			<script>
				function fetchRandomTitles() {
					const maxRepetitions = Math.floor(Math.random() * 250)
					document.getElementById("out").innerText = "Fetching " + maxRepetitions + " random titles, see console output"
					for(var i = 0; i < maxRepetitions; ++i) {
						const title = Math.floor(Math.random() * ${numberOfTitles})
						console.log("Fetching title " + title)
						fetch("/library/" + title, {cache: 'no-cache'}) // @
					}
				}
			</script>
		</head>
		<body>
			<h1>Welcome to Netflix</h1>
			<p>
				<a href="javascript: fetchRandomTitles();">Randomly simulate some views</a>
				<span id="out"></span>
			</p>
			${html}
			<hr>
			<h2>Information about the generated page</h4>
			<ul>
				<li>Server: ${os.hostname()}</li>
				<li>Date: ${new Date()}</li>
				<li>Using ${memcachedServers.length} memcached Servers: ${memcachedServers}</li>
				<li>Cached result: ${cachedResult}</li>
			</ul>
		</body>
	</html>
	`)
}

// -------------------------------------------------------
// Start page
// -------------------------------------------------------

// Get list of available titles for library (from cache or db)
async function getLibrary() {
	const key = 'library'
	let cachedata = await getFromCache(key)

	if (cachedata) {
		console.log(`Cache hit for key=${key}, cachedata = `, cachedata)
		return { result: cachedata, cached: true }
	} else {
		console.log(`Cache miss for key=${key}, querying database`)
		const data = await executeQuery("SELECT show_id, title FROM netflix_titles", [])
		if (data) {
			let result = data.map(row => ({show_id: row?.[0], title: row?.[1]})) // @
			console.log("Got result=", result, "storing in cache")
			if (memcached)
				await memcached.set(key, result, cacheTimeSecs);
			return { result: result, cached: false }
		} else {
			throw "No shows found in cache or database"
		}
	}
}

// Get shows with best rating (from db only)
async function getPopular(maxCount) {
	const data = await executeQuery("SELECT show_id, title, rating FROM rating ORDER BY rating DESC LIMIT ?", [maxCount])
	if (data) {
		let result = data.map(row => ({show_id: row?.[0],  title: row?.[1], rating: row?.[2]}))
		console.log("Got popular shows: ", result)
		return result
	} else {
		console.log("Could not get any popular shows")
	}

	// const query = "SELECT show_id, rating FROM rating ORDER BY rating DESC LIMIT ?"
	// return (await executeQuery(query, [maxCount]))
	// 	.map(row => ({ show_id: row?.[0], rating: row?.[1] })) //@?
}

// Return HTML for start page
app.get("/", (req, res) => {
	Promise.all([getLibrary(), getPopular(15)]).then(values => {
		const library = values[0]
		const popular = values[1]

		const libraryHTML = library.result
			.map(show => `<a href='library/${show[show_id]}'>${show[title]}</a>`) // @
			.join(", ")

		const popularHTML = popular
			.map(pop => `<li> <a href='library/${pop.show_id}'>${pop.title}</a> (${pop.rating} rating) </li>`)
			.join("\n")

		const html = `
			<h1>Popular Shows and Movies:</h1>
			<p>
				<ol style="margin-left: 2em;"> ${popularHTML} </ol> 
			</p>
			<h1>Full Library:</h1>
			<p> ${libraryHTML} </p>
		`
		sendResponse(res, html, library.cached)
	})
})

// -------------------------------------------------------
// Get a specific title (from cache or DB)
// -------------------------------------------------------

async function getTitle(show_id) {
	const query = "SELECT * FROM netflix_titles WHERE show_id = ?"
	const key = 'show_id_' + show_id //@?
	let cachedata = await getFromCache(key)

	console.log("Trying to fetch title with id ", show_id)

	if (cachedata) {
		console.log(`Cache hit for key=${key}, cachedata = ${cachedata}`)
		return { ...cachedata, cached: true }
	} else {
		console.log(`Cache miss for key=${key}, querying database`)

		let data = (await executeQuery(query, [show_id]))?.[0] // first (and only) entry
		if (data) {
			let result = { show_id: data?.[0], title: data?.[1], director: data?.[2], cast: data?.[3], country: data?.[4], 
				release_year: data?.[5], duration: data?.[6], genre: data?.[7], description: data?.[8] }
			console.log(`Got result=${result}, storing in cache...`)
			// store result to cache
			if (memcached)
				await memcached.set(key, result, cacheTimeSecs);
			return { ...result, cached: false }
		} else {
			throw "No data found for this title"
		}
	}
}

app.get("/library/:show_id", (req, res) => {
	// Get data of show
	let show_data = getTitle(req.params["show_id"])

	// Send the tracking message to Kafka
	sendTrackingMessage({
		show_data, // @ sent as single entries not dict, also "cached" included
		timestamp: Math.floor(new Date() / 1000)
	}).then(() => console.log(`Sent title with id ${show_data.show_id} to kafka topic ${options.kafkaTopicTracking}`))
		.catch(e => console.log("Error sending to kafka", e))

	// Send reply to browser
	getTitle(show_data.show_id).then(data => {
		sendResponse(res, `<h1>${data.title}(${data.release_year})</h1><p>${data.description}</p><b>${data.duration}</b> ` +
		`<p>Director: ${data.director}</p><p>Actors: ${data.cast}</p><p>Country: ${data.country}</p><p>Tags: ${data.genre}</p> `,
			// data.description.split("\n").map(p => `<p>${p}</p>`).join("\n"),
			data.cached
		)
	}).catch(err => {
		sendResponse(res, `<h1>Error</h1><p>${err}</p>`, false)
	})
});

// -------------------------------------------------------
// Main method
// -------------------------------------------------------

app.listen(options.port, function () {
	console.log("Node app is running at http://localhost:" + options.port)
});
