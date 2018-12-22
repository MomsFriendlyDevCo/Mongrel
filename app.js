#!/usr/bin/env node

var _ = require('lodash');
var colors = require('chalk');
var fs = require('fs').promises;
var mongoose = require('mongoose');
var program = require('commander');

program
	.version(require('./package.json').version)
	.usage('-c <collection> [-i | -o]')
	.option('-c, --collection <name>', 'Work on a specific collection')
	.option('-i, --input [file]', 'Accept into Mongo')
	.option('-o, --output [file]', 'Export a collection from Mongo to file')
	.option('--nuke', 'When importing, remove all records and replace them with the input')
	.option('-v, --verbose', 'Be verbose - use multiple to increase verbosity', (v, total) => total + 1, 0)
	.option('-d, --db <databse>', 'Database to use')
	.option('--uri <uri>', 'URI string to use when connection, defaults to localhost + database')
	.option('--ignore <fields...>', 'Ignore the specified fields either when reading or writing', '')
	.option('-q, --query <key=val>', 'Set a query filter (dotted notation supported)', (v, total) => {
		var bits = [key, val] = v.split(/\s*=\s*/, 2);
		if (!bits.length == 2) throw `Failed to parse setting "${v}"`;
		_.set(total, key, // Set the key, accepting various shorthand boolean values
			val === 'true' ? true
			: val === 'false' ? false
			: val
		);
		return total
	}, {})
	.parse(process.argv);

Promise.resolve()
	// Sanity checks {{{
	.then(()=> {
		if (!program.collection) throw '--collection <name> must be specified';
		if (!program.input && !program.output) throw '--input or --output must be specified as an operation';
		if (program.input && program.ouput) throw 'Only one of --input OR --output can be specified';
		if (!program.uri && !program.db) throw 'Either --db or --uri must be specified';

		if (!program.uri) program.uri = `mongodb://localhost/${program.db}`;
		program.ignore = program.ignore.split(/\s*,\s*/);
	})
	// }}}
	// Connect {{{
	.then(()=> mongoose.connect(program.uri, {useNewUrlParser: true}))
	// }}}
	// Input {{{
	.then(()=> {
		if (!program.input) return;
		return Promise.resolve()
			.then(()=> {
				if (_.isString(program.input)) {
					if (program.verbose) process.stdout.write(`Reading input file ${program.input}...\n`);
					return fs.readFile(program.input);
				} else {
					if (program.verbose) process.stdout.write(`Reading STDIN...\n`);
					return new Promise(resolve => {
						var buf = '';
						process.stdin.on('data', data => buf += data.toString())
						process.stdin.on('close', resolve(buf));
					});
				}
			})
			.then(raw => {
				if (program.verbose) process.stdout.write('Processing input...\n');
				return JSON.parse(raw);
			})
			.then(raw => {
				if (!program.nuke) return raw;
				if (program.verbose) process.stdout.write(`Nuking ${program.collection}...`);
				return mongoose.connection.collection(program.collection).remove();
			})
			.then(docs => program.ignore.length ? docs.map(doc => _.omit(doc, program.ignore)) : docs)
			.then(docs => Promise.all(docs.map(doc =>
				mongoose.connection.collection(program.collection).insertOne(doc)
			)).then(()=> docs))
			.then(docs => {
				if (program.verbose) process.stdout.write(`Written ${docs.length} to collection "${program.collection}"\n`);
			})
	})
	// }}}
	// Output {{{
	.then(()=> {
		if (!program.output) return;
		return Promise.resolve()
			.then(()=> mongoose.connection.collection(program.collection).find(program.query).toArray())
			.then(docs => program.ignore.length ? docs.map(doc => _.omit(doc, program.ignore)) : docs)
			.then(docs => {
				if (_.isString(program.output)) {
					return fs.writeFile(program.output, JSON.stringify(docs, null, '\t'));
					process.stderr.write(`Written ${docs.length} records to "${program.output}"\n`);
				} else {
					process.stdout.write(JSON.stringify(docs, null, '\t'));
				}
			})
	})
	// }}}
	// End {{{
	.then(()=> {
		process.exit(0);
	})
	.catch(err => {
		console.log(colors.red('ERR'), err.toString());
		process.exit(1);
	})
	// }}}
