const fs = require('fs');
const readline = require('readline');
const { DateTime, Duration } = require('luxon');

// Process a single row
function processRow(row, minDate, maxDate, now, duration) {
    const readingDate = DateTime.fromISO(row.readingDate).toUTC();
    const normalizedDate = now
        .minus({ days: 20 })
        .plus(
            Duration.fromObject({
                seconds: (15 * (readingDate.diff(minDate).as('seconds'))) / duration,
            })
        );

    return {
        ...row,
        readingDate: normalizedDate.toISO(),
        _start: normalizedDate.toISO(),
        _stop: normalizedDate.toISO(),
    };
}

// Process a chunk iteratively
function processChunk(chunk, now) {
    let minDate = DateTime.fromISO(chunk[0].readingDate).toUTC();
    let maxDate = minDate;

    // Calculate min and max dates manually (iterative)
    for (let i = 1; i < chunk.length; i++) {
        const date = DateTime.fromISO(chunk[i].readingDate).toUTC();
        if (date < minDate) minDate = date;
        if (date > maxDate) maxDate = date;
    }

    const duration = maxDate.diff(minDate).as('seconds');

    // Normalize dates iteratively
    if (duration > 0) {
        for (let i = 0; i < chunk.length; i++) {
            chunk[i] = processRow(chunk[i], minDate, maxDate, now, duration);
        }
    }

    return chunk;
}

// Write a processed chunk to a CSV file
async function writeChunkToFile(chunk, annotations, chunkNum) {
    const outputFile = `influx_batch_${chunkNum}.csv`;

    const rows = [];
    for (let i = 0; i < chunk.length; i++) {
        const row = chunk[i];
        const outputRow = [
            '',
            '',
            '0',
            row._start,
            row._stop,
            row.readingDate,
            row.value || '',
            'reading',
            'metrics',
            row.displayReference,
            row.location,
            row.readingType || '',
        ];
        rows.push(outputRow.join(','));
    }

    const fileContent = [...annotations, ...rows].join('\n');
    await fs.promises.writeFile(outputFile, fileContent, 'utf8');
}

// Transform CSV chunks iteratively
async function transformChunks(inputFile, chunkSize = 210000) {
    const now = DateTime.utc();
    console.log('Processing CSV in chunks...');

    const annotations = [
        '#group,false,false,true,true,false,false,true,true,true,true,true',
        '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string',
        '#default,mean,,,,,,,,,,',
        ',result,table,_start,_stop,_time,_value,_field,_measurement,displayReference,location,readingType',
    ];

    let chunk = [];
    let chunkNum = 0;
    let totalRows = 0;

    const fileStream = fs.createReadStream(inputFile);
    const rl = readline.createInterface({ input: fileStream });

    for await (const line of rl) {
        if (!line.trim()) continue;

        const row = parseRow(line);
        chunk.push(row);

        if (chunk.length >= chunkSize) {
            const processedChunk = processChunk(chunk, now);
            await writeChunkToFile(processedChunk, annotations, chunkNum);
            totalRows += chunk.length;

            console.log(
                `Processed chunk ${chunkNum + 1}, rows: ${chunk.length}, total: ${totalRows}`
            );

            chunk = [];
            chunkNum++;
        }
    }

    // Process the remaining rows
    if (chunk.length > 0) {
        const processedChunk = processChunk(chunk, now);
        await writeChunkToFile(processedChunk, annotations, chunkNum);
        totalRows += chunk.length;

        console.log(
            `Processed final chunk ${chunkNum + 1}, rows: ${chunk.length}, total: ${totalRows}`
        );
    }

    console.log('Transformation complete!');
}

// Parse a CSV row into an object
function parseRow(line) {
    const columns = line.split(',');
    return {
        readingDate: columns[0],
        value: columns[1],
        displayReference: columns[2],
        location: columns[3],
        readingType: columns[4],
    };
}

// Entry point
(async () => {
    try {
        const inputFile = 'READING_2023-06-01.csv';
        const chunkSize = 210000; // Large chunk size retained
        await transformChunks(inputFile, chunkSize);
    } catch (error) {
        console.error(`Error occurred: ${error.message}`);
        process.exit(1);
    }
})();
