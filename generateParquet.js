// generateParquetFiles.js
import parquet from "parquetjs";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

// Helper for ES modules:
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Directory to store individual job files
const logsDir = path.join(__dirname, "logs");
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

// Define the Parquet schema
const schema = new parquet.ParquetSchema({
  id: { type: "UTF8" },
  startTime: { type: "UTF8" },
  endTime: { type: "UTF8" },
  duration: { type: "INT64" },
  numExecutors: { type: "INT32" },
  status: { type: "UTF8" },
  errors: { type: "UTF8", repeated: true },
  operators: {
    repeated: true,
    fields: {
      operatorId: { type: "UTF8" },
      operatorType: { type: "UTF8" },
      duration: { type: "INT32" },
      dependencies: { type: "UTF8", repeated: true },
      errors: { type: "UTF8", repeated: true },
    },
  },
});

// Helper function to get a random integer between min and max (inclusive)
function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Function that generates sequential jobs – each starts right after the previous one ends
function generateSequentialMockLogs(count) {
  const logs = [];
  let currentTime = new Date("2025-02-25T08:00:00Z");

  const operatorTypes = [
    "ReadParquet",
    "Filter",
    "Join",
    "Aggregation",
    "ReadCSV",
    "Sort",
    "ExtractData",
    "Transform",
    "LoadToDB",
    "ReadJSON",
    "WriteToDB",
    "ExtractFromDB",
    "LoadToWarehouse",
    "ValidationCheck",
  ];

  for (let i = 0; i < count; i++) {
    const jobId = `job-${(i + 1).toString().padStart(3, "0")}`;

    // Generate a random duration (in seconds) for the job
    const duration = getRandomInt(300, 2100);
    const startTime = new Date(currentTime);
    const endTime = new Date(startTime.getTime() + duration * 1000);
    // Update currentTime so the next job starts immediately after this one
    currentTime = new Date(endTime);

    const numExecutors = getRandomInt(8, 30);
    const status = Math.random() < 0.7 ? "success" : "error";
    const errors = status === "error" ? [`Error in job ${jobId}`] : [];

    // Create between 2 and 5 operators for this job
    const operatorCount = getRandomInt(2, 5);
    const operators = [];
    for (let j = 0; j < operatorCount; j++) {
      const operatorId = `op-${(i + 1) * 100 + j + 1}`;
      const operatorType =
        operatorTypes[getRandomInt(0, operatorTypes.length - 1)];
      const opDuration = getRandomInt(30, 300);
      // Each operator (after the first) depends on the previous operator
      const dependencies = j > 0 ? [operators[j - 1].operatorId] : [];
      const opErrors =
        status === "error" && Math.random() < 0.5
          ? [`Error in operator ${operatorId}`]
          : [];

      operators.push({
        operatorId,
        operatorType,
        duration: opDuration,
        dependencies,
        errors: opErrors,
      });
    }

    logs.push({
      id: jobId,
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      duration,
      numExecutors,
      status,
      errors,
      operators,
    });
  }
  return logs;
}

async function writeParquetFiles() {
  const mockLogs = generateSequentialMockLogs(100); // Change the count as needed

  // Write each job to its own Parquet file
  for (const log of mockLogs) {
    const filePath = path.join(logsDir, `${log.id}.parquet`);
    const writer = await parquet.ParquetWriter.openFile(schema, filePath);
    await writer.appendRow(log);
    await writer.close();
    console.log(`Wrote ${filePath}`);
  }
}

writeParquetFiles().catch(console.error);
