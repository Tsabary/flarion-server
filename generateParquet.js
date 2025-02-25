// generateParquet.js
import parquet from "parquetjs";
import path from "path";
import { fileURLToPath } from "url";

// Helper if you're using ES modules in Node:
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 1) Define a schema for your logs:
const schema = new parquet.ParquetSchema({
  id: { type: "UTF8" },
  startTime: { type: "UTF8" }, // Store as string since Parquet lacks a direct datetime type
  endTime: { type: "UTF8" },
  duration: { type: "INT64" },
  numExecutors: { type: "INT32" },
  status: { type: "UTF8" },
  errors: { type: "UTF8", repeated: true }, // Array of error messages

  // Nested structure for operators
  operators: {
    repeated: true,
    fields: {
      operatorId: { type: "UTF8" },
      operatorType: { type: "UTF8" },
      duration: { type: "INT32" },
      dependencies: { type: "UTF8", repeated: true }, // Array of strings
      errors: { type: "UTF8", repeated: true },
    },
  },
});

const mockLogs = [
  {
    id: "job-001",
    startTime: "2025-02-25T08:00:00Z",
    endTime: "2025-02-25T08:05:00Z",
    duration: 300,
    numExecutors: 8,
    status: "success",
    errors: [],
    operators: [
      {
        operatorId: "op-101",
        operatorType: "ReadParquet",
        duration: 50,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-102",
        operatorType: "Filter",
        duration: 30,
        dependencies: ["op-101"],
        errors: [],
      },
      {
        operatorId: "op-103",
        operatorType: "Join",
        duration: 70,
        dependencies: ["op-102"],
        errors: [],
      },
    ],
  },
  {
    id: "job-002",
    startTime: "2025-02-25T09:00:00Z",
    endTime: "2025-02-25T09:10:00Z",
    duration: 600,
    numExecutors: 12,
    status: "error",
    errors: ["Out of memory in operator op-202"],
    operators: [
      {
        operatorId: "op-201",
        operatorType: "ReadCSV",
        duration: 100,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-202",
        operatorType: "Sort",
        duration: 200,
        dependencies: ["op-201"],
        errors: ["Out of memory"],
      },
      {
        operatorId: "op-203",
        operatorType: "Aggregation",
        duration: 150,
        dependencies: ["op-202"],
        errors: [],
      },
    ],
  },
  {
    id: "job-003",
    startTime: "2025-02-25T10:15:00Z",
    endTime: "2025-02-25T10:22:00Z",
    duration: 420,
    numExecutors: 10,
    status: "success",
    errors: [],
    operators: [
      {
        operatorId: "op-301",
        operatorType: "ReadParquet",
        duration: 80,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-302",
        operatorType: "Filter",
        duration: 60,
        dependencies: ["op-301"],
        errors: [],
      },
      {
        operatorId: "op-303",
        operatorType: "Join",
        duration: 110,
        dependencies: ["op-302"],
        errors: [],
      },
      {
        operatorId: "op-304",
        operatorType: "Aggregation",
        duration: 90,
        dependencies: ["op-303"],
        errors: [],
      },
    ],
  },
  {
    id: "job-004",
    startTime: "2025-02-25T11:30:00Z",
    endTime: "2025-02-25T11:45:00Z",
    duration: 900,
    numExecutors: 15,
    status: "error",
    errors: ["Disk I/O failure in operator op-402"],
    operators: [
      {
        operatorId: "op-401",
        operatorType: "ReadCSV",
        duration: 120,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-402",
        operatorType: "Join",
        duration: 250,
        dependencies: ["op-401"],
        errors: ["Disk I/O failure"],
      },
      {
        operatorId: "op-403",
        operatorType: "Sort",
        duration: 180,
        dependencies: ["op-402"],
        errors: [],
      },
      {
        operatorId: "op-404",
        operatorType: "Aggregation",
        duration: 210,
        dependencies: ["op-403"],
        errors: [],
      },
    ],
  },
  {
    id: "job-005",
    startTime: "2025-02-25T12:00:00Z",
    endTime: "2025-02-25T12:20:00Z",
    duration: 1200,
    numExecutors: 18,
    status: "success",
    errors: [],
    operators: [
      {
        operatorId: "op-501",
        operatorType: "ReadJSON",
        duration: 90,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-502",
        operatorType: "Transform",
        duration: 150,
        dependencies: ["op-501"],
        errors: [],
      },
      {
        operatorId: "op-503",
        operatorType: "Filter",
        duration: 120,
        dependencies: ["op-502"],
        errors: [],
      },
      {
        operatorId: "op-504",
        operatorType: "Aggregation",
        duration: 250,
        dependencies: ["op-503"],
        errors: [],
      },
      {
        operatorId: "op-505",
        operatorType: "WriteToDB",
        duration: 180,
        dependencies: ["op-504"],
        errors: [],
      },
    ],
  },
  {
    id: "job-006",
    startTime: "2025-02-25T13:00:00Z",
    endTime: "2025-02-25T13:25:00Z",
    duration: 1500,
    numExecutors: 20,
    status: "success",
    errors: [],
    operators: [
      {
        operatorId: "op-601",
        operatorType: "ReadParquet",
        duration: 100,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-602",
        operatorType: "Filter",
        duration: 80,
        dependencies: ["op-601"],
        errors: [],
      },
      {
        operatorId: "op-603",
        operatorType: "Join",
        duration: 200,
        dependencies: ["op-602"],
        errors: [],
      },
    ],
  },
  {
    id: "job-007",
    startTime: "2025-02-25T14:00:00Z",
    endTime: "2025-02-25T14:30:00Z",
    duration: 1800,
    numExecutors: 22,
    status: "error",
    errors: ["Execution timeout on operator op-702"],
    operators: [
      {
        operatorId: "op-701",
        operatorType: "ReadCSV",
        duration: 150,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-702",
        operatorType: "Sort",
        duration: 300,
        dependencies: ["op-701"],
        errors: ["Execution timeout"],
      },
    ],
  },
  {
    id: "job-008",
    startTime: "2025-02-25T15:00:00Z",
    endTime: "2025-02-25T15:50:00Z",
    duration: 3000,
    numExecutors: 25,
    status: "success",
    errors: [],
    operators: [
      {
        operatorId: "op-801",
        operatorType: "ExtractData",
        duration: 200,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-802",
        operatorType: "Transform",
        duration: 250,
        dependencies: ["op-801"],
        errors: [],
      },
      {
        operatorId: "op-803",
        operatorType: "LoadToDB",
        duration: 400,
        dependencies: ["op-802"],
        errors: [],
      },
    ],
  },
  {
    id: "job-009",
    startTime: "2025-02-25T16:15:00Z",
    endTime: "2025-02-25T16:45:00Z",
    duration: 1800,
    numExecutors: 18,
    status: "error",
    errors: ["Data inconsistency detected in operator op-902"],
    operators: [
      {
        operatorId: "op-901",
        operatorType: "ReadJSON",
        duration: 130,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-902",
        operatorType: "ValidationCheck",
        duration: 140,
        dependencies: ["op-901"],
        errors: ["Data inconsistency detected"],
      },
    ],
  },
  {
    id: "job-010",
    startTime: "2025-02-25T17:00:00Z",
    endTime: "2025-02-25T17:35:00Z",
    duration: 2100,
    numExecutors: 30,
    status: "success",
    errors: [],
    operators: [
      {
        operatorId: "op-1001",
        operatorType: "ExtractFromDB",
        duration: 200,
        dependencies: [],
        errors: [],
      },
      {
        operatorId: "op-1002",
        operatorType: "Transform",
        duration: 220,
        dependencies: ["op-1001"],
        errors: [],
      },
      {
        operatorId: "op-1003",
        operatorType: "LoadToWarehouse",
        duration: 300,
        dependencies: ["op-1002"],
        errors: [],
      },
    ],
  },
];

// 3) Write them to a Parquet file:
async function writeParquet() {
  const filePath = path.join(__dirname, "logs.parquet");
  const writer = await parquet.ParquetWriter.openFile(schema, filePath);

  for (let log of mockLogs) {
    await writer.appendRow(log);
  }

  await writer.close();
  console.log(`Parquet file written to ${filePath}`);
}

writeParquet().catch(console.error);
