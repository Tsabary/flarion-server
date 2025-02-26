import { Request, Response } from "express";
import parquet from "parquetjs";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";

// Helper for ES modules:
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Define the logs directory as a constant outside the handler
const logsDir = path.join(__dirname, "logs");

export default async (req: Request, res: Response): Promise<void> => {
  // Parse paging parameters from query with type safety
  const pageParam = req.query.page;
  const pageSizeParam = req.query.size;

  const page =
    typeof pageParam === "string" && !isNaN(parseInt(pageParam, 10))
      ? parseInt(pageParam, 10)
      : 1;
  const pageSize =
    typeof pageSizeParam === "string" && !isNaN(parseInt(pageSizeParam, 10))
      ? parseInt(pageSizeParam, 10)
      : 10;

  // Parse filter parameters from query (if provided)
  const search = typeof req.query.search === "string" ? req.query.search : undefined;
  const status = typeof req.query.status === "string" ? req.query.status : undefined;
  const startDate = req.query.startDate ? new Date(req.query.startDate as string) : undefined;
  const endDate = req.query.endDate ? new Date(req.query.endDate as string) : undefined;

  try {
    // Get list of Parquet files in the logs directory
    const files = fs
      .readdirSync(logsDir)
      .filter((f) => f.endsWith(".parquet"))
      .sort(); // Assumes file names are sortable (e.g., job-001.parquet, job-002.parquet)

    const allLogs: any[] = [];

    // Read all files to build the full dataset
    for (const file of files) {
      const filePath = path.join(logsDir, file);
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      // Expecting one row per file
      const log = await cursor.next();
      await reader.close();
      if (log) {
        allLogs.push(log);
      }
    }

    // Apply filters if provided
    let filteredLogs = allLogs;
    if (search) {
      filteredLogs = filteredLogs.filter((log) =>
        log.id.toLowerCase().includes(search.toLowerCase())
      );
    }
    if (status && status !== "all") {
      filteredLogs = filteredLogs.filter((log) => log.status === status);
    }
    if (startDate) {
      filteredLogs = filteredLogs.filter(
        (log) => new Date(log.startTime) >= startDate
      );
    }
    if (endDate) {
      filteredLogs = filteredLogs.filter(
        (log) => new Date(log.endTime) <= endDate
      );
    }

    const totalFiles = filteredLogs.length;
    const startIndex = (page - 1) * pageSize;
    const pageLogs = filteredLogs.slice(startIndex, startIndex + pageSize);

    res.json({
      page,
      pageSize,
      totalFiles,
      logs: pageLogs,
    });
  } catch (error) {
    console.error("Error reading logs:", error);
    res.status(500).json({ error: "Failed to read logs" });
  }
};
