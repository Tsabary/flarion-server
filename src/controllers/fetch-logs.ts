// controller.ts
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

  try {
    // Get list of Parquet files in the logs directory
    const files = fs
      .readdirSync(logsDir)
      .filter((f) => f.endsWith(".parquet"))
      .sort(); // Assumes file names are sortable (e.g., job-001.parquet, job-002.parquet)

    const totalFiles = files.length;
    const startIndex = (page - 1) * pageSize;
    const pageFiles = files.slice(startIndex, startIndex + pageSize);

    const logs: any[] = [];
    // Read each file in the current page
    for (const file of pageFiles) {
      const filePath = path.join(logsDir, file);
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      // Expecting one row per file
      const log = await cursor.next();
      await reader.close();
      logs.push(log);
    }

    res.json({
      page,
      pageSize,
      totalFiles,
      logs,
    });
  } catch (error) {
    console.error("Error reading logs:", error);
    res.status(500).json({ error: "Failed to read logs" });
  }
};
