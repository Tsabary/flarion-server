import { Request as ExReq, Response as ExRes } from "express";
import parquet from "parquetjs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default async (_: ExReq, res: ExRes) => {
  try {
    const filePath = path.join(__dirname, "..", "data", "logs.parquet");

    // Open the Parquet file for reading
    const reader = await parquet.ParquetReader.openFile(filePath);

    // Create a new cursor
    const cursor = reader.getCursor();

    let record;
    const allLogs = [];

    // Read all rows
    while ((record = await cursor.next())) {
      allLogs.push(record);
    }

    await reader.close();

    return res.json(allLogs);
  } catch (error) {
    console.error("Error reading parquet:", error);
    return res.status(500).json({ error: "Failed to read logs" });
  }
};
