import { Router } from "express";
import fetchLogs from "../controllers/fetch-logs.js";

const router = Router();

// Route: Return Spark logs as JSON
router.get("/", fetchLogs);

export default router;
