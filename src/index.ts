// server.js
import express from "express";
import cors from "cors";
import logRoutes from "./routes/logs.js";
import { corsOptions } from "./config/corsOptions.js";

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors(corsOptions));

// Route: Return Spark logs as JSON
app.use("/api/v1/logs", logRoutes);

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
