// server.js
import express from "express";
import logRoutes from "./routes/logs.js";

const app = express();
const PORT = process.env.PORT || 3000;

// Route: Return Spark logs as JSON
app.use("/api/v1/logs", logRoutes);

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
