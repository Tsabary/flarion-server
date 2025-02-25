// server.js
import express, {
  Request as ExReq,
  Response as ExRes,
  NextFunction,
} from "express";
import logRoutes from "./routes/logs.js";

const app = express();
const PORT = process.env.PORT || 3000;

app.use((req: ExReq, res: ExRes, next: NextFunction) => {
  let logString = `New request: ${req.method} ${req.path}`;
  console.log(logString);
  next(); // Pass control to the next middleware or route handler
});
// Route: Return Spark logs as JSON
app.use("/api/v1/logs", logRoutes);

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
