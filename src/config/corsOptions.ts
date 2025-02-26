import { CorsOptions } from "cors";

export const corsOptions: CorsOptions = {
  origin: async (
    origin: string | undefined,
    callback: (err: Error | null, allow?: boolean | string | string[]) => void
  ) => {
    if (
      !origin ||
      origin.startsWith("http://localhost") ||
      origin === "https://flarion-assignment.pages.dev"
    ) {
      return callback(null, true);
    }

    callback(new Error());
  },
  credentials: true,
  optionsSuccessStatus: 200,
};
