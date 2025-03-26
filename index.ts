import express, { Request, Response } from "express";
import pkg from "pg";
const { Pool } = pkg;
import { createClient } from "redis";
import { configDotenv } from "dotenv";
import { RedisCommandArgument } from "@redis/client/dist/lib/commands/index.js";
import { ZRangeByScoreOptions } from "@redis/client/dist/lib/commands/ZRANGEBYSCORE.js";

configDotenv();
const app = express();
app.use(express.json());

const PORT = process.env.MODE === 'production' ? process.env.PRODUCTION_PORT : process.env.DEVELOPMENT_PORT;
const PG_HOST = process.env.PG_HOST;
const PG_PORT = process.env.PG_PORT;
const PG_USER = process.env.PG_USER
const PG_PASSWORD = process.env.PG_PASSWORD;
const PG_DATABASE = process.env.PG_DATABASE
const REDIS_HOST = process.env.REDIS_HOST
const REDIS_PORT = process.env.REDIS_PORT;

const redisClient = createClient({
  url: `redis://${REDIS_HOST}:${REDIS_PORT}`,
  socket: {
    reconnectStrategy: (retries) => {
      const delay = Math.min(retries * 50, 2000);
      console.log(`Redis reconnecting, attempt ${retries}, next try in ${delay}ms`);
      return delay;
    }
  }
});

// Add Redis connection handling
redisClient.on('error', (err) => console.error('Redis Client Error', err));
redisClient.on('connect', () => console.log('Redis connecting...'));
redisClient.on('ready', () => console.log('Redis connected and ready'));
redisClient.on('reconnecting', () => console.log('Redis reconnecting...'));
redisClient.on('end', () => console.log('Redis connection closed'));

// Initialize Redis connection
(async () => {
  try {
    await redisClient.connect();
  } catch (err) {
    console.error('Failed to connect to Redis:', err);
  }
})();

const pgPool = new Pool({
  host: PG_HOST,
  port: Number(PG_PORT),
  user: PG_USER,
  password: PG_PASSWORD,
  database: PG_DATABASE,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

const isRedisConnected = () => redisClient.isReady || redisClient.isOpen;

app.get("/api/candles", async (req: Request, res: Response): Promise<void> => {
  console.log("Entered in the api route");
  
  try {
    const { symbol: symbolParam, fsym: fsymParam, tsym: tsymParam, resolution, toTs, frTs, limit, tt } = req.query;

    let symbol: string;
    if (typeof symbolParam === 'string') {
      symbol = symbolParam;
    } else if (!symbolParam && typeof fsymParam === 'string' && typeof tsymParam === 'string') {
      symbol = `${fsymParam}${tsymParam}`;
    } else {
      res.status(400).json({ success: false, message: "Invalid symbol parameters" });
      return;
    }

    const resolutionMap: Record<string, string> = {
      "1M": "M1",
      "1H": "H1",
      "1D": "D1",
    };

    if (!resolution || !resolutionMap[resolution as string]) {
      res.status(400).json({ success: false, message: "Invalid or missing resolution (Allowed: 1M, 1H, 1D)" });
      return;
    }
    const candleSize = resolutionMap[resolution as string];

    const to = toTs ? parseInt(toTs as string) : Math.floor(Date.now() / 1000);
    if (isNaN(to)) {
      res.status(400).json({ success: false, message: "Invalid toTs parameter" });
      return;
    }

    let from = 0;
    let fetchLimit = 0;

    if (frTs) {
      from = parseInt(frTs as string);
      if (isNaN(from)) {
        res.status(400).json({ success: false, message: "Invalid frTs parameter" });
        return;
      }
      fetchLimit = 0;
    } else if (limit) {
      fetchLimit = parseInt(limit as string);
      if (isNaN(fetchLimit) || fetchLimit <= 0) {
        res.status(400).json({ success: false, message: "Invalid limit parameter" });
        return;
      }
      from = 0;
    }

    const redisKey = `${symbol}_${candleSize}`;
    const tableName = `candles_${symbol.toLowerCase()}_bid`;

    console.log('Parameters parsed:', { symbol, candleSize, to, from, fetchLimit }); 

    // Function to fetch candles from database (using PHP-style parameterized queries)
    const fetchFromDatabase = async () => {
      try {
        let query: string;
        let params: any[] = [candleSize, to];


        if (fetchLimit > 0) {
          console.log('Executing database query...');
          query = `
            SELECT 
              EXTRACT(EPOCH FROM candletime) AS timestamp,
              open, high, low, close
            FROM ${tableName}
            WHERE candlesize = $1
            AND candletime <= to_timestamp($2)
            ORDER BY candletime DESC
            LIMIT $3
          `;
          params.push(fetchLimit);
        } else {
          query = `
            SELECT 
              EXTRACT(EPOCH FROM candletime) AS timestamp,
              open, high, low, close
            FROM ${tableName}
            WHERE candlesize = $1
            AND candletime <= to_timestamp($2)
            AND candletime >= to_timestamp($3)
            ORDER BY candletime DESC
          `;
          params.push(from);
        }

        const { rows } = await pgPool.query(query, params);
        console.log('Database query completed, rows:', rows.length);
        return rows;
      } catch (error) {
        console.error("Database fetch error:", error);
        throw error;
      }
    };

    // Try to get data from Redis first
    let candleData: string[] = [];
    let fromRedis = false;

    // try {
    //   if (!isRedisConnected()) {
    //     await redisClient.connect();
    //   }

    //   if (frTs) {
    //     candleData = await redisClient.zRange(redisKey, to, from, {
    //       BY: 'SCORE',
    //       REV: true
    //     });
    //   } else if (limit) {
    //     candleData = await redisClient.zRange(redisKey, to, '-inf', {
    //       BY: 'SCORE',
    //       REV: true,
    //       LIMIT: {
    //         offset: 0,
    //         count: fetchLimit
    //       }
    //     });
    //   } else {
    //     candleData = await redisClient.zRange(redisKey, '-inf', to, {
    //       BY: 'SCORE',
    //       REV: true
    //     });
    //   }

    //   if (candleData.length === 0) {
    //     fromRedis = false;
    //     throw new Error("No data in Redis");
    //   }
    // } catch (redisError) {
    //   console.error("Redis fetch error:", redisError);
    //   fromRedis = false;
    // }

    // Fallback to database if Redis fails
      try {
        const dbCandles = await fetchFromDatabase();

        // Store in Redis for future requests
        const pipeline = redisClient.multi();
        dbCandles.forEach(candle => {
          pipeline.zAdd(redisKey, {
            score: candle.timestamp,
            value: JSON.stringify({
              time: candle.timestamp,
              open: candle.open,
              high: candle.high,
              low: candle.low,
              close: candle.close
            })
          });
        });
        
        await pipeline.exec();

        console.log(`Loaded ${dbCandles.length} candles into Redis for ${redisKey}`);
        candleData = dbCandles.map(candle => JSON.stringify({
          time: candle.timestamp,
          open: candle.open,
          high: candle.high,
          low: candle.low,
          close: candle.close
        }));
      } catch (dbError) {
        console.error("Failed to fetch from database:", dbError);
        res.status(500).json({ success: false, message: "Failed to fetch candle data" });
        return;
      }
    

    // Apply trader type markup if specified
    let markup = 0;
    if (tt && typeof tt === "string" && ["R", "I", "S", "T"].includes(tt)) {
      const markupKey = `markup_${symbol}_${tt}_B`;
      const markupValue = await redisClient.get(markupKey);
      if (markupValue) markup = parseFloat(markupValue);
    }

    // Parse and format candle data
    const parsedCandles = candleData
      .map((value) => JSON.parse(value))
      .map((candle) => ({
        ...candle,
        open: parseFloat((candle.open - markup).toFixed(10)),
        high: parseFloat((candle.high - markup).toFixed(10)),
        low: parseFloat((candle.low - markup).toFixed(10)),
        close: parseFloat((candle.close - markup).toFixed(10)),
      }))
      .reverse(); // Reverse to match PHP output order

    res.status(200).json({
      bars: parsedCandles
    });
  } catch (error) {
    console.error("Error fetching candle data:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch candle data",
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

app.listen(Number(PORT), '0.0.0.0', ()=>{
  console.log(`Server is listening on port ${PORT}`);
  
})

