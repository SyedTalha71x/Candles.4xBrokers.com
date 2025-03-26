import pkg from "pg";
const { Pool } = pkg;
import Bull from "bull";
import { configDotenv } from "dotenv";
import { createClient } from "redis";
import express from "express";
import type { Request, Response } from "express";
import { RedisCommandArgument } from "@redis/client/dist/lib/commands/index.js";
import { ZRangeByScoreOptions } from "@redis/client/dist/lib/commands/ZRANGEBYSCORE.js";
import WebSocket, { WebSocketServer } from "ws";


// Initialize configuration and express app
configDotenv();
const app = express();
app.use(express.json());

// Configuration constants
const PORT = process.env.PORT || 3000;
const PG_HOST = process.env.PG_HOST;
const PG_PORT = process.env.PG_PORT ? Number(process.env.PG_PORT) : 5432;
const PG_USER = process.env.PG_USER;
const PG_PASSWORD = process.env.PG_PASSWORD;
const PG_DATABASE = process.env.PG_DATABASE;
const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379");
const WS_SERVER_URL = process.env.WS_SERVER_URL || "ws://50.19.20.84:8081/";
const WS_RECONNECT_DELAY = parseInt(process.env.WS_RECONNECT_DELAY || "5000");
const MAX_WS_RECONNECT_ATTEMPTS = parseInt(process.env.MAX_WS_RECONNECT_ATTEMPTS || "10");

// Time frames for candle data
const timeFrames = {
  M1: 60000,
  H1: 3600000,
  D1: 86400000,
};

// Interfaces
interface TickData {
  symbol: string;
  price: number;
  timestamp: Date;
  lots: number;
}

interface CurrencyPairInfo {
  currpair: string;
  contractsize: number | null;
}

interface WsClient {
  socket: WebSocket | null;
  reconnectAttempts: number;
  subscribedPairs: Set<string>;
  subscribedAll: boolean;
  isConnected: boolean;
  pendingSubscriptions: Array<{ action: string; pairs?: string[] }>;
  lastPong: number;
}

// Initialize clients and queues
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

const pgPool = new Pool({
  host: PG_HOST,
  port: PG_PORT,
  user: PG_USER,
  password: PG_PASSWORD,
  database: PG_DATABASE,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

const marketDataQueue = new Bull("marketData", {
  redis: {
    host: REDIS_HOST,
    port: REDIS_PORT,
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1000,
    },
    removeOnComplete: true,
    timeout: 30000,
  },
  limiter: {
    max: 100,
    duration: 1000,
  },
  settings: {
    maxStalledCount: 1,
  },
});

const candleProcessingQueue = new Bull("candleProcessing", {
  redis: {
    host: REDIS_HOST,
    port: REDIS_PORT,
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1000,
    },
    removeOnComplete: true,
    timeout: 30000,
  },
  limiter: {
    max: 50,
    duration: 1000,
  },
});


let availableCurrencyPairs: CurrencyPairInfo[] = []; WS_SERVER_URL
const subscribedPairs: Set<string> = new Set();

let wsClient: WsClient = {
  socket: null,
  reconnectAttempts: 0,
  subscribedPairs: new Set(),
  subscribedAll: false,
  isConnected: false,
  pendingSubscriptions: [],
  lastPong: 0
};

// Initialize WebSocket Connection
function initializeWebSocket() {
  if (wsClient.socket) {
    wsClient.socket.removeAllListeners();
    wsClient.socket.close();
  }

  console.log(`Connecting to WebSocket server at ${WS_SERVER_URL}`);

  try {
    wsClient.socket = new WebSocket(WS_SERVER_URL);

    wsClient.socket.on('open', () => {
      console.log('✅ WebSocket connection established');
      wsClient.isConnected = true;
      wsClient.reconnectAttempts = 0;
      wsClient.lastPong = Date.now();
      startHeartbeat();

      setTimeout(() => {
        if (wsClient.isConnected) {
          subscribeToAll();
        } else {
          console.error("Connection not established within 2 seconds");
        }
      }, 1000);
    });


    wsClient.socket.on('message', (data) => {
      try {
        const message = JSON.parse(data.toString());
        handleServerMessage(message);
      } catch (error) {
        console.error('Error parsing message:', error);
      }
    });

    wsClient.socket.on('close', (code, reason) => {
      console.log(`🚪 Connection closed (Code: ${code}, Reason: ${reason.toString()})`);
      handleDisconnection();
    });

    wsClient.socket.on('error', (error) => {
      console.error('WebSocket error:', error);
      handleDisconnection();
    });

    wsClient.socket.on('pong', () => {
      wsClient.lastPong = Date.now();
      console.debug('Received pong from server');
    });

  } catch (error) {
    console.error('WebSocket initialization error:', error);
    scheduleReconnection();
  }
}

// Heartbeat management
let heartbeatInterval: NodeJS.Timeout;

function startHeartbeat() {
  heartbeatInterval = setInterval(() => {
    if (wsClient.socket?.readyState === WebSocket.OPEN) {
      if (Date.now() - wsClient.lastPong > 60000) {
        console.error('No pong received in last 60 seconds, reconnecting...');
        wsClient.socket.close();
        return;
      }
      wsClient.socket.ping();
      console.debug('Sent ping to server');
    }
  }, 30000);
}

function stopHeartbeat() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
  }
}

// Message handling
function handleServerMessage(message: any) {
  console.log('Received message from server:', message);

  if (message.action && message.status) {
    if (message.status === 'success') {
      console.log(`Successfully processed ${message.action} action`);

      if (message.action === 'SubAdd' && message.subs) {
        message.subs.forEach((sub: string) => {
          const pair = sub.split('~')[1];
          if (pair) wsClient.subscribedPairs.add(pair);
        });
      }
      else if (message.action === 'SubAddAll') {
        wsClient.subscribedAll = true;
        wsClient.subscribedPairs.clear();
      }
      else if (message.action === 'SubRemove' && message.subs) {
        message.subs.forEach((sub: string) => {
          const pair = sub.split('~')[1];
          if (pair) wsClient.subscribedPairs.delete(pair);
        });
      }
    } else {
      console.error(`Action ${message.action} failed: ${message.message || 'Unknown error'}`);
    }
  }

  if (message.p && message.symbol && message.ts) {
    processMarketData(message);
  }
}

// Market data processing
function processMarketData(data: any) {

  const lots = (data.lots && !isNaN(data.lots)) && parseInt(data.lots);

  const tickData = {
    symbol: data.symbol,
    price: parseFloat(data.p),
    timestamp: new Date(data.ts),
    lots: lots,
    type: data.bora === 'B' ? 'BID' : 'ASK'
  };

  marketDataQueue.add(tickData);
  console.log(`Processed tick for ${tickData.symbol}: ${tickData.price} with lots ${tickData.lots}`);
}

// Subscription management
function subscribeToPairs(pairs: string[]) {
  const message = {
    action: 'SubAdd',
    subs: pairs.map(pair => `0~${pair}`)
  };

  if (wsClient.socket?.readyState === WebSocket.OPEN) {
    wsClient.socket.send(JSON.stringify(message));
    pairs.forEach(pair => wsClient.subscribedPairs.add(pair));
  } else {
    wsClient.pendingSubscriptions.push({ action: 'SubAdd', pairs });
  }
}

function subscribeToAll() {
  const message = {
    action: 'SubAddAll'
  };

  if (wsClient.socket?.readyState === WebSocket.OPEN) {
    wsClient.socket.send(JSON.stringify(message));
    wsClient.subscribedAll = true;
  } else {
    wsClient.pendingSubscriptions.push({ action: 'SubAddAll' });
  }
}

// Connection management
function handleDisconnection() {
  wsClient.isConnected = false;
  stopHeartbeat();
  scheduleReconnection();
}

function scheduleReconnection() {
  if (wsClient.reconnectAttempts >= MAX_WS_RECONNECT_ATTEMPTS) {
    console.log('Max reconnection attempts reached');
    return;
  }

  wsClient.reconnectAttempts++;
  const delay = Math.min(WS_RECONNECT_DELAY * Math.pow(2, wsClient.reconnectAttempts - 1), 30000);

  console.log(`Reconnecting in ${delay}ms (attempt ${wsClient.reconnectAttempts})`);

  setTimeout(() => {
    if (!wsClient.isConnected) {
      initializeWebSocket();
    }
  }, delay);
}




async function processTickForCandles(tickData: TickData) {
  try {
    // Add to candle processing queue
    await candleProcessingQueue.add(
      {
        tickData,
        timeFrames: Object.keys(timeFrames),
      },
      {
        jobId: `candle_${tickData.symbol}_${Date.now()}`,
      }
    );
  } catch (error) {
    console.error("Error adding tick to candle processing queue:", error);
  }
}

// Database Functions
async function initDatabase() {
  try {
    console.log('Flushing all data in Redis');
    await redisClient.FLUSHALL();

    await fetchAllCurrencyPairs();
    await populateRedisWithMarkuplots();

    // for (const pair of availableCurrencyPairs) {
    //   const tableName = `candles_${pair.currpair.toLowerCase()}_bid`;
    //   await ensureCandleTableExists(tableName);
    //   console.log(`Ensured candle table exists for ${pair.currpair}`);
    // }

    // const resolutions = ["M1", "H1", "D1"];
    // const BATCH_SIZE = 5;

    // console.log(availableCurrencyPairs, "Available Currency Pairs");

    // for (let i = 0; i < availableCurrencyPairs.length; i += BATCH_SIZE) {
    //   const batch = availableCurrencyPairs.slice(i, i + BATCH_SIZE);

    //   await Promise.all(
    //     batch.flatMap(pair => 
    //       resolutions.map(resolution => 
    //         populateRedisWithCandles(pair.currpair, resolution)
    //           .catch(err => console.error(`Failed ${pair.currpair} ${resolution}:`, err))
    //       )
    //   )
    // );
    //   console.log(` - Processed batch ${i/BATCH_SIZE + 1} of ${Math.ceil(availableCurrencyPairs.length/BATCH_SIZE)}`);
    // }

    return true;
  } catch (error) {
    console.error("Database init failed:", error);
    throw error;
  }
}
async function ensureCandleTableExists(tableName: string): Promise<void> {
  try {
    const tableCheck = await pgPool.query(
      `SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = $1
      )`,
      [tableName]
    );

    if (!tableCheck.rows[0].exists) {
      await pgPool.query(`
        CREATE TABLE ${tableName} (
          candlesize TEXT NOT NULL,
          lots SMALLINT NOT NULL,
          candletime TIMESTAMP WITH TIME ZONE NOT NULL,
          open NUMERIC(12,5) NOT NULL,
          high NUMERIC(12,5) NOT NULL,
          low NUMERIC(12,5) NOT NULL,
          close NUMERIC(12,5) NOT NULL,
          PRIMARY KEY (candlesize, lots, candletime)
        )
      `);
      console.log(`Created candle table ${tableName}`);
    }
  } catch (error) {
    console.error(`Error ensuring candle table ${tableName} exists:`, error);
    throw error;
  }
}

async function processTick(tickData: {
  currpair: string;
  lots: number;
  price: number;
  tickepoch: number;
}) {
  const { currpair, lots, price, tickepoch } = tickData;

  const resolutions = ["M1", "H1", "D1"];

  for (const resolution of resolutions) {
    await processTickResolution(currpair, lots, price, tickepoch, resolution);
  }
}

async function processTickResolution(
  currpair: string,
  lots: number,
  price: number,
  tickepoch: number,
  resolution: string
) {
  const redisKey = `${currpair}_${resolution}`;

  let floor;
  switch (resolution) {
    case "M1":
      floor = Math.floor(tickepoch / 60) * 60;
      break;
    case "H1":
      floor = Math.floor(tickepoch / 3600) * 3600;
      break;
    case "D1":
      const date = new Date(tickepoch * 1000);
      floor =
        new Date(
          date.getUTCFullYear(),
          date.getUTCMonth(),
          date.getUTCDate()
        ).getTime() / 1000;
      break;
    default:
      return;
  }

  // Check if a candle already exists for this timeframe
  const existingCandle = await redisClient.zRangeByScore(
    redisKey,
    floor,
    floor
  );

  if (existingCandle.length > 0) {
    // Update existing candle
    const candle = JSON.parse(existingCandle[0]);
    candle.close = price;
    candle.high = Math.max(candle.high, price);
    candle.low = Math.min(candle.low, price);

    await addRedisRecord(redisKey, { candleepoch: floor, ...candle }, true);
  } else {
    // Create new candle
    const newCandle = {
      candleepoch: floor,
      open: price,
      high: price,
      low: price,
      close: price,
    };

    await addRedisRecord(redisKey, newCandle);
  }
}

async function addRedisRecord(
  redisKey: string,
  candleData: any,
  deleteExisting = false
) {
  try {
    if (
      candleData.candleepoch === undefined ||
      isNaN(Number(candleData.candleepoch))
    ) {
      throw new Error(`Invalid score: ${candleData.candleepoch}`);
    }

    if (deleteExisting) {
      const score = Number(candleData.candleepoch);
      await redisClient.zRemRangeByScore(redisKey, score, score);
    }

    const score = Number(candleData.candleepoch);
    const record = JSON.stringify({
      time: candleData.candleepoch,
      open: candleData.open,
      high: candleData.high,
      low: candleData.low,
      close: candleData.close,
    });

    await redisClient.zAdd(redisKey, [
      {
        score: score,
        value: record,
      },
    ]);
  } catch (error) {
    console.error(`Error adding/updating Redis record for ${redisKey}:`, error);
  }
}

async function fetchAllCurrencyPairs() {
  try {
    const result = await pgPool.query(
      "SELECT currpair, contractsize FROM currpairdetails"
    );
    availableCurrencyPairs = result.rows;

    const validPairs = availableCurrencyPairs.filter(
      (pair) => pair.contractsize !== null
    );

    const invalidPairs = availableCurrencyPairs.filter(
      (pair) => pair.contractsize === null
    );

    // if (invalidPairs.length > 0) {
    //   console.log(
    //     "Skipping subscription for the following pairs due to null contract size:"
    //   );
    //   invalidPairs.forEach((pair) => console.log(`- ${pair.currpair}`));
    // }

    // Add valid pairs to subscribed set
    validPairs.forEach((pair) => {
      subscribedPairs.add(pair.currpair);
    });

    for (const pair of validPairs) {
      await ensureTableExists(
        `ticks_${pair.currpair.toLowerCase()}_bid`,
        "BID"
      );
      await ensureTableExists(
        `ticks_${pair.currpair.toLowerCase()}_ask`,
        "ASK"
      );
    }
  } catch (error) {
    console.error("Error fetching currency pairs:", error);
  }
}

async function ensureTableExists(
  tableName: string,
  type: "BID" | "ASK"
): Promise<void> {
  try {
    const tableCheck = await pgPool.query(
      `
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = $1
            )
        `,
      [tableName]
    );

    if (!tableCheck.rows[0].exists) {
      await pgPool.query(`
                CREATE TABLE ${tableName} (
                    ticktime TIMESTAMP WITH TIME ZONE NOT NULL,
                    lots INTEGER PRIMARY KEY,
                    price NUMERIC NOT NULL
                )
            `);
    }
  } catch (error) {
    console.error(`Error ensuring table ${tableName} exists:`, error);
    throw error;
  }
}

async function populateRedisWithCandles(symbol: string, resolution: string): Promise<void> {
  try {
    const redisKey = `${symbol}_${resolution}`;
    await redisClient.del(redisKey);

    const candleStream = streamCandlesFromDatabase(symbol, resolution);
    let totalLoaded = 0;
    let batchCount = 0;

    for await (const batch of candleStream) {
      batchCount++;

      // Process batch with pipeline
      const pipeline = redisClient.multi();
      for (const candle of batch) {
        const candleData = {
          time: candle.timestamp,
          open: candle.open,
          high: candle.high,
          low: candle.low,
          close: candle.close,
        };
        pipeline.zAdd(redisKey, { score: candle.timestamp, value: JSON.stringify(candleData) });
        console.log(`✅ Added candle - Time: ${candle.timestamp}, Open: ${candle.open}, High: ${candle.high}`);

      }

      await pipeline.exec();
      totalLoaded += batch.length;

      // Force garbage collection every few batches
      if (batchCount % 10 === 0) {
        if (global.gc) {
          global.gc();
        }
        console.log(`Processed ${totalLoaded} candles for ${symbol} (${resolution})`);
      }
    }

    console.log(`Successfully loaded ${totalLoaded} candles for ${symbol} (${resolution})`);
  } catch (error) {
    console.error(`Error loading candles for ${symbol} (${resolution}):`, error);
    throw error;
  }
}

async function* streamCandlesFromDatabase(symbol: string, resolution: string, batchSize = 1000) {
  const tableName = `candles_${symbol.toLowerCase()}_bid`;
  let offset = 0;
  let hasMore = true;

  while (hasMore) {
    const query = {
      text: `
        SELECT 
          EXTRACT(EPOCH FROM candletime) AS timestamp,
          open, high, low, close
        FROM ${tableName}
        WHERE candlesize = $1
        ORDER BY candletime ASC
        LIMIT $2 OFFSET $3
      `,
      values: [resolution, batchSize, offset],
    };

    const result = await pgPool.query(query);
    if (result.rows.length === 0) {
      hasMore = false;
    } else {
      yield result.rows;
      offset += batchSize;
    }
  }
}

async function populateRedisWithMarkuplots(): Promise<void> {
  try {
    const markuplotsData = await fetchMarkuplotsFromDatabase();

    for (const row of markuplotsData) {
      const { currpair, tradertype, decimals, mu_b, mu_a } = row;

      // Ensure mu_b and mu_a are numbers
      const markuppipsBid = typeof mu_b === 'number' ? parseFloat(mu_b.toFixed(decimals)) : 0;
      const markuppipsAsk = typeof mu_a === 'number' ? parseFloat(mu_a.toFixed(decimals)) : 0;

      // Create Redis keys
      const redisKeyBid = `markup_${currpair}_${tradertype}_B`;
      const redisKeyAsk = `markup_${currpair}_${tradertype}_A`;

      // Store values in Redis
      await redisClient.set(redisKeyBid, markuppipsBid);
      await redisClient.set(redisKeyAsk, markuppipsAsk);

      console.log(`Stored markuplots for ${currpair} (${tradertype}): Bid=${markuppipsBid}, Ask=${markuppipsAsk}`);
    }

    console.log("Successfully populated Redis with markuplots data");
  } catch (error) {
    console.error("Error populating Redis with markuplots data:", error);
  }
}

async function fetchMarkuplotsFromDatabase(): Promise<any[]> {
  const query = `
    WITH cpd AS (
      SELECT currpair, pointsperunit, decimals, 
             ROW_NUMBER() OVER (PARTITION BY currpair ORDER BY effdate DESC) AS rn 
      FROM currpairdetails 
      WHERE effdate <= NOW()
    ),
    mul AS (
      SELECT currpair, tradertype, markuppips_bid, markuppips_ask, 
             ROW_NUMBER() OVER (PARTITION BY currpair, tradertype ORDER BY effdate DESC) AS rn 
      FROM markuplots 
      WHERE effdate <= NOW()
    )
    SELECT mul.currpair, mul.tradertype, decimals, 
           10 * markuppips_bid / pointsperunit AS mu_b, 
           10 * markuppips_ask / pointsperunit AS mu_a
    FROM cpd, mul
    WHERE cpd.currpair = mul.currpair
      AND cpd.rn = 1
      AND mul.rn = 1
    ORDER BY mul.currpair, mul.tradertype;
  `;

  try {
    const result = await pgPool.query(query);
    return result.rows;
  } catch (error) {
    console.error("Error fetching markuplots data from database:", error);
    throw error;
  }
}

// Queue Processing
marketDataQueue.process(5, async (job) => {
  try {
    const data = job.data;
    console.log(`Processing market data job for ${data.symbol} (${data.type})`);

    // Validate incoming data
    if (!data.symbol || !data.price || !data.lots) {
      throw new Error(`Invalid market data: ${JSON.stringify(data)}`);
    }

    let lots = parseInt(data.lots);

    let ticktime: Date;
    if (data.ts) {
      ticktime = new Date(data.ts);
    } else if (data.timestamp) {
      ticktime = new Date(data.timestamp);
    } else {
      throw new Error('No timestamp available in market data');
    }

    // Validate the timestamp
    if (isNaN(ticktime.getTime())) {
      throw new Error(`Invalid timestamp received: ${data.ts || data.timestamp}`);
    }

    // Determine which table to use based on the type
    let tableName: string;
    const symbolLower = data.symbol.toLowerCase();

    if (data.type === "BID") {
      tableName = `ticks_${symbolLower}_bid`;
    } else {
      tableName = `ticks_${symbolLower}_ask`;
    }

    await ensureTableExists(tableName, data.type);

    console.log(`Processing tick - Symbol: ${data.symbol}, Type: ${data.type}, Price: ${data.price}, Lots: ${lots}, Time: ${ticktime}`);

    // const insertQuery = {
    //   text: `
    //     INSERT INTO ${tableName} 
    //     (ticktime, lots, price)
    //     VALUES ($1, $2, $3)
    //     ON CONFLICT (lots) DO NOTHING;
    //   `,
    //   values: [
    //     ticktime.toISOString(), 
    //     lots, 
    //     parseFloat(data.price.toString()) // Ensure price is a valid number
    //   ],
    // };

    // const result = await pgPool.query(insertQuery);

    // if (result.rowCount && result.rowCount > 0) {
    //   console.log(`✅ Successfully inserted into ${tableName}:`, {
    //     symbol: data.symbol,
    //     type: data.type,
    //     price: data.price,
    //     lots: lots,
    //     timestamp: ticktime.toISOString(),
    //     rowsAffected: result.rowCount
    //   });
    // } else {
    //   console.log(`ℹ️ No rows inserted (likely duplicate) into ${tableName}:`, {
    //     symbol: data.symbol,
    //     lots: lots
    //   });
    // }


    // Process tick in Redis
    await processTick({
      currpair: data.symbol,
      lots: lots,
      price: data.price,
      tickepoch: Math.floor(ticktime.getTime() / 1000),
    });

    if (data.type === "BID") {

      await processTickForCandles({
        symbol: data.symbol,
        price: data.price,
        timestamp: ticktime,
        lots: lots
      });
    }
    console.log(`Successfully processed tick for ${data.symbol}`);

    return { success: true, symbol: data.symbol, type: data.type };
  } catch (error) {
    console.error(`Error processing market data job for ${job.data?.symbol}:`, error);
    throw error;
  }
});

candleProcessingQueue.process(async (job) => {
  const { tickData, timeFrames } = job.data;
  const { symbol, price, timestamp } = tickData;

  const lots = 1;
  const tableName = `candles_${symbol.toLowerCase()}_bid`;

  await ensureCandleTableExists(tableName);


  // Robust timestamp parsing
  let processedTimestamp: Date;
  if (timestamp instanceof Date && !isNaN(timestamp.getTime())) {
    processedTimestamp = timestamp;
  } else if (timestamp) {
    try {
      processedTimestamp = new Date(timestamp);
      if (isNaN(processedTimestamp.getTime())) {
        const numTimestamp = Number(timestamp);
        processedTimestamp = new Date(
          numTimestamp > 1000000000000 ? numTimestamp : numTimestamp * 1000
        );
      }
    } catch (error) {
      console.error(`Failed to parse timestamp for ${symbol}:`, timestamp);
      throw new Error(`Invalid timestamp for ${symbol}: ${timestamp}`);
    }
  } else {
    processedTimestamp = new Date();
  }

  if (isNaN(processedTimestamp.getTime())) {
    throw new Error(`Invalid timestamp for ${symbol}: ${timestamp}`);
  }

  const resolvedTimeFrames = {
    M1: 60000,
    H1: 3600000,
    D1: 86400000,
  };

  // Process each timeframe
  for (const [timeframe, duration] of Object.entries(resolvedTimeFrames)) {
    try {
      const candleTime = new Date(
        Math.floor(processedTimestamp.getTime() / duration) * duration
      );

      // Check if candle exists in PostgreSQL
      const existingCandle = await pgPool.query({
        text: `
          SELECT * FROM ${tableName}
          WHERE candlesize = $1 AND lots = $2 AND candletime = $3
        `,
        values: [timeframe, lots, candleTime.toISOString()],
      });

      if (existingCandle.rows.length > 0) {
        // const updateResult= await pgPool.query({
        //   text: `
        //     UPDATE ${tableName}
        //     SET high = GREATEST(high, $1),
        //         low = LEAST(low, $2),
        //         close = $3
        //     WHERE candlesize = $4
        //     AND lots = $5
        //     AND candletime = $6
        //     RETURNING *
        //   `,
        //   values: [
        //     price,
        //     price,
        //     price,
        //     timeframe,
        //     lots,
        //     candleTime.toISOString(),
        //   ],
        // });
        // console.log(`🔄 Updated ${timeframe} candle for ${symbol}:`, updateResult.rows[0]);
      } else {
        //  const insertResult = await pgPool.query({
        //      text: `
        //        INSERT INTO ${tableName}
        //        (candlesize, lots, candletime, open, high, low, close)
        //      VALUES ($1, $2, $3, $4, $5, $6, $7)
        //        RETURNING *
        //      `,
        //      values: [
        //        timeframe,
        //        lots,
        //        candleTime.toISOString(),
        //        price, // open
        //        price, // high
        //        price, // low
        //        price, // close
        //      ],
        //    });
        //    console.log(`✅ Created new ${timeframe} candle for ${symbol}:`, insertResult.rows[0]);
      }

      // Process in Redis
      await processTick({
        currpair: symbol,
        lots: 1,
        price: price,
        tickepoch: Math.floor(processedTimestamp.getTime() / 1000),
      });

    } catch (error) {
      console.error(`Error processing ${timeframe} candle for ${symbol}:`, error);
      throw error;
    }
  }

  return { success: true, symbol };
});


// Helper Functions
async function getContractSize(symbol: string): Promise<number> {
  try {
    const pairInfo = availableCurrencyPairs.find(
      (pair) => pair.currpair === symbol
    );

    if (pairInfo && pairInfo.contractsize !== null) {
      return parseFloat(pairInfo.contractsize.toString());
    } else {
      // Try to get the contract size directly from the database as a fallback
      try {
        const result = await pgPool.query(
          "SELECT contractsize FROM currpairdetails WHERE currpair = $1",
          [symbol]
        );

        if (result.rows.length > 0 && result.rows[0].contractsize !== null) {
          return parseFloat(result.rows[0].contractsize.toString());
        }
      } catch (dbError) {
        console.error(`Database lookup for contract size failed:`, dbError);
      }

      throw new Error(
        `Cannot process data for ${symbol}: No valid contract size found`
      );
    }
  } catch (error) {
    console.error(`Error getting contract size for ${symbol}:`, error);
    throw new Error(`Cannot process market data: ${error.message}`);
  }
}

function calculateLots(quantity: number, contractSize: number): number {
  return Math.round(quantity / contractSize);
}

// Redis Functions
function setupRedisHealthCheck() {
  const HEALTH_CHECK_INTERVAL = 30000;

  setInterval(async () => {
    try {
      if (!redisClient.isOpen) {
        console.log("Redis connection is down, attempting to reconnect...");
        await redisClient.connect();
        console.log("Redis connection restored");
      } else {
        // Perform a simple ping to verify the connection is actually working
        await redisClient.ping();
      }
    } catch (error) {
      console.error("Redis health check failed:", error);

      // If we get here, the connection is broken but isOpen might still be true
      // Force a reconnection
      try {
        if (redisClient.isOpen) {
          await redisClient.disconnect();
        }
        await redisClient.connect();
        console.log("Redis connection restored after forced reconnection");
      } catch (reconnectError) {
        console.error("Failed to restore Redis connection:", reconnectError);
      }
    }
  }, HEALTH_CHECK_INTERVAL);
}

// Application Initialization
async function initializeApplication() {
  try {
    // Connect to Redis
    await redisClient.connect();
    console.log("Connected to Redis");
    setupRedisHealthCheck();

    // Initialize database
    await initDatabase();

    // Initialize WebSocket connection
    // initializeWebSocket();

    // REST API Endpoints
    app.get("/api/candles", async (req: Request, res: Response): Promise<void> => {
      try {
        const { symbol: symbolParam, fsym: fsymParam, tsym: tsymParam, resolution, startDate, endDate, limit, tt } = req.query;

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

        // Timestamp & Limit Handling
        let startTimestamp = 0;
        let endTimestamp = 0;
        let fetchLimit = 0;

        if (startDate && endDate) {
          startTimestamp = Math.floor(new Date(startDate as string).getTime() / 1000);
          endTimestamp = Math.floor(new Date(endDate as string).getTime() / 1000);
          if (isNaN(startTimestamp) || isNaN(endTimestamp)) {
            res.status(400).json({ success: false, message: "Invalid date format. Use ISO format." });
            return;
          }
        } else if (limit) {
          fetchLimit = parseInt(limit as string);
          if (isNaN(fetchLimit) || fetchLimit <= 0) {
            res.status(400).json({ success: false, message: "Invalid limit value." });
            return;
          }
        } else {
          res.status(400).json({ success: false, message: "Either (startDate & endDate) or limit is required." });
          return;
        }

        // Redis key in original format (without lowercase)
        const redisKey = `${symbol}_${candleSize}`;

        // Database table name in lowercase with _bid suffix
        const tableName = `candles_${symbol.toLowerCase()}_bid`;

        // Function to fetch candles from database
        const fetchFromDatabase = async () => {
          try {
            let query = `
          SELECT 
            EXTRACT(EPOCH FROM candletime) AS timestamp,
            open, high, low, close
          FROM ${tableName}
          WHERE candlesize = $1
        `;
            const params: any[] = [candleSize];

            if (startTimestamp && endTimestamp) {
              query += ` AND candletime BETWEEN to_timestamp($2) AND to_timestamp($3)`;
              params.push(startTimestamp, endTimestamp);
            }

            if (fetchLimit > 0) {
              query += ` ORDER BY candletime DESC LIMIT $${params.length + 1}`;
              params.push(fetchLimit);
            } else {
              query += ` ORDER BY candletime ASC`;
            }

            const { rows } = await pgPool.query(query, params);
            return rows;
          } catch (error) {
            console.error("Database fetch error:", error);
            throw error;
          }
        };

        let candleData: string[] = [];
        let fromRedis = true;

        try {
          if (fetchLimit > 0) {
            candleData = await redisClient.zRange(redisKey, -fetchLimit, -1);
          } else {
            candleData = await redisClient.zRangeByScore(
              redisKey as RedisCommandArgument,
              startTimestamp.toString(),
              endTimestamp.toString(),
              { WITHSCORES: true } as ZRangeByScoreOptions
            );
          }

          if (candleData.length === 0) {
            fromRedis = false;
            throw new Error("No data in Redis");
          }
        } catch (redisError) {
          console.error("Redis fetch error:", redisError);
          fromRedis = false;
        }

        // If no data in Redis, fetch from database
        if (!fromRedis) {
          try {
            const dbCandles = await fetchFromDatabase();

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

            // Convert database results to Redis format
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
        }

        // Fetch Trader Type Markup
        let markup = 0;
        if (tt && ["R", "I", "S", "T"].includes(tt as string)) {
          const markupKey = `markup_${symbol}_${tt}_B`;
          const markupValue = await redisClient.get(markupKey);
          if (markupValue) markup = parseFloat(markupValue);
        }

        // Parse Candlestick Data & Apply Markup
        const parsedCandles = candleData
          .map((value, index) => (index % 2 === 0 ? JSON.parse(value) : null))
          .filter((value) => value !== null)
          .map((candle) => ({
            ...candle,
            open: parseFloat((candle.open - markup).toFixed(10)),
            high: parseFloat((candle.high - markup).toFixed(10)),
            low: parseFloat((candle.low - markup).toFixed(10)),
            close: parseFloat((candle.close - markup).toFixed(10)),
          }));

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




    // Start the server
    app.listen(Number(PORT), '0.0.0.0', () => {
      console.log(`Server is running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Application initialization failed:", error);
    process.exit(1);
  }
}

// Event Listeners
marketDataQueue.on("completed", (job, result) => {
  console.log(`Job ${job.id} completed: ${result.symbol} ${result.type}`);
});

marketDataQueue.on("failed", (job, error) => {
  console.error(`Job ${job.id} failed:`, error);
});

candleProcessingQueue.on("completed", (job, result) => {
  console.log(`Job ${job.id} completed: ${result.symbol}`);
});

candleProcessingQueue.on("failed", (job, error) => {
  console.error(`Job ${job.id} failed:`, error);
});

// Process Cleanup
process.on("SIGINT", async () => {
  console.log("Shutting down...");

  // First close WebSocket connection
  if (wsClient.socket) {
    wsClient.socket.close();
  }


  // Then close other connections
  console.log("Closing Bull queues...");
  await marketDataQueue.close();
  await candleProcessingQueue.close();

  console.log("Closing Redis connection...");
  await redisClient.quit();

  await pgPool.end().then(() => {
    console.log("Database connection closed");
    process.exit(0);
  });
});

// Start the application
initializeApplication();