import fastify from "fastify";
import fastifyIO from "fastify-socket.io";
import mysql from "mysql2/promise";
import { Subject } from "rxjs";
import { Server, Socket } from "socket.io";

const dbCredentials = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  port: process.env.DB_PORT ? parseInt(process.env.DB_PORT) : undefined,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
};

Object.entries(dbCredentials).some((credential) => {
  if (!credential[1]) throw new Error(`Undefined credential ${credential[0]}`);
});

enum Room {
  AAPL = "AAPL",
  GOOGL = "GOOGL",
  MSFT = "MSFT",
  AMZN = "AMZN",
}

interface Order {
  secnum: number;
  quantity: number;
  price: number;
  symbol: string;
  side: "ask" | "bid";
}

class SimpleOrder {
  price: number;
  quantity: number;
  symbol: string;
  side: "ask" | "bid";
  constructor(
    quantity: number,
    price: number,
    symbol: string,
    side: "ask" | "bid",
  ) {
    this.quantity = quantity;
    this.price = price;
    this.symbol = symbol;
    this.side = side;
  }
}

interface Execution extends Order {}

class SimpleExecution {
  price: number;
  side: "ask" | "bid";
  symbol: string;
  constructor(price: number, side: "ask" | "bid", symbol: string) {
    this.price = price;
    this.side = side;
    this.symbol = symbol;
  }
}

interface QuantityPerPriceAndSide {
  quantity: number;
  price: number;
  side: string;
}

interface Avg {
  side: string;
  interval: Date;
  average: number;
}

const server = fastify();
server.register(fastifyIO, {
  cors: {
    origin: "http://localhost:8080", // Replace with your frontend's URL
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type", "Authorization"],
  },
});
const pool = mysql.createPool(dbCredentials);
const eventProcessor = new Subject<SimpleOrder | SimpleExecution[]>();
const dataFeeder = new Subject<QuantityPerPriceAndSide[]>();

async function getAveragePrice(symbol: string) {
  const now = new Date();
  const query =
    "SELECT " +
    [
      "orders.side",
      "DATE_FORMAT(orders.timestamp, 'Y%-%m-%d %H:%i) AS interval",
      "SUM(orders.price * orders.quantity) / SUM(orders.quantity) as average",
    ].join(", ") +
    " " +
    "FROM orders " +
    "WHERE orders.symbol = ? " +
    "GROUP BY orders.interval, orders.side " +
    "ORDER BY orders.interval, orders.side";
  const query2 =
    "SELECT * FROM orders WHERE orders.symbol = ? ORDER BY orders.timestamp ASC";
  return pool
    .execute(query2, [symbol])
    .then((rows) => rows[0] as unknown as Avg[]);
}

async function getOrderBook(symbol: string) {
  const asksQuery =
    "SELECT " +
    [
      "orders.side",
      "orders.price",
      "SUM(orders.quantity) - COALESCE(SUM(executions.quantity), 0) AS quantity",
    ].join(", ") +
    " " +
    "FROM orders LEFT JOIN executions ON orders.secnum = executions.secnum " +
    "WHERE orders.filled = FALSE AND orders.symbol = ? " +
    "GROUP BY orders.price, orders.side " +
    "ORDER BY " +
    "CASE " +
    "WHEN orders.side = 'ask' THEN 2 " +
    "WHEN orders.side = 'bid' THEN 1 " +
    "END, " +
    "CASE WHEN orders.side = 'bid' THEN orders.price END DESC, " +
    "CASE WHEN orders.side = 'ask' THEN orders.price END ASC";
  return pool
    .execute(asksQuery, [symbol])
    .then((rows) => rows[0] as unknown as QuantityPerPriceAndSide[]);
}

async function getQuantitiesPerPrice(
  asks: SimpleExecution[],
  bids: SimpleExecution[],
) {
  const createQuery = (
    sort: "DESC" | "ASC",
    side: "bid" | "ask",
    params: string[],
  ) =>
    "SELECT " +
    [
      "orders.side",
      "orders.price",
      "SUM(orders.quantity) - COALESCE(SUM(executions.quantity), 0) AS quantity",
    ].join(", ") +
    " " +
    "FROM orders LEFT JOIN executions ON orders.secnum = executions.secnum " +
    "WHERE orders.filled = FALSE AND orders.symbol = ? AND orders.price " +
    `IN (${params.join(", ")}) ` +
    `AND orders.side = '${side}' ` +
    "GROUP BY orders.price, orders.side " +
    `ORDER BY orders.price ${sort}`;
  const bidQuery = createQuery(
    "DESC",
    "bid",
    bids.map((_) => "?"),
  );
  const askQuery = createQuery(
    "ASC",
    "ask",
    asks.map((_) => "?"),
  );
  return Promise.all([
    pool
      .execute(askQuery, [asks[0].symbol, ...asks.map((e) => e.price)])
      .then((rows) => rows[0] as unknown as QuantityPerPriceAndSide[]),
    pool
      .execute(bidQuery, [bids[0].symbol, ...bids.map((e) => e.price)])
      .then((rows) => rows[0] as unknown as QuantityPerPriceAndSide[]),
  ]);
}

async function getQuantityPerPrice(order: SimpleOrder) {
  const createQuery = (sort: "DESC" | "ASC") =>
    "SELECT " +
    [
      "orders.side",
      "orders.price",
      "SUM(orders.quantity) - COALESCE(SUM(executions.quantity), 0) AS quantity",
    ].join(", ") +
    " " +
    "FROM orders LEFT JOIN executions ON orders.secnum = executions.secnum " +
    "WHERE orders.filled = FALSE AND orders.symbol = ? AND orders.price = ? " +
    "AND orders.side = ? " +
    "GROUP BY orders.price, orders.side " +
    `ORDER BY orders.price ${sort}`;
  return pool.execute(createQuery(order.side === "ask" ? "ASC" : "DESC"), [
    order.symbol,
    order.price,
    order.side,
  ]);
}

server.post("/order", async (request, replyTo) => {
  const order = request.body as unknown as Order;
  eventProcessor.next(
    new SimpleOrder(order.quantity, order.price, order.symbol, order.side),
  );
  replyTo.code(200).send();
});

function handleExecutions(arr: Execution[]) {
  if (arr.length === 0) return;
  const symbol = arr[0].symbol;
  const filtered = arr.filter((e) => e.symbol === symbol);
  eventProcessor.next(
    filtered.map((e) => new SimpleExecution(e.price, e.side, e.symbol)),
  );
  if (filtered.length === arr.length) return;
  else handleExecutions(arr.filter((e) => e.symbol !== symbol));
}

server.post("/executions", async (request, _) => {
  const executions = request.body as unknown as Execution[];
  handleExecutions(executions);
  return;
});

server.get("/", async (_, replyTo) => {
  replyTo.send("Publisher operational");
});

server.ready().then(() => {
  server.io.on("connection", (socket: Socket) => {
    socket.on("joinRoom", (room: string) => {
      switch (room) {
        case Room.MSFT:
        case Room.AAPL:
        case Room.AMZN:
        case Room.GOOGL:
          socket.join(room);
          getOrderBook(room)
            .then((results) => socket.emit("orderBook", results[0]))
            .catch((e: any) => {
              console.error(e);
            });
          getAveragePrice(room)
            .then((results) => socket.emit("avgPricePerMin", results[0]))
            .catch((e: any) => {
              console.error(e);
            });
          break;
        default:
          break;
      }
    });
    socket.on("leaveRoom", (room: string) => {
      console.log(`leaveRoom ${room}`);
      socket.leave(room);
    });
  });
  eventProcessor.subscribe((event) => {
    if (Array.isArray(event)) {
      event as SimpleExecution[];
      const eventAsks = event.filter((e) => e.side === "ask");
      const eventBids = event.filter((e) => e.side === "bid");
      getQuantitiesPerPrice(eventAsks, eventBids).then(([asks, bids]) => {
        eventAsks.forEach((eA) => {
          if (asks.findIndex((a) => a.price === eA.price) === -1) {
            asks.push({ price: eA.price, side: eA.side, quantity: 0 });
          }
        });
        eventBids.forEach((eventBid) => {
          if (bids.findIndex((b) => b.price === eventBid.price))
            bids.push({
              price: eventBid.price,
              side: eventBid.side,
              quantity: 0,
            });
        });
        server.io.to(event[0].symbol).emit("updates", {
          asks: asks,
          bids: bids,
        });
      });
    } else {
      event as SimpleOrder;
      getQuantityPerPrice(event).then((result) => {
        server.io.to(event.symbol).emit("update", result[0]);
      });
    }
  });
});

server.listen({ port: 3000, host: "0.0.0.0" }, () => {
  console.log(`Server listening on port: 3000 `);
});

declare module "fastify" {
  interface FastifyInstance {
    io: Server;
  }
}
