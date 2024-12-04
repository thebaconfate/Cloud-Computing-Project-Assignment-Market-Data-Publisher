import Fastify from "fastify";
import http from "http";
import mysql from "mysql2/promise";
import { Subject } from "rxjs";
import { Server } from "socket.io";

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

enum EventType {
  ORDER,
  EXECUTION,
}

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
  side: string;
}

interface Execution extends Order {}

interface StoredOrder extends Order {
  filled: boolean;
}

interface Avg {
  side: string;
  interval: Date;
  average: number;
}

class Event implements Execution, Order {
  type: EventType;
  secnum: number;
  quantity: number;
  price: number;
  symbol: string;
  side: string;

  constructor(event: Execution | Order, type: EventType) {
    this.type = type;
    this.secnum = event.secnum;
    this.quantity = event.quantity;
    this.price = event.quantity;
    this.symbol = event.symbol;
    this.side = event.side;
  }
}

const fastify = Fastify();
const server = http.createServer(fastify.server);
const io = new Server(server);
const pool = mysql.createPool(dbCredentials);
const eventFeeder = new Subject<Event>();
eventFeeder.subscribe((event) => {
  switch (event.type) {
    case EventType.ORDER:
      io.to(event.symbol).emit("order", event);
      break;
    case EventType.EXECUTION:
      io.to(event.symbol).emit("execution", event);
      break;
  }
});

async function getAveragePrice(symbol: string) {
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
  return pool
    .execute(query, [symbol])
    .then((rows) => rows[0] as unknown as Avg[]);
}

async function getOrderBook(symbol: string) {
  const query =
    "SELECT " +
    [
      "orders.secnum",
      "orders.symbol",
      "orders.side",
      "orders.price",
      "orders.quantity",
      "COALESCE(SUM(executions.quantity),0) as filled",
    ].join(", ") +
    " " +
    "FROM orders LEFT JOIN executions WHERE orders.filled = FALSE GROUP BY orders.id " +
    "WHERE orders.symbol = ?";
  return pool
    .execute(query, [symbol])
    .then((rows) => rows[0] as unknown as StoredOrder[]);
}

fastify.post("/order", async (request, replyTo) => {
  const order = request.body as unknown as Order;
  eventFeeder.next(new Event(order, EventType.ORDER));
  replyTo.status(200).send();
});

fastify.post("/execution", async (request, replyTo) => {
  const executions = request.body as unknown as Execution[];
  executions
    .reverse()
    .forEach((execution) =>
      eventFeeder.next(new Event(execution, EventType.EXECUTION)),
    );
  replyTo.status(200).send();
});

io.on("connection", (socket) => {
  socket.on("joinRoom", (room) => {
    switch (room) {
      case Room.MSFT:
      case Room.AAPL:
      case Room.AMZN:
      case Room.GOOGL:
        socket.join(room);
        Promise.all([getOrderBook(room), getAveragePrice(room)])
          .then((results) => {
            socket.emit("joinResult", {
              orderBook: results[0],
              averages: results[1],
            });
          })
          .catch((e: any) => {
            console.error(e);
          });
        break;
      default:
        break;
    }
  });

  socket.on("leaveRoom", (room) => {
    socket.leave(room);
  });
});

server.listen({ port: 3000, host: "0.0.0.0" }, () => {
  console.log(`Server listening on port: 3000 `);
});
