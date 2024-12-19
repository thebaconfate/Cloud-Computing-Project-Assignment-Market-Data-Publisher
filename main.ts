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

interface OrderInterface {
  secnum: number;
  quantity: number;
  price: number;
  symbol: string;
  side: "ask" | "bid";
}

interface Execution {
  asks: Order[];
  bids: Order[];
}

class OrderWithoutSymbol {
  secnum: number;
  side: string;
  price: number;
  quantity: number;
  constructor(secnum: number, price: number, quantity: number, side: string) {
    this.secnum = secnum;
    this.price = price;
    this.quantity = quantity;
    this.side = side;
  }
}

class Order extends OrderWithoutSymbol {
  symbol: string;
  constructor(
    secnum: number,
    price: number,
    quantity: number,
    side: string,
    symbol: string,
  ) {
    super(secnum, price, quantity, side);
    this.symbol = symbol;
  }
}

interface Avg {
  symbol: string;
  side: string;
  interval: Date;
  price: number;
}

interface DBAvg {
  symbol: string;
  side: string;
  interval: string;
  price: string;
}

interface DBOrder {
  secnum: string;
  side: string;
  quantity: string;
  price: string;
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
const eventProcessor = new Subject<Order | Execution>();

function stripDate(date: Date) {
  return new Date(
    date.getFullYear(),
    date.getMonth(),
    date.getDate(),
    date.getHours(),
    date.getMinutes(),
  );
}

function toSQLDatetime(date: Date) {
  return date.toISOString().slice(0, 19).replace("T", " ");
}

async function getAveragePricesPerTimestamp(symbol: string) {
  const nowExact = new Date();
  const now = stripDate(nowExact);
  const query =
    "SELECT " +
    [
      "orders.side",
      "DATE_FORMAT(orders.timestamp, '%Y-%m-%d %H:%i') AS `interval`",
      "SUM(orders.price * orders.quantity) / SUM(orders.quantity) as price",
    ].join(", ") +
    " " +
    "FROM orders " +
    "WHERE orders.symbol = ? AND orders.timestamp < ? " +
    "GROUP BY `interval`, orders.side " +
    "ORDER BY `interval`, orders.side";
  // There have to be backticks around interval because it's a reserved keyword
  return pool
    .execute(query, [symbol, toSQLDatetime(now)])
    .then((rows) => rows[0] as unknown as DBAvg[])
    .then((avgs) =>
      avgs.map((e) => {
        return {
          ...e,
          interval: new Date(e.interval.replace(" ", "T")),
          price: Number(e.price),
        };
      }),
    );
}

async function getAveragePricePerTimestamp() {
  const nowExact = new Date();
  const now = stripDate(nowExact);
  const guard = new Date(now);
  guard.setMinutes(guard.getMinutes() - 1);
  const query =
    "SELECT " +
    [
      "orders.side",
      "orders.symbol",
      "DATE_FORMAT(orders.timestamp, '%Y-%m-%d %H:%i') AS `interval`",
      "SUM(orders.price * orders.quantity) / SUM(orders.quantity) as price",
    ].join(", ") +
    " " +
    "FROM orders " +
    "WHERE orders.timestamp >= ? AND orders.timestamp < ? " +
    "GROUP BY `interval`, orders.side, orders.symbol";
  return pool
    .execute(query, [toSQLDatetime(guard), toSQLDatetime(now)])
    .then(([rows]) => {
      const castedRows = rows as unknown as DBAvg[];
      return castedRows.map((e) => {
        return {
          ...e,
          interval: new Date(e.interval.replace(" ", "T")),
          price: Number(e.price),
        };
      });
    });
}

async function getOrderBook(symbol: string) {
  const asksQuery =
    "SELECT " +
    [
      "orders.secnum",
      "orders.side",
      "orders.price",
      "orders.quantity_left AS quantity",
    ].join(", ") +
    " " +
    "FROM orders " +
    "WHERE orders.quantity_left > 0  AND orders.symbol = ? " +
    "ORDER BY " +
    "CASE " +
    "WHEN orders.side = 'ask' THEN 2 " +
    "WHEN orders.side = 'bid' THEN 1 " +
    "END, " +
    "CASE WHEN orders.side = 'bid' THEN orders.price END ASC, " +
    "CASE WHEN orders.side = 'ask' THEN orders.price END ASC";
  return pool.execute(asksQuery, [symbol]).then(([rows]) => {
    const result = rows as DBOrder[];
    const castedResults = result.map(
      (e) =>
        new OrderWithoutSymbol(
          Number(e.secnum),
          Number(e.price),
          Number(e.quantity),
          e.side,
        ),
    );
    return {
      asks: castedResults.filter((e) => e.side === "ask"),
      bids: castedResults.filter((e) => e.side === "bid"),
    };
  });
}

async function getCorrection(secnum: number) {
  const query =
    "SELECT " +
    ["secnum, price, quantity_left AS quantity", "side"].join(", ") +
    " " +
    "FROM orders " +
    "WHERE secnum = ?";
  return pool.execute(query, [secnum]).then(([rows]) => {
    const result = rows as DBOrder[];
    return result.map(
      (e) =>
        new OrderWithoutSymbol(
          Number(e.secnum),
          Number(e.price),
          Number(e.quantity),
          e.side,
        ),
    );
  });
}

function computePriceEachMinute(sendToClient: (avg: Avg[]) => void) {
  const now = new Date();
  const delay = 60000 - (now.getSeconds() * 1000 + now.getMilliseconds());
  setTimeout(() => {
    setInterval(() => {
      getAveragePricePerTimestamp().then((res) => {
        const map = new Map<string, Avg[]>();
        res.forEach((e) => {
          const value = map.get(e.symbol);
          if (value) value.push(e);
          else map.set(e.symbol, [e]);
        });
        for (const value of map.values()) sendToClient(value);
      });
    }, 60000);
  }, delay);
}

function handleExecutionsEvent(asks: any[], bids: any[]) {
  server.io.to(asks[0].symbol).emit("updates", { asks: asks, bids: bids });
}

function handleNewOrderEvent(order: Order) {
  server.io.to(order.symbol).emit("newOrder", order);
}

server.post("/executions", async (request, _) => {
  const executions = request.body as unknown as Execution;
  eventProcessor.next(executions);
  return;
});

server.post("/order", async (request, _) => {
  const order = request.body as unknown as OrderInterface;
  eventProcessor.next(
    new Order(
      order.secnum,
      order.price,
      order.quantity,
      order.side,
      order.symbol,
    ),
  );
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
            .then((results) => socket.emit("orderBook", results))
            .catch((e: any) => console.error(e));
          getAveragePricesPerTimestamp(room)
            .then((results) => {
              socket.emit("avgPricesPerMin", {
                asks: results.filter((r) => r.side === "ask"),
                bids: results.filter((r) => r.side === "bid"),
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
    socket.on("getOrderBook", (room: string) => {
      getOrderBook(room)
        .then((results) => socket.emit("orderBook", results))
        .catch((e: any) => console.error(e));
    });
    socket.on("leaveRoom", (room: string) => {
      socket.leave(room);
    });
    socket.on("getCorrection", (req: { ask: Order; bid: Order }) => {
      Promise.all([
        getCorrection(req.ask.secnum),
        getCorrection(req.bid.secnum),
      ]).then(([ask, bid]) => {
        const handleOrder = (
          order: OrderWithoutSymbol | undefined,
          original: Order,
        ) => {
          if (!order || order.quantity === original.quantity) return;
          else if (order.quantity === 0) socket.emit("delete", order);
          else socket.emit("update", order);
        };
        handleOrder(ask[0], req.ask);
        handleOrder(bid[0], req.bid);
      });
    });
  });
  eventProcessor.subscribe((event) => {
    if (event instanceof Order) {
      handleNewOrderEvent(event);
    } else {
      handleExecutionsEvent(event.asks, event.bids);
    }
  });
  computePriceEachMinute((avg: Avg[]) => {
    server.io.to(avg[0].symbol).emit("avgPricePerMin", {
      asks: avg.find((e) => e.side === "ask"),
      bids: avg.find((e) => e.side === "bid"),
    });
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
