FROM node:20.15

WORKDIR /app

COPY package*.json ./

RUN npm install
RUN npm install -g ts-node

COPY . .

EXPOSE 3000

ENTRYPOINT ["ts-node", "main.ts" ]
