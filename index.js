import 'dotenv/config';
import http from 'node:http';
import path from 'node:path';

import express from 'express';
import session from 'express-session';
import cookieParser from 'cookie-parser';
import axios from 'axios';
import { Server } from 'socket.io';

import { kafkaClient } from './kafka-client.js';

async function main() {
  const {
    PORT = 8000,
    CLIENT_ID,
    CLIENT_SECRET,
    ISSUER_URL,
    REDIRECT_URI,
    SESSION_SECRET,
  } = process.env;

  const app = express();
  const server = http.createServer(app);
  const io = new Server(server);

  // Session Middleware
  const sessionMiddleware = session({
    secret: SESSION_SECRET || 'fallback-secret',
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false }, // Set to true if using https
  });

  app.use(cookieParser());
  app.use(sessionMiddleware);

  // Share session with Socket.IO
  io.use((socket, next) => {
    sessionMiddleware(socket.request, {}, next);
  });

  // Auth Middleware for Sockets
  io.use((socket, next) => {
    if (socket.request.session && socket.request.session.user) {
      return next();
    }
    return next(new Error('unauthorized'));
  });

  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${PORT}`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ['location-updates'],
    fromBeginning: true,
  });

  kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`KafkaConsumer Data Received`, { data });
      io.emit('server:location:update', {
        id: data.id,
        name: data.name,
        latitude: data.latitude,
        longitude: data.longitude,
      });
      await heartbeat();
    },
  });

  io.on('connection', (socket) => {
    const user = socket.request.session.user;
    console.log(`[Socket:${socket.id}]: User ${user.email} Connected...`);

    socket.on('client:location:update', async (locationData) => {
      const { latitude, longitude } = locationData;

      if (typeof latitude !== 'number' || typeof longitude !== 'number') {
        return console.error(
          `Invalid location data from ${user.email}:`,
          locationData,
        );
      }

      console.log(
        `[Socket:${socket.id}]: client:location:update from ${user.email}:`,
        locationData,
      );

      await kafkaProducer.send({
        topic: 'location-updates',
        messages: [
          {
            key: user.id || user.sub,
            value: JSON.stringify({
              id: user.id || user.sub,
              name: user.name || user.email,
              latitude,
              longitude,
            }),
          },
        ],
      });
    });

    socket.on('disconnect', () => {
      console.log(`[Socket:${socket.id}]: User ${user.email} Disconnected`);
      io.emit('server:user:disconnect', { id: user.id || user.sub });
    });
  });

  // Auth Routes
  app.get('/login', (req, res) => {
    const authUrl = `${ISSUER_URL}/authorize?client_id=${CLIENT_ID}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}&response_type=code&scope=openid profile email`;
    res.redirect(authUrl);
  });

  app.get('/callback', async (req, res) => {
    const { code } = req.query;
    if (!code) return res.status(400).send('No code provided');

    try {
      // Exchange code for token
      const tokenResponse = await axios.post(`${ISSUER_URL}/token`, {
        grant_type: 'authorization_code',
        code,
        client_id: CLIENT_ID,
        client_secret: CLIENT_SECRET,
        redirect_uri: REDIRECT_URI,
      });

      const { access_token } = tokenResponse.data;

      // Get user profile
      const userResponse = await axios.get(`${ISSUER_URL}/userinfo`, {
        headers: { Authorization: `Bearer ${access_token}` },
      });

      req.session.user = userResponse.data;
      res.redirect('/');
    } catch (error) {
      console.error('Auth Error:', error.response?.data || error.message);
      res.status(500).send('Authentication failed');
    }
  });

  app.get('/logout', (req, res) => {
    req.session.destroy();
    res.redirect('/logged-out');
  });

  app.get('/logged-out', (req, res) => {
    res.send(`
      <div style="font-family: sans-serif; text-align: center; margin-top: 50px;">
        <h1>Logged Out</h1>
        <p>You have been successfully logged out of the tracker.</p>
        <a href="/login" style="display: inline-block; background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Login Again</a>
      </div>
    `);
  });

  // Protect static files
  app.use((req, res, next) => {
    if (
      req.path === '/login' ||
      req.path === '/callback' ||
      req.path === '/health' ||
      req.path === '/logged-out'
    ) {
      return next();
    }
    if (!req.session.user) {
      return res.redirect('/login');
    }
    next();
  });

  app.use(express.static(path.resolve('./public')));

  app.get('/health', (req, res) => {
    return res.json({ healthy: true });
  });

  server.listen(PORT, () =>
    console.log(`Server running on http://localhost:${PORT}`),
  );
}

main();
