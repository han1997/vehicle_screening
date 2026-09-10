"use strict";

const connections = new WeakMap();
function trackServer(server) {
  const sockets = new Set();
  connections.set(server, sockets);
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  return server;
}
async function closeServer(server) {
  if (!server || !server.listening) return;
  await new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
    // Electron's embedded Node can lack closeAllConnections. Track our test sockets.
    for (const socket of connections.get(server) || []) socket.destroy();
    if (server.closeAllConnections) server.closeAllConnections();
  });
}
module.exports = { trackServer, closeServer };
