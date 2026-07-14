// Load environment variables from .env file
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

try {
  const envPath = path.join(__dirname, '../.env');
  const envFile = fs.readFileSync(envPath, 'utf8');
  envFile.split('\n').forEach(line => {
    const trimmedLine = line.trim();
    if (trimmedLine && !trimmedLine.startsWith('#')) {
      const [key, ...valueParts] = trimmedLine.split('=');
      if (key && valueParts.length > 0 && !process.env[key]) {
        process.env[key] = valueParts.join('=').trim();
      }
    }
  });
} catch (e) {
  // console.log('No .env file found or error reading it:', e.message);
}

const PORT = process.env.PORT || 3001;

import express from 'express';
import { WebSocketServer } from 'ws';
import http from 'http';
import cors from 'cors';
import { promises as fsPromises } from 'fs';
import { spawn, execSync } from 'child_process';
import os from 'os';
import pty from 'node-pty';
import fetch from 'node-fetch';
import mime from 'mime-types';

import { getProjects, getSessions, getSessionMessages, renameProject, deleteSession, deleteProject, addProjectManually, extractProjectDirectory, clearProjectDirectoryCache } from './projects.js';
import { spawnGemini, abortGeminiSession } from './gemini-cli.js';
import sessionManager from './sessionManager.js';
import gitRoutes from './routes/git.js';
import authRoutes from './routes/auth.js';
import mcpRoutes from './routes/mcp.js';
import { initializeDatabase } from './database/db.js';
import { validateApiKey, authenticateToken, authenticateWebSocket } from './middleware/auth.js';

// File system watcher for projects folder
let projectsWatcher = null;
const connectedClients = new Set();

// Setup file system watcher for Gemini projects folder using chokidar
async function setupProjectsWatcher() {
  const chokidar = (await import('chokidar')).default;
  const geminiProjectsPath = path.join(process.env.HOME || os.homedir(), '.gemini', 'projects');
  
  if (projectsWatcher) {
    projectsWatcher.close();
  }
  
  try {
    // Initialize chokidar watcher with optimized settings
    projectsWatcher = chokidar.watch(geminiProjectsPath, {
      ignored: [
        '**/node_modules/**',
        '**/.git/**',
        '**/dist/**',
        '**/build/**',
        '**/*.tmp',
        '**/*.swp',
        '**/.DS_Store'
      ],
      persistent: true,
      ignoreInitial: true, // Don't fire events for existing files on startup
      followSymlinks: false,
      depth: 10, // Reasonable depth limit
      awaitWriteFinish: {
        stabilityThreshold: 100, // Wait 100ms for file to stabilize
        pollInterval: 50
      }
    });
    
    // Debounce function to prevent excessive notifications
    let debounceTimer;
    const debouncedUpdate = async (eventType, filePath) => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(async () => {
        try {
          // Clear project directory cache when files change
          clearProjectDirectoryCache();
          
          // Get updated projects list
          const updatedProjects = await getProjects();
          
          // Notify all connected clients about the project changes
          const updateMessage = JSON.stringify({
            type: 'projects_updated',
            projects: updatedProjects,
            timestamp: new Date().toISOString(),
            changeType: eventType,
            changedFile: path.relative(geminiProjectsPath, filePath)
          });
          
          connectedClients.forEach(client => {
            if (client.readyState === client.OPEN) {
              client.send(updateMessage);
            }
          });
          
        } catch (error) {
          // console.error('❌ Error handling project changes:', error);
        }
      }, 300); // 300ms debounce
    };
    
    // Set up event listeners
    projectsWatcher
      .on('add', (filePath) => debouncedUpdate('add', filePath))
      .on('change', (filePath) => debouncedUpdate('change', filePath))
      .on('unlink', (filePath) => debouncedUpdate('unlink', filePath))
      .on('addDir', (dirPath) => debouncedUpdate('addDir', dirPath))
      .on('unlinkDir', (dirPath) => debouncedUpdate('unlinkDir', dirPath))
      .on('error', (error) => {
        // console.error('❌ Chokidar watcher error:', error);
      })
      .on('ready', () => {});
    
  } catch (error) {
    // console.error('❌ Failed to setup projects watcher:', error);
  }
}

const app = express();
const server = http.createServer(app);

// Single WebSocket server that handles both paths
const wss = new WebSocketServer({ 
  server,
  verifyClient: (info) => {
    // Extract token from query parameters or headers
    const url = new URL(info.req.url, 'http://localhost');
    const token = url.searchParams.get('token') || 
                  info.req.headers.authorization?.split(' ')[1];
    
    // Verify token
    const user = authenticateWebSocket(token);
    if (!user) {
      return false;
    }
    
    // Store user info in the request for later use
    info.req.user = user;
    return true;
  }
});

app.use(cors());
app.use(express.json());

// Optional API key validation (if configured)
app.use('/api', validateApiKey);

// Authentication routes (public)
app.use('/api/auth', authRoutes);

// Git API Routes (protected)
app.use('/api/git', authenticateToken, gitRoutes);

// MCP API Routes (protected)
app.use('/api/mcp', authenticateToken, mcpRoutes);

// Static files served after API routes
const frontendPath = path.join(process.cwd(), 'dist');

console.log('Frontend path:', frontendPath);
console.log('Frontend exists:', fs.existsSync(frontendPath));
console.log('Index exists:', fs.existsSync(path.join(frontendPath, 'index.html')));

app.use(express.static(frontendPath));

// API Routes (protected)
app.get('/api/config', authenticateToken, (req, res) => {
  const host = req.headers.host || `${req.hostname}:${PORT}`;
  const protocol = req.protocol === 'https' || req.get('x-forwarded-proto') === 'https' ? 'wss' : 'ws';
  
  res.json({
    serverPort: PORT,
    wsUrl: `${protocol}://${host}`
  });
});

app.get('/api/projects', authenticateToken, async (req, res) => {
  try {
    const projects = await getProjects();
    res.json(projects);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/projects/:projectName/sessions', authenticateToken, async (req, res) => {
  try {
    const projectPath = await extractProjectDirectory(req.params.projectName);
    const sessions = sessionManager.getProjectSessions(projectPath);
    
    const { limit = 5, offset = 0 } = req.query;
    const paginatedSessions = sessions.slice(parseInt(offset), parseInt(offset) + parseInt(limit));
    
    res.json({
      sessions: paginatedSessions,
      total: sessions.length
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/projects/:projectName/sessions/:sessionId/messages', authenticateToken, async (req, res) => {
  try {
    const { sessionId } = req.params;
    const messages = sessionManager.getSessionMessages(sessionId);
    res.json({ messages });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.put('/api/projects/:projectName/rename', authenticateToken, async (req, res) => {
  try {
    const { displayName } = req.body;
    await renameProject(req.params.projectName, displayName);
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.delete('/api/projects/:projectName/sessions/:sessionId', authenticateToken, async (req, res) => {
  try {
    const { sessionId } = req.params;
    await sessionManager.deleteSession(sessionId);
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.delete('/api/projects/:projectName', authenticateToken, async (req, res) => {
  try {
    const { projectName } = req.params;
    await deleteProject(projectName);
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/projects/create', authenticateToken, async (req, res) => {
  try {
    const { path: projectPath } = req.body;
    
    if (!projectPath || !projectPath.trim()) {
      return res.status(400).json({ error: 'Project path is required' });
    }
    
    const project = await addProjectManually(projectPath.trim());
    res.json({ success: true, project });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/projects/:projectName/file', authenticateToken, async (req, res) => {
  try {
    const { filePath } = req.query;
    
    if (!filePath || !path.isAbsolute(filePath)) {
      return res.status(400).json({ error: 'Invalid file path' });
    }
    
    const content = await fsPromises.readFile(filePath, 'utf8');
    res.json({ content, path: filePath });
  } catch (error) {
    if (error.code === 'ENOENT') {
      res.status(404).json({ error: 'File not found' });
    } else if (error.code === 'EACCES') {
      res.status(403).json({ error: 'Permission denied' });
    } else {
      res.status(500).json({ error: error.message });
    }
  }
});

app.get('/api/projects/:projectName/files/content', authenticateToken, async (req, res) => {
  try {
    const { filePath } = req.query;
    
    if (!filePath || !path.isAbsolute(filePath)) {
      return res.status(400).json({ error: 'Invalid file path' });
    }
    
    try {
      await fsPromises.access(filePath);
    } catch (error) {
      return res.status(404).json({ error: 'File not found' });
    }
    
    const mimeType = mime.lookup(filePath) || 'application/octet-stream';
    res.setHeader('Content-Type', mimeType);
    
    const fileStream = fs.createReadStream(filePath);
    fileStream.pipe(res);
    
    fileStream.on('error', (error) => {
      if (!res.headersSent) {
        res.status(500).json({ error: 'Error reading file' });
      }
    });
    
  } catch (error) {
    if (!res.headersSent) {
      res.status(500).json({ error: error.message });
    }
  }
});

app.put('/api/projects/:projectName/file', authenticateToken, async (req, res) => {
  try {
    const { filePath, content } = req.body;
    
    if (!filePath || !path.isAbsolute(filePath)) {
      return res.status(400).json({ error: 'Invalid file path' });
    }
    
    if (content === undefined) {
      return res.status(400).json({ error: 'Content is required' });
    }
    
    try {
      const backupPath = filePath + '.backup.' + Date.now();
      await fsPromises.copyFile(filePath, backupPath);
    } catch (backupError) {
      // console.warn('Could not create backup:', backupError.message);
    }
    
    await fsPromises.writeFile(filePath, content, 'utf8');
    
    res.json({ 
      success: true, 
      path: filePath,
      message: 'File saved successfully' 
    });
  } catch (error) {
    if (error.code === 'ENOENT') {
      res.status(404).json({ error: 'File or directory not found' });
    } else if (error.code === 'EACCES') {
      res.status(403).json({ error: 'Permission denied' });
    } else {
      res.status(500).json({ error: error.message });
    }
  }
});

app.get('/api/projects/:projectName/files', authenticateToken, async (req, res) => {
  try {
    let actualPath;
    try {
      actualPath = await extractProjectDirectory(req.params.projectName);
    } catch (error) {
      actualPath = req.params.projectName.replace(/-/g, '/');
    }
    
    try {
      await fsPromises.access(actualPath);
    } catch (e) {
      return res.status(404).json({ error: `Project path not found: ${actualPath}` });
    }
    
    const files = await getFileTree(actualPath, 3, 0, true);
    res.json(files);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// WebSocket connection handler that routes based on URL path
wss.on('connection', (ws, request) => {
  const url = request.url;
  const urlObj = new URL(url, 'http://localhost');
  const pathname = urlObj.pathname;
  
  if (pathname === '/shell') {
    handleShellConnection(ws);
  } else if (pathname === '/ws') {
    handleChatConnection(ws);
  } else {
    ws.close();
  }
});

// Handle chat WebSocket connections
function handleChatConnection(ws) {
  connectedClients.add(ws);
  
  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message);
      
      if (data.type === 'gemini-command') {
        await spawnGemini(data.command, data.options, ws);
      } else if (data.type === 'abort-session') {
        const success = abortGeminiSession(data.sessionId);
        ws.send(JSON.stringify({
          type: 'session-aborted',
          sessionId: data.sessionId,
          success
        }));
      }
    } catch (error) {
      ws.send(JSON.stringify({
        type: 'error',
        error: error.message
      }));
    }
  });
  
  ws.on('close', () => {
    connectedClients.delete(ws);
  });
}

// Handle shell WebSocket connections (including recovery from snippet truncation)
function handleShellConnection(ws) {
  let shellProcess = null;
  
  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message);
      
      if (data.type === 'init') {
        const projectPath = data.projectPath || process.cwd();
        const sessionId = data.sessionId;
        const hasSession = data.hasSession;
        
        const welcomeMsg = hasSession ? 
          `\x1b[36mResuming Gemini session ${sessionId} in: ${projectPath}\x1b[0m\r\n` :
          `\x1b[36mStarting new Gemini session in: ${projectPath}\x1b[0m\r\n`;
        
        ws.send(JSON.stringify({
          type: 'output',
          data: welcomeMsg
        }));
        
        try {
          const geminiPath = process.env.GEMINI_PATH || 'gemini';
          
          try {
            execSync(`which ${geminiPath}`, { stdio: 'ignore' });
          } catch (error) {
            ws.send(JSON.stringify({
              type: 'output',
              data: `\r\n\x1b[31mError: Gemini CLI not found. Please check:\x1b[0m\r\n\x1b[33m1. Install gemini globally: npm install -g @google/generative-ai-cli\x1b[0m\r\n\x1b[33m2. Or set GEMINI_PATH in .env file\x1b[0m\r\n`
            }));
            return;
          }
          
          let geminiCommand = geminiPath;
          if (hasSession && sessionId) {
            geminiCommand = `${geminiPath} --resume ${sessionId} || ${geminiPath}`;
          }
          
          const shellCommand = `cd "${projectPath}" && ${geminiCommand}`;
          
          shellProcess = pty.spawn('bash', ['-c', shellCommand], {
            name: 'xterm-256color',
            cols: 80,
            rows: 24,
            cwd: process.env.HOME || '/',
            env: { 
              ...process.env,
              TERM: 'xterm-256color',
              COLORTERM: 'truecolor',
              FORCE_COLOR: '3',
              BROWSER: 'echo "OPEN_URL:"'
            }
          });
          
          shellProcess.onData((outputData) => {
            if (ws.readyState === ws.OPEN) {
              const patterns = [
                /(?:xdg-open|open|start)\s+(https?:\/\/[^\s\x1b\x07]+)/g,
                /OPEN_URL:\s*(https?:\/\/[^\s\x1b\x07]+)/g,
                /Opening\s+(https?:\/\/[^\s\x1b\x07]+)/gi,
                /Visit:\s*(https?:\/\/[^\s\x1b\x07]+)/gi,
                /View at:\s*(https?:\/\/[^\s\x1b\x07]+)/gi,
                /Browse to:\s*(https?:\/\/[^\s\x1b\x07]+)/gi
              ];
              
              patterns.forEach(pattern => {
                let match;
                while ((match = pattern.exec(outputData)) !== null) {
                  const url = match[1];
                  ws.send(JSON.stringify({
                    type: 'open_url',
                    url: url
                  }));
                }
              });
              
              ws.send(JSON.stringify({
                type: 'output',
                data: outputData
              }));
            }
          });
          
          shellProcess.onExit(({ exitCode, signal }) => {
            if (ws.readyState === ws.OPEN) {
              ws.send(JSON.stringify({
                type: 'exit',
                exitCode,
                signal
              }));
              ws.close();
            }
          });
          
        } catch (error) {
          ws.send(JSON.stringify({
            type: 'output',
            data: `\r\n\x1b[31mError starting terminal: ${error.message}\x1b[0m\r\n`
          }));
        }
      } else if (data.type === 'input') {
        if (shellProcess) {
          shellProcess.write(data.data);
        }
      } else if (data.type === 'resize') {
        if (shellProcess && data.cols && data.rows) {
          shellProcess.resize(data.cols, data.rows);
        }
      }
    } catch (error) {
      // console.error('Shell error:', error);
    }
  });
  
  ws.on('close', () => {
    if (shellProcess) {
      try {
        shellProcess.kill();
      } catch (e) {}
    }
  });
}

// Helper: Generates a directory layout tree
async function getFileTree(dirPath, maxDepth = 3, currentDepth = 0, includeHidden = false) {
  if (currentDepth > maxDepth) return [];
  try {
    const items = await fsPromises.readdir(dirPath, { withFileTypes: true });
    const result = [];
    
    for (const item of items) {
      if (!includeHidden && item.name.startsWith('.')) continue;
      // Skip heavy build/system directories for responsiveness
      if (['node_modules', '.git', 'dist', 'build', '.DS_Store'].includes(item.name)) continue;
      
      const fullPath = path.join(dirPath, item.name);
      const fileData = {
        name: item.name,
        path: fullPath,
        isDirectory: item.isDirectory()
      };
      
      if (item.isDirectory()) {
        fileData.children = await getFileTree(fullPath, maxDepth, currentDepth + 1, includeHidden);
      }
      
      result.push(fileData);
    }
    return result.sort((a, b) => (b.isDirectory ? 1 : 0) - (a.isDirectory ? 1 : 0) || a.name.localeCompare(b.name));
  } catch (error) {
    return [];
  }
}

// Serve React/Vite frontend (SPA fallback with logs)
app.get('*', (req, res) => {
  const indexFile = path.join(frontendPath, 'index.html');

  if (fs.existsSync(indexFile)) {
    res.sendFile(indexFile);
  } else {
    res.status(404).send('Frontend build not found');
  }
});

// Initialize and Start Server
async function startServer() {
  try {
    await initializeDatabase();
    await setupProjectsWatcher();
    
    server.listen(PORT, () => {
      console.log(`🚀 Server listening on port ${PORT}`);
    });
  } catch (err) {
    console.error('❌ Server startup failed:', err);
    process.exit(1);
  }
}

startServer();
